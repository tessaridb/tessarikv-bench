//! Write amplification and write throughput of the engine per compaction style: a random fill of
//! `keys` keys, then as many uniform-random overwrites of the same keys, measured. Single-entry
//! batches, buffered durability, closed loop (the tails are service times, not response times at
//! a fixed arrival rate). The LSM is scaled down so 232 MB of user data has a real level shape.
//! Usage: `tessarikv-bench engine <dir> [keys] [style-writers]` (the last runs one case only); every run uses a fresh directory under `<dir>`
//! and removes it afterwards. A one-shot benchmark binary: no signal handling, no tracing.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use tessarikv::{
    CompactionStyle, Db, DbOptions, Durability, KeyspaceId, KeyspaceOptions, Storage, WriteBatch,
    WriteOptions,
};

use crate::counting::{Bytes, CountingStorage};

const KEY: u64 = 16;
const VALUE: usize = 100;
const RUNS: usize = 2;
/// One write in this many is timed.
const SAMPLE: u64 = 8;
const MIB: u64 = 1 << 20;

struct Case {
    style: &'static str,
    writers: u64,
}

const CASES: [Case; 6] = [
    Case {
        style: "leveled",
        writers: 1,
    },
    Case {
        style: "leveled",
        writers: 4,
    },
    Case {
        style: "leveled",
        writers: 10,
    },
    Case {
        style: "leveled",
        writers: 20,
    },
    Case {
        style: "universal",
        writers: 10,
    },
    Case {
        style: "fifo",
        writers: 10,
    },
];

/// What one phase measured.
struct Phase {
    writes: u64,
    elapsed: Duration,
    latency: Histogram<u64>,
    written: Bytes,
}

/// Nanoseconds from 1 to a minute at three significant digits; a write slower than that saturates.
fn latency_histogram() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 60_000_000_000, 3).expect("valid bounds")
}

/// A bijective 64-bit mixer, so ids `0..n` map to `n` distinct, uniformly scattered keys.
fn mix(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn key(id: u64) -> [u8; 16] {
    let mut key = [0; 16];
    key[..8].copy_from_slice(&mix(id).to_be_bytes());
    key[8..].copy_from_slice(&id.to_be_bytes());
    key
}

/// Pseudo-random value bytes, distinct per id and version, so compression gains nothing.
fn value(id: u64, version: u64, out: &mut [u8; VALUE]) {
    let mut state = mix(id ^ mix(version).rotate_left(17)) | 1;
    for chunk in out.chunks_mut(8) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        chunk.copy_from_slice(&state.to_be_bytes()[..chunk.len()]);
    }
}

fn options(style: &str) -> DbOptions {
    let compaction = match style {
        "universal" => CompactionStyle::universal(),
        "fifo" => CompactionStyle::Fifo {
            max_table_bytes: 1024 * MIB,
        },
        _ => CompactionStyle::Leveled,
    };
    let keyspace = KeyspaceOptions::builder()
        .memtable_bytes(8 * MIB)
        .level_base_bytes(32 * MIB)
        .target_file_bytes(8 * MIB)
        .compaction(compaction)
        .build()
        .expect("valid keyspace options");
    DbOptions::builder()
        .default_keyspace(keyspace)
        .build()
        .expect("valid options")
}

/// Writes `keys` entries from `writers` tasks: in a fill, writer `w` writes ids `w, w + writers,
/// ...`; otherwise each picks uniform-random ids from its own generator.
async fn phase(
    db: &Arc<Db>,
    storage: &CountingStorage,
    keys: u64,
    writers: u64,
    fill: bool,
) -> Phase {
    let before = storage.counters.read();
    let started = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    for writer in 0..writers {
        let db = Arc::clone(db);
        tasks.spawn(async move {
            let mut options = WriteOptions::default();
            options.durability = Durability::Buffered;
            let mut latency = latency_histogram();
            let mut bytes = [0; VALUE];
            let mut state = mix(writer.wrapping_add(0x5eed)) | 1;
            let share = keys / writers + u64::from(writer < keys % writers);
            for step in 0..share {
                let id = if fill {
                    step.wrapping_mul(writers).wrapping_add(writer)
                } else {
                    state = mix(state);
                    state % keys
                };
                value(id, u64::from(!fill).wrapping_add(step), &mut bytes);
                let mut batch = WriteBatch::new();
                batch
                    .put(KeyspaceId::DEFAULT, &key(id), &bytes)
                    .expect("a put");
                let start = Instant::now();
                db.write(&batch, &options).await.expect("a write");
                if step % SAMPLE == 0 {
                    let nanos = u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX);
                    latency.saturating_record(nanos);
                }
            }
            (share, latency)
        });
    }
    let mut writes = 0_u64;
    let mut latency = latency_histogram();
    while let Some(done) = tasks.join_next().await {
        let (count, histogram) = done.expect("a writer");
        writes = writes.saturating_add(count);
        latency.add(histogram).expect("same bounds");
    }
    let elapsed = started.elapsed();
    Phase {
        writes,
        elapsed,
        latency,
        written: storage.counters.read().since(before),
    }
}

fn ratio(bytes: u64, user: u64) -> f64 {
    let kib = |value: u64| u32::try_from(value / 1024).map_or(f64::MAX, f64::from);
    kib(bytes) / kib(user).max(1.0)
}

/// One table row; `drained` adds what `close` wrote after the overwrite and how long it took.
fn report(case: &Case, run: usize, name: &str, phase: &Phase, drained: Option<(Bytes, Duration)>) {
    let user = phase
        .writes
        .saturating_mul(KEY + u64::try_from(VALUE).expect("a value size"));
    let rate =
        u32::try_from(phase.writes).map_or(f64::MAX, f64::from) / phase.elapsed.as_secs_f64();
    let extra = drained.map_or(Bytes::default(), |(bytes, _)| bytes);
    let tables = phase.written.tables.saturating_add(extra.tables);
    let all = phase.written.all().saturating_add(extra.all());
    let close_ms = drained.map_or(0, |(_, wait)| wait.as_millis());
    println!(
        "| {} | {} | {run} | {name} | {:.0} | {:.1} | {} | {} | {} | {} | {:.2} | {:.2} | {} | {} | {close_ms} |",
        case.style,
        case.writers,
        rate / 1e3,
        phase.elapsed.as_secs_f64(),
        phase.latency.value_at_quantile(0.5),
        phase.latency.value_at_quantile(0.99),
        phase.latency.value_at_quantile(0.999),
        phase.latency.max(),
        ratio(tables, user),
        ratio(all, user),
        phase.written.log / MIB,
        tables / MIB,
    );
}

async fn run_case(root: &Path, case: &Case, run: usize, keys: u64) {
    let dir = root.join(format!("engine-{}-{}-{run}", case.style, case.writers));
    let storage = Arc::new(CountingStorage::default());
    let backend: Arc<dyn Storage> = Arc::clone(&storage) as Arc<dyn Storage>;
    let db = Db::open_with_storage(&dir, options(case.style), backend)
        .await
        .expect("an open database");
    let db = Arc::new(db);
    let fill = phase(&db, &storage, keys, case.writers, true).await;
    report(case, run, "fill", &fill, None);
    let overwrite = phase(&db, &storage, keys, case.writers, false).await;
    let before = storage.counters.read();
    let started = Instant::now();
    Arc::into_inner(db)
        .expect("no writer left")
        .close()
        .await
        .expect("a clean close");
    let drained = (storage.counters.read().since(before), started.elapsed());
    report(case, run, "overwrite", &overwrite, Some(drained));
    if std::env::var_os("TABLES").is_some() {
        let tables = storage.tables.lock().expect("an unpoisoned table list");
        let sizes: Vec<String> = tables
            .iter()
            .map(|(_, bytes)| format!("{:.1}", ratio(*bytes, 1024 * 1024)))
            .collect();
        eprintln!(
            "tables ({}, MiB in close order): {}",
            tables.len(),
            sizes.join(" ")
        );
    }
    std::fs::remove_dir_all(&dir).expect("the run's own directory removed");
}

pub fn main(root: &Path, keys: u64, only: Option<&str>) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("a runtime");
    println!(
        "| style | writers | run | phase | kwrites/s | s | p50 ns | p99 ns | p99.9 ns | max ns | WA tables | WA all | WAL MiB | tables MiB | close ms |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|");
    runtime.block_on(async {
        for case in CASES.iter().filter(|case| {
            only.is_none_or(|name| name == format!("{}-{}", case.style, case.writers))
        }) {
            for run in 0..RUNS {
                run_case(root, case, run, keys).await;
            }
        }
    });
}
