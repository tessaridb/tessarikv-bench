//! Steady-state curves on both engines: each is filled once with KEYS records of about 200
//! bytes, flushed and left until compaction has settled, then measured on that database —
//! single-put overwrites of random existing keys over writer counts (buffered on both sides),
//! then iterator scans of SCAN entries from a random existing key over reader counts. Engines
//! alternate within every run. Options alike: 8 MiB write buffer and target files, 32 MiB level
//! base, no compression, 10-bit bloom filters, a 64 MiB block cache, 4 background jobs.
//! Usage: `tessarikv-bench steady <dir>`; KEYS (1000000), WRITERS (1,4,16), READERS (1,4,16),
//! SCAN (100), MILLIS per run (3000) and RUNS per point (3) override the shape.

use std::hint::black_box;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use tessarikv::{
    Db, DbOptions, Durability, KeyspaceId, KeyspaceOptions, ReadOptions, WriteBatch, WriteOptions,
};

use crate::compression::{float, key, record};

const MIB: u64 = 1 << 20;

/// With PROBE set: when each write slower than 1 ms ended and how long it took, and when each
/// flush or compaction completed, in microseconds since the start.
static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
static SLOW: std::sync::Mutex<Vec<(u64, u64)>> = std::sync::Mutex::new(Vec::new());
static DONE: std::sync::Mutex<Vec<(u64, &'static str)>> = std::sync::Mutex::new(Vec::new());

fn since_start() -> u64 {
    u64::try_from(START.get_or_init(Instant::now).elapsed().as_micros()).unwrap_or(u64::MAX)
}

struct Probe;

impl tessarikv::EventListener for Probe {
    fn name(&self) -> &str {
        "probe"
    }
    fn flush_completed(&self, _event: &tessarikv::FlushCompleted) {
        DONE.lock().expect("lock").push((since_start(), "flush"));
    }
    fn compaction_completed(&self, _event: &tessarikv::CompactionCompleted) {
        DONE.lock()
            .expect("lock")
            .push((since_start(), "compaction"));
    }
}

/// How many slow writes overlap (within 2 ms) a flush or a compaction completing.
fn probe_report() {
    let slow = SLOW.lock().expect("lock");
    let done = DONE.lock().expect("lock");
    let near = |kind: &str| {
        slow.iter()
            .filter(|(end, took)| {
                let start = end.saturating_sub(*took).saturating_sub(2000);
                done.iter()
                    .any(|(at, k)| *k == kind && *at >= start && *at <= end + 2000)
            })
            .count()
    };
    let flushes = done.iter().filter(|(_, k)| *k == "flush").count();
    let mut ends: Vec<(u64, u64)> = slow.clone();
    ends.sort_unstable();
    let mut clusters: Vec<(u64, u64, usize)> = Vec::new();
    for (end, took) in ends {
        match clusters.last_mut() {
            Some((_, last, count)) if end <= *last + 5000 => {
                *last = end;
                *count += 1;
            }
            _ => clusters.push((end.saturating_sub(took), end, 1)),
        }
    }
    let spans: Vec<u64> = clusters
        .iter()
        .map(|(first, last, _)| (last - first) / 1000)
        .collect();
    println!(
        "probe: {} clusters of slow writes; span ms (first 20): {:?}",
        clusters.len(),
        &spans[..spans.len().min(20)]
    );
    println!(
        "probe: {} slow writes (> 1 ms); near a flush completing: {}; near a compaction completing: {}; flushes {flushes}, compactions {}",
        slow.len(),
        near("flush"),
        near("compaction"),
        done.len() - flushes
    );
}
const FILL_BATCH: u64 = 100;
const FILL_WORKERS: u64 = 8;

fn histogram() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 60_000_000_000, 3).expect("bounds")
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn counts(name: &str, default: &str) -> Vec<u64> {
    env_or(name, default)
        .split(',')
        .map(|count| count.trim().parse().expect("a count"))
        .collect()
}

/// A random id below `keys`, the `n`-th of a stream.
fn pick(n: u64, keys: u64) -> u64 {
    let mut x = n.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    (x ^ (x >> 31)) % keys
}

fn buffered() -> WriteOptions {
    let mut options = WriteOptions::default();
    options.durability = Durability::Buffered;
    options
}

fn nanos(at: Instant) -> u64 {
    u64::try_from(at.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

/// One measured run: operations done and their latencies.
struct Run {
    done: u64,
    elapsed: Duration,
    latency: Histogram<u64>,
}

struct Tessari {
    runtime: tokio::runtime::Runtime,
    db: Arc<Db>,
}

impl Tessari {
    fn open(dir: &Path) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(8)
            .enable_all()
            .build()
            .expect("runtime");
        let db = runtime.block_on(async {
            let keyspace = KeyspaceOptions::builder()
                .memtable_bytes(env_or("MEMTABLE_MIB", "8").parse::<u64>().expect("MiB") * MIB)
                .level_base_bytes(32 * MIB)
                .target_file_bytes(8 * MIB)
                .compression(tessarikv::Compression::None)
                .disable_auto_compactions(std::env::var("NO_COMPACTION").is_ok())
                .build()
                .expect("valid keyspace options");
            let options = DbOptions::builder()
                .default_keyspace(keyspace)
                .block_cache_bytes(64 * MIB)
                .statistics(true)
                .event_listener(Arc::new(Probe))
                .wal_segment_bytes(
                    env_or("WAL_SEGMENT_MIB", "64").parse::<u64>().expect("MiB") * MIB,
                )
                .build()
                .expect("valid options");
            Arc::new(Db::open(dir, options).await.expect("opened"))
        });
        Self { runtime, db }
    }

    fn fill(&self, keys: u64) {
        self.runtime.block_on(async {
            let next = Arc::new(AtomicU64::new(0));
            let mut tasks = tokio::task::JoinSet::new();
            for _ in 0..FILL_WORKERS {
                let (db, next) = (Arc::clone(&self.db), Arc::clone(&next));
                tasks.spawn(async move {
                    let options = buffered();
                    loop {
                        let first = next.fetch_add(FILL_BATCH, Ordering::Relaxed);
                        if first >= keys {
                            break;
                        }
                        let mut batch = WriteBatch::new();
                        for id in first..(first + FILL_BATCH).min(keys) {
                            batch
                                .put(KeyspaceId::DEFAULT, &key(id), &record(id))
                                .expect("entry");
                        }
                        db.write(&batch, &options).await.expect("written");
                    }
                });
            }
            while let Some(done) = tasks.join_next().await {
                done.expect("filler");
            }
            self.db.flush().await.expect("flushed");
            self.db.wait_for_compact().await.expect("settled");
        });
    }

    fn overwrite(&self, writers: u64, keys: u64, millis: u64, seed: u64) -> Run {
        self.runtime.block_on(async {
            let deadline = Instant::now() + Duration::from_millis(millis);
            let next = Arc::new(AtomicU64::new(seed));
            let started = Instant::now();
            let mut tasks = tokio::task::JoinSet::new();
            for _ in 0..writers {
                let (db, next) = (Arc::clone(&self.db), Arc::clone(&next));
                tasks.spawn(async move {
                    let mut latency = histogram();
                    let options = buffered();
                    while Instant::now() < deadline {
                        let id = pick(next.fetch_add(1, Ordering::Relaxed), keys);
                        let mut batch = WriteBatch::new();
                        batch
                            .put(KeyspaceId::DEFAULT, &key(id), &record(id))
                            .expect("entry");
                        let at = Instant::now();
                        db.write(&batch, &options).await.expect("written");
                        let took = nanos(at);
                        if took > 1_000_000 {
                            SLOW.lock()
                                .expect("lock")
                                .push((since_start(), took / 1000));
                        }
                        latency.record(took).expect("within a minute");
                    }
                    latency
                });
            }
            let mut latency = histogram();
            while let Some(done) = tasks.join_next().await {
                latency.add(done.expect("writer")).expect("same bounds");
            }
            Run {
                done: next.load(Ordering::Relaxed) - seed,
                elapsed: started.elapsed(),
                latency,
            }
        })
    }

    fn scan(&self, readers: u64, keys: u64, length: u64, millis: u64, seed: u64) -> Run {
        self.runtime.block_on(async {
            let deadline = Instant::now() + Duration::from_millis(millis);
            let next = Arc::new(AtomicU64::new(seed));
            let started = Instant::now();
            let mut tasks = tokio::task::JoinSet::new();
            for _ in 0..readers {
                let (db, next) = (Arc::clone(&self.db), Arc::clone(&next));
                tasks.spawn(async move {
                    let mut latency = histogram();
                    let options = ReadOptions::default();
                    while Instant::now() < deadline {
                        let id = pick(next.fetch_add(1, Ordering::Relaxed), keys);
                        let at = Instant::now();
                        let mut entries = db.iter(KeyspaceId::DEFAULT, &options).expect("iterator");
                        entries.seek(&key(id)).await.expect("seek");
                        let mut read = 0;
                        while entries.valid() && read < length {
                            black_box(entries.value());
                            read += 1;
                            entries.next().await.expect("next");
                        }
                        latency.record(nanos(at)).expect("within a minute");
                    }
                    latency
                });
            }
            let mut latency = histogram();
            while let Some(done) = tasks.join_next().await {
                latency.add(done.expect("reader")).expect("same bounds");
            }
            Run {
                done: next.load(Ordering::Relaxed) - seed,
                elapsed: started.elapsed(),
                latency,
            }
        })
    }
}

fn reference_open(dir: &Path) -> Arc<rocksdb::DB> {
    let mut table = rocksdb::BlockBasedOptions::default();
    table.set_block_cache(&rocksdb::Cache::new_lru_cache(64 * 1024 * 1024));
    table.set_bloom_filter(10.0, false);
    let mut options = rocksdb::Options::default();
    options.create_if_missing(true);
    options.set_write_buffer_size(8 * 1024 * 1024);
    options.set_target_file_size_base(8 * 1024 * 1024);
    options.set_max_bytes_for_level_base(32 * 1024 * 1024);
    options.set_compression_type(rocksdb::DBCompressionType::None);
    options.set_max_background_jobs(4);
    options.set_block_based_table_factory(&table);
    Arc::new(rocksdb::DB::open(&options, dir).expect("opened"))
}

fn reference_fill(db: &Arc<rocksdb::DB>, keys: u64) {
    let next = Arc::new(AtomicU64::new(0));
    let threads: Vec<_> = (0..FILL_WORKERS)
        .map(|_| {
            let (db, next) = (Arc::clone(db), Arc::clone(&next));
            // bgv-allow(raw-thread-spawn): the reference engine's writes block; one thread per writer is the workload.
            std::thread::spawn(move || {
                let mut write = rocksdb::WriteOptions::default();
                write.set_sync(false);
                loop {
                    let first = next.fetch_add(FILL_BATCH, Ordering::Relaxed);
                    if first >= keys {
                        break;
                    }
                    let mut batch = rocksdb::WriteBatch::default();
                    for id in first..(first + FILL_BATCH).min(keys) {
                        batch.put(key(id), record(id));
                    }
                    db.write_opt(batch, &write).expect("written");
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().expect("filler");
    }
    db.flush().expect("flushed");
    let busy = |name: &str| db.property_int_value(name).expect("property").unwrap_or(0) > 0;
    while busy("rocksdb.compaction-pending")
        || busy("rocksdb.num-running-compactions")
        || busy("rocksdb.num-running-flushes")
    {
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Runs `body` on `threads` threads until `millis` pass; each call is one timed operation.
fn reference_timed(
    threads: u64,
    millis: u64,
    seed: u64,
    body: impl Fn(u64) + Send + Sync + 'static,
) -> Run {
    let body = Arc::new(body);
    let deadline = Instant::now() + Duration::from_millis(millis);
    let next = Arc::new(AtomicU64::new(seed));
    let started = Instant::now();
    let handles: Vec<_> = (0..threads)
        .map(|_| {
            let (body, next) = (Arc::clone(&body), Arc::clone(&next));
            // bgv-allow(raw-thread-spawn): the reference engine blocks; one thread per caller is the workload.
            std::thread::spawn(move || {
                let mut latency = histogram();
                while Instant::now() < deadline {
                    let n = next.fetch_add(1, Ordering::Relaxed);
                    let at = Instant::now();
                    body(n);
                    latency.record(nanos(at)).expect("within a minute");
                }
                latency
            })
        })
        .collect();
    let mut latency = histogram();
    for handle in handles {
        latency
            .add(handle.join().expect("caller"))
            .expect("same bounds");
    }
    Run {
        done: next.load(Ordering::Relaxed) - seed,
        elapsed: started.elapsed(),
        latency,
    }
}

fn micros(latency: &Histogram<u64>, quantile: f64) -> f64 {
    float(latency.value_at_quantile(quantile)) / 1000.0
}

fn report(title: &str, unit: &str, count: u64, results: &[(&str, Vec<f64>, Histogram<u64>)]) {
    let _ = (title, unit);
    for (engine, rates, latency) in results {
        let rates: Vec<String> = rates.iter().map(|rate| format!("{rate:.0}")).collect();
        println!(
            "| {count} | {engine} | {} | {:.1} | {:.1} | {:.1} |",
            rates.join(" / "),
            micros(latency, 0.5),
            micros(latency, 0.99),
            micros(latency, 0.999),
        );
    }
}

pub fn main(root: &Path) {
    let keys: u64 = env_or("KEYS", "1000000").parse().expect("a key count");
    let millis: u64 = env_or("MILLIS", "3000").parse().expect("milliseconds");
    let runs: u64 = env_or("RUNS", "3").parse().expect("a run count");
    let length: u64 = env_or("SCAN", "100").parse().expect("a scan length");
    let tessari_dir = root.join("steady-tessarikv");
    let reference_dir = root.join("steady-reference");
    let _ = std::fs::remove_dir_all(&tessari_dir);
    let _ = std::fs::remove_dir_all(&reference_dir);
    since_start();
    let tessari = Tessari::open(&tessari_dir);
    let reference = reference_open(&reference_dir);
    let at = Instant::now();
    tessari.fill(keys);
    println!(
        "fill + settle TessariKV: {:.1} s",
        at.elapsed().as_secs_f64()
    );
    let at = Instant::now();
    reference_fill(&reference, keys);
    println!("fill + settle RocksDB: {:.1} s", at.elapsed().as_secs_f64());
    let mut seed = 0_u64;
    println!(
        "\n## Overwrites (buffered)\n| writers | engine | writes/s per run | p50 µs | p99 µs | p99.9 µs |\n|---|---|---|---|---|---|"
    );
    for count in counts("WRITERS", "1,4,16") {
        let mut results = vec![
            ("TessariKV", Vec::new(), histogram()),
            ("RocksDB", Vec::new(), histogram()),
        ];
        for _ in 0..runs {
            seed += 1 << 32;
            let done = tessari.overwrite(count, keys, millis, seed);
            results[0]
                .1
                .push(float(done.done) / done.elapsed.as_secs_f64());
            results[0].2.add(&done.latency).expect("same bounds");
            let db = Arc::clone(&reference);
            let done = reference_timed(count, millis, seed, move |n| {
                let id = pick(n, keys);
                let mut write = rocksdb::WriteOptions::default();
                write.set_sync(false);
                db.put_opt(key(id), record(id), &write).expect("written");
            });
            results[1]
                .1
                .push(float(done.done) / done.elapsed.as_secs_f64());
            results[1].2.add(&done.latency).expect("same bounds");
        }
        report("overwrites", "writes", count, &results);
    }
    if std::env::var("PROBE").is_ok() {
        probe_report();
    }
    let stalled = tessari
        .db
        .statistics()
        .map_or(0, |s| s.ticker(tessarikv::Ticker::StallMicros));
    let syncs = tessari
        .db
        .statistics()
        .map_or(0, |s| s.ticker(tessarikv::Ticker::WalSyncs));
    println!("\nstalls after the writes: TessariKV {stalled} µs; log syncs {syncs}");
    let stats = reference
        .property_value("rocksdb.stats")
        .expect("stats")
        .unwrap_or_default();
    for line in stats
        .lines()
        .filter(|line| line.contains("Cumulative stall"))
    {
        println!("RocksDB {line}");
    }
    println!(
        "\n## Scans of {length} entries from a random key\n| readers | engine | scans/s per run | p50 µs | p99 µs | p99.9 µs |\n|---|---|---|---|---|---|"
    );
    for count in counts("READERS", "1,4,16") {
        let mut results = vec![
            ("TessariKV", Vec::new(), histogram()),
            ("RocksDB", Vec::new(), histogram()),
        ];
        for _ in 0..runs {
            seed += 1 << 32;
            let done = tessari.scan(count, keys, length, millis, seed);
            results[0]
                .1
                .push(float(done.done) / done.elapsed.as_secs_f64());
            results[0].2.add(&done.latency).expect("same bounds");
            let db = Arc::clone(&reference);
            let done = reference_timed(count, millis, seed, move |n| {
                let id = pick(n, keys);
                let mut entries = db.raw_iterator();
                entries.seek(key(id));
                let mut read = 0;
                while entries.valid() && read < length {
                    black_box(entries.value());
                    read += 1;
                    entries.next();
                }
            });
            results[1]
                .1
                .push(float(done.done) / done.elapsed.as_secs_f64());
            results[1].2.add(&done.latency).expect("same bounds");
        }
        report("scans", "scans", count, &results);
    }
    let Tessari { runtime, db } = tessari;
    runtime.block_on(async {
        Arc::into_inner(db)
            .expect("unshared")
            .close()
            .await
            .expect("closed")
    });
    drop(reference);
    let _ = std::fs::remove_dir_all(&tessari_dir);
    let _ = std::fs::remove_dir_all(&reference_dir);
}
