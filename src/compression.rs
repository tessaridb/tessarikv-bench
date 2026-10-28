//! Table compression per codec: a random fill of `keys` compressible records from four writers,
//! then, after a clean close and reopen with a small block cache, uniform-random point reads from
//! four readers. Reports the on-disk ratio, write throughput and amplification, and read latency.
//! Usage: `tessarikv-bench compression <dir> [keys] [codec]`. A one-shot benchmark binary.

use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use hdrhistogram::Histogram;
use tessarikv::{
    Compression, Db, DbOptions, Durability, KeyspaceId, KeyspaceOptions, ReadOptions, Storage,
    WriteBatch, WriteOptions,
};

use crate::counting::CountingStorage;

const MIB: u64 = 1 << 20;
const TASKS: u64 = 4;

/// Reader tasks; `READERS` overrides the default of [`TASKS`].
pub(crate) fn readers() -> u64 {
    std::env::var("READERS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(TASKS)
}
/// Point reads per run; `READS` overrides it.
pub(crate) fn reads() -> u64 {
    std::env::var("READS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(400_000)
}

const CODECS: [(&str, Compression); 7] = [
    ("none", Compression::None),
    ("lz4", Compression::Lz4),
    ("snappy", Compression::Snappy),
    ("zstd", Compression::Zstd),
    ("deflate1", Compression::Deflate { level: 1 }),
    ("deflate6", Compression::Deflate { level: 6 }),
    ("deflate9", Compression::Deflate { level: 9 }),
];

const WORDS: [&str; 16] = [
    "login", "logout", "purchase", "view", "search", "click", "share", "comment", "upload",
    "download", "refund", "signup", "rating", "invite", "follow", "report",
];

pub(crate) fn mix(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

pub(crate) fn key(id: u64) -> [u8; 16] {
    let mut key = [0; 16];
    key[..8].copy_from_slice(&mix(id).to_be_bytes());
    key[8..].copy_from_slice(&id.to_be_bytes());
    key
}

/// An event record of about 200 bytes: repeated field names, a small vocabulary, random numbers.
pub(crate) fn record(id: u64) -> Vec<u8> {
    let r = mix(id ^ 0x5eed);
    let word = |shift: u32| WORDS[usize::try_from((r >> shift) & 15).expect("small")];
    format!(
        "{{\"user\":\"user_{:05}\",\"event\":\"{}\",\"ts\":{},\"session\":\"{:016x}\",\
         \"tags\":[\"{}\",\"{}\"],\"score\":{}.{:02},\"client\":\"web/{}.{}\"}}",
        r % 20_000,
        word(8),
        1_760_000_000_000 + id * 37,
        mix(r),
        word(12),
        word(16),
        (r >> 20) % 1000,
        (r >> 32) % 100,
        (r >> 40) % 9,
        (r >> 44) % 30,
    )
    .into_bytes()
}

fn options(codec: Compression) -> DbOptions {
    let keyspace = KeyspaceOptions::builder()
        .memtable_bytes(8 * MIB)
        .level_base_bytes(32 * MIB)
        .target_file_bytes(8 * MIB)
        .compression(codec)
        .build()
        .expect("valid keyspace options");
    DbOptions::builder()
        .default_keyspace(keyspace)
        .block_cache_bytes(
            std::env::var("CACHE_MIB")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(8)
                * MIB,
        )
        .build()
        .expect("valid options")
}

/// A count or byte total as a float, exact below 2^53, which every figure here is.
pub(crate) fn float(value: u64) -> f64 {
    let high = u32::try_from(value >> 32).expect("below 2^64");
    let low = u32::try_from(value & u64::from(u32::MAX)).expect("masked");
    f64::from(high) * 4_294_967_296.0 + f64::from(low)
}

fn tables_on_disk(dir: &Path) -> u64 {
    std::fs::read_dir(dir)
        .expect("listable")
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(".sst"))
        .map(|entry| entry.metadata().expect("stat").len())
        .sum()
}

async fn run(root: &Path, name: &str, codec: Compression, keys: u64) {
    let dir = root.join(format!("c4-{name}"));
    let storage = Arc::new(CountingStorage::default());
    let backend: Arc<dyn Storage> = Arc::clone(&storage) as Arc<dyn Storage>;
    let db = Arc::new(
        Db::open_with_storage(&dir, options(codec), backend)
            .await
            .expect("opened"),
    );
    // READS_ONLY reuses the directory a KEEP run left behind and measures the reads alone.
    let reads_only = std::env::var_os("READS_ONLY").is_some();
    let mut write_secs = f64::NAN;
    if reads_only {
        Arc::into_inner(db)
            .expect("no writer")
            .close()
            .await
            .expect("closed");
    } else {
        let started = Instant::now();
        let mut tasks = tokio::task::JoinSet::new();
        for writer in 0..TASKS {
            let db = Arc::clone(&db);
            tasks.spawn(async move {
                let mut options = WriteOptions::default();
                options.durability = Durability::Buffered;
                let mut id = writer;
                while id < keys {
                    let mut batch = WriteBatch::new();
                    batch
                        .put(KeyspaceId::DEFAULT, &key(id), &record(id))
                        .expect("a put");
                    db.write(&batch, &options).await.expect("a write");
                    id += TASKS;
                }
            });
        }
        while tasks.join_next().await.is_some() {}
        write_secs = started.elapsed().as_secs_f64();
        Arc::into_inner(db)
            .expect("writers done")
            .close()
            .await
            .expect("closed");
    }
    let user: u64 = (0..keys)
        .map(|id| 16 + u64::try_from(record(id).len()).expect("fits"))
        .sum();
    let written = storage.counters.read();
    let disk = tables_on_disk(&dir);

    // The reads are counted only on request: the counting wrapper costs every file read two shared
    // atomic additions, which the reference engine is not charged.
    let count_reads = std::env::var_os("COUNT_READS").is_some();
    let counting = Arc::new(CountingStorage::default());
    let reads_counter = Arc::clone(&counting.reads);
    let storage: Arc<dyn Storage> = counting;
    let db = Arc::new(if count_reads {
        Db::open_with_storage(&dir, options(codec), storage)
            .await
            .expect("reopened")
    } else {
        Db::open(&dir, options(codec)).await.expect("reopened")
    });
    let opened_reads = reads_counter.read();
    let usage = crate::cpu::sample();
    let started = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    for reader in 0..readers() {
        let db = Arc::clone(&db);
        tasks.spawn(async move {
            let mut latency =
                Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).expect("bounds");
            let mut state = mix(reader + 77) | 1;
            for _ in 0..reads() / readers() {
                state = mix(state);
                let id = state % keys;
                let start = Instant::now();
                let found = db
                    .get(KeyspaceId::DEFAULT, &key(id), &ReadOptions::default())
                    .await
                    .expect("a read");
                latency.saturating_record(
                    u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX),
                );
                assert_eq!(found.as_deref(), Some(record(id).as_slice()));
            }
            latency
        });
    }
    let mut latency = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).expect("bounds");
    while let Some(done) = tasks.join_next().await {
        latency.add(done.expect("a reader")).expect("same bounds");
    }
    let read_secs = started.elapsed().as_secs_f64();
    crate::cpu::report(&usage, reads());
    if count_reads {
        let during = reads_counter.read().since(opened_reads);
        eprintln!(
            "file reads per get {:.3}, bytes per file read {:.0}",
            float(during.log) / float(reads()),
            float(during.tables) / float(during.log.max(1)),
        );
    }
    Arc::into_inner(db)
        .expect("readers done")
        .close()
        .await
        .expect("closed");
    let micros = |quantile| float(latency.value_at_quantile(quantile)) / 1e3;
    println!(
        "| {name} | {:.3} | {:.0} | {:.0} | {:.2} | {:.0} | {:.1} | {:.1} | {:.1} |",
        float(disk) / float(user),
        float(disk) / float(MIB),
        float(keys) / write_secs / 1e3,
        float(written.tables) / float(user),
        float(reads()) / read_secs / 1e3,
        micros(0.5),
        micros(0.99),
        micros(0.999),
    );
    if std::env::var_os("KEEP").is_none() {
        std::fs::remove_dir_all(&dir).expect("the run's own directory removed");
    }
}

pub fn main(root: &Path, keys: u64, only: Option<&str>) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("a runtime");
    println!(
        "| codec | disk/user | disk MiB | kwrites/s | WA tables | kreads/s | read p50 µs | p99 µs | p99.9 µs |"
    );
    println!("|---|---|---|---|---|---|---|---|---|");
    runtime.block_on(async {
        for (name, codec) in CODECS {
            if only.is_none_or(|wanted| wanted == name) {
                run(root, name, codec, keys).await;
            }
        }
    });
}
