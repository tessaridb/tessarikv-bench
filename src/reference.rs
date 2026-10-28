//! The reference engine on the `compression` workload: the same keys and records from four
//! writers, then after a reopen and a drained compaction backlog, uniform-random point reads from
//! READERS threads through a CACHE_MIB block cache. Configured alike: 8 MiB write buffer and
//! target files, 32 MiB level base, 4 KiB blocks, restart interval 16, full bloom at 10 bits per
//! key, no compression, the log written but not synced.
//! Usage: `tessarikv-bench reference <dir> [keys]`; KEEP and READS_ONLY as for `compression`;
//! CODEC=lz4 compresses every level with LZ4 instead (its own directory).

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use rocksdb::{BlockBasedOptions, Cache, DB, DBCompressionType, Options, WriteOptions};

use crate::compression::{float, key, mix, readers, reads, record};

const MIB: usize = 1 << 20;
const WRITERS: u64 = 4;

fn options(cache: &Cache) -> Options {
    let mut table = BlockBasedOptions::default();
    table.set_block_size(4096);
    table.set_block_restart_interval(16);
    table.set_bloom_filter(10.0, false);
    table.set_block_cache(cache);
    let mut options = Options::default();
    options.create_if_missing(true);
    options.set_write_buffer_size(8 * MIB);
    options.set_target_file_size_base(8 * 1024 * 1024);
    options.set_max_bytes_for_level_base(32 * 1024 * 1024);
    options.set_compression_type(if lz4() {
        DBCompressionType::Lz4
    } else {
        DBCompressionType::None
    });
    options.set_block_based_table_factory(&table);
    options
}

fn lz4() -> bool {
    std::env::var("CODEC").is_ok_and(|codec| codec == "lz4")
}

fn cache_bytes() -> usize {
    std::env::var("CACHE_MIB")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(8)
        * MIB
}

fn property(db: &DB, name: &str) -> u64 {
    db.property_int_value(name).ok().flatten().unwrap_or(0)
}

/// Waits until no compaction runs or is due: the measured phase starts at steady state.
fn drain(db: &DB) {
    while property(db, "rocksdb.compaction-pending") != 0
        || property(db, "rocksdb.num-running-compactions") != 0
        || property(db, "rocksdb.num-running-flushes") != 0
    {
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn fill(dir: &Path, keys: u64, cache: &Cache) {
    let db = Arc::new(DB::open(&options(cache), dir).expect("opened"));
    let started = Instant::now();
    let writers: Vec<_> = (0..WRITERS)
        .map(|writer| {
            let db = Arc::clone(&db);
            std::thread::spawn(move || {
                let mut write = WriteOptions::default();
                write.set_sync(false);
                let mut id = writer;
                while id < keys {
                    db.put_opt(key(id), record(id), &write).expect("a write");
                    id += WRITERS;
                }
            })
        })
        .collect();
    for writer in writers {
        writer.join().expect("a writer");
    }
    eprintln!("fill {:.1} s", started.elapsed().as_secs_f64());
}

pub fn main(root: &Path, keys: u64) {
    let dir = root.join(if lz4() { "reference-lz4" } else { "reference" });
    let cache = Cache::new_lru_cache(cache_bytes());
    if std::env::var_os("READS_ONLY").is_none() {
        fill(&dir, keys, &cache);
    }
    let db = Arc::new(DB::open(&options(&cache), &dir).expect("reopened"));
    drain(&db);
    let levels: Vec<u64> = (0..7)
        .map(|level| property(&db, &format!("rocksdb.num-files-at-level{level}")))
        .collect();
    eprintln!("files per level {levels:?}");
    let usage = crate::cpu::sample();
    let started = Instant::now();
    let tasks: Vec<_> = (0..readers())
        .map(|reader| {
            let db = Arc::clone(&db);
            std::thread::spawn(move || {
                let mut latency =
                    Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).expect("bounds");
                let mut state = mix(reader + 77) | 1;
                for _ in 0..reads() / readers() {
                    state = mix(state);
                    let id = state % keys;
                    let start = Instant::now();
                    let found = db.get(key(id)).expect("a read");
                    latency.saturating_record(
                        u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX),
                    );
                    assert_eq!(found.as_deref(), Some(record(id).as_slice()));
                }
                latency
            })
        })
        .collect();
    let mut latency = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).expect("bounds");
    for task in tasks {
        latency
            .add(task.join().expect("a reader"))
            .expect("same bounds");
    }
    let read_secs = started.elapsed().as_secs_f64();
    crate::cpu::report(&usage, reads());
    let micros = |quantile| float(latency.value_at_quantile(quantile)) / 1e3;
    println!(
        "| reference | {:.0} | {:.1} | {:.1} | {:.1} |",
        float(reads()) / read_secs / 1e3,
        micros(0.5),
        micros(0.99),
        micros(0.999),
    );
    drop(db);
    if std::env::var_os("KEEP").is_none() {
        std::fs::remove_dir_all(&dir).expect("the run's own directory removed");
    }
}
