//! Single-put writes as a curve over writer counts, on both engines, at one durability level:
//! each writer puts fresh random keys with records of about 200 bytes for a fixed time into a
//! new database. Configured as in `compression` and `reference` (8 MiB write buffer and target
//! files, 32 MiB level base, no compression). Runs alternate between the engines.
//! Usage: `tessarikv-bench writecurve <dir> <sync|buffered>`; WRITERS (default 1,2,4,8,16),
//! MILLIS per run (3000) and RUNS per point (3) override the shape.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use tessarikv::{Db, DbOptions, Durability, KeyspaceId, KeyspaceOptions, WriteBatch, WriteOptions};

use crate::compression::{float, key, record};

const MIB: u64 = 1 << 20;

fn histogram() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 60_000_000_000, 3).expect("bounds")
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

/// One run: writes done and their latencies.
struct Run {
    writes: u64,
    elapsed: Duration,
    latency: Histogram<u64>,
}

fn tessarikv_run(dir: &Path, writers: u64, sync: bool, millis: u64) -> Run {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let keyspace = KeyspaceOptions::builder()
            .memtable_bytes(8 * MIB)
            .level_base_bytes(32 * MIB)
            .target_file_bytes(8 * MIB)
            .compression(tessarikv::Compression::None)
            .build()
            .expect("valid keyspace options");
        let options = DbOptions::builder()
            .default_keyspace(keyspace)
            .statistics(true)
            .build()
            .expect("valid options");
        let db = Arc::new(Db::open(dir, options).await.expect("opened"));
        let deadline = Instant::now() + Duration::from_millis(millis);
        let next = Arc::new(AtomicU64::new(0));
        let started = Instant::now();
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..writers {
            let db = Arc::clone(&db);
            let next = Arc::clone(&next);
            tasks.spawn(async move {
                let mut latency = histogram();
                let mut options = WriteOptions::default();
                options.durability = if sync {
                    Durability::Sync
                } else {
                    Durability::Buffered
                };
                while Instant::now() < deadline {
                    let id = next.fetch_add(1, Ordering::Relaxed);
                    let mut batch = WriteBatch::new();
                    batch
                        .put(KeyspaceId::DEFAULT, &key(id), &record(id))
                        .expect("entry");
                    let at = Instant::now();
                    db.write(&batch, &options).await.expect("written");
                    latency
                        .record(u64::try_from(at.elapsed().as_nanos()).unwrap_or(u64::MAX))
                        .expect("within a minute");
                }
                latency
            });
        }
        let mut latency = histogram();
        while let Some(done) = tasks.join_next().await {
            latency.add(done.expect("writer")).expect("same bounds");
        }
        let elapsed = started.elapsed();
        if let Some(stats) = db.statistics() {
            eprintln!(
                "writers {writers}: keys {} log syncs {}",
                stats.ticker(tessarikv::Ticker::KeysWritten),
                stats.ticker(tessarikv::Ticker::WalSyncs)
            );
        }
        let db = Arc::into_inner(db).expect("writers done");
        db.close().await.expect("closed");
        Run {
            writes: next.load(Ordering::Relaxed),
            elapsed,
            latency,
        }
    })
}

fn reference_run(dir: &Path, writers: u64, sync: bool, millis: u64) -> Run {
    let mut options = rocksdb::Options::default();
    options.create_if_missing(true);
    options.set_write_buffer_size(8 * 1024 * 1024);
    options.set_target_file_size_base(8 * 1024 * 1024);
    options.set_max_bytes_for_level_base(32 * 1024 * 1024);
    options.set_compression_type(rocksdb::DBCompressionType::None);
    let db = Arc::new(rocksdb::DB::open(&options, dir).expect("opened"));
    let deadline = Instant::now() + Duration::from_millis(millis);
    let next = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    let threads: Vec<_> = (0..writers)
        .map(|_| {
            let db = Arc::clone(&db);
            let next = Arc::clone(&next);
            // bgv-allow(raw-thread-spawn): the reference engine's writes block; one thread per writer is the workload.
            std::thread::spawn(move || {
                let mut latency = histogram();
                let mut write = rocksdb::WriteOptions::default();
                write.set_sync(sync);
                while Instant::now() < deadline {
                    let id = next.fetch_add(1, Ordering::Relaxed);
                    let at = Instant::now();
                    db.put_opt(key(id), record(id), &write).expect("written");
                    latency
                        .record(u64::try_from(at.elapsed().as_nanos()).unwrap_or(u64::MAX))
                        .expect("within a minute");
                }
                latency
            })
        })
        .collect();
    let mut latency = histogram();
    for thread in threads {
        latency
            .add(thread.join().expect("writer"))
            .expect("same bounds");
    }
    let elapsed = started.elapsed();
    drop(db);
    Run {
        writes: next.load(Ordering::Relaxed),
        elapsed,
        latency,
    }
}

fn micros(latency: &Histogram<u64>, quantile: f64) -> f64 {
    float(latency.value_at_quantile(quantile)) / 1000.0
}

pub fn main(root: &Path, durability: &str) {
    let sync = match durability {
        "sync" => true,
        "buffered" => false,
        other => panic!("durability is sync or buffered, not {other}"),
    };
    let writers: Vec<u64> = env_or("WRITERS", "1,2,4,8,16")
        .split(',')
        .map(|count| count.trim().parse().expect("a writer count"))
        .collect();
    let millis: u64 = env_or("MILLIS", "3000").parse().expect("milliseconds");
    let runs: usize = env_or("RUNS", "3").parse().expect("a run count");
    println!("| writers | engine | writes/s per run | p50 µs | p99 µs | p99.9 µs |");
    println!("|---|---|---|---|---|---|");
    for &count in &writers {
        let mut results: Vec<(&str, Vec<f64>, Histogram<u64>)> = vec![
            ("TessariKV", Vec::new(), histogram()),
            ("RocksDB", Vec::new(), histogram()),
        ];
        for run in 0..runs {
            for (index, entry) in results.iter_mut().enumerate() {
                let dir = root.join(format!("writecurve-{durability}-{count}-{run}-{index}"));
                let _ = std::fs::remove_dir_all(&dir);
                let done = if index == 0 {
                    tessarikv_run(&dir, count, sync, millis)
                } else {
                    reference_run(&dir, count, sync, millis)
                };
                std::fs::remove_dir_all(&dir).expect("removed");
                entry
                    .1
                    .push(float(done.writes) / done.elapsed.as_secs_f64());
                entry.2.add(&done.latency).expect("same bounds");
            }
        }
        for (engine, rates, latency) in &results {
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
}
