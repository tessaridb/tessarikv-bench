//! Thread-scaling curve of the block cache: the sharded-mutex baseline against the CLOCK cache,
//! on 4 KiB blocks in a 64 MiB cache. Usage: `tessarikv-bench [millis-per-run]`; the engine's
//! write amplification is `tessarikv-bench engine <dir> [keys] [case]` (see `engine`).

mod baseline;
mod blockseek;
mod compression;
mod counting;
mod cpu;
mod engine;
mod memwrite;
mod reference;
mod steady;
mod writecurve;

use std::hint::black_box;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use baseline::{BlockCache, BlockKey};
use hdrhistogram::Histogram;
use tessarikv_format::{Block, BlockBuilder};
use tessarikv_sync::ClockCache;

const CAPACITY: u64 = 64 << 20;
const BLOCK: usize = 4096;
const THREADS: [usize; 7] = [1, 2, 4, 8, 10, 16, 20];
const RUNS: usize = 3;
/// One operation in this many is timed.
const SAMPLE: u64 = 8;
const STREAM: usize = 1 << 16;
/// Distinct blocks: every key maps to one, so hits do not all share one reference count.
const POOL: usize = 16384;

trait Cache: Send + Sync {
    fn get(&self, key: BlockKey) -> Option<Block>;
    fn insert(&self, key: BlockKey, block: Block);
}

impl Cache for BlockCache {
    fn get(&self, key: BlockKey) -> Option<Block> {
        BlockCache::get(self, key)
    }
    fn insert(&self, key: BlockKey, block: Block) {
        BlockCache::insert(self, key, block);
    }
}

impl Cache for ClockCache<BlockKey, Block> {
    fn get(&self, key: BlockKey) -> Option<Block> {
        ClockCache::get(self, &key)
    }
    fn insert(&self, key: BlockKey, block: Block) {
        let charge = u64::try_from(block.size_bytes()).unwrap_or(u64::MAX);
        let _ = ClockCache::insert(self, key, block, charge);
    }
}

#[derive(Clone, Copy)]
struct Workload {
    name: &'static str,
    keys: u64,
    preload: bool,
    zipf: bool,
}

const WORKLOADS: [Workload; 3] = [
    Workload {
        name: "hit uniform",
        keys: 8192,
        preload: true,
        zipf: false,
    },
    Workload {
        name: "hit zipf1.0",
        keys: 8192,
        preload: true,
        zipf: true,
    },
    Workload {
        name: "miss 4x uniform",
        keys: 65536,
        preload: false,
        zipf: false,
    },
];

fn block() -> Block {
    let mut builder = BlockBuilder::new(16, |left: &[u8], right: &[u8]| left.cmp(right));
    builder
        .add(b"key", &vec![7; BLOCK - 64])
        .expect("one entry");
    Block::new(builder.finish(), |left, right| left.cmp(right)).expect("a valid block")
}

fn key(index: u64) -> BlockKey {
    BlockKey {
        file: index >> 10,
        offset: (index & 1023) * 4096,
    }
}

/// A per-thread stream of key indexes, uniform or Zipf(1.0) over `keys`.
fn stream(seed: u64, workload: Workload) -> Vec<u64> {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    if !workload.zipf {
        return (0..STREAM).map(|_| next() % workload.keys).collect();
    }
    let ranks = u32::try_from(workload.keys).expect("a key space below 2^32");
    let weights: Vec<f64> = (1..=ranks).map(|rank| 1.0 / f64::from(rank)).collect();
    let total: f64 = weights.iter().sum();
    let mut cdf = Vec::with_capacity(weights.len());
    let mut running = 0.0;
    for weight in weights {
        running += weight / total;
        cdf.push(running);
    }
    (0..STREAM)
        .map(|_| {
            let high = u32::try_from(next() >> 32).expect("32 bits");
            let point = f64::from(high) / f64::from(u32::MAX);
            let rank =
                u64::try_from(cdf.partition_point(|&value| value < point)).expect("an index");
            rank % workload.keys
        })
        .collect()
}

/// One timed run: (operations per second, latency histogram in ns).
fn run(
    cache: &Arc<dyn Cache>,
    pool: &Arc<Vec<Block>>,
    workload: Workload,
    threads: usize,
    time: Duration,
) -> (f64, Histogram<u64>) {
    let barrier = Arc::new(Barrier::new(threads + 1));
    let stop = Arc::new(AtomicBool::new(false));
    let workers: Vec<_> = (0..threads)
        .map(|thread| {
            let (cache, barrier, stop, pool) = (
                Arc::clone(cache),
                Arc::clone(&barrier),
                Arc::clone(&stop),
                Arc::clone(pool),
            );
            let keys = stream(u64::try_from(thread).expect("a thread index") + 1, workload);
            // OS threads on purpose: the curve measures contention at a chosen thread count.
            std::thread::spawn(move || {
                let mut histogram = Histogram::<u64>::new(3).expect("a histogram");
                let mut operations = 0_u64;
                let mut position = 0_usize;
                barrier.wait();
                while !stop.load(Ordering::Relaxed) {
                    for _ in 0..64 {
                        let index = keys[position];
                        position = (position + 1) % STREAM;
                        let timed = operations.is_multiple_of(SAMPLE);
                        let start = timed.then(Instant::now);
                        let key = key(index);
                        if cache.get(black_box(key)).is_none() {
                            cache.insert(
                                key,
                                pool[usize::try_from(index).expect("an index") % POOL].clone(),
                            );
                        }
                        if let Some(start) = start {
                            let nanos =
                                u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX);
                            let _ = histogram.record(nanos);
                        }
                        operations += 1;
                    }
                }
                (operations, histogram)
            })
        })
        .collect();
    barrier.wait();
    let started = Instant::now();
    std::thread::sleep(time);
    stop.store(true, Ordering::Relaxed);
    let mut histogram = Histogram::<u64>::new(3).expect("a histogram");
    let mut operations = 0;
    for worker in workers {
        let (count, thread_histogram) = worker.join().expect("a worker");
        operations += count;
        histogram.add(thread_histogram).expect("same bounds");
    }
    let operations = u32::try_from(operations / 1000).map_or(f64::MAX, f64::from) * 1000.0;
    (operations / started.elapsed().as_secs_f64(), histogram)
}

fn fresh(name: &str, workload: Workload, pool: &[Block]) -> Arc<dyn Cache> {
    let cache: Arc<dyn Cache> = if name == "mutex" {
        Arc::new(BlockCache::new(CAPACITY))
    } else {
        Arc::new(ClockCache::<BlockKey, Block>::new(
            CAPACITY,
            u64::try_from(BLOCK).expect("4 KiB"),
        ))
    };
    if workload.preload {
        for index in 0..workload.keys {
            cache.insert(
                key(index),
                pool[usize::try_from(index).expect("an index") % POOL].clone(),
            );
        }
    }
    cache
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("compression") {
        let root = std::path::PathBuf::from(args.get(2).expect("a directory for the runs"));
        let keys = args
            .get(3)
            .and_then(|value| value.parse().ok())
            .unwrap_or(1 << 20);
        compression::main(&root, keys, args.get(4).map(String::as_str));
        return;
    }
    if args.get(1).map(String::as_str) == Some("memwrite") {
        let writes = args
            .get(2)
            .and_then(|value| value.parse().ok())
            .unwrap_or(1_000_000);
        let rounds = args
            .get(3)
            .and_then(|value| value.parse().ok())
            .unwrap_or(5);
        memwrite::main(writes, rounds);
        return;
    }
    if args.get(1).map(String::as_str) == Some("blockseek") {
        let file = std::path::PathBuf::from(args.get(2).expect("a table file"));
        let seeks = args
            .get(3)
            .and_then(|value| value.parse().ok())
            .unwrap_or(2_000_000);
        blockseek::main(&file, seeks);
        return;
    }
    if args.get(1).map(String::as_str) == Some("reference") {
        let root = std::path::PathBuf::from(args.get(2).expect("a directory for the runs"));
        let keys = args
            .get(3)
            .and_then(|value| value.parse().ok())
            .unwrap_or(1 << 20);
        reference::main(&root, keys);
        return;
    }
    if args.get(1).map(String::as_str) == Some("steady") {
        steady::main(&std::path::PathBuf::from(
            args.get(2).expect("a directory for the runs"),
        ));
        return;
    }
    if args.get(1).map(String::as_str) == Some("writecurve") {
        let root = std::path::PathBuf::from(args.get(2).expect("a directory for the runs"));
        writecurve::main(&root, args.get(3).map_or("buffered", String::as_str));
        return;
    }
    if args.get(1).map(String::as_str) == Some("engine") {
        let root = std::path::PathBuf::from(args.get(2).expect("a directory for the runs"));
        let keys = args
            .get(3)
            .and_then(|value| value.parse().ok())
            .unwrap_or(2 << 20);
        engine::main(&root, keys, args.get(4).map(String::as_str));
        return;
    }
    let millis = std::env::args()
        .nth(1)
        .and_then(|value| value.parse().ok())
        .unwrap_or(1000);
    let time = Duration::from_millis(millis);
    let pool = Arc::new((0..POOL).map(|_| block()).collect::<Vec<_>>());
    println!(
        "| workload | threads | cache | Mops/s median (min–max) | p50 ns | p99 ns | p99.9 ns | max ns |"
    );
    println!("|---|---|---|---|---|---|---|---|");
    for workload in WORKLOADS {
        for threads in THREADS {
            for name in ["mutex", "clock"] {
                let cache = fresh(name, workload, &pool);
                let _ = run(&cache, &pool, workload, threads, Duration::from_millis(200));
                let mut results: Vec<(f64, Histogram<u64>)> = (0..RUNS)
                    .map(|_| run(&cache, &pool, workload, threads, time))
                    .collect();
                results.sort_by(|left, right| left.0.total_cmp(&right.0));
                let (low, high) = (results[0].0, results[RUNS - 1].0);
                let (median, histogram) = &results[RUNS / 2];
                println!(
                    "| {} | {threads} | {name} | {:.2} ({:.2}–{:.2}) | {} | {} | {} | {} |",
                    workload.name,
                    median / 1e6,
                    low / 1e6,
                    high / 1e6,
                    histogram.value_at_quantile(0.5),
                    histogram.value_at_quantile(0.99),
                    histogram.value_at_quantile(0.999),
                    histogram.max(),
                );
            }
        }
    }
}
