//! Plain write throughput into an in-memory database: `tasks` writers each commit single-put
//! batches of 16-byte keys and 32-byte values, random keys, no log. Measures the commit path
//! alone (claim, publish, apply, complete). Usage: `tessarikv-bench memwrite [writes] [rounds]`.

use std::sync::Arc;
use std::time::Instant;

use tessarikv::{Db, DbOptions, KeyspaceId, WriteBatch, WriteOptions};

async fn round(writes: u64, tasks: u64) -> f64 {
    let serializable = std::env::var_os("SERIALIZABLE").is_some();
    let options = DbOptions::in_memory()
        .to_builder()
        .serializable_transactions(serializable)
        .build()
        .expect("valid");
    let db = Arc::new(Db::open("memwrite", options).await.expect("opened"));
    let started = Instant::now();
    let mut set = tokio::task::JoinSet::new();
    for task in 0..tasks {
        let db = Arc::clone(&db);
        set.spawn(async move {
            let options = WriteOptions::default();
            let mut state = task.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
            let mut batch = WriteBatch::new();
            for _ in 0..writes / tasks {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                batch.clear();
                let key = format!("{state:016x}");
                batch
                    .put(KeyspaceId::DEFAULT, key.as_bytes(), &[7_u8; 32])
                    .expect("a put");
                db.write(&batch, &options).await.expect("a write");
            }
        });
    }
    while set.join_next().await.is_some() {}
    writes as f64 / started.elapsed().as_secs_f64() / 1e6
}

pub fn main(writes: u64, rounds: usize) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    println!("| tasks | Mwrites/s median (min–max) |");
    println!("|---|---|");
    for tasks in [1_u64, 4, 8, 16] {
        let mut results: Vec<f64> = (0..rounds)
            .map(|_| runtime.block_on(round(writes, tasks)))
            .collect();
        results.sort_by(f64::total_cmp);
        println!(
            "| {tasks} | {:.3} ({:.3}–{:.3}) |",
            results[results.len() / 2],
            results[0],
            results[results.len() - 1]
        );
    }
}
