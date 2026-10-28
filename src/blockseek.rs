//! Point seeks within one table file held in memory, every data block decoded once and kept: the
//! CPU of the index and data block search, with no checksum, copy, storage or runtime around it. Usage: `tessarikv-bench blockseek <file> [seeks]`.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use tessarikv_format::{
    Block, BlockHandle, BlockSource, Compression, Result, Table, TableBuilder, TableOptions,
    unseal_block,
};
use tessarikv_types::{SequenceNumber, compare_internal, encode_internal, seek_trailer};

use crate::compression::mix;

/// The user key of an internal key: all but the eight trailer bytes.
fn user_key(internal: &[u8]) -> &[u8] {
    &internal[..internal.len().saturating_sub(8)]
}

/// The file's data blocks, each decoded the first time it is asked for.
#[derive(Debug)]
struct Decoded {
    file: Vec<u8>,
    blocks: Mutex<HashMap<u64, Block>>,
}

impl BlockSource for Decoded {
    fn block(&self, handle: BlockHandle) -> Result<Option<Block>> {
        let mut blocks = self.blocks.lock().expect("an unpoisoned map");
        if let Some(block) = blocks.get(&handle.offset()) {
            return Ok(Some(block.clone()));
        }
        let start = usize::try_from(handle.offset()).expect("fits");
        let end = start + usize::try_from(handle.size()).expect("fits");
        let block = unseal_block(
            &self.file[start..end],
            handle.offset(),
            None,
            compare_internal,
        )?;
        blocks.insert(handle.offset(), block.clone());
        Ok(Some(block))
    }
}

/// The table's entries written again by this build's builder: 4 KiB blocks, restart 16, ten
/// bloom bits per key, no compression.
fn rebuilt(bytes: Vec<u8>) -> Vec<u8> {
    let table = Table::open(bytes, compare_internal, user_key).expect("a table");
    let options = TableOptions {
        block_bytes: 4096,
        restart_interval: 16,
        bloom_bits_per_key: 10,
        filter: tessarikv_format::FilterPolicy::Bloom,
        compression: Compression::None,
        prefix_length: None,
    };
    let mut builder = TableBuilder::new(options, compare_internal, user_key);
    let mut walk = table.iter();
    walk.seek_to_first();
    while walk.valid() {
        builder
            .add(walk.key(), walk.value())
            .expect("ordered entries");
        walk.next();
    }
    builder.finish().expect("a table")
}

pub fn main(file: &Path, seeks: u64) {
    let mut bytes = std::fs::read(file).expect("a readable table file");
    if std::env::var_os("REBUILD").is_some() {
        bytes = rebuilt(bytes);
    }
    let len = u64::try_from(bytes.len()).expect("fits");
    let source = Arc::new(Decoded {
        file: bytes.clone(),
        blocks: Mutex::new(HashMap::new()),
    });
    let table = Table::open_with(
        len,
        |at, buf: &mut [u8]| {
            let start = usize::try_from(at).expect("fits");
            buf.copy_from_slice(&bytes[start..start + buf.len()]);
            Ok::<(), ()>(())
        },
        source,
        None,
        compare_internal,
        user_key,
    )
    .expect("a table");
    let mut keys = Vec::new();
    let mut walk = table.iter();
    walk.seek_to_first();
    while walk.valid() {
        keys.push(user_key(walk.key()).to_vec());
        walk.next();
    }
    let count = u64::try_from(keys.len()).expect("fits");
    let mut targets = Vec::with_capacity(keys.len());
    for key in &keys {
        let mut target = Vec::new();
        encode_internal(key, seek_trailer(SequenceNumber::MAX), &mut target);
        targets.push(target);
    }
    let mut state = 77_u64;
    let mut found = 0_u64;
    let started = Instant::now();
    for _ in 0..seeks {
        state = mix(state);
        let target = &targets[usize::try_from(state % count).expect("fits")];
        let mut iter = table.iter();
        iter.seek(target);
        found += u64::from(iter.valid() && user_key(iter.key()) == user_key(target));
    }
    let nanos = started.elapsed().as_nanos() as f64 / seeks as f64;
    assert_eq!(found, seeks, "every seek lands on its key");
    println!("| blockseek | {count} entries | {nanos:.0} ns/seek |");
}
