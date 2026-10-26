//! The block cache: decoded data blocks of table files, kept for the next read that needs them.
//!
//! One cache serves every table file of a database, bounded by
//! [`crate::DbOptions::block_cache_bytes`]. It is keyed by `(table file id, block offset)`: file
//! ids are never reused and a table file never changes, so an entry never goes stale and needs
//! no invalidation; the blocks of a removed file are simply never asked for again and leave as
//! newer blocks arrive. A block is shared, not copied, so a reader keeps the block it got even
//! after the cache lets it go.
//!
//! The cache is split into shards, each a mutex over a map, the order its blocks arrived in, and
//! the bytes they hold. A shard never holds more than its share of the capacity: a block larger
//! than a shard is not kept, and the oldest blocks leave first to make room. Locks are held for
//! a map operation only, never across I/O or an `.await`.
//!
//! This is the baseline cache, and the mutex per shard is deliberate: it is the comparison a
//! non-blocking replacement must beat and the reference model it is checked against, and its
//! eviction and byte accounting must change together, which a concurrent map's single-entry
//! operations do not provide.

use std::collections::hash_map::RandomState;
use std::collections::{HashMap, VecDeque};
use std::hash::BuildHasher;
use std::sync::{Mutex, MutexGuard, PoisonError};

use tessarikv_format::Block;

/// Where a block is: its table file and its offset in it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockKey {
    pub file: u64,
    pub offset: u64,
}

/// Bytes below which a cache is not split further.
const SHARD_BYTES: u64 = 1 << 20;

/// The most shards a cache is split into.
const MAX_SHARDS: u64 = 16;

/// Decoded blocks of table files, bounded in bytes.
#[derive(Debug)]
pub struct BlockCache {
    shards: Box<[Shard]>,
    hasher: RandomState,
}

#[derive(Debug)]
struct Shard {
    capacity: u64,
    state: Mutex<ShardState>,
}

#[derive(Debug, Default)]
struct ShardState {
    blocks: HashMap<BlockKey, Block>,
    /// Keys in the order their blocks arrived, oldest first.
    arrived: VecDeque<BlockKey>,
    used: u64,
}

impl BlockCache {
    /// A cache holding at most `capacity` bytes of blocks; zero keeps nothing.
    pub fn new(capacity: u64) -> Self {
        // The largest power of two of shards, up to the cap, that leaves each a full share.
        let wanted = (capacity / SHARD_BYTES).clamp(1, MAX_SHARDS);
        let count = if capacity == 0 {
            0
        } else {
            1_u64 << wanted.ilog2()
        };
        let share = capacity.checked_div(count).unwrap_or(0);
        let shards = (0..count)
            .map(|_| Shard {
                capacity: share,
                state: Mutex::new(ShardState::default()),
            })
            .collect();
        Self {
            shards,
            hasher: RandomState::new(),
        }
    }

    /// The block at `key`, when the cache holds it.
    pub fn get(&self, key: BlockKey) -> Option<Block> {
        let shard = self.shard(key)?;
        lock(shard).blocks.get(&key).cloned()
    }

    /// Keeps `block` as the block at `key`, making room by dropping the oldest blocks. A block
    /// larger than its shard, or one the cache already holds, is not added.
    pub fn insert(&self, key: BlockKey, block: Block) {
        let Some(shard) = self.shard(key) else {
            return;
        };
        let size = charge(&block);
        if size > shard.capacity {
            return;
        }
        let mut state = lock(shard);
        if state.blocks.contains_key(&key) {
            return;
        }
        while state.used.saturating_add(size) > shard.capacity {
            let Some(oldest) = state.arrived.pop_front() else {
                break;
            };
            if let Some(dropped) = state.blocks.remove(&oldest) {
                state.used = state.used.saturating_sub(charge(&dropped));
            }
        }
        state.used = state.used.saturating_add(size);
        state.arrived.push_back(key);
        state.blocks.insert(key, block);
    }

    /// The shard `key` belongs to; none for a cache of no bytes.
    fn shard(&self, key: BlockKey) -> Option<&Shard> {
        let count = u64::try_from(self.shards.len()).ok()?;
        // The count is a power of two, so the mask keeps exactly the bits that pick a shard.
        let index = self.hasher.hash_one(key) & count.checked_sub(1)?;
        self.shards.get(usize::try_from(index).ok()?)
    }
}

/// Bytes a block is charged for.
fn charge(block: &Block) -> u64 {
    u64::try_from(block.size_bytes()).unwrap_or(u64::MAX)
}

/// The state of `shard`. A thread that panicked while holding it left the state whole: each
/// change under the lock completes before the next starts, so the state is used as it is.
fn lock(shard: &Shard) -> MutexGuard<'_, ShardState> {
    shard.state.lock().unwrap_or_else(PoisonError::into_inner)
}
