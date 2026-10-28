<div align="center">

<img src="assets/tessarikv-mark-128.png" alt="" width="88" height="88">

# TessariKV bench

**Benchmarks of TessariKV, the storage engine under TessariDB, run against RocksDB as the reference.**

[![tessarikv](https://img.shields.io/crates/v/tessarikv?style=flat-square&label=tessarikv)](https://crates.io/crates/tessarikv)
[![licence](https://img.shields.io/badge/licence-Apache--2.0-6B5FD1?style=flat-square)](LICENSE)
[![MSRV](https://img.shields.io/badge/MSRV-1.89-6B5FD1?style=flat-square)](#building)

[TessariKV](https://github.com/tessaridb/tessarikv) · [kv.tessaridb.com](https://kv.tessaridb.com) ·
[TessariDB](https://github.com/tessaridb/tessaridb)

</div>

One binary, several modes. Each mode measures one part of the engine, from the block cache under contention to
write amplification per compaction style, and prints a Markdown table. Where a mode has a counterpart on RocksDB,
both run the same workload, configured alike and at the same durability.

TessariKV itself is pure Rust with no C or C++ dependency. This repository is kept separate because the `rocksdb`
crate builds C++.

## Modes

| Mode | What it measures |
|---|---|
| `tessarikv-bench [millis-per-run]` | The block cache as a thread-scaling curve: a sharded-mutex cache against the lock-free CLOCK cache, 64 MiB of 4 KiB blocks, 1 to 20 threads, three runs per point, with p50 / p99 / p99.9 / max. Hits over uniform and Zipf keys, and misses over four times what the cache holds. Default 1000 ms per run. |
| `tessarikv-bench engine <dir> [keys] [style-writers]` | Write amplification and write throughput per compaction style (leveled, universal, FIFO) and writer count: a random fill, then as many uniform-random overwrites. Every byte the engine appends is counted by file kind. Default 2 Mi keys; `leveled-4` runs one case. |
| `tessarikv-bench compression <dir> [keys] [codec]` | Table compression per codec (none, LZ4, Snappy, Zstandard, Deflate 1/6/9): on-disk ratio, write throughput and amplification, then point-read latency after a reopen with a small block cache. Default 1 Mi keys. |
| `tessarikv-bench reference <dir> [keys]` | RocksDB on the `compression` workload: the same keys and records from four writers, then uniform-random point reads after a reopen and a drained compaction backlog. Default 1 Mi keys; `CODEC=lz4` compresses every level. |
| `tessarikv-bench writecurve <dir> <sync\|buffered>` | Single-put writes on both engines as a curve over writer counts at one durability level: fresh random keys with records of about 200 bytes for a fixed time into a new database, the engines alternating run by run. `WRITERS` (1,2,4,8,16), `MILLIS` per run (3000) and `RUNS` per point (3) override the shape. |
| `tessarikv-bench steady <dir>` | Both engines at steady state: each is filled once (`KEYS`, default 1 000 000 records of about 200 bytes), flushed and left until compaction settles, then measured with single-put overwrites over writer counts (buffered on both sides) and iterator scans of `SCAN` entries over reader counts. `WRITERS`, `READERS`, `SCAN`, `MILLIS` and `RUNS` override the shape. |
| `tessarikv-bench memwrite [writes] [rounds]` | The commit path alone: writers committing single-put batches into an in-memory database with no log. Default 1 000 000 writes, 5 rounds. |
| `tessarikv-bench blockseek <file> [seeks]` | The CPU of index and data-block search in one table file held in memory, with no checksum, copy, storage or runtime around it. Default 2 000 000 seeks; `REBUILD=1` writes the table again with this build's builder first. |

Every run that takes a `<dir>` works in a fresh directory under it and removes it afterwards. `compression` and
`reference` keep it when `KEEP=1` is set, and `READS_ONLY=1` then measures the reads alone over what a `KEEP` run
left behind. `READERS`, `READS` and
`CACHE_MIB` override the reader count (4), the point reads per run (400 000) and the block cache (8 MiB) of the
`compression` mode.

The point-read runs of `compression` and `reference` also print the process CPU per read (user + system), the
system share, and minor faults and context switches per read. `COUNT_READS=1` additionally counts the file reads
TessariKV makes per point read; it is off by default because the counting wrapper adds two shared atomic
additions to every file read, a cost the reference engine is not charged.

### Durability

Write modes use buffered durability on both engines unless the mode takes the level as an argument (`writecurve`):
every write goes through the log, which is handed to the OS and not synced (`Durability::Buffered` in TessariKV, `set_sync(false)` in RocksDB). A comparison at different
durability levels measures the durability gap rather than the engine, so the level is stated beside every result.

## Building

```sh
cargo build --release
./target/release/tessarikv-bench
```

Rust 1.89 or later, plus a C++ toolchain and `libclang` for the `rocksdb` crate. The engine crates come from
[crates.io](https://crates.io/crates/tessarikv) at `0.2.0`.

## Reading a result

A number from one run on a quiet machine is noise wearing a number. A result here is a curve across thread counts,
several runs per point, with tail percentiles, and it names the build, the host, the data size and the durability.
Run the two engines in interleaved pairs, because a long batch drifts as the machine warms.

Recorded results live in [`results/`](results), one dated file each:

- [2026-10-16 — block cache scaling](results/2026-10-16-block-cache-scaling.md)

## Licence

[Apache-2.0](LICENSE).
