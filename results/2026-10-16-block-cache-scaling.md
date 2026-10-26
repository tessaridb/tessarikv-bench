# Block cache scaling — 2026-10-16

The default mode (`tessarikv-bench [millis-per-run]`): the sharded-mutex block cache against the lock-free CLOCK
cache, 64 MiB of 4 KiB blocks, 1 to 20 threads, three runs per point (median, min–max). Each operation reads a block
and inserts it when the read misses; one operation in eight is timed.

- Engine: TessariKV development tree of 2026-10-16, before the 0.1.0 release.
- Host: Apple M1 Max, 10 cores, 32 GiB, macOS; release build (`lto = "fat"`, `codegen-units = 1`).
- Workloads: `hit` — 8 192 distinct blocks (32 MiB, preloaded) read uniformly or by Zipf (s = 1.0); `miss 4x` —
  65 536 distinct blocks, four times what the cache holds, read uniformly from empty.
- Points above 10 threads oversubscribe the 10 cores.

| workload | threads | cache | Mops/s median (min–max) | p50 ns | p99 ns | p99.9 ns | max ns |
|---|---|---|---|---|---|---|---|
| hit uniform | 1 | mutex | 10.34 (10.26–10.38) | 125 | 209 | 375 | 49503 |
| hit uniform | 1 | clock | 12.62 (12.53–12.99) | 84 | 250 | 375 | 72831 |
| hit uniform | 2 | mutex | 9.83 (9.72–10.61) | 125 | 3083 | 6251 | 94143 |
| hit uniform | 2 | clock | 22.88 (21.30–22.98) | 125 | 333 | 375 | 41919 |
| hit uniform | 4 | mutex | 11.24 (9.81–11.88) | 125 | 5835 | 15127 | 71103 |
| hit uniform | 4 | clock | 38.74 (37.69–39.75) | 125 | 334 | 416 | 57023 |
| hit uniform | 8 | mutex | 8.94 (8.79–9.06) | 209 | 16463 | 35263 | 127423 |
| hit uniform | 8 | clock | 52.92 (51.21–56.10) | 167 | 416 | 542 | 2347007 |
| hit uniform | 10 | mutex | 8.30 (8.26–8.39) | 209 | 23919 | 56863 | 140543 |
| hit uniform | 10 | clock | 54.33 (53.67–58.06) | 208 | 417 | 583 | 12959743 |
| hit uniform | 16 | mutex | 8.35 (8.32–8.59) | 250 | 43071 | 87807 | 7819263 |
| hit uniform | 16 | clock | 58.27 (57.92–58.74) | 208 | 417 | 584 | 30162943 |
| hit uniform | 20 | mutex | 8.47 (8.45–8.52) | 250 | 52863 | 98175 | 20316159 |
| hit uniform | 20 | clock | 56.22 (55.60–57.86) | 208 | 417 | 583 | 51707903 |
| hit zipf1.0 | 1 | mutex | 14.62 (14.54–14.71) | 83 | 209 | 334 | 146303 |
| hit zipf1.0 | 1 | clock | 21.12 (20.79–21.36) | 42 | 208 | 333 | 244991 |
| hit zipf1.0 | 2 | mutex | 13.20 (12.30–13.43) | 84 | 2585 | 5375 | 1585151 |
| hit zipf1.0 | 2 | clock | 32.06 (31.09–32.14) | 83 | 333 | 583 | 37343 |
| hit zipf1.0 | 4 | mutex | 13.67 (13.47–13.71) | 84 | 5375 | 14167 | 92671 |
| hit zipf1.0 | 4 | clock | 44.37 (42.29–44.60) | 84 | 542 | 791 | 122559 |
| hit zipf1.0 | 8 | mutex | 8.12 (8.04–8.14) | 209 | 17471 | 36351 | 154751 |
| hit zipf1.0 | 8 | clock | 39.27 (39.16–39.28) | 250 | 875 | 1125 | 8323071 |
| hit zipf1.0 | 10 | mutex | 7.06 (6.77–7.09) | 209 | 27551 | 63167 | 166143 |
| hit zipf1.0 | 10 | clock | 39.35 (39.34–39.36) | 292 | 1083 | 1459 | 9388031 |
| hit zipf1.0 | 16 | mutex | 7.46 (7.41–7.46) | 250 | 48383 | 94591 | 8065023 |
| hit zipf1.0 | 16 | clock | 38.82 (38.42–39.16) | 292 | 1208 | 1750 | 36831231 |
| hit zipf1.0 | 20 | mutex | 8.01 (7.84–8.10) | 250 | 43935 | 146559 | 58490879 |
| hit zipf1.0 | 20 | clock | 39.30 (39.03–39.41) | 292 | 1083 | 1500 | 36765695 |
| miss 4x uniform | 1 | mutex | 5.07 (4.72–5.07) | 209 | 459 | 916 | 55423 |
| miss 4x uniform | 1 | clock | 4.56 (4.12–4.60) | 250 | 542 | 917 | 41887 |
| miss 4x uniform | 2 | mutex | 5.37 (5.27–5.45) | 250 | 3335 | 8043 | 63263 |
| miss 4x uniform | 2 | clock | 5.97 (5.67–6.14) | 333 | 1583 | 2375 | 39775 |
| miss 4x uniform | 4 | mutex | 5.66 (5.64–5.90) | 292 | 8043 | 18639 | 98175 |
| miss 4x uniform | 4 | clock | 7.58 (7.49–7.61) | 458 | 2000 | 2875 | 50047 |
| miss 4x uniform | 8 | mutex | 4.58 (4.51–4.59) | 459 | 20927 | 40543 | 239103 |
| miss 4x uniform | 8 | clock | 6.89 (6.86–6.91) | 1042 | 3209 | 5459 | 3563519 |
| miss 4x uniform | 10 | mutex | 4.14 (4.13–4.14) | 459 | 34591 | 68095 | 159103 |
| miss 4x uniform | 10 | clock | 6.21 (6.20–6.22) | 1250 | 4335 | 6627 | 8953855 |
| miss 4x uniform | 16 | mutex | 4.06 (4.03–4.12) | 500 | 52031 | 94143 | 41648127 |
| miss 4x uniform | 16 | clock | 6.20 (6.16–6.21) | 1209 | 4375 | 6835 | 46923775 |
| miss 4x uniform | 20 | mutex | 4.21 (4.15–4.25) | 500 | 74175 | 238463 | 41320447 |
| miss 4x uniform | 20 | clock | 6.18 (6.17–6.21) | 1250 | 4375 | 6875 | 40370175 |
