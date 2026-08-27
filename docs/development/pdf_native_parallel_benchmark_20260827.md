# Native PDF Worker Parallelism Canary

The read-only canary used six hash-bound local annual reports: the frozen
600036.SH, 001322.SZ, 002376.SZ, and 000717.SZ cases plus 603268.SH and
002496.SZ. Physical pages 1, 2, 19, and 41 were requested from each report;
each width ran 20 rounds (480 page requests per width).

| Workers | Elapsed (s) | P50 (s) | P95 (s) | Pages | Restarts | Diagnostics |
|---:|---:|---:|---:|---:|---:|---|
| 1 | 14.15 | 0.053 | 0.254 | 480/480 | 0 | none |
| 2 | 9.01 | 0.064 | 0.255 | 480/480 | 0 | none |
| 4 | 8.40 | 0.076 | 0.276 | 480/480 | 0 | none |
| 6 | 8.86 | 0.077 | 0.411 | 480/480 | 0 | none |
| 8 | 9.94 | 0.081 | 0.305 | 480/480 | 0 | none |
| 10 | 10.52 | 0.079 | 0.314 | 480/480 | 0 | none |

Width 4 is the throughput-optimal setting in this sample. Width 10 is the
highest setting that passed crash isolation and page-preservation gates, not a
recommendation to increase production concurrency blindly. Production should
start at 4; wider settings require a new canary with larger page mixes and
memory/queue measurements.

The machine-readable source is
`pdf_native_parallel_benchmark_20260827.json`.
