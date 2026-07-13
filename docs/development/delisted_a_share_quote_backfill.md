# Delisted A-share Quote Backfill

Use this workflow when local `instruments` contains delisted A-share stocks with `listed_date` and `delisted_date`, but `daily_quotes` has no or incomplete historical rows.

Run dry-run first:

```bash
/home/python/miniconda3/envs/Quote/bin/python scripts/backfill_delisted_a_share_quotes.py \
  --delisted-year-start 1999 \
  --delisted-year-end 2024 \
  --limit 20
```

Dry-run is read-only and uses a lightweight SQLite audit path. `target_count`
reports the full uncovered/partial universe matching the filters; `limited_target_count`
reports only the current bounded sample size when `--limit` is set.

Live execution requires `--execute` and should be batched:

```bash
/home/python/miniconda3/envs/Quote/bin/python scripts/backfill_delisted_a_share_quotes.py \
  --execute \
  --delisted-year-start 1999 \
  --delisted-year-end 2024 \
  --limit 20 \
  --timeout-sec 120
```

Use `--instrument-ids` for a small validation batch before a year-range run:

```bash
/home/python/miniconda3/envs/Quote/bin/python scripts/backfill_delisted_a_share_quotes.py \
  --execute \
  --instrument-ids 000508.SZ \
  --timeout-sec 120
```

The workflow only writes via the existing `daily_quotes` upsert path. It does not change instrument lifecycle fields and does not delete existing quote rows. Old delisted instruments may be unavailable from configured free sources; those cases are reported as `source_empty` or failures rather than treated as covered.

Coverage status is lifecycle-based:

- `missing`: no local `daily_quotes` rows for the instrument.
- `partial`: local rows exist, but they do not cover `listed_date` through `delisted_date`.
- `covered`: local rows span from listing date through delisting date.

As of the 2026-07-13 audit, 1999-2024 delisted A-share stocks had 288 missing targets.
The 2025-2026 delisted group had quote rows, but the stricter lifecycle audit still
classified them as partial because local rows did not span each listing-to-delisting window.
