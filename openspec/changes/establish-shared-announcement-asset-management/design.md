# Design

## Architecture

The shared module owns three responsibilities:

1. Discover and classify annual-report announcement metadata.
2. Store attachment bytes once by SHA-256 and maintain database references.
3. Resolve the effective report for `(instrument_id, fiscal_year)`.

Business modules call the shared API and remain responsible for parsing and
business-specific processing.

## Storage

- SQLite tables store announcements, attachments, attachment versions, blobs,
  effective annual reports, discovery cursors, retries, and job operations.
- Attachment bytes live at
  `data/filings/announcements/blobs/{hash_prefix}/{sha256}.pdf`.
- Existing valid files may be registered and reused without moving or
  redownloading them.

## Selection Rules

- Only complete Chinese annual-report PDFs are eligible in V1.
- Summaries, English versions, notices, presentations, audit-only documents,
  and semiannual/quarterly reports are excluded.
- A valid correction replaces its predecessor for the same fiscal year.
- Ambiguous candidates remain non-current until later evidence resolves them.
- Historical rows may remain for audit, but API consumers see one current
  effective asset per stock and fiscal year.

## Workflows

### On-Demand

Resolve locally first. If absent and the caller authorizes acquisition, discover
the requested scope, persist metadata, download the selected full report, verify
PDF/hash, and return the local asset.

### Latest-Only Bootstrap

Build the active SSE/SZSE/BSE A-share universe and search newest fiscal years
first until one effective report is found or the bounded search is exhausted.

### Daily Update

Use persisted per-source cursors with a short overlap window. Persist metadata
before attachment download, download new eligible originals/corrections, and
advance a discovery window only after its pages complete. Attachment failures
remain retryable without discarding discovered metadata.

## API Boundary

The public surface is API-only: ensure/get/list/content plus operator endpoints
for latest bootstrap and daily update. No backup, restore, or Web UI endpoints
are part of this change.
