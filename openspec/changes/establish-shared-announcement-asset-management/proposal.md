# Change: Establish Shared Announcement Asset Management

## Why

Annual reports are needed by more than one business module. Downloading and
storing the same filing independently creates duplicate network requests,
duplicate files, and inconsistent correction handling.

## What Changes

- Provide one shared announcement-asset module backed by SQLite metadata and a
  content-addressed attachment archive under `data/filings/announcements`.
- Support A-share annual reports first; other announcement types may be added
  later without changing the V1 contract.
- Run a daily discovery job for new annual reports and corrections.
- Provide latest-only historical bootstrap for all active A shares.
- Provide local-first on-demand API access for other business modules.
- Reuse existing valid announcement files when they can be identified and
  verified.

## Explicitly Out Of Scope

- Backup, restore, disaster recovery, backup scheduling, or backup capacity
  approval.
- Web UI work; this project is consumed through APIs.
- Business-profile or broker parser completion.
- General traceability frameworks, release evidence matrices, and consumer
  migration approval gates.
- Downloading every announcement type or every historical annual report.

## Success Criteria

1. A caller requesting an existing annual report receives the local asset with
   zero provider requests.
2. A caller requesting a missing annual report may create metadata and download
   it through the shared module.
3. Daily discovery persists new annual-report metadata and downloads eligible
   full-report attachments.
4. For one stock and fiscal year, only the newest valid full report or correction
   is current.
5. Latest-only bootstrap records one latest effective annual report or an
   explicit missing/retry state for each active A-share instrument.
