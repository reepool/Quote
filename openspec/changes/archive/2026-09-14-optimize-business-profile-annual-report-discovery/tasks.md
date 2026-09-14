## 1. Provider Category Contracts

- [x] 1.1 Add source-neutral annual and semiannual category normalization with CNInfo compatibility mappings.
- [x] 1.2 Enable and test SSE/SZSE market-scope periodic category requests using official parameters.
- [x] 1.3 Prevent page-bound partial results from being discarded through fallback routing.

## 2. Business-Profile Discovery

- [x] 2.1 Pass the annual category through market discovery and expose category/filter telemetry.
- [x] 2.2 Derive the default unscoped bootstrap start from the current filing year and update rollout configuration.
- [x] 2.3 Add bounded rotating instrument-scoped repair for issuers missing the expected annual period.
- [x] 2.4 Preserve and report partial selected records, split windows, unsplittable windows, and repair progress.

## 3. Corrected Report Selection

- [x] 3.1 Recognize abbreviated BSE annual-report titles and periods without widening summary or related-document admission.
- [x] 3.2 Make frontier correction precedence and lineage independent of announcement arrival order.
- [x] 3.3 Make latest-annual queue ordering correction-first and supersede unstarted original work.

## 4. Validation And Operations

- [x] 4.1 Add focused provider, discovery, frontier, queue, configuration, and title-classification tests.
- [x] 4.2 Update the business-profile production runbook with category discovery, current-season bootstrap, targeted repair, and correction behavior.
- [x] 4.3 Run focused tests, static/compile validation, OpenSpec validation, and review the complete uncommitted diff.
