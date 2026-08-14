## 1. Provider Contracts

- [x] 1.1 Add the normalized combined periodic-report category with CNInfo, SSE, SZSE, and BSE request mappings and focused mapping tests.
- [x] 1.2 Reconcile CNInfo reported and record-derived page totals so final partial pages are scanned, with regression tests.

## 2. Financial Discovery

- [x] 2.1 Add a periodic-report anomaly selector that requires an explicit report period and excludes generic trading-risk notices, with title-classification tests.
- [x] 2.2 Split financial discovery into category-filtered main scopes, narrow anomaly scopes, and the BSE official filtered scope while preserving independent cursors and audit evidence.
- [x] 2.3 Propagate absent, failed, and incomplete stream results into financial task status and diagnostics so truncated scans cannot report success.

## 3. Validation

- [x] 3.1 Run focused announcement-provider and financial-sync unit tests, compile/static checks, and strict OpenSpec validation.
- [x] 3.2 Run a bounded live CNInfo/BSE comparison proving filtered discovery reaches the final page and materially reduces page/record volume.
- [x] 3.3 Review only the current change for blocking defects and update operator documentation with the new acquisition and completeness semantics.
