## 1. Validation Core

- [x] 1.1 Add source-explicit normalization for TDX and Eastmoney implemented distribution events
- [x] 1.2 Add event-field reconciliation with exact, shifted, conflict, single-sided, and unsupported classifications
- [x] 1.3 Add normalized cumulative factor path comparison at year-end and latest anchors

## 2. Source Orchestration

- [x] 2.1 Add bounded Eastmoney report-period acquisition with coverage and failure diagnostics
- [x] 2.2 Add CNInfo implementation-announcement metadata scanning and existence-evidence matching
- [x] 2.3 Add a read-only DataManager validation service combining event, official, and cumulative evidence

## 3. Operator Surface

- [x] 3.1 Add a manual-only scheduler task and bounded Telegram report
- [x] 3.2 Add scheduler configuration and operator documentation with source and threshold semantics

## 4. Verification

- [x] 4.1 Add focused unit tests for normalization, matching, cumulative thresholds, source coverage, and scheduler status
- [x] 4.2 Run focused regression tests, syntax/diff checks, and strict OpenSpec validation
