## 1. CNInfo Pagination Contract

- [x] 1.1 Parse and validate provider total-page metadata with a count-derived fallback.
- [x] 1.2 Add opt-in preflight early-stop and bounded start/next-page diagnostics to CNInfo scans.
- [x] 1.3 Classify the preflight stop reason as resumable so partial primary-source records are preserved.

## 2. Business-Profile Window Planning

- [x] 2.1 Enable total-page preflight for splittable multi-day discovery windows.
- [x] 2.2 Persist and resume next-page checkpoints for closed unsplittable single-day backlog windows only.
- [x] 2.3 Expose pagination estimates, ranges, split decisions, and continuation state in discovery telemetry and logs.

## 3. Validation

- [x] 3.1 Add provider tests for valid, missing, malformed, derived, over-bound, and resumed pagination cases.
- [x] 3.2 Add acquisition and production-operation tests for fallback suppression, early splitting, legacy state, safe one-day resume, and completion.
- [x] 3.3 Run focused tests, compile/static checks, strict OpenSpec validation, and review the complete uncommitted diff.
