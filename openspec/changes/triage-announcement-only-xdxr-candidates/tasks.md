## 1. Announcement Case Domain

- [x] 1.1 Implement mode and threshold validation, action-family grouping, stable provisional case identity, and bounded association
- [x] 1.2 Implement deterministic evidence-role ranking, primary/supporting selection, and supersession lineage
- [x] 1.3 Implement the bounded announcement-case LLM schema, prompt, response validation, and active/shadow routing decisions
- [x] 1.4 Reuse official corporate-action document bundles for bounded full-announcement text input and fail conservatively on document errors

## 2. Daily Workflow Integration

- [x] 2.1 Load and persist bounded announcement cases and inactive watches in existing announcement scan metadata
- [x] 2.2 Integrate announcement-only case triage with unmatched-special governance without changing structured-event or canonical owners
- [x] 2.3 Reactivate inactive cases from changed CNInfo events, TDX events, and material reconciliation evidence
- [x] 2.4 Preserve deterministic title queue behavior in disabled mode and preserve queue membership in shadow mode

## 3. Scheduler Contract And Reporting

- [x] 3.1 Add mode, profile, threshold, case-cap, and bundle-cap parameters to the existing daily scheduler job and maintenance command
- [x] 3.2 Report event-centric case counts, inactive watches, reactivations, primary changes, and failures without treating disabled mode as partial

## 4. Verification And Documentation

- [x] 4.1 Add pure tests for validation, grouping, multi-announcement primary selection, thresholds, and source reactivation
- [x] 4.2 Add daily workflow tests for disabled fallback, shadow isolation, active inactive-watch routing, persistence, and no synthetic event/factor writes
- [x] 4.3 Add scheduler configuration/report tests and representative full-document LLM fixture coverage
- [x] 4.4 Update the corporate-action W6 program note, run focused and related regression tests, strict OpenSpec validation, Ruff, diff checks, and blocking-defect review
