## 1. Deterministic Unit Governance

- [x] 1.1 Add versioned deterministic definitions and tests for `项`, `艘`, `重箱`, `重量箱`, `万重箱`, and `万重量箱`, including the governed weight-case mass conversion.
- [x] 1.2 Strengthen unit-proposal proof so an unknown source base token cannot borrow an unrelated governed primitive, and update tests that encoded the former unsafe alias behavior.
- [x] 1.3 Add reconciliation tests proving the unsafe `万重箱 -> 10000 unit` runtime rule is superseded by deterministic mass semantics without direct production-database mutation.

## 2. Semantic Conversion Replay

- [x] 2.1 Refactor structured operating-row conversion to preserve row-level unresolved-unit diagnostics while retaining independently convertible records.
- [x] 2.2 Replay the persisted structured response immediately with a fresh overlay after auto-approval, with zero extraction LLM calls and zero work-attempt cost.
- [x] 2.3 Add focused temporary-database tests for inline success, quarantined cross-dimensional rows, all-rows-pending responses, later deterministic reconciliation, and owning-work recovery.

## 3. PDF Parsing Benchmark

- [x] 3.1 Implement a read-only same-corpus parser benchmark with explicit local inputs, isolated temporary artifacts, bounded concurrency/elapsed time, fidelity hashes, warnings, timings, throughput, and available resource metrics.
- [x] 3.2 Add a developer CLI that defaults to the 4/6/8 pypdf concurrency matrix and cannot discover, download, or write production state.
- [x] 3.3 Add unit tests for identical corpus enforcement, cache isolation, bounded execution, partial failures, fidelity rejection, and report schema.
- [x] 3.4 Run a bounded benchmark against cached annual reports and retain the result as change evidence without changing production parser defaults unless the evidence supports it.
- [x] 3.5 Set the active structured-shadow production parse concurrency to four after the retained same-corpus benchmark identifies four as the fastest fidelity-passing candidate.

## 4. Validation And Handoff

- [x] 4.1 Run focused unit tests, Python compilation, and OpenSpec strict validation for all changed behavior.
- [x] 4.2 Confirm the diff does not touch announcement discovery/download/archive ownership, shared-asset APIs, DataManager, scheduler, or other-session baseline files.
- [x] 4.3 Review all uncommitted changes, assess findings for real defects versus over-strict suggestions, fix confirmed defects, and rerun validation.
