## 1. Checkpoint Identity

- [x] 1.1 Exclude `resume` from checkpoint identity while preserving audit parameters and data-affecting identity fields
- [x] 1.2 Add backward-compatible discovery and validation for legacy checkpoint files
- [x] 1.3 Wire scheduler resume operations to prefer a compatible existing checkpoint without changing explicit ID behavior

## 2. Verification

- [x] 2.1 Add helper tests for resume-neutral identity, legacy selection, explicit IDs, and incompatible parameter changes
- [x] 2.2 Run focused unit tests, syntax checks, diff checks, and strict OpenSpec validation
