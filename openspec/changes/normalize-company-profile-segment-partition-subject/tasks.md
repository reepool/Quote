## 1. Subject normalization

- [x] 1.1 Normalize ordinary Evidence-bound partition rows from unclear/default-group drafts to `business_segment` before identity comparison.
- [x] 1.2 Preserve explicit group, issuer, named-subsidiary, and consolidation-adjustment subject distinctions.

## 2. Provider-free regression

- [x] 2.1 Add a regression for the exact canary conflict shape: default-group revenue partition plus unclear/business-segment cost and margin partitions.
- [x] 2.2 Add negative coverage proving affirmative consolidated-group and consolidation-adjustment subjects are not normalized away.

## 3. Verification and close

- [x] 3.1 Verify the immutable canary manifest/report hashes remain unchanged and do not call a provider or rerun the canary.
- [x] 3.2 Run focused tests, Ruff, compilation, and strict OpenSpec validation.
- [x] 3.3 Review only blocking defects, confirm Git isolation, then commit and push without touching pre-existing workspace files.
