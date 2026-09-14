## 1. Source-context-safe reuse

- [x] 1.1 Add a reusable semantic-family manifest validator that loads the recorded selected artifact, checks its hash/document identity, and verifies all persisted evidence spans exactly.
- [x] 1.2 Make semantic-family reuse return and use the validated source selected-artifact path/hash, while rejecting invalid candidates with bounded diagnostics and falling back to current-context extraction.
- [x] 1.3 Persist explicit evidence-context identity in semantic-run metadata and ensure fresh extraction records the current selected artifact as its source.

## 2. Verification and recovery consistency

- [x] 2.1 Scope verify resume and inherited machine rework to the matching evidence context, treating legacy context-less verify artifacts as stale.
- [x] 2.2 Resolve stale evidence-provenance exceptions only after successful verification or fresh extraction on the validated context, without weakening terminal persistence guards.
- [x] 2.3 Add progress and failure diagnostics that distinguish reused source context, stale reuse rejection, and fresh extraction fallback.

## 3. Regression validation

- [x] 3.1 Test compatible reuse where current and source selected artifacts differ but source evidence is valid.
- [x] 3.2 Test missing/corrupt/mismatched source artifacts and evidence spans fall back to extraction and do not inherit stale verification state.
- [x] 3.3 Run focused semantic runtime/repository tests, compile checks, strict OpenSpec validation, and review the final diff for unrelated changes.
