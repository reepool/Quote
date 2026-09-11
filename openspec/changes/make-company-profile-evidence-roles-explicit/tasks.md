## 1. Contract artifacts

- [x] 1.1 Add explicit Evidence role metadata to the provider catalog while preserving existing field bindings.
- [x] 1.2 Add role-aware legal-empty, regime, segment, and material-input instructions to extract/verify requests.

## 2. Provider-free verification

- [x] 2.1 Add focused tests for field-owner, context-only, and mixed catalog entries.
- [x] 2.2 Add focused tests asserting the role and semantic guard instructions are present in extract and verify payloads.
- [x] 2.3 Run focused tests, Ruff, and strict OpenSpec validation without any provider call.

## 3. Controlled follow-up

- [x] 3.1 Record the implementation and test hashes in the change audit.
- [x] 3.2 Stop before any external replay; prepare a separate validation plan only after provider-free checks pass.
