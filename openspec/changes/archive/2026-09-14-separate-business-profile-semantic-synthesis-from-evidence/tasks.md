## 1. Semantic Contract

- [x] 1.1 Audit all business-profile LLM extraction and verification paths for verbatim-text assumptions and classify retained structural gates
- [x] 1.2 Version prompts and schemas so model fields are explicit semantic conclusions rather than source transcription
- [x] 1.3 Remove lexical equality gates while preserving schema, scope, finite-number, range, and evidence-identifier validation
- [x] 1.4 Support deterministic composite evidence for multiple spans across selected sections of one source document

## 2. Runtime Correctness

- [x] 2.1 Map semantic conclusions into existing candidate records with explicit semantic-synthesis lineage
- [x] 2.2 Classify unit normalization and downstream transformation failures without collapsing them into `context_incomplete`
- [x] 2.3 Reuse completed field-family semantic runs and apply token budgets independently to unfinished field-family requests
- [x] 2.4 Prevent one stage-worker invocation from reclaiming the same retryable work ID after backoff expiry
- [x] 2.5 Rotate semantic identities while preserving annual-report and selected-section assets

## 3. Observability

- [x] 3.1 Persist bounded semantic outputs, evidence references, row decisions, usage, hashes, and exception details for success and failure paths
- [x] 3.2 Add INFO lifecycle/aggregate logs and DEBUG content, transformation, persistence, and traceback logs with redaction and bounds
- [x] 3.3 Expose semantic acceptance, failure categories, checkpoint reuse, and duplicate-retry prevention in stage progress metrics

## 4. Verification

- [x] 4.1 Add extraction tests for paraphrases, normalized units, multi-section evidence, invalid identifiers, and structural rejection
- [x] 4.2 Add runtime tests for successful semantic persistence, conversion diagnostics, field-family reuse, independent budgets, and detailed logging
- [x] 4.3 Add async queue tests proving a work ID is claimed at most once per worker invocation
- [x] 4.4 Run focused tests and static validation, review the complete task diff, and fix every confirmed finding without weakening semantic synthesis
