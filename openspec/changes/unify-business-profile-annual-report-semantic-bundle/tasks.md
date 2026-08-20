## 1. Annual-Report Evidence Bundle

- [x] 1.1 Expand common annual-report subsection aliases for industry, products and applications, operating model, main-business analysis, orders, and major customers and suppliers.
- [x] 1.2 Add the bounded internal semantic-bundle selection family and make activity and relationship work share its content-addressed selected artifact.
- [x] 1.3 Add focused selector tests for chapter scope, subsection coverage, page deduplication, and stable bundle identity.

## 2. Joint LLM Contract And Replay

- [x] 2.1 Add a versioned joint extraction request and closed JSON schema returning activities and relationships with Chinese summaries, raw fields, and evidence span identifiers.
- [x] 2.2 Preserve existing single-family compatibility while adding deterministic request identity and validated-response reconstruction for durable replay.
- [x] 2.3 Persist joint responses in the existing semantic-artifact repository before conversion and test exact replay and changed-identity misses.

## 3. Runtime Integration

- [x] 3.1 Make semantic runtime use one in-run joint response per report and project only the requested records into each independent field family.
- [x] 3.2 Preserve field-family-specific empty-result, verification, exception, persistence, and downstream program-derivation semantics.
- [x] 3.3 Add joint request, replay, sibling reuse, and saved-call metrics and structured logs.

## 4. Verification And Delivery

- [x] 4.1 Add runtime tests proving one LLM call yields two independently persisted field-family results and restart replay does not call the LLM.
- [x] 4.2 Run focused selector, extraction, artifact, runtime, async, and rollout regressions plus compile, lint, and OpenSpec validation.
- [x] 4.3 Review only blocking defects, update task status, commit the isolated change, and push it to the configured upstream.
- [x] 4.4 Preserve multiple distinct anonymous concentration facts supported by the same evidence span, and add a production-shaped regression test.
