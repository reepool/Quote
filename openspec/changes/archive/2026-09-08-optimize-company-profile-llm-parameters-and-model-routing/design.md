## Context

Stage 5 currently requires callers to choose one output-token value and one request deadline. The failed BaoSteel run used 4,000 output tokens, 180 seconds per request, and an unrelated 420-second shell timeout. The configured provider profiles already permit 300-second attempts and 620-second execution windows. Initial identical-request comparisons show that 12,000 tokens removes the original 4,000-token parse failure for all three models in one trial, while repeated structured reliability differs by model.

## Goals / Non-Goals

**Goals:**

- Produce a reproducible, source-identical comparison of Grok, GLM, and Gemini on narrative and table-heavy scopes.
- Select a deterministic Stage 5 primary model using structured success first, semantic support second, and latency/token volume third.
- Give the operator safe bounded defaults while preserving explicit overrides.
- Complete one new BaoSteel run without an outer deadline shorter than the workflow budget.

**Non-Goals:**

- Changing Evidence, acceptance policy, or production authorization; one locally reproduced adapter mismatch may be corrected without relaxing source validation.
- Building a general model-ranking platform or changing unrelated LLM workloads.
- Treating reported reasoning/output usage above `max_completion_tokens` as a valid result without recording the warning.

## Decisions

1. Compare concrete profiles, not the weighted `semantic_extraction` pool, so each request has an attributable model result.
2. Use the exact frozen request and temperature zero. Record local parse success, repair use, candidates by field/object, Evidence references, source-text support, latency, and reported token usage.
3. Choose the primary model only after both narrative and table-heavy comparisons. A fast model that intermittently fails JSON parsing is not the primary.
4. If all models are rejected by the same local adapter rule, diagnose and correct that source-contract mismatch before ranking models; do not score a shared local failure as model quality.
5. Set Stage 5 operator defaults to the selected concrete profile, 20,000 extract/repair output tokens, 18,000 verify tokens, 300 seconds per request, and 27 provider calls. Explicit positive overrides remain available.
6. Do not place a short shell timeout around a complete run. The existing request deadline, retry count, and provider-call budget remain the bounded controls.

## Risks / Trade-offs

- [Provider usage may include hidden reasoning tokens] → Record both configured cap and provider-reported usage; judge success by returned content plus local validation, not usage alone.
- [One sample and two scopes cannot prove universal model quality] → Limit the conclusion to annual-report Stage 5 and retain explicit overrides.
- [Larger caps can increase cost and latency] → Use a smaller verify default and keep bounded call counts.
