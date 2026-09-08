## Why

The first out-of-sample annual-report run failed after using an operator-selected 4,000-token output budget, 180-second request deadline, and 420-second shell wrapper, even though the configured providers support longer bounded execution. A controlled comparison is needed to select a reliable model and safe Stage 5 parameters before another complete validation run.

## What Changes

- Compare `grok-4.6`, `glm-5.3-flash`, and `gemini-3.8-flash-high` with identical frozen annual-report requests and record structured-parse success, semantic output shape, latency, token usage, repair use, and provider errors.
- Choose a Stage 5 extraction profile from measured structured reliability first and latency second; keep other enabled models as explicit comparison/fallback choices rather than randomizing the primary result.
- Replace one global operator output limit with bounded call/scope-aware defaults sufficient for compact extract and verify responses, while retaining explicit overrides.
- Use the configured request deadline instead of a shorter ad-hoc value and document a complete-run time budget that does not terminate the service before its own bounded attempts finish.
- Run one new isolated BaoSteel validation after tests and parameter selection; retain `accepted_for_review` and `production_authorization=not_authorized`.

## Capabilities

### New Capabilities

- `company-profile-llm-execution-tuning`: Defines reproducible model comparison, Stage 5 parameter selection, deterministic primary routing, and bounded validation of the chosen settings.

### Modified Capabilities

None.

## Impact

- Affects the Stage 5 operator/provider parameter boundary, one reproduced plan-bound dimension validation bug, focused tests, and a development-only comparison artifact.
- Reuses the existing LLM profiles, semantic service, Evidence plan, and bundle store; it does not change extraction schemas, semantic policy, production databases, approved tables, or Stage 6.
