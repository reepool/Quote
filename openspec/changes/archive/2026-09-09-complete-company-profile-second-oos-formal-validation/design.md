## Context

The archived second OOS comparison already froze the `000717.SZ` report, selected `gemini-3.8-flash`, and preserved the first formal failure. Its Evidence plan incorrectly named `energy_input`, although the Stage 5 contract intentionally represents both raw materials and energy through `material_input`. It also named the customer/supplier relationship field as `relationship` instead of the existing `counterparty_relationship`. The business-regime scope also named a redundant `business_event` target although the contract uses `business_regime`. The original preparation-only path did not invoke the already implemented field-contract check. The Stage 5 service now validates all prepared field IDs before provider execution.

The previous Scorpio DNS failures occurred inside a restricted sandbox. This completion run therefore needs an explicit out-of-sandbox connectivity check and execution, without changing retry, timeout, routing, or semantic behavior.

## Goals / Non-Goals

**Goals:**

- Freeze a new plan revision differing from the archived v1 plan only by replacing all unsupported checklist aliases with `material_input`, `counterparty_relationship`, and `business_regime`.
- Prove offline that the complete plan is accepted by the existing field-contract check before any provider call.
- Execute exactly one complete Gemini formal run with a new identity and preserve either its immutable bundle or typed failure.
- Produce a researcher-readable assessment of the six chapters and production restrictions.

**Non-Goals:**

- Repeating the three-model comparison or changing the selected model.
- Adding `energy_input`, changing extraction/verification prompts, altering semantic rules, or tuning timeouts.
- Running targeted retries, combining historical/comparison candidates, or modifying old bundles.
- Starting Stage 6 or authorizing any production consumer.

## Decisions

1. **Reuse the archived manifest.** The report identity, PDF hash, page set, and sample boundary remain unchanged; only a new Evidence-plan revision is created.
2. **Reuse existing semantic fields.** Both procurement narrative and energy/material cost-table scopes request `material_input`; the customer/supplier scope requests `counterparty_relationship`, and the regime scope requests only `business_regime`. Energy and counterparty roles remain distinguished by source-native text and Evidence, not parallel aliases.
3. **Validate before network access.** Preparation-only execution must invoke the service field-contract check across every prepared scope before connectivity probing or formal execution.
4. **Treat sandbox DNS separately.** The connectivity probe and formal run execute with the user's authorized external-network permission. A failure there is recorded as a real provider/transport observation; a sandbox-only failure is not.
5. **One completion run.** The run uses `semantic_extraction__scorpio_gemini`, extract budget 16,000, verify budget 12,000, 300-second per-attempt deadline, and the existing maximum-call budget. Its state may be `usable`, `usable_with_caveats`, `hold`, or `failed`.

## Risks / Trade-offs

- **[Scorpio is still unreachable outside the sandbox]** → Preserve the typed connectivity or formal-run failure and stop without changing the company-profile change.
- **[Gemini returns a semantic hold]** → Treat the hold as a valid OOS result; do not modify prompts or run another model.
- **[The corrected plan changes more than the invalid field]** → Compare normalized JSON against v1 and require only plan identity plus the supported field replacements.
- **[A complete run is lengthy]** → Retain the previously measured bounded parameters and avoid all targeted reruns.
