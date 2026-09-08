# Stage 5 annual-report model and parameter comparison

Date: 2026-09-08

## Fixed comparison contract

- Sample: Baoshan Iron & Steel (`600019.SH`) 2025 annual report.
- Narrative scope: `business_overview`, physical pages 9-10.
- Table-heavy scope: `segment_industry_product_region_mode`, physical pages 14-15.
- Evidence, request scope, compact schema, temperature (`0`), and per-trial deadline (`300s`) were identical across concrete profiles.
- Narrative trials used 12,000 output tokens. The table trial used 16,000 output tokens.
- Results stayed outside production; no approved, commodity, value-chain, DCF, scheduler, API, or Telegram path was written.

## Narrative results

| Profile | Trial 1 | Trial 2 | Observed conclusion |
| --- | --- | --- | --- |
| `semantic_extraction__scorpio_grok` | success, 145.6s, 5,498 input / 10,050 output | success, 215.7s, provider reported 14,561 output | 2/2 locally structured; slow and one provider-usage-over-cap warning |
| `semantic_extraction__zai` | success, 140.5s, 2,803 input / 6,612 output | success, 139.0s, 6,375 output | 2/2 locally structured; most compact narrative output |
| `semantic_extraction__scorpio_gemini` | success, 38.0s, 3,137 input / 11,503 output | failed after 103.9s: response parse error, then bounded repair HTTP 400 | fastest successful call, but only 1/2 structured reliability |

The 4,000-token setting from the failed out-of-sample run is not safe for Stage 5 extraction. Twelve thousand tokens was sufficient for successful narrative calls, but provider usage may include hidden reasoning and may exceed the requested cap; the gateway warning remains material even when returned content validates.

## Table-heavy results

Before the local adapter correction, all three returned payloads were rejected by the same local error: the frozen Evidence plan bound the source-native table header as `销售模式`, while the adapter incorrectly reapplied the free-model-output rule requiring a heading beginning with `分` or `按`. The model schema did not ask any model to return this dimension. The correction keeps the strict rule for model-returned dimensions and accepts a plan-bound dimension only after preparation and candidate Evidence both confirm that it occurs in the source.

After that correction, the identical 16,000-token request produced:

| Profile | Result | Elapsed | Usage (input/output) | Candidate shape | Assessment |
| --- | --- | ---: | ---: | --- | --- |
| `semantic_extraction__scorpio_grok` | success | 123.4s | 7,159 / 8,650 | 14 Segment + 41 Measurement | Correct: the adjustment row has revenue/cost but no numeric margin |
| `semantic_extraction__zai` | success | 184.6s | 4,075 / 9,864 | 14 Segment + 42 Measurement | Over-extracted the `/` adjustment margin as a Measurement |
| `semantic_extraction__scorpio_gemini` | success | 14.5s | 4,812 / 4,934 | 14 Segment + 41 Measurement | Correct shape and fastest table result |

All three retained the 14 approved source row labels, the five source-native dimensions (`分行业`, `分产品`, `分地区`, `销售模式`, and local `adjustment`), and complete Evidence bindings. Candidate counts are source-derived: 13 ordinary rows have three numeric cells and the consolidation-adjustment row has only revenue and cost, yielding 41 Measurements.

## Bounded decision

`semantic_extraction__scorpio_grok` is the Stage 5 primary for this annual-report workflow. It is the only compared model with repeated narrative structured success and the correct table candidate shape. This is not a claim of general model superiority.

- Gemini remains an explicit speed-oriented comparison/fallback, but its repeated narrative parse reliability is not sufficient for primary routing.
- GLM remains an explicit alternative and is compact on narrative text, but the table trial contained one source-present yet economically invalid Measurement.
- The operator does not use the weighted pool for the default result, so retries do not silently change model semantics.

## Measured operating defaults

- Primary profile: `semantic_extraction__scorpio_grok`
- Extract and repair output budget: 20,000 tokens
- Verify output budget: 18,000 tokens
- Request execution deadline: 300 seconds
- Provider-call budget for one nine-scope report: 27 calls (extract + at most one repair + verify per scope)

The complete run used the provisional 16,000/12,000 settings and completed all 18 calls, but its heaviest related-party extract reported 16,487 output tokens and its verify reported 14,798. Both returned valid content while emitting `provider_output_budget_exceeded`; final defaults therefore use 20,000/18,000 to retain measured headroom.
Explicit positive overrides remain available. The legacy global output override applies to all three call types for compatibility.

A complete run must rely on these internal bounds. Do not wrap it in a shell timeout shorter than the workflow budget. The hard theoretical call budget is `27 × 300s = 8,100s` before process overhead; the recommended operator invocation has no additional shell timeout. A completed run may finish much sooner, but a 420-second wrapper cannot represent the workflow contract.
