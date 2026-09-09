## Provider-free result

The frozen twenty-report manifest and immutable v1 Evidence plan were replayed on
September 9, 2026 without an LLM call. The refined v2 plan reused the same report,
PDF-artifact, recovery, and governed-section identities. It did not modify the v1 plan
or the provider-bearing batch `manufacturing-materials-shadow-gemini-20260909-a`.

| Metric | v1 baseline | v2 refined | Change |
|---|---:|---:|---:|
| Reports | 20 | 20 | unchanged |
| Request scopes | 168 | 165 | -3 |
| Field/scope pairs | 568 | 374 | -194 (-34.2%) |
| Field-bound Evidence copies | 1,026 | 707 | -319 (-31.1%) |
| Field-bound Evidence serialized characters | 2,024,952 | 1,408,031 | -616,921 (-30.5%) |
| Evidence traceability | 100% | 100% | unchanged |
| Unsupported field assignments | — | 0 | pass |
| Missing required-field owners | — | 0 | pass |
| Incomplete selected table contexts | — | 0 | pass |
| Provider calls | 0 | 0 | unchanged |

The largest mechanical reduction is in operating quantities (217 to 91 field/scope
pairs) and counterparties/concentration (111 to 66). This directly removes fields that
the selected pages did not signal; it does not suppress verifier failures or assert that
the prior 127 runtime `required_coverage_missing` items have been semantically resolved.

The result proves that scope construction is materially smaller and source-aligned. It
does not establish model precision, report usability, production readiness, or Stage 6
eligibility. A later provider-bearing validation must use a separately frozen full run
identity. `production_authorization=not_authorized` remains unchanged.
