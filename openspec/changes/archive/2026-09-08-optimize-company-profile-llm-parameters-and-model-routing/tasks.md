## 1. Comparable model evidence

- [x] 1.1 Freeze the same BaoSteel business-overview request and compare Grok, GLM, and Gemini at temperature zero, 12,000 output tokens, and 300 seconds.
- [x] 1.2 Compare the same table-heavy request across all three models and record parse success, latency, token usage, candidate shape, Evidence binding, and source support.
- [x] 1.3 Select the Stage 5 primary model and document the bounded conclusion without claiming general LLM superiority.

## 2. Parameter and routing implementation

- [x] 2.1 Give the Stage 5 operator measured defaults for concrete primary profile, extract/repair output tokens, verify output tokens, request timeout, and provider-call budget while preserving overrides.
- [x] 2.2 Keep the semantic service/provider owner unchanged and add focused tests for defaults, call-specific budgets, and invalid overrides.
- [x] 2.3 Document that complete runs rely on internal bounded deadlines and must not use an outer timeout shorter than the workflow budget.

## 3. Validation

- [x] 3.1 Run focused Stage 5 tests, Ruff, and strict OpenSpec validation.
- [x] 3.2 Execute one new isolated BaoSteel run with the selected model and measured parameters, without historical splicing or production writes.
- [x] 3.3 Record report status, provider calls, execution time, output-budget warnings, and remaining semantic caveats; retain `production_authorization=not_authorized`.

## 4. Closure

- [x] 4.1 Review the scoped diff, confirm old authority hashes and production freezes remain unchanged, then commit and push only this change.
