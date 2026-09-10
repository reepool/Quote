## 1. Freeze replay inputs and identity

- [x] 1.1 Add one routing/continuation replay contract bound to the committed twenty-report manifest/PDFs, v5 plan and audits, owner-closure baseline, frozen Gemini profile, budgets, source-review rule, readiness gates, and a new immutable batch ID.
- [x] 1.2 Add a thin operator mode that admits only the exact contract, rejects generic v5 execution and pre-existing output, and delegates execution to the existing report-local service.

## 2. Verify admission before network use

- [x] 2.1 Add focused tests for valid admission, plan/audit/baseline drift, output identity reuse, mode isolation, and unchanged research-only production boundaries.
- [x] 2.2 Run focused tests, Ruff, artifact/hash and output-absence checks, `git diff --check`, and strict OpenSpec validation before provider creation.

## 3. Execute the sole v5 replay

- [ ] 3.1 Perform one read-only Scorpio/Gemini connectivity preflight and use the authorized sandbox-external path for the provider-bearing process.
- [ ] 3.2 Run exactly one complete twenty-report replay under the frozen batch identity, preserving every report-local typed result without tuning, targeted reruns, model substitution, or result splicing.

## 4. Review and decide readiness

- [ ] 4.1 Generate the full source-review package and outcomes, including recurrence checks for the three v5 corrections, previously closed high-risk semantic errors, and positive controls.
- [ ] 4.2 Generate the readiness audit and immutable comparison against the owner-closure replay, reporting execution, usability, precision, critical errors, review workload, provider/tokens/latency, and exact failed gates.

## 5. Close the validation

- [ ] 5.1 Record the sole replay honestly as `ready`, `hold`, or `failed`, retain `production_authorization=not_authorized`, and identify only the next source-bound business blocker if a gate fails.
- [ ] 5.2 Re-run focused verification, review only blocking defects, commit and push isolated changes, and leave the change ready for external review/archive.
