## 1. Chinese Semantic Contract

- [x] 1.1 Version structured and narrative business-profile prompts/schemas with source-native label/value/unit fields, Chinese semantic-summary fields, and program-owned canonical fields
- [x] 1.2 Require Simplified Chinese conclusions, preserve Chinese source labels/proper nouns/acronyms/units, and remove prompt language that invites translation or model-side conversion
- [x] 1.3 Implement a bounded Chinese-language contract validator with one automatic repair request, field-level fail-soft handling, and no routine human-review escalation
- [x] 1.4 Update runtime record builders so `*_raw` remains source-native, semantic summaries remain Chinese, and stable canonical IDs never depend on model translation
- [x] 1.5 Add tests for Chinese labels, English-only violations, mixed Chinese/acronym text, source symbols, semantic paraphrases, and stable cross-period identities
- [x] 1.6 Audit every business-profile prompt/schema so model-derived hints are non-authoritative, while program code owns authoritative conversions, percentages, ratios, totals, differences, margins, concentration, rankings, materiality, confidence, and numeric exposure values
- [x] 1.7 Preserve optional `model_derived_hints` as diagnostics and ignore/recompute them without rejecting otherwise valid semantic responses; add field-level partial-acceptance tests

## 2. Deterministic Unit Governance

- [x] 2.1 Replace whole-string-only unit resolution with a versioned `UnitResolution` contract containing source unit, normalized lexeme, dimension, canonical unit, scale, compound structure, rule lineage, and status
- [x] 2.2 Implement Unicode/punctuation normalization and deterministic Chinese/SI magnitude grammar for ten, hundred, thousand, ten-thousand, million, ten-million, hundred-million, kilo, mega, and giga scales
- [x] 2.3 Expand governed dimensions and aliases for currency, count classifiers, mass, area, volume, energy, power, length, duration, ratios, prices, and capacities, including every unit observed in the 2026-08-09 batch
- [x] 2.4 Convert source values with `Decimal`, preserve source value/unit lineage, prohibit implicit FX conversion, and record every multiplier and catalog rule
- [x] 2.5 Represent unresolved or cross-dimension units as `unit_resolution_pending` without discarding the semantic response or consuming another LLM attempt
- [x] 2.6 Add catalog replay and tests proving pending units resolve after a catalog-only version change with zero new model calls
- [x] 2.7 Add property and table-driven tests for magnitude composition, compound classifiers, plural aliases, incompatible dimensions, precision, overflow bounds, and unknown units
- [x] 2.8 Add the append-only runtime unit-rule proposal/overlay tables with proposal, proof, quarantine, auto-approval, catalog-version, and replay lineage
- [x] 2.9 Implement the optional closed-schema unit-proposal LLM path without catalog write authority or company-value conversion
- [x] 2.10 Implement deterministic formula proof by primitive-reference validation, multiplier recomputation, dimensional analysis, cycle/prohibited-rule checks, and exact round-trip tests
- [x] 2.11 Auto-promote only mechanically provable overlay rules, make uncommitted proofs unusable by canonical publication, and quarantine new dimensions, contextual/non-linear rules, FX, ambiguous mappings, and model-only assertions
- [x] 2.12 Persist every unknown-unit proposal with append-only lifecycle states (`proposed`, `shadow_active`, `auto_approved`, `quarantined`, `superseded`) and affected-fact/replay lineage
- [x] 2.13 Add automated corroboration and promotion thresholds for bounded linear shadow rules without routine manual approval
- [x] 2.14 Send deduplicated Telegram notifications for new, shadow-active, promoted, quarantined, and superseding unit rules
- [x] 2.15 Implement superseding-rule correction and automatic replay of all affected semantic artifacts without rewriting prior catalog history

## 3. Replayable Semantic Artifacts

- [x] 3.1 Add the semantic-artifact storage schema and repository API with immutable scope, input/response hashes, bounded JSON, evidence IDs, model lineage, usage, conversion status, and catalog identities
- [x] 3.2 Persist a validated model artifact before unit conversion, arithmetic reconciliation, or candidate persistence on both success and conversion-failure paths
- [x] 3.3 Replay compatible conversion-pending artifacts before gateway admission and reject replay when document, evidence, prompt/schema, or input identity differs
- [x] 3.4 Expose artifact receipt, replay, rejection, conversion, saved-token, and persistence IDs in INFO metrics and bounded DEBUG logs
- [x] 3.5 Add restart and idempotency tests proving conversion retries do not duplicate artifacts, candidates, evidence, or LLM requests

## 4. Numeric Correctness

- [x] 4.1 Remove every unconditional `numeric_reconciliation_valid=True` assignment and audit all deterministic, structured-semantic, verifier, promotion, and reuse paths for equivalent assumed-success flags
- [x] 4.2 Implement precision-aware `Decimal` reconciliation for revenue, cost, and gross margin after canonical unit conversion, with explicit passed, failed, derived, and not-applicable states
- [x] 4.3 Reject the whole affected semantic bundle when a reported numeric identity conflicts beyond tolerance and retain both reported and calculated diagnostics without overwriting either value
- [x] 4.4 Add regression fixtures for the two inconsistent `600403.SH` rows and representative correct, rounded, missing-margin, zero-revenue, negative-margin, and incompatible-unit cases
- [x] 4.5 Ensure promotion/readiness cannot accept a candidate lacking an executed applicable reconciliation result
- [x] 4.6 Move authoritative percentage, ratio, total, difference, concentration, ranking, materiality, confidence, and numeric exposure arithmetic into versioned program functions and test that LLM-calculated hints are retained diagnostically but never accepted as authority

## 5. Automatic Recovery And Data Hygiene

- [x] 5.1 Implement an idempotent audit that finds shadow candidates created under obsolete reconciliation, language, label-identity, or unit contracts
- [x] 5.2 Mark affected candidates non-publishable through governed rejected/superseded history and requeue only the earliest necessary stage without deleting approved records or immutable evidence
- [x] 5.3 Recover the 12 known unit-blocked work items from persisted audit responses when hashes match, preserving attempts and avoiding annual-report download, parse, and LLM repetition
- [x] 5.4 Rotate only affected prompt/schema/unit/runtime identities while reusing PDF, page, selected-section, and valid semantic artifacts
- [x] 5.5 Add temporary-database migration tests for inconsistent candidates, unit retries, approved-history blockers, repeated recovery, and interrupted recovery resume

## 6. Async Throughput And SQLite Availability

- [x] 6.1 Move full `ResearchStorage.initialize()` and migration readiness to one pre-worker run boundary and pass a readiness token/dependency to every stage
- [x] 6.2 Remove per-work-item full initialization and add tests that multiple acquire/parse/semantic/publish items initialize configured databases exactly once
- [x] 6.3 Restrict bundle evidence validation queries to referenced evidence IDs and preserve one bounded bulk transaction per field-family bundle
- [x] 6.4 Add writer transaction p50/p95/max latency, lock-duty, wait, initialization-count, and inter-write-yield metrics with configurable degraded thresholds
- [x] 6.5 Raise structured-shadow semantic concurrency from 2 to 10, keep the shared logical-profile gateway authoritative, and expose requested/admitted/in-flight/throttled/provider-congestion metrics
- [x] 6.6 Add adaptive stage reduction/recovery around gateway congestion without coupling semantic concurrency to the single SQLite writer
- [x] 6.7 Add concurrency tests proving parse/LLM work runs in parallel outside the write gate, `max_active_writers` remains 1, and another SQLite client can write during a long semantic batch
- [x] 6.8 Add a bounded performance regression comparing initialization count, transaction count, writer duty, elapsed time, and LLM throughput with the 2026-08-09 baseline

## 7. Safe HKEX Factor Parsing

- [x] 7.1 Implement a bounded project-owned Sina qfq-factor response parser that validates HTTP status, assignment shape, body size, row schema, base row, and numeric factors without `eval` or `exec`
- [x] 7.2 Route HKEX AkShare-compatible factor acquisition through the safe parser while preserving empty-list success, `None` indeterminate fallback, and multiplicative factor behavior
- [x] 7.3 Log stable network/parser error codes with provider, endpoint family, symbol, status, response hash, exception type, and bounded DEBUG traceback without response bodies or secrets
- [x] 7.4 Add fixtures for valid factors, base-only responses, HTML/error pages, empty/truncated/oversized/malformed assignments, missing columns, and the former `invalid syntax (<string>, line 1)` case
- [x] 7.5 Verify representative factor output remains compatible with the existing backward/forward adjustment model and source-factory fallback contract

## 8. Production Validation And Review

- [x] 8.1 Update rollout configuration, unit/prompt/schema identities, DEBUG runbook queries, recovery instructions, and readiness thresholds without enabling structured promotion
- [x] 8.2 Run focused semantic extraction/runtime/async/unit/storage/HKEX tests, JSON/schema validation, AST compilation, and static checks
- [x] 8.3 Run the expanded business-profile and adjustment-factor regression suites and fix every confirmed correctness or compatibility finding
- [x] 8.4 Execute a bounded 20-company shadow validation with semantic concurrency 10, verify zero terminal failures and zero repeated LLM calls for conversion retries, and inspect Chinese outputs, field-level partial acceptance, unit lifecycle/proposals/proofs, automatic corroboration, Telegram notifications, arithmetic gates, artifacts, gateway pressure, and writer metrics
- [x] 8.5 Keep promotion disabled unless the machine-readable readiness manifest has no language, unproved/quarantined-unit-use, numeric, replay, writer, gateway, or approved-history blocker
- [x] 8.6 Review the complete implementation diff, classify findings for real trigger paths versus over-strict suggestions, fix confirmed issues, and record the final validation evidence

## Final Validation Evidence

- The bounded 20-company structured-shadow cohort used semantic concurrency 10 and promotion disabled. Final queue state was 17 `completed`, 3 classified `machine_rework`, 0 retryable work, and 0 terminal failures. The three deferred data-quality outcomes were `600403.SH` numeric reconciliation failure, `600583.SH` selector/context insufficiency, and `600930.SH` unresolved unit normalization.
- The real run persisted 46 model semantic artifacts (17 structured segments and 29 operating-fact artifacts), 43 converted events, and one artifact replay that saved 11,166 tokens. Final conversion/numeric retries used zero structured fallback calls, so no successful extraction LLM call was repeated for conversion work.
- Sample accepted Chinese outputs preserved source units `颗`, `腔`, and `万台（套）`; deterministic code normalized them to governed count units. The unknown `万张` proposal was persisted as `proposed` then `quarantined`, was not used canonically, and its deduplicated Telegram notification reached `delivered`.
- Inconsistent `600403.SH` rows remained rejected and non-publishable. No open approved-history blocker or unproved/quarantined runtime rule publication was observed. Structured promotion remained disabled.
- The clean no-competing-test replay reported `max_active_writers=1`, 192 write transactions, transaction p50 0.009270 seconds, p95 0.132397 seconds, maximum 0.342970 seconds, writer duty 0.147097, and requested semantic concurrency 10 with zero provider-congestion events.
- Final regression result: 531 passed with six pre-existing deprecation warnings. Python compilation, JSON validation, `git diff --check`, and strict OpenSpec validation passed.
- `codex review --uncommitted` was attempted outside the sandbox but could not authenticate (`refresh_token_invalidated` / `token_expired`). Equivalent manual review classified and fixed real queue, unit, replay, transaction-scope, and immutable-record trigger paths; style-only, speculative, and unrelated pre-existing worktree findings were not changed.

## 9. Production-Observed Unit Automation Completion

- [x] 9.1 Constrain unit-proposal LLM dimensions, canonical units, primitive dimensions, and round-trip vectors to a program-supplied governed vocabulary while retaining Chinese semantic explanations
- [x] 9.2 Add deterministic count aliases and arbitrary-length same-dimension classifier alternatives for all 2026-08-10 observations
- [x] 9.3 Add governed `Ah`/`mAh`/`kAh`/`万Ah` electric-charge conversion without implicit energy conversion
- [x] 9.4 Reconcile deterministically resolvable quarantined rules into append-only auto-approved replacements and replay affected semantic artifacts without extraction LLM calls
- [x] 9.5 Automatically return an explicit-table empty semantic result to expanded selection once before final machine rework
- [x] 9.6 Make Telegram unit notifications state effective status, proof/quarantine reasons, affected instruments, and deduplicate by normalized unit lifecycle window
- [x] 9.7 Add regression coverage for all six observed units, automatic rule replacement/replay, actionable notifications, and the bounded `601390.SH` empty-table retry path
- [x] 9.8 Run focused and expanded validation, strict OpenSpec validation, and review the complete session diff for confirmed correctness findings

## 10. Concurrency Twenty And Exceptional Rule Correction

- [x] 10.1 Raise the active structured-shadow, shared semantic pool, and Luna provider/profile ceilings from 10 to 20 while preserving adaptive congestion reduction and the single SQLite writer
- [x] 10.2 Add append-only operator inspection/correction for a remaining wrong unit proposal using governed dimension, canonical unit, and exact positive multiplier inputs only
- [x] 10.3 Automatically supersede the old rule, replay affected semantic artifacts, and dispatch the replacement and superseded Telegram lifecycle notices
- [x] 10.4 Add configuration, registry, scheduler-command, and regression coverage and run strict OpenSpec validation plus code review

### 2026-08-10 Concurrency Twenty And Correction Evidence

- The active structured-shadow stage, shared semantic pool, Luna provider resource, and Luna semantic profile all request a ceiling of 20; the gateway retains 18 bulk slots, 2 reserved slots, weighted-fair admission, RPM control, and adaptive reduction.
- Operator correction accepts only a governed dimension/canonical-unit pair and exact positive decimal multiplier. It appends a distinct replacement, supersedes history, resumes across interrupted catalog commits, replays affected artifacts, and uses durable notification rows as its idempotent completion marker.
- Focused and expanded business-profile, scheduler, rollout, LLM routing, and orchestration suites passed 173 tests. AST compilation, JSON parsing, focused Ruff fatal/undefined/import checks, `git diff --check`, and strict OpenSpec validation passed.
- `codex review --uncommitted` could not produce a formal verdict because the local Codex refresh token is revoked (`refresh_token_invalidated`, followed by `token_expired`). Equivalent review of the task diff confirmed and fixed the effective-gateway ceiling, interrupted-correction recovery, post-lifecycle replay/notification recovery, and decimal-text idempotency findings; unrelated pre-existing worktree changes were excluded.

### 2026-08-10 Production-Observed Closure Evidence

- The six quarantined units from run `business-profile-20260810083411252525` were reconciled in an isolated catalog migration probe and then in `data/research.db`: six append-only auto-approved replacements, six superseded proposals, and six conversion-pending artifact replays; no extraction LLM calls were made.
- The new unit catalog resolves `万粒`, `万羽`, `个/片/套/只`, `万Ah`, `瓶/支/盒/袋/板`, and `瓶/袋/支`; `Ah` remains an electric-charge unit and is not convertible to energy without voltage lineage.
- Focused business-profile regression result: 209 passed across extraction, schema, unit, artifact, runtime, async production, rollout, and production-readiness suites. Additional async rework tests passed, including the one-time context expansion retry.
- Strict OpenSpec validation passed and Python compilation/diff checks passed. Ruff reports pre-existing repository-wide style findings in touched legacy files; no new test or runtime failure was introduced.
- `codex review --uncommitted` was attempted with escalation but could not authenticate because the session refresh token was revoked/expired. Equivalent manual review was completed for only this session's files; unrelated baseline worktree changes were not modified.
