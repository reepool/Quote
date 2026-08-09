## Context

The semantic/provenance separation and structured fallback changes are deployed in
shadow mode. The 2026-08-09 run demonstrated that the gateway can complete 27
structured requests and return closed JSON, but the adapter still has an unsafe
boundary between model output and deterministic business facts. The current
structured path marks numeric reconciliation successful before checking it,
passes free-form model units to a small alias catalog, and stores translated
model labels in `*_raw` identity fields. Twelve work items were retried after
successful model calls because conversion failed. Two inconsistent gross margins
were accepted as candidates. The same run spent most of its time in repeated
storage initialization and serialized write transactions.

The implementation must preserve the user's operating principle: no routine
manual review, automatic resumability, immutable annual-report assets, and a
single SQLite writer that leaves other applications a usable database channel.
LLM calls remain asynchronous and independently bounded; deterministic facts and
provenance must remain reproducible without the LLM.

## Goals / Non-Goals

**Goals:**

- Make the LLM contract Chinese-first and source-preserving without requiring
  semantic summaries to be literal source substrings.
- Separate source-native values/labels/units, Chinese semantic summaries, and
  program-generated canonical identifiers.
- Normalize all units and numeric values in deterministic code with a versioned,
  extensible catalog and exact decimal arithmetic.
- Persist a replayable validated semantic artifact before conversion so catalog
  fixes do not spend another LLM request.
- Reject or quarantine arithmetic inconsistencies before candidate persistence and
  automatically recover the affected shadow work.
- Raise structured semantic concurrency to four by default, with shared gateway
  admission, provider-specific limits, adaptive congestion reduction, and clear
  observability.
- Initialize storage once per production run, keep computation outside the write
  gate, use bounded bulk transactions, and measure writer duty and transaction
  latency.
- Parse Sina/AkShare factor responses without evaluating network text and expose
  actionable upstream parser diagnostics.

**Non-Goals:**

- Do not make the LLM calculate unit multipliers, currency conversions, margins,
  identifiers, page offsets, hashes, or approval decisions.
- Do not require every semantic summary to reproduce source wording.
- Do not delete immutable PDFs, evidence spans, prior audits, or approved history.
- Do not auto-promote shadow data or broaden the rollout to value-chain and
  commodity field families in this change.
- Do not add a second database writer or bypass the shared LLM gateway.

## Decisions

### 1. Use a four-part semantic row contract

Every structured row will distinguish:

1. `source_label_raw`, `source_value`, and `source_unit_raw`: source-native
   Chinese text or the exact source symbol, selected by the LLM from supplied
   evidence and retained for audit. Internal JSON keys remain stable machine
   identifiers; human-readable values and summaries are Chinese/source-native.
2. `semantic_summary_zh`: an optional concise Chinese conclusion produced by the
   LLM. It may paraphrase or aggregate multiple spans and is never used as an
   identity key.
3. `canonical_*`: program-generated names, IDs, dimensions, units, and values
   resolved by catalogs and deterministic code.
4. `evidence_span_ids`: locally resolved provenance references only.

The prompt will state “use Simplified Chinese; never translate source labels,
proper nouns, product names, units, or quoted text; preserve acronyms such as
LED/MOSFET exactly.” A bounded language gate will reject English-only values for
source-label and summary fields and automatically issue one Chinese repair
request. It will not reject legitimate Latin acronyms, registered names, or unit
symbols. The gate is a language/contract check, not a substring check on semantic
meaning.

Alternative rejected: storing the model's paraphrase in existing `*_raw` columns
and treating `*_normalized` as an alias. The 2026-08-09 output showed that this
makes cross-year IDs depend on translation choices and loses source-native query
keys.

### 2. Add a deterministic unit ontology and parser

The unit catalog becomes a versioned ontology rather than a finite list of
whole-string aliases. A `UnitResolution` contains the original unit, normalized
lexeme, dimension, canonical unit, scale multiplier, numerator/denominator for
compound units, parser rule ID, catalog version, and resolution status.

The parser applies the following deterministic stages:

- Unicode NFKC, full-width punctuation, whitespace, bracket, case, and separator
  normalization while retaining the untouched source unit.
- Tokenization of Chinese magnitude prefixes (`十/百/千/万/百万/千万/亿`), SI
  prefixes (`k/M/G`), and suffixes (`/年`, `/日`, `/吨`, `/件`).
- Dimension vocabulary for mass, count/classifier, currency, energy, power,
  area, volume, length, duration, ratio, and governed prices/capacities.
- Classifier aliases such as `颗`, `只`, `瓶`, `腔`, `台`, and `套` map to the
  count dimension while the original classifier remains in lineage. A compound
  classifier such as `只/瓶` is accepted only when both alternatives resolve to
  the same dimension; otherwise it is pending and non-publishable.
- Exact `Decimal` multiplication and division. Currency and cross-currency
  conversion remain prohibited without an explicit dated FX lineage.

The LLM returns only source value and source unit; it never returns a canonical
unit or a converted number. Unknown or ambiguous units create a durable
`unit_resolution_pending` artifact containing the semantic response and evidence.
The next catalog version replays that artifact automatically. It does not call
the LLM again and does not require a human for units that the parser can resolve
from the generic grammar. Truly ambiguous dimensions remain blocked from
publication with a machine-rework reason.

Alternative rejected: continuously append ad-hoc aliases to JSON. That approach
cannot distinguish `万台（套）`, `千只`, `亿千瓦时`, and currency scales or prove
that a new alias has the correct dimension.

### 3. Enforce numeric reconciliation after conversion

For each structured segment with revenue, cost, and reported gross margin:

- Convert revenue and cost to the same canonical currency using the unit
  resolution and exact decimal multiplier.
- Compute `(revenue - cost) / revenue` locally when revenue is non-zero.
- Compare the reported margin using a precision-derived tolerance. The tolerance
  is the larger of the disclosed rounding error propagated from revenue/cost and
  one basis point (`0.0001` as a fraction), capped at ten basis points (`0.001`)
  unless an explicitly versioned industry rule is present. A mismatch becomes
  `numeric_reconciliation_failed` and the entire semantic bundle is machine-rework;
  the reported value is never silently replaced.
- If margin is not disclosed, a derived margin may be stored only as a separate
  derived value with explicit derivation lineage. Missing values are not treated
  as successful reconciliation.

The same validation layer will check finite values, non-negative count/mass
quantities where the fact definition requires it, same-dimension units, and
period/scope consistency. No code path may set `numeric_reconciliation_valid`
unconditionally. Existing inconsistent shadow candidates are automatically marked
non-publishable/superseded and reprocessed; approved records are preserved and
reported as a readiness blocker rather than silently changed.

### 4. Persist semantic artifacts before conversion

Add a durable semantic-artifact manifest keyed by instrument, report, field
family, input hash, schema/prompt identity, and response hash. The artifact stores
bounded model JSON, evidence IDs, model usage, and status (`received`,
`conversion_pending`, `converted`, `rejected`). It is immutable after receipt.
`business_profile_semantic_runs` continues to represent a completed converted
bundle. Unit/catalog and arithmetic retries first replay a matching artifact;
only missing or invalid artifacts invoke the gateway again.

Alternative rejected: retain the response only in an exception JSON blob. That is
hard to query, cannot be safely replayed across catalog versions, and encourages
unnecessary LLM calls.

### 5. Raise concurrency without bypassing controls

Set structured semantic `max_concurrency` to 4 in shadow configuration. The stage
creates at most four async requests, while the common gateway remains authoritative
for pool admission, provider limits, token budgets, retries, and circuit breaking.
The service records requested, admitted, in-flight, throttled, failover, and
provider-congestion counts. A configured adaptive controller can lower the stage
limit when timeout/error or queue-wait thresholds are exceeded and restore it after
the cooldown window. Parse and semantic CPU/network work remains outside the
SQLite writer.

Alternative rejected: setting concurrency to a large fixed number. The previous
run had successful calls but a saturated writer; unbounded LLM concurrency would
hide the real bottleneck and increase provider failures during filing season.

### 6. Make storage initialization and writes genuinely short

Initialize all configured databases once before worker tasks begin, record an
initialization identity, and make subsequent stage calls use a cheap readiness
check. Remove per-work-item calls to the full schema/migration initializer.
Persistence keeps one writer but performs one bounded transaction per semantic
bundle and one compact queue-state transaction per work item. Evidence validation
queries only the referenced evidence IDs instead of selecting the entire evidence
table. The writer exposes transaction count, p50/p95/max write duration, cumulative
lock duty, queue wait, and inter-write idle time. A run reports degraded when the
writer duty or transaction duration crosses configured operational thresholds.

### 7. Replace unsafe HKEX response evaluation

Keep the existing Sina qfq-factor endpoint and fallback contract, but move response
decoding into a project-owned bounded parser. It validates HTTP status, assignment
shape, maximum body size, and row schema, then uses JSON-compatible parsing (with a
strictly bounded JavaScript-literal compatibility path if required) rather than
Python `eval`. Parser errors include provider, endpoint family, symbol, status,
response hash, exception type/code, and a DEBUG traceback; response bodies and
credentials are not logged. The fallback still receives `None` for indeterminate
source results.

## Risks / Trade-offs

- [Source labels are missing or the model paraphrases a label] -> Ask for a
  source-native label field, resolve it against selected evidence locally, and
  preserve an evidence-backed pending row instead of using an unstable translated
  identity.
- [The unit grammar misclassifies an unfamiliar classifier] -> Preserve raw unit,
  create a pending artifact, and block only canonical publication; never guess a
  cross-dimension multiplier.
- [A strict arithmetic gate rejects a table with issuer-specific margin rules] ->
  Store the exact disclosed row and classify the mismatch for machine rework with
  the configured tolerance and source scope; do not overwrite the issuer value.
- [Concurrency increase causes provider congestion] -> Let the common gateway
  reduce admission, expose provider metrics, and keep an operator kill switch.
- [Initialization removal leaves a stale schema] -> Compare initialization and
  schema identities once at run start; fail the run before claiming work if the
  database is not ready.
- [The safe parser cannot decode a changed Sina response] -> Return `None`, use the
  configured fallback, and preserve bounded diagnostics rather than executing
  untrusted text.

## Migration Plan

1. Deploy code, catalog/schema versions, artifact manifest tables, and configuration
   with promotion still disabled.
2. Run an automatic migration that marks the known inconsistent shadow candidates
   and unit-blocked work as non-publishable/replayable; do not delete PDFs or prior
   audits.
3. Replay stored semantic artifacts under the new unit and numeric contracts. Only
   artifacts that fail hash/evidence scope validation are sent to the LLM again.
4. Run a bounded 20-company shadow batch with semantic concurrency 4 and DEBUG
   logging. Require zero unconditional reconciliation flags, zero repeated
   initialization calls, no terminal failures, and all unresolved units represented
   as pending artifacts.
5. Compare writer duty, p95 transaction time, gateway timeout rate, accepted rows,
   unit-resolution counts, and Chinese-language contract violations with the
   2026-08-09 baseline. Keep promotion disabled until the readiness manifest passes.
6. Roll back by disabling the new rollout identity and preserving artifacts and
   immutable source assets. Do not restore invalid candidates or delete the new
   audit history.

## Open Questions

None for implementation. Additional industry dimensions, such as chemical
concentration or semiconductor wafer area, are versioned catalog releases with
artifact replay rather than prompt or runtime-contract changes.
