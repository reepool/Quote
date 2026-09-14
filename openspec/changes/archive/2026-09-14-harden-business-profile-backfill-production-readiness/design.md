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
- Raise structured semantic concurrency to twenty by default, with shared gateway
  admission, provider-specific limits, adaptive congestion reduction, and clear
  observability.
- Initialize storage once per production run, keep computation outside the write
  gate, use bounded bulk transactions, and measure writer duty and transaction
  latency.
- Parse Sina/AkShare factor responses without evaluating network text and expose
  actionable upstream parser diagnostics.

**Non-Goals:**

- Do not make a production extraction LLM calculate unit multipliers, currency
  conversions, percentages, ratios, totals, differences, margins, concentration,
  rankings, materiality, confidence, exposure values, identifiers, page offsets,
  hashes, or approval decisions.
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
LED/MOSFET exactly.” A bounded language gate will reject only invalid
source-label/summary fields and automatically issue one Chinese repair request.
It will not reject legitimate Latin acronyms, registered names, unit symbols, or
optional model-derived hints. The gate is a language/contract check, not a
substring check on semantic meaning.

The response validator is fail-soft at field level. It accepts the valid source
facts, semantic conclusions, and evidence references even when the model also
returns `model_derived_hints` such as a suggested margin or scale. Such hints are
retained as non-authoritative diagnostics, are never used as canonical inputs, and
are recomputed or ignored by program code. A whole response is retried only for
malformed JSON, missing required source fields, invalid evidence scope, or an
unrepairable type/contract violation. This prevents useful semantic work from
being discarded merely because the model volunteered arithmetic.

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

The LLM must return source value and source unit as the only authoritative numeric
inputs. It may also return an optional candidate unit interpretation or derived
hint, but program code treats those fields as untrusted suggestions. Unknown or
ambiguous units create a durable `unit_resolution_pending` artifact containing the
semantic response and evidence. The next catalog version replays that artifact
automatically and does not call the extraction LLM again.

Alternative rejected: continuously append ad-hoc aliases to JSON. That approach
cannot distinguish `万台（套）`, `千只`, `亿千瓦时`, and currency scales or prove
that a new alias has the correct dimension.

Unknown units use an automated three-tier maintenance path and a persistent rule
lifecycle:

1. The deterministic grammar first attempts to compose the unit from governed
   prefixes, base units, classifiers, numerators, and denominators. Successful
   compositions are runtime resolutions and do not require a new catalog row.
2. If tokens remain unknown, a separate unit-proposal LLM profile may receive the
   raw unit, bounded Chinese context, and the closed list of governed primitives.
   It returns a data-only proposal containing token decomposition, proposed
   dimension, canonical primitive references, and a declarative multiplier. It
   does not convert any company value and cannot write the catalog.
3. Program code parses the proposal without evaluating expressions, recomputes
   the multiplier from governed prefixes, proves numerator/denominator dimensions,
   checks dependency cycles and prohibited transformations, and runs exact
   round-trip test vectors. Every proposal is appended to the governed runtime
   registry with status `proposed`, `shadow_active`, `auto_approved`,
   `quarantined`, or `superseded`; it is never a one-off conversion.

Rules fully derivable from existing primitives become `auto_approved` and take
effect after the catalog-version transaction commits. A linear alias or multiplier
that cannot yet be mechanically derived may enter `shadow_active` for candidate
calculations only, while the system gathers independent model agreement, repeated
source observations, and reconciliation outcomes. It is promoted automatically
after configured corroboration thresholds are met. New dimensions, contextual or
non-linear conversions, FX formulas, contradictory proposals, or unsafe rules stay
`quarantined` and cannot affect canonical publication.

The runtime overlay is stored in a governed unit-rule table rather than rewriting
the source JSON file. Every proposal, proof result, shadow activation, corroboration,
rejection, promotion, superseding correction, catalog version, affected-fact count,
and replay outcome is auditable and replayable. Telegram sends deduplicated batch
notifications for new rules, promotions, quarantines, and superseding corrections;
notifications are informational and do not create a routine approval queue. A rule
is unusable by canonical publication until its applicable lifecycle state and
deterministic proof transaction commit; model confidence alone cannot substitute
for that state.

### 3. Enforce numeric reconciliation after conversion

All authoritative arithmetic belongs to deterministic code. The extraction LLM
identifies which source values and units belong to a fact, summarizes qualitative
business meaning, and may provide non-authoritative derived hints. Program code
handles percent-to-fraction conversion, scale application, totals, ratios, changes,
margins, concentration, rankings, materiality thresholds, confidence formulas,
numeric exposure aggregation, and every other derived numeric field.
If a source explicitly reports a percentage or total, the model returns that raw
reported value and unit; the program separately normalizes and verifies it.

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

Set structured semantic `max_concurrency` to 20 in shadow configuration. The stage
creates at most twenty async requests, while the common gateway remains authoritative
for pool admission, provider limits, token budgets, retries, and circuit breaking.
The active shared semantic pool and Luna provider resource are raised to twenty
with two reserved slots and eighteen default bulk slots; adaptive controls may
still admit fewer requests.
These values are rollout configuration, not code constants: a future increase
must update the stage, pool, provider/profile, and HTTP connection ceilings
together, while retaining the same quota and adaptive-governance checks.
The service records requested, admitted, in-flight, throttled, failover, and
provider-congestion counts. A configured adaptive controller can lower the stage
limit when timeout/error or queue-wait thresholds are exceeded and restore it after
the cooldown window. Parse and semantic CPU/network work remains outside the
SQLite writer.

Alternative rejected: bypassing admission or setting an unbounded fixed number.
The previous run had successful calls but a saturated writer; the requested limit
of twenty remains a stage ceiling, while gateway and provider controls may admit fewer
requests under filing-season pressure.

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

### 8. Complete the unknown-unit automation loop

The 2026-08-10 structured-shadow run exposed a gap between the intended lifecycle
and the implementation. Six ordinary units were persisted and safely quarantined,
but none could become reusable: free-form model dimensions/canonical units were
not constrained to the governed vocabulary, round-trip examples contained unit
text instead of numeric test vectors, count aliases were incomplete, slash
alternatives supported only two tokens, and `Ah` had no governed dimension. The
system therefore implemented safe rejection but not automatic maintenance.

The corrected lifecycle is:

1. Deterministic parsing resolves known aliases, arbitrary-length same-dimension
   classifier alternatives, and governed physical units first.
2. For an unknown token, the LLM receives the allowed dimensions, canonical
   units, and dimensioned primitive rules. It selects only from those targets and
   returns numeric round-trip vectors. The program validates target compatibility,
   recomputes the multiplier, checks source magnitude, and rejects prohibited or
   cross-dimension formulas.
3. A bounded linear alias into an existing governed dimension becomes a reusable
   runtime rule after these checks. The rule is persisted, versioned, replayed,
   and notified automatically. A later correction appends a superseding rule.
4. New dimensions, FX, non-linear/contextual formulas, inconsistent targets, and
   ambiguous mappings remain quarantined. This is the exceptional safety path,
   not a routine approval queue.
5. When a catalog release makes a quarantined rule deterministically resolvable,
   the runtime automatically appends a proved replacement, supersedes the old
   proposal, and replays every affected semantic artifact without extraction LLM
   usage.
6. If the automated proposal remains wrong, an exceptional operator control accepts
   only a governed dimension, its governed canonical unit, and an exact positive
   multiplier. It appends a separately identified replacement, supersedes the old
   lifecycle, replays affected semantic artifacts, and sends both lifecycle notices.
   It never accepts executable formulas or rewrites historical rows.

`Ah` is governed as electric charge/battery charge capacity, not energy. `Ah` may
be scaled to `mAh`, `kAh`, or `万Ah`, but conversion to `Wh`/`kWh` is prohibited
without an explicit voltage and derivation lineage.

An explicit structured table that receives an empty LLM result is not immediately
final machine rework. The queue automatically returns the item to selection once,
expands the relevant table context, and retries extraction. A second empty result
is finalized as machine rework so the workflow remains bounded.

### 9. Make pipeline concurrency work-conserving and unit notifications final-state aware

The 2026-08-10 run requested semantic concurrency twenty but reached only three
simultaneous extraction calls. The semantic queue started empty, parse supplied
one to four items at a time, and each stage waited for its whole claimed wave
before claiming again. The reported `throttled_requests` counter measured unused
stage slots rather than gateway rejection. PDF selection also called the page
artifact extractor once per field-family plan; the supposed reuse helper parsed
the complete PDF before discovering that the immutable output already existed.

The worker loop becomes work-conserving: it keeps up to the effective stage limit
in flight, claims replacement work as individual tasks finish, and waits for the
upstream completion event only when no claimable work exists. Stage active-work
time excludes idle waits for upstream, so publish does not exhaust its budget
while semantic is still producing rows. Stop requests cease new claims and drain
already admitted work. Provider failures still reduce semantic admission; queue
underfill is reported separately and is not called provider throttling.

The PDF asset path is resolved from the verified content hash, extractor version,
and parameter hash before extraction. A valid immutable artifact is hydrated and
reused; a corrupt or identity-mismatched artifact fails closed. Within one select
stage, all field-family plans for the same source document share the hydrated
artifact and outline. INFO logs report bounded timings and cache status, while
repetitive handled pypdf warnings are counted and emitted once per document.

Unit notifications remain append-only lifecycle events, but delivery groups
pending events by normalized unit and impact window. The message distinguishes
the event history from the current final state, identifies the active replacement
rule when one exists, and states stable quarantine reasons otherwise. A prior
quarantine followed by `auto_approved` and `superseded` therefore produces one
clear current-state message instead of apparently contradictory alerts.

The observed `万张`, `点`, and `万粒/万瓶` units are count classifiers or
same-scale count alternatives and become deterministically resolvable. The source
unit `万台（万千瓦时）` combines count and energy dimensions; it remains pending
and must be split into separately evidenced facts rather than assigned one
canonical multiplier.

### 10. Make governed unit aliases and proposal transport production-safe

The 2026-08-10 production trace for `603601.SH` contained an explicit operating
table whose unit was `PCS`. In that table `PCS` is the conventional plural of
piece, not a product acronym, and therefore resolves to the governed count
dimension with canonical unit `unit` and multiplier one. The same run observed
`立方` and `平方` as abbreviated table units for cubic metres and square metres.
These aliases belong in deterministic unit resolution; normalization is scoped to
the source-unit field and never rewrites product names, labels, or evidence text.

The optional unit-proposal request currently combines `Decimal` primitive
multipliers with dictionaries whose values can overwrite their JSON-safe string
forms. The request then fails locally during `json.dumps`, before shared-gateway
admission. Request construction will apply the definition first and overwrite its
multiplier last with canonical decimal text. This keeps the wire payload data-only
and JSON-safe without weakening the closed response schema or deterministic proof.

Proposal fallback logs will include the source unit, exception type, and a bounded
single-line error message at WARNING; DEBUG retains the traceback. Once the new
aliases are loaded, startup reconciliation supersedes matching quarantined rules,
copies their observations, and replays persisted conversion-pending semantic
artifacts. It does not redownload PDFs or call the extraction LLM again.

### 11. Close production replay gaps and increase bounded PDF throughput

The next shadow run showed that PDF extraction remains the dominant stage: four
parse workers stayed busy while twenty reports consumed roughly 2,674 cumulative
seconds. On the current 16-physical-core host the rollout therefore raises only
the parse-stage ceiling from four to eight. Parsing remains outside the database
write gate, claims remain bounded and work-conserving, and publication retains one
SQLite writer. A process pool or alternate PDF library is not introduced until a
representative annual-report fidelity benchmark proves equal page text, ordering,
outline, evidence hashes, and resource behavior.

Two source-unit cases receive deterministic treatment. `吨千米` and `亿吨千米`
belong to a governed freight-turnover dimension with canonical `tonne_km` and
exact multipliers one and one hundred million. Exact annual-report table headers
such as `元币种：人民币` are normalized to source currency unit `元`; the rewrite is
strictly bounded to known header forms and never changes arbitrary evidence text.
Catalog reconciliation supersedes matching quarantined rules and replays their
persisted semantic artifacts without extraction calls.

Rows derived by the deterministic parser never enter the semantic verifier. Local
proof decides whether they are eligible for canonical promotion; a non-promoted
parser manifest remains a locally confirmed shadow result instead of becoming a
gateway failure. During bundle persistence, rows sharing a primary key are
collapsed only when their complete prepared payloads are identical. Conflicting
payloads remain an explicit terminal correctness error. Historical terminal work
whose stored error is the former identical-duplicate failure is requeued at the
earliest reusable stage while preserving checkpoint and history.

Unit lifecycle notifications are rendered from the final current state after
replacement and supersession reconciliation. A verified effective rule starts
with `✅`; quarantined, pending, ineffective, or otherwise unsuccessful outcomes
start with `⚠️`. The icon is presentation only and does not alter lifecycle
authority.

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

1. Deploy code, catalog/schema versions, artifact manifest and unit-rule registry tables, and configuration
   with promotion still disabled.
2. Run an automatic migration that marks the known inconsistent shadow candidates
   and unit-blocked work as non-publishable/replayable; do not delete PDFs or prior
   audits.
3. Replay stored semantic artifacts under the new unit and numeric contracts. Only
   artifacts that fail hash/evidence scope validation are sent to the LLM again.
4. Run a bounded 20-company shadow batch with semantic concurrency 20 and DEBUG
   logging. Require zero unconditional reconciliation flags, zero repeated
   initialization calls, no terminal failures, and all unresolved units represented
   as pending artifacts. Require zero canonical publication decisions that reference
   an unproved or quarantined unit rule, and verify every unknown-unit proposal has
   a persistent lifecycle record and a deduplicated Telegram notification.
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
