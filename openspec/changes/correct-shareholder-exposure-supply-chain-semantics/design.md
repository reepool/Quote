## Context

Three existing production domains are involved, but they do not have the same owner:

1. The shareholder sync service owns remote routing, normalized shareholder snapshots, control-change history, readiness, and writes to the local research database.
2. The business-profile pipeline owns annual-report activities, supply-chain relationships, exposure facts, and governed publications.
3. The DCF input path is a local read consumer. It must not reinterpret diagnostic or industry-default context as approved company evidence.

The defects share one root cause: labels, evidence occurrences, or fallback records are being treated as authoritative business identities. The correction therefore establishes authority and temporal rules at each existing owner rather than adding another validation service. Company-profile reads do not need a second shareholder acquisition path because the project already persists shareholder snapshots and actual-controller change history locally.

The implementation must preserve the existing scheduler, API, Telegram, and `business_profile_backfill` entry points. Existing dirty data also needs a bounded local repair path; changing only future writes would leave current API and valuation results wrong.

## Goals / Non-Goals

**Goals:**

- Make shareholder completeness, merge, and readiness reflect fields actually present for the same instrument.
- Keep shareholder acquisition under the existing shareholder sync owner and make all profile-side shareholder consumption local-only.
- Preserve evidence-backed named relationships without pretending every disclosed name is a verified legal entity.
- Make relationship current/history resolution stable across annual-report evidence occurrences.
- Preserve distinct commodity exposure legs and keep incomplete publications visible without treating them as success.
- Ensure only current approved company mappings can automatically alter company DCF, with role-correct revenue and cost behavior.
- Audit and repair existing affected rows from local persisted evidence with an idempotent dry-run-first operator flow.

**Non-Goals:**

- Building a new shareholder scraper, calling CNInfo from the business-profile pipeline, or using an LLM to extract shareholder facts.
- Replacing the existing shareholder provider router or adding a global enterprise directory.
- Making industry defaults or every disclosed product automatically investable or price-linked.
- Redesigning the full DCF engine, review framework, repository layer, or scheduler architecture.
- Recalling the extraction LLM when a persisted semantic artifact or local shareholder source record is sufficient for repair.

## Business Acceptance And Authoritative Flows

The change is complete only when the following existing chains agree on the same business meaning:

```text
Configured shareholder providers
  -> shareholder sync owner (identity check, normalization, per-scope merge)
  -> local shareholder snapshot/control history
  -> shareholder and company-profile local query projection

Annual-report semantic artifact
  -> relationship/exposure production owners
  -> review and temporal/publication state
  -> approved profile and exposure queries
  -> approved-only DCF input assembly

Persisted local source/evidence records
  -> bounded repair application service
  -> the same merge/review/publication owners above
```

CLI, scheduler, Telegram, API, and the repair operator are adapters around these owners. They do not select sources, normalize dates, merge scopes, classify relationships, promote exposures, or build a second repair state machine. The production invariants are:

- one canonical instrument identity is checked before any shareholder response is accepted;
- one normalized report-date representation is used for comparison, hashes, persistence, and readiness;
- a selected shareholder value and its source/raw provenance change together;
- a disclosed counterparty name is not equivalent to a governed legal entity;
- occurrence evidence is not a stable cross-report relationship identity;
- diagnostic candidates never affect approved model inputs or their fingerprints;
- `degraded` means useful partial work with unresolved gaps, never unqualified success; and
- historical repair changes only locally reconstructable derived state and is dry-run by default.

## Decisions

### 1. Preserve the existing domain owners and use local shareholder reads

The shareholder sync application service remains the only owner of shareholder acquisition and snapshot writes. It can continue using its configured existing providers, including the existing official route, but this change adds no new remote call and no profile-specific fetch. A company-profile projection that needs top holders, actual controller, or control method reads the local shareholder snapshot/control-history repository at the requested knowledge cutoff.

This is preferred over adding shareholder fields to annual-report LLM extraction because the local domain already has structured data, source routing, and availability dates. It also prevents duplicated network behavior and inconsistent shareholder answers between APIs.

### 2. Evaluate and merge shareholder scopes independently

Coverage is derived from content:

- `holder_count` requires a numeric count and a usable report date.
- `top10_holders` requires a coherent disclosed holder set for one report period, unique ranks/names, and the exchange-specific completeness rule.
- `reference_only_ownership_clues` requires an actual local ownership/control field; a top-holder list alone never satisfies it.

Provider dates are normalized to one canonical date before comparison, hashing, persistence, or readiness. Each scope is selected independently. A later report period wins; for the same report period the configured authoritative source wins; otherwise the more complete internally consistent record wins. `scope_sources` and `scope_periods` record the selected source and period. Top-level source fields identify the primary selected scope or explicitly report a composite snapshot, and raw provenance remains attributable per selected scope; they must not continue to describe a displaced first writer. Readiness counts the intersection of instruments that satisfy every required scope, not independent aggregate scope counts.

The first top holder remains a top-holder fact only. It never populates actual-controller fields. Full, force-merge, incremental, and repair paths share the same canonical `instrument_id`/symbol/exchange acceptance rule. Ambiguous BSE aliases, response-symbol mismatches, mixed report dates, and one-instrument provider exceptions fail closed for the affected instrument without aborting unrelated instruments. Candidate limits preserve missing-scope priority and newest-first ordering. Scheduler and Telegram adapters preserve `degraded` as a warning result rather than returning success.

### 3. Separate relationship occurrence, lineage, and entity status

Relationship records use two explicit identities:

- stable business lineage: issuer, relationship direction/type, governed entity ID or normalized disclosed name, and business scope/object; it excludes evidence ID, report period, and source-row occurrence;
- evidence occurrence: stable lineage plus report period, evidence ID, and an evidence-independent contract/source-row discriminator used only to keep distinct disclosures within one report cohort.

Current-state selection first chooses the newest eligible report cohort for each stable lineage, subject to explicit half-open validity and the configured relationship freshness window, then retains every distinct occurrence inside that cohort. This makes the same customer in consecutive annual reports one current lineage while preserving two contracts disclosed in the same report. A source-row key that embeds `evidence_id` cannot be used as cross-report lineage. Historical occurrences remain queryable; physical supersession fields may support audit, but they do not replace correct as-of selection.

Counterparty identity is resolved only from local governed identity data at the knowledge cutoff. `company_profiles.company_name` or an equivalent governed full-name field supplies legal names. `instruments.name`, ticker labels, and company-profile short names are securities display names and never become `legal_name` merely because they are unique. Explicitly reviewed aliases remain allowed. Counterparty status has two valid business meanings:

- `resolved_entity`: exact governed legal name, official identifier, or explicitly approved alias resolves to a local entity ID.
- `disclosed_name_only`: the annual report clearly names an external counterparty but no governed local entity is available. The relationship may be approved as a disclosure fact without claiming legal-entity resolution or enabling cross-company joins.

Unique ticker short names are not approved aliases. Human review of an unresolved name must explicitly choose `disclosed_name_only` or bind a governed entity; a generic approve action cannot silently claim entity resolution. This avoids requiring a world-wide company catalog while keeping evidence-backed suppliers and customers.

Anonymous concentration facts are selected by deterministic aggregate labels and direction together. Labels such as top-five customers/suppliers may force anonymous concentration even when the LLM omits the flag. Generic labels such as `关联方` do not by themselves establish anonymity or customer/supplier direction. Program code normalizes raw percentages/fractions, validates a finite `[0, 1]` value, and rejects contradictory or unit-ambiguous inputs. The LLM does not perform the percentage calculation.

### 4. Give every exposure leg a non-colliding identity

Exposure lineage includes the source fact/action class, role, scope, commodity mapping, and assumption lineage. Purchases and consumption may both exist for the same commodity without superseding one another. Consumer-specific publications include `consumer_id` in their identity, and synonymous assumption names are canonicalized before lookup so two aliases cannot overwrite one field.

Deterministic product/catalog rules choose `feedstock_cost` versus `energy_cost`; the LLM supplies the disclosed activity/object, not the financial direction calculation. Hedge activities remain approved facts but are `fact_only` unless a deterministic supported hedge publication rule exists. Unknown roles also remain facts and fail closed for publication.

A `fact_only` result is terminal for the current run but remains an explicit non-retryable publication gap. It does not clear the gap, consume repeated LLM retries, or count as a published exposure.

### 5. Separate research defaults from executable DCF mappings

The resolver returns two collections with different contracts:

- `industry_default_profile.exposure_mappings`: contextual research defaults, never labeled as company-approved.
- `executable_exposure_mappings`: current approved company publications only.

The automatic DCF path explicitly requests `include_candidates=False` and accepts only mappings whose source is `approved_company_business_profile`, whose role is known, and whose market series is active at the valuation cutoff. It never selects the first diagnostics entry by iteration order. Revenue and cost legs use explicit opposite cycle direction; a mixed or ambiguous set without a governed spread/selection rule produces no automatic cycle adjustment. Candidates and industry defaults can remain in separately requested diagnostic output but cannot alter assumptions, formulas, `profile_version`, executable input hashes, or DCF lineage. Missing roles fail closed at every executable consumer rather than defaulting to revenue.

Legacy exposure componentization is a migration input to this same publication owner. It may construct locally proven components, but it cannot call a generic promotion method to bypass the current publication classifier and promotion gates.

### 6. Repair derived data from local persisted sources

One bounded repair application service performs `audit` and `apply` modes; `audit` is the default. A thin operator command calls that service. The service delegates shareholder recomputation, relationship state transitions, and exposure replay to the same domain owners as future writes and does not implement a second merge, review, or publication loop.

The audit identifies at least:

- inferred controller fields unsupported by local control history;
- scope labels unsupported by actual snapshot fields;
- older or lower-authority scope values incorrectly retained;
- approved short-name resolutions and relationship lineages left concurrently current;
- purchase/consumption or consumer publications linked by the old collision key;
- fact-only gaps incorrectly closed; and
- DCF contexts labeled governed using industry-only mappings.

Apply mode runs transactionally per instrument, records before/after counts and stable IDs, and is idempotent. It recomputes from local raw payloads, approved evidence, semantic artifacts, control history, facts, and catalogs. Unsupported inferred fields are cleared; historical relationships are superseded rather than erased; derived exposure publications are replayed from approved facts. Rows that cannot be reconstructed locally are marked incomplete/held and reported for normal owner-managed follow-up, not silently guessed or remotely reacquired by the repair command.

Source documents, evidence, valid historical occurrences, and review decisions are immutable audit inputs and are never deleted by repair. Incorrect derived rows normally move through existing held/rejected/superseded states. Physical deletion is limited to exact machine-derived duplicates that were never independently valid, have no inbound lineage/review references, and are listed with a reason in both audit and apply reports.

The repair report distinguishes `would_change`, `changed`, `unchanged`, `held`, and `failed`, includes reasons and affected stable IDs, and proves that audit performed zero writes. Apply requires an explicit instrument set or an explicit all-scope confirmation, runs on a backed-up/copied database before production cohorts, and never treats partial repair as full success.

## Risks / Trade-offs

- [Corrected readiness may fall sharply] -> Report old and corrected coverage side by side during audit and do not call the drop a sync failure.
- [Removing inferred controllers creates temporary nulls] -> Prefer an honest missing value and local control-history recovery over a false controller; normal shareholder sync remains the owner of later replenishment.
- [Relationship lineage can collapse genuinely distinct relationships] -> Keep contract reference and same-period occurrence identity, and test repeated counterparties with multiple contracts.
- [Approved short-name relationships may be demoted] -> Audit by resolution basis and retain raw evidence; allow explicit `disclosed_name_only` approval without fabricating an entity.
- [DCF results change after industry fallback is removed] -> Emit a valuation-lineage reason showing that no approved company mapping was available and compare representative valuations before rollout.
- [Repair touches persisted approved data] -> Default to dry-run, require explicit instrument/all scope and apply flag, transact per instrument, and emit an idempotent repair report suitable for rollback from existing records/backups.
- [Current uncommitted shareholder work may overlap implementation] -> Rebase the implementation plan on the then-current worktree and do not overwrite unrelated edits.

## Migration Plan

1. Implement and test future-write shareholder field/merge/readiness rules without changing production data.
2. Implement relationship and exposure identity/publication corrections, including dual reading of existing records where necessary.
3. Enforce DCF executable-source and role-direction rules; validate representative revenue, cost, mixed, and industry-only cases.
4. Run the repair audit on a temporary database copy and review counts/samples for all affected categories.
5. Deploy code, run bounded apply by instrument cohort, and verify shareholder APIs, business-profile APIs, exposure APIs, and DCF lineage after each cohort.
6. Remove compatibility handling only after no old collision/current-state rows remain and targeted backfills succeed.

Rollback consists of disabling repair apply, reverting the code release, and restoring affected local database files from the normal backup made before migration. The audit report contains stable IDs needed to verify restoration; no remote source mutation occurs.

## Open Questions

None block implementation. The implementation must use the current configured source-authority ordering from the shareholder sync owner rather than introduce a second hard-coded provider ranking.
