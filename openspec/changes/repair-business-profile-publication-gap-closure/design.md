## Context

The authoritative path is the existing `business_profile_backfill` application flow. It reads shared annual-report assets, persists semantic records, verifies candidates, promotes governed records, derives value-chain roles and exposure facts, and publishes commodity identities through one database writer.

The 601088.SH acceptance run completed every stage but exposed related closure defects: derived candidates inherited historical evidence catalog versions; automated contract recovery was mistaken for human review; an older less-normalized activity remained effective beside its replacement; valid fact-only disclosures were treated as publication failures; and runtime exceptions were keyed too narrowly to close after a gate or canonical identity changed.

## Goals / Non-Goals

**Goals:**

- Make one targeted rerun converge without false or duplicate publication gaps.
- Preserve approved historical evidence while binding each newly derived record to current runtime catalogs.
- Prefer the most normalized duplicate semantic activity from the same filing evidence.
- Publish known commodity identities even when an executable market series is unavailable.
- Preserve valid coarse or composite facts without treating them as failed commodity publications.
- Automatically close machine exceptions when the exact target, evidence-backed relationship, or replacement fact has succeeded.
- Keep gap counts aligned with unresolved actionable results.

**Non-Goals:**

- No database migration, new queue, new LLM call, new catalog-management framework, or new manual-review workflow.
- No rewriting of approved evidence metadata or production history.
- No forced mapping from a broad process label to a specific tradable commodity or price series.
- No changes to the Telegram command, scheduler contract, annual-report asset manager, or LLM prompt contract.

## Decisions

### Derived records use current catalogs; evidence remains immutable

`_bind_promotion_validation` will copy evidence-origin quality gates but replace `catalog_versions` with `_current_catalog_versions()` for the newly computed record. The promotion gate will continue to compare the record's bound versions with the current runtime versions.

Alternative considered: updating approved evidence to the current product catalog. Rejected because the evidence is an immutable historical extraction artifact and its original processing identity must remain auditable.

### Automated reviewers are machine identities

Review history whose reviewer starts with `system:` or `automation:` will not count as a human decision. Any other reviewer remains a hard block on automatic reopen or promotion.

Alternative considered: allowlisting one contract-recovery reviewer string. Rejected because all repository-owned `automation:` identities have the same non-human semantics and version suffixes can change.

### Exact semantic duplicates prefer the more normalized activity

Before derivation, approved activities from the same instrument, report period, evidence, subject scope, action, object type, raw object, segment, geography, value, unit, share, and business regime will be collapsed deterministically. A candidate with a canonical `object_id` outranks an otherwise identical unmapped candidate; remaining ties use knowledge/version/update order. Activities for different issuers or subsidiaries, geographies, object types, or business regimes remain distinct. This selection affects derivation and publication, not immutable activity history.

Alternative considered: changing global temporal stable-identity fields or mutating old approved activities. Rejected because that broadens the change and can alter historical query semantics outside this production path.

### Fact-only outcomes are successful preservation, not publication gaps

`produces` receives the deterministic positive output/revenue direction used by known product production. If an approved exposure fact has no canonical product identity, the publisher returns a `fact_only` result with the reason retained instead of raising a machine-rework exception. This covers composite processes such as `煤制烯烃` and broad disclosures that cannot safely select a commodity. A known `product_id` with no valid catalog mapping remains an actionable catalog error.

Alternative considered: mapping every raw label to a generic or tradable commodity. Rejected because it fabricates specificity not present in the filing.

### Commodity identity is independent of price-series execution

A unique product-to-commodity mapping publishes the commodity identity. Candidate-only mappings or mappings with multiple market references publish with `price_series_id=NULL`; they do not block the company commodity exposure. This is existing intended behavior and receives regression coverage for polyethylene and polypropylene.

### Exception closure follows current business outcomes

Before inserting a new gate-signature exception, older open exceptions for the same target type and target are resolved. Successful or fact-only publication resolves open publication exceptions for the exact fact. A newly promoted named relationship resolves a prior `catalog_proposal` only for its persisted semantic assertion ID; for legacy records, the runtime reconstructs the finite set of exact IDs allowed by the old relationship contract and still closes only an exact target match. Another relationship sharing the same evidence remains open. Inactive duplicate facts have their exact publication exceptions resolved.

This retains audit history while preventing stale open backlogs. It avoids broad evidence- or document-level closure when several relationships share one evidence item.

### Gap reporting uses one actionable definition

Any non-promoted current candidate in machine, quick, or deep review counts once by target ID. Published, unchanged, and fact-only outcomes do not count. Runtime input gaps count once by fact ID. Resolved or inactive historical exceptions do not affect the current batch count.

## Risks / Trade-offs

- [Duplicate selection could collapse two genuinely distinct identical disclosures in one evidence span] -> The key includes subject scope, action, object type, segment, geography, value, unit, share, business regime, report period, and evidence ID; only exact semantic duplicates are collapsed.
- [Fact-only outcomes can hide missing catalog coverage] -> The result preserves an explicit reason in the stage artifact and logs, while only known product IDs with broken mappings remain actionable failures.
- [Several relationships can share one evidence span] -> Each promoted relationship carries its source semantic assertion ID and closes only that exact proposal target.
- [Existing stale exceptions remain until a rerun] -> The targeted rerun is the migration mechanism; no direct production-data rewrite is required.

## Migration Plan

1. Deploy code and tests without schema or configuration migration.
2. Rerun the existing targeted 601088.SH command with `force=true`.
3. Confirm all stages complete, current queue terminal count is zero, valid exposure facts are approved, known commodity identities are published, and actionable `publication_gaps` is zero.
4. Confirm stale duplicate/catalog proposal exceptions are resolved and no extraction LLM replay occurs when durable artifacts remain valid.
5. Roll back by reverting the code commit; stored evidence and approved history remain compatible.

## Open Questions

None for this change.
