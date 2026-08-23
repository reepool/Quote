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
- Make independent verification evaluate the filing assertion rather than opaque storage identifiers, and make promotion consume only a self-consistent proof.

**Non-Goals:**

- No database migration, new queue, new LLM request type, new catalog-management framework, or new manual-review workflow. Existing semantic-synthesis rows may now make the already-governed independent-verifier call that they previously skipped incorrectly.
- No rewriting of approved evidence metadata or production history.
- No forced mapping from a broad process label to a specific tradable commodity or price series.
- No changes to the Telegram command, scheduler contract, annual-report asset manager, extraction prompt, or output fields requested from the extraction LLM.

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

### Forced contract recovery invalidates completed output

The contract audit reopens only records rejected by its own versioned automation identity and leaves operator decisions unchanged. It requeues affected work as `semantic/retry_due`. A targeted `force=true` enqueue treats that state as replayable, rotates to a new checkpoint, clears persisted stage results, and restarts at `acquire`; active `running` leases and ordinary `pending` work remain untouched. The previous checkpoint and a bounded recovery-history entry are retained for audit.

Alternative considered: resume the recovered item from its semantic stage. Rejected because the completed checkpoint can contain zero-candidate semantic, verification, and publication output produced before the record was reopened.

### Verification uses business labels and a program-enforced proof contract

Independent verification receives the original Chinese anonymous scope label and business object retained in candidate metadata. Stable hashes remain record identities only and are not presented as semantic claims. A verifier response is accepted only when `confirmed` has all six checks true, while a non-confirmed decision has at least one failed check. Promotion independently applies the same rule; deterministic proofs require `canonical_promotion_allowed=true`.

If a legacy anonymous concentration lacks its readable label, verification omits the opaque scope identity and fails closed instead of asking the LLM to interpret a hash. Deterministic proofs carry an explicit proof version and are recomputed from the current parser, unit, evidence, and manifest state whenever verify resumes; only semantic LLM results with the current verifier identity are reused.

`semantic_synthesis` rows remain semantic conclusions even when their numbers and evidence references pass deterministic checks. They therefore use the independent verifier. Only records produced by a promoted deterministic parser may bypass the LLM verifier, and a locally blocked proof is represented as `held` rather than a contradictory confirmed decision.

Promotion never infers proof from the document-level `semantic` marker. Every business record must carry either a current independent-verifier result or a current versioned deterministic proof. When a deterministic proof is recomputed on resume, it replaces stale verify-stage machine rework for that exact target so a recovered local result cannot remain blocked by obsolete retry state.

The verifier identity advances to v6 because the input and acceptance contract changed. This intentionally prevents old verifier artifacts and queued processing identities from being treated as equivalent to the repaired contract. Shared annual-report and PDF artifacts remain reusable, so identity rotation does not require duplicate source downloads.

## Risks / Trade-offs

- [Duplicate selection could collapse two genuinely distinct identical disclosures in one evidence span] -> The key includes subject scope, action, object type, segment, geography, value, unit, share, business regime, report period, and evidence ID; only exact semantic duplicates are collapsed.
- [Fact-only outcomes can hide missing catalog coverage] -> The result preserves an explicit reason in the stage artifact and logs, while only known product IDs with broken mappings remain actionable failures.
- [Several relationships can share one evidence span] -> Each promoted relationship carries its source semantic assertion ID and closes only that exact proposal target.
- [Existing stale exceptions remain until a rerun] -> The targeted rerun is the migration mechanism; no direct production-data rewrite is required.
- [Verifier identity rotation supersedes old queued work identities] -> Correctness requires the changed proof contract to be distinguishable; source PDF/page assets remain content-addressed and reusable, and superseded work remains auditable.
- [Recomputing deterministic proofs increases resume work] -> The calculation is local and does not invoke the LLM; it prevents stale held or allowed decisions after local rules change.
- [Semantic-synthesis rows require an additional verifier request] -> The call uses the existing bounded verifier pool and resumable checkpoint; only truly deterministic parser results retain the zero-LLM verification path.
- [Strict per-record proof can expose previously hidden missing-verification states] -> This is intentional fail-closed behavior; the resumable verify stage must produce the missing proof rather than promotion guessing from document metadata.

## Migration Plan

1. Deploy code, verifier v6 rollout identities, and tests without a database schema migration.
2. Rerun the existing targeted 601088.SH command with `force=true`.
3. Confirm all stages complete, current queue terminal count is zero, valid exposure facts are approved, known commodity identities are published, and actionable `publication_gaps` is zero.
4. Confirm stale duplicate/catalog proposal exceptions are resolved and no extraction LLM replay occurs when durable artifacts remain valid.
5. Roll back by reverting the code commit; stored evidence and approved history remain compatible.

## Open Questions

None for this change.
