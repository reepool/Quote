## Why

The local shareholder snapshot, supply-chain relationship, and commodity-exposure paths currently contain semantic identity and authority errors that can make incomplete data appear ready, keep stale relationships current, discard valid exposure legs, or let industry defaults alter company DCF results as if they were approved company evidence. These are production-result defects and require one coordinated correction of the owning domain rules, existing persisted data, and downstream reads.

## What Changes

- Make shareholder coverage depend on actual fields, not claimed scope labels; stop inferring actual control from the first top holder; and merge each shareholder scope by report period and source authority.
- Normalize shareholder report dates before comparison and hashing, validate returned security identity in every full/incremental path, isolate one-instrument provider failures, and keep top-level plus per-scope provenance consistent with the selected data.
- Keep shareholder acquisition and writes in the existing shareholder sync owner. Business-profile and API reads use the project's local shareholder snapshot and control-history data only and never trigger CNInfo, another remote provider, or LLM extraction.
- Correct relationship identity, entity approval, concentration direction, disclosed-share validation, and cross-year validity. Governed legal names come from the project's local full-name data, never securities short names; current selection keeps all distinct occurrences in the latest eligible report cohort while preserving older evidence as history.
- Correct commodity exposure identity so purchases, consumption, revenue, energy cost, hedge facts, and consumer-specific assumptions cannot overwrite one another. Fact-only outcomes remain explicit unresolved publication gaps rather than false success.
- Separate industry-default commodity context from approved company mappings. DCF execution requests an approved-only profile, accepts only current approved company mappings as governed company evidence, excludes candidates from model inputs and executable fingerprints, applies revenue and cost cycles with the correct direction, and fails closed for unknown roles.
- **BREAKING (valuation semantics)**: industry-default mappings no longer change a company's DCF automatically or appear as governed company evidence; they remain visible as non-executable research context unless a separate explicit valuation policy opts into them.
- Add one bounded audit and repair application command that identifies rows produced by the incorrect rules, recomputes derived snapshots/relationships/exposures through the corrected domain owners, and reports proposed clearing, state transition, replay, or deletion of proven unreferenced derived duplicates before any write. Source evidence and valid history are never deleted. The operator adapter contains no second business loop; no remote reacquisition or LLM call is required for repairable records.
- Preserve existing public API shapes where possible; add explicit readiness, source-authority, current/history, and publication-gap diagnostics needed to explain corrected results.

## Capabilities

### New Capabilities

- `shareholder-snapshot-integrity`: Field-backed shareholder coverage, authoritative per-scope merging, local-only business-profile consumption, and controlled repair of incorrect snapshots.
- `supply-chain-relationship-integrity`: Exact entity governance, concentration semantics, disclosed-share validation, and report-aware current/history relationship resolution.
- `commodity-exposure-dcf-integrity`: Non-colliding exposure identities, explicit fact-only publication state, and fail-closed DCF consumption of approved company mappings.

### Modified Capabilities

None. The repository has no current top-level OpenSpec capability covering these combined production contracts.

## Impact

- Affected owners: shareholder sync/snapshot policy, local shareholder query projection, business-profile activity and temporal production, counterparty resolution/review, commodity exposure publication, business-profile resolver, DCF input assembly, and one bounded repair application service.
- Affected local stores: existing shareholder snapshots/control history and business-profile relationship/exposure records; schema changes are allowed only when required to represent authority, temporal identity, or repair state.
- API compatibility: approved profile, shareholder, exposure, and DCF endpoints retain their routes. Candidate/history diagnostics remain explicitly requested and do not become valuation inputs.
- Operational compatibility: existing scheduler and `/run business_profile_backfill` entry points continue to call their current application services. Repair is a separate bounded operator mode with dry-run default and no implicit remote access.
- No new external dependency, provider, LLM route, enterprise directory, generalized validation framework, or parallel write path is introduced.
