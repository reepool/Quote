## Why

The local shareholder snapshot, supply-chain relationship, and commodity-exposure paths currently contain semantic identity and authority errors that can make incomplete data appear ready, keep stale relationships current, discard valid exposure legs, or let industry defaults alter company DCF results as if they were approved company evidence. These are production-result defects and require one coordinated correction of the owning domain rules, existing persisted data, and downstream reads.

## What Changes

- Make shareholder coverage depend on actual fields, not claimed scope labels; stop inferring actual control from the first top holder; and merge each shareholder scope by report period and source authority.
- Keep shareholder acquisition and writes in the existing shareholder sync owner. Business-profile and API reads use the project's local shareholder snapshot and control-history data only and never trigger CNInfo, another remote provider, or LLM extraction.
- Correct relationship identity, entity approval, concentration direction, disclosed-share validation, and cross-year validity so only the latest supported relationship is current while historical evidence remains queryable.
- Correct commodity exposure identity so purchases, consumption, revenue, energy cost, hedge facts, and consumer-specific assumptions cannot overwrite one another. Fact-only outcomes remain explicit unresolved publication gaps rather than false success.
- Separate industry-default commodity context from approved company mappings. DCF execution accepts only current approved company mappings as governed company evidence, applies revenue and cost cycles with the correct direction, and fails closed for unknown roles.
- **BREAKING (valuation semantics)**: industry-default mappings no longer change a company's DCF automatically or appear as governed company evidence; they remain visible as non-executable research context unless a separate explicit valuation policy opts into them.
- Add a bounded audit and repair command that identifies rows produced by the incorrect rules, recomputes derived snapshots/relationships/exposures from existing local source records, and reports proposed deletion, supersession, or replay before any write. No remote reacquisition or LLM call is required for repairable records.
- Preserve existing public API shapes where possible; add explicit readiness, source-authority, current/history, and publication-gap diagnostics needed to explain corrected results.

## Capabilities

### New Capabilities

- `shareholder-snapshot-integrity`: Field-backed shareholder coverage, authoritative per-scope merging, local-only business-profile consumption, and controlled repair of incorrect snapshots.
- `supply-chain-relationship-integrity`: Exact entity governance, concentration semantics, disclosed-share validation, and report-aware current/history relationship resolution.
- `commodity-exposure-dcf-integrity`: Non-colliding exposure identities, explicit fact-only publication state, and fail-closed DCF consumption of approved company mappings.

### Modified Capabilities

None. The repository has no current top-level OpenSpec capability covering these combined production contracts.

## Impact

- Affected owners: shareholder sync/snapshot policy, business-profile activity and temporal production, counterparty resolution/review, commodity exposure publication, business-profile resolver, and DCF input assembly.
- Affected local stores: existing shareholder snapshots/control history and business-profile relationship/exposure records; schema changes are allowed only when required to represent authority, temporal identity, or repair state.
- API compatibility: approved profile, shareholder, exposure, and DCF endpoints retain their routes. Candidate/history diagnostics remain explicitly requested and do not become valuation inputs.
- Operational compatibility: existing scheduler and `/run business_profile_backfill` entry points continue to call their current application services. Repair is a separate bounded operator mode with dry-run default and no implicit remote access.
- No new external dependency, provider, LLM route, generalized validation framework, or parallel write path is introduced.
