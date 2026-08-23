## 1. Read-model status calculation

- [x] 1.1 Add a pure helper in the business-profile governance layer that derives profile status from eligible approved company facts and derives market-link status from approved exposures and executable mappings.
- [x] 1.2 Update `BusinessProfileResolver.resolve` to preserve approved facts/exposures and populate `market_link_status` plus `readiness.market_link` without using market mappings as the profile readiness gate.
- [x] 1.3 Preserve existing mapping-gap warnings and add unresolved exposure identifiers and explicit profile input gaps to the response.

## 2. API contract and compatibility

- [x] 2.1 Update the business-profile response model and commodity-exposure projection to accept and expose the new status fields without changing existing tables or approved-data fields.
- [x] 2.2 Confirm history and review-queue endpoints remain unchanged and continue returning approved relationships, value-chain roles, and exposure facts.
- [x] 2.3 Update current API documentation or endpoint schema descriptions to explain that market links are optional enrichment, not profile readiness.

## 3. Verification

- [x] 3.1 Add focused tests for ready profile/unlinked market exposure, partial market links, fully linked exposures, no exposures, and no approved facts.
- [x] 3.2 Run the focused business-profile governance/API tests and a read-only `601088.SH` query against the configured research database.
- [x] 3.3 Review the diff for accidental changes to LLM extraction, annual-report assets, exposure facts, or database schemas.
