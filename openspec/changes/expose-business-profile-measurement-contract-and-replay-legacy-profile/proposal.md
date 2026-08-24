## Why

The public company business-profile API already returns activities and operating facts, but its OpenAPI contract does not explain that operating facts are the authoritative measurement set while activity values are compatibility projections. Existing approved `601088.SH` rows also predate the linked-measurement metadata, so the API contract and representative production data need to be brought into alignment without breaking current clients.

## What Changes

- Add an explicit, additive measurement contract to the company business-profile response.
- Type the company-specific profile fields so OpenAPI exposes activities, operating facts, value-chain roles, named supply-chain relationships, and commodity exposures directly.
- Report whether operating facts are linked to activities through `metadata.source_activity_id`, including compatibility status for historical unlinked rows.
- Replay `601088.SH` through the existing authoritative backfill service so its approved profile conforms to the new activity/measurement representation.
- Document the existing annual update trigger and lifecycle for annual reports and corrected annual reports.

## Capabilities

### New Capabilities

- `business-profile-api-measurement-contract`: Defines the additive API contract for authoritative operating measurements, activity compatibility projections, linkage status, and legacy compatibility.

### Modified Capabilities

None.

## Impact

- Affects the business-profile API response models and resolver projection, plus their focused tests.
- Uses the existing business-profile backfill command and single writer for production replay; no database schema or parallel write path is introduced.
- Preserves `company_business_profile.v2`, existing response fields, existing endpoint paths, and reads of historical unlinked records.
