## Why

The atomic business-profile pipeline currently treats each LLM measurement assertion as an independent activity during semantic extraction, but later regenerates the persistence identity without the measurement. Two correct measurements for the same activity can therefore collide and terminate publication, as observed for `300708.SZ`, while discarding either measurement would make the company profile incomplete.

## What Changes

- Group semantically identical activity assertions independently of their numeric measurements.
- Persist every disclosed measurement as a distinct operating fact linked to the grouped activity.
- Classify sales revenue, sales volume, production volume, inventory volume, purchase amount, purchase volume, and otherwise unclassified measurements without dropping the LLM result.
- Keep one physical measurement on the activity as an explicitly marked legacy compatibility projection so existing exposure derivation and already-published companies remain compatible.
- Permit terminal semantic work to replay from its persisted semantic artifact after this deterministic transformation changes, without another LLM call.
- Add regression coverage for the `300708.SZ` collision and for preservation of the already-published `601088.SH` profile, value-chain roles, and commodity exposures.

## Capabilities

### New Capabilities

- `business-profile-activity-measurement-separation`: Defines grouping of semantic activity assertions, persistence of their measurements as operating facts, deterministic replay, and compatibility behavior for existing profiles.

### Modified Capabilities

None.

## Impact

- Affects atomic semantic bundle transformation, activity candidate identity and metadata, operating fact classification/identity, and terminal replay eligibility.
- Extends the existing business-profile fact catalog with the minimum additional sales and purchase measurement types.
- Does not change public API schemas or database table schemas.
- Existing approved data remains readable; compatibility projections prevent current activity-derived exposure behavior from regressing while operating facts become the authoritative measurement records.
