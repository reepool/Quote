## Why

Compatibility methods, root-level live probes, one-time migrations, old backup entry points, and dev-validation scripts remain after their original changes complete. Without an explicit retirement pass, the repository continues to advertise multiple apparent solutions and future work can accidentally revive obsolete paths.

## What Changes

- Inventory compatibility callers for DataManager, ResearchStorageManager, ScheduledTasks, scripts, and old operator commands.
- Classify every candidate as production, operator, migration, compatibility, or obsolete.
- Migrate remaining callers and delete zero-caller compatibility paths rather than keeping permanent aliases.
- Use a deprecation period and replacement map for compatibility aliases that may be consumed outside the repository before physical deletion.
- Remove root-level probe files, completed migrations, obsolete dev-validation tools, and deprecated backup/maintenance entry points after proving replacement coverage.
- Require retained operator tools to call authoritative application services and have current runbooks.
- Archive completed OpenSpec changes and close the framework program only after all prior workstreams pass.

## Capabilities

### New Capabilities

- `obsolete-path-retirement`: Defines evidence, classification, caller migration, deletion, operator-tool retention, and final residue acceptance.

### Modified Capabilities

None.

## Impact

- Affects compatibility facades, `scripts/`, root probe files, old operational helpers, docs, tests, and OpenSpec lifecycle.
- Depends on W1 through W7 and W9 and is intentionally the final implementation workstream.
- Implements W8, FR-02, FR-12, and FR-13 without deleting production or recovery capabilities.
