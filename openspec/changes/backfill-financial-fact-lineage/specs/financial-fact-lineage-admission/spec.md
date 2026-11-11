## ADDED Requirements

### Requirement: Existing facts admitted by the current catalog must carry current-version lineage
Rows in the financial fact tables whose canonical source field is approved in the current mapping catalog SHALL carry a local_core_mapping lineage stamp at the current mapping version - whether the row was left unstamped by a legacy import or stamped under a superseded version.

#### Scenario: Superseded-version stamp is refreshed
- **WHEN** a row carries local_core_mapping whose mapping_version is superseded and the current approved catalog still maps its source field and profile
- **THEN** the backfill SHALL refresh the stamp to the lineage the import path would produce today
- **AND** the row's row_hash SHALL be recomputed with the import path's hash function

#### Scenario: Legacy unstamped row matches the current catalog
- **WHEN** a numeric fact row lacks local_core_mapping in raw_fact and the current approved catalog maps its source field and profile
- **THEN** the backfill SHALL stamp that row with the same lineage the import path would produce
- **AND** the row's row_hash SHALL be recomputed with the import path's hash function

#### Scenario: Row with no catalog match stays untouched
- **WHEN** a row's source field has no approved mapping in the current catalog (unstamped, or stamped under a version whose field the current catalog withdrew)
- **THEN** the backfill SHALL leave the row unchanged and count it as unmapped or stale-unmapped

#### Scenario: Current-version stamps are never rewritten
- **WHEN** a row already carries local_core_mapping at the current mapping version
- **THEN** the backfill SHALL not modify it in any way

### Requirement: Backfill must preserve storage semantics
The backfill SHALL preserve every column except raw_fact_json and row_hash, batch updates in per-report-period transactions within a verified no-writer window, and keep a restorable backup of affected rows.

#### Scenario: Recency ordering is unchanged
- **WHEN** the backfill rewrites a row's raw_fact_json
- **THEN** updated_at and all other columns SHALL keep their original values

#### Scenario: Rollback restores affected rows
- **WHEN** a backfill batch must be rolled back
- **THEN** affected rows SHALL be fully restored from the backup table taken before the batch

### Requirement: Phantom maintenance gaps must be eliminated after backfill
After a completed backfill and one reconciliation window, instruments whose facts exist and are catalog-admitted SHALL evaluate ready, and the repair loop SHALL no longer re-target them.

#### Scenario: Probe instrument becomes ready
- **WHEN** readiness evaluates 000001.SZ for report period 2024-03-31 after the backfill
- **THEN** the evaluation SHALL be ready with no missing required core facts

#### Scenario: Repair targets collapse to real gaps
- **WHEN** the next full reconciliation runs after the backfill
- **THEN** per-exchange per-period fetch targets SHALL drop from thousands to the real-gap scale (~150 per period)
- **AND** no fast-cycling unchanged-skip batches shall dominate the run
