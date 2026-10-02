# deliver-company-profile-m4-v3-next-small-batch Specification

## Purpose

The v3 next batch is service company `600008.SH` and manufacturing company `600019.SH`, cutoff `2026-09-17`, with one shared 50000-token budget. The measured run took 19.38 seconds and used 0 tokens. The independent reading is recall 6/11 and accuracy 10/15, with critical numeric errors 0. Recall misses are both principal sentences, scrap procurement, and energy-medium sales and procurement. Five delivered steel-sales associations are inaccurate because they come from sales mode, sales region, a sales department, or an investee business. `expansion_gates_met` is false. `production_authorization` stays `not_authorized` and `scale_quality_claim_allowed` stays false. The older 5/9, 18/18, 19/21, and v3 18/18 reviews stay unchanged, and the earlier v3 elapsed value stays unassessed.

## Requirements

### Requirement: The next batch uses the v3 identity and its own directory
The next batch MUST use `revenue_sentence_repair=v3` on top of the default processing identity, a caller-selected snapshot directory, exactly two companies, and one shared 50000-token budget. It MUST NOT change `default_processing_identity()`. It MUST NOT write its plan, live-run, or source review into `reports/m4_next_small_batch/`. Elapsed time MUST be recorded only as a measured duration of the controlled run. An unassessed elapsed value MUST NOT be replaced with an estimate, including the existing v3 review.

#### Scenario: The batch contract is fixed before selection
- **WHEN** the next batch is specified
- **THEN** its identity is v3, its directory is caller-selected, and its budget is 50000 tokens for two companies
- **AND** the existing v3 review keeps `elapsed_seconds` unassessed

### Requirement: Freezing a plan writes and reads the same directory
`freeze_m4_next_batch_plan` MUST pass the caller-selected directory to both the save and the following read. A repeat read MUST return that same plan. The archived unique plan and the existing repair snapshots MUST keep their bytes.

#### Scenario: A new directory freeze does not replace the old plan
- **WHEN** an old plan already exists under `reports/m4_next_small_batch/` and freeze is invoked with another directory
- **THEN** the new plan is stored only in that directory
- **AND** a second read returns the same plan
- **AND** the old plan bytes stay unchanged

### Requirement: Two new reports are delivered through the existing owner
After the directory fix, the batch MUST select one service company and one manufacturing company that have not been delivered under v3. Selection MUST follow SSE, then SZSE, then BSE, and then instrument id within each disclosure form. The frozen report identity MUST then pass through acceptance, persistence, query, and export. Each core dimension MUST be answered or carry an explicit gap. An explicit commodity role MUST be queryable with its source, and an unmapped source name MUST stay pending.

#### Scenario: The fixed name list is observed on new reports
- **WHEN** the two new reports are queried and exported
- **THEN** delivered commodity roles show their source names and evidence
- **AND** a name outside the current repair list is not invented to fill coverage

### Requirement: The review uses the source checklist and measured cost
Recall MUST be computed from disclosures read in the source, with one item per company, direction, and source name. A restatement on another page MUST be scored for accuracy and MUST NOT add a second recall item. Accuracy MUST cover every delivered association. The review MUST record the actual tokens and the measured elapsed seconds before computing the expansion gate. A miss MUST stay a miss. The archived 5/9, 18/18, 19/21, and v3 18/18 observations MUST remain unchanged. `production_authorization` MUST remain `not_authorized` and `scale_quality_claim_allowed` MUST remain false.

#### Scenario: The gate is computed after the measurement
- **WHEN** the independent review is written
- **THEN** its recall and accuracy follow the source reading and the delivered associations
- **AND** elapsed seconds is the measured run duration, or it stays unassessed when no measurement exists
- **AND** the older source reviews keep their bytes
