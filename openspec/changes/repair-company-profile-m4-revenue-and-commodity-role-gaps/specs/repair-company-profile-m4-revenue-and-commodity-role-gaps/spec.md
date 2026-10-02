## ADDED Requirements

### Requirement: The repair stays closed until this change is accepted
Before implementation starts, the change MUST only contain its scope documents. It MUST NOT change Python. It MUST NOT enqueue, query, or export a repair run. It MUST NOT modify the archived 5/9 observation, the historical 8/9 or 9/9 observations, or the existing export bytes. It MUST NOT authorize production or scale quality.

#### Scenario: Writing this change does not start a repair run
- **WHEN** the change documents are created
- **THEN** no repair snapshot and no new runtime record exist
- **AND** the archived batch source review remains recall 5/9, accuracy 5/5, and critical numeric errors 0

### Requirement: An explicit revenue sentence in the same bounded evidence is delivered
The overview projection MUST keep an explicit company revenue sentence that appears in the same bounded evidence as the principal-business sentence. That sentence MUST pass through the existing acceptance, persistence, query, and export path. A negative sentence MUST NOT become the company's revenue answer. Revenue attributed to a third party MUST NOT become the company's revenue answer.

#### Scenario: The service company's revenue sentence survives the first principal sentence
- **WHEN** `600007.SH` page 14 states both the principal business and that operating revenue mainly comes from property leasing and hotel operations
- **THEN** the revenue dimension is a substantive answer bound to that evidence
- **AND** the archived missing reason `overview_lacks_dimension` is not copied onto the repair record

#### Scenario: A denial or a third party's revenue is not adopted
- **WHEN** the same section says a non-core business does not apply, or states revenue of another party
- **THEN** that text is not stored as the company's revenue source

### Requirement: Explicit sales, procurement, and energy roles are selected before acceptance
The repair MUST select disclosures in which the company itself sells, procures, or consumes a named commodity, before facts enter the acceptance chain. It MUST reuse the existing Activity, Relationship, and commodity projection. Sales and procurement MUST remain separate directions. A source name that the catalog does not map MUST still be delivered with mapping status pending. The service company's steam, hot water, and electricity MUST remain one combined fee of `7407073` yuan for 2025. That amount MUST NOT be split into per-commodity amounts or physical quantities.

#### Scenario: Each named role is checked on its own
- **WHEN** the successor is reviewed
- **THEN** the checklist names steam, hot water, and electricity for `600007.SH`
- **AND** it names steel, rare-earth concentrate, fluorite, and coke-product sales for `600010.SH`
- **AND** it names procurement of iron ore, lime, limestone, imported ore, and coke for `600010.SH`

#### Scenario: Production and sales are not collapsed
- **WHEN** the source says the company both produces and sells a named commodity
- **THEN** the sales direction is retained as a commodity role
- **AND** the role carries its report period and source reference in query and export

### Requirement: The repair observation is isolated from the archived round
The repair MUST use a new processing identity and MUST NOT change `default_processing_identity()`. `CompanyProfileTaskService` MUST accept an explicit repair plan path and snapshot directory. The unique-plan reader for `reports/m4_next_small_batch/` MUST continue to read only that archived plan. A repair run MUST NOT write its plan, live-run, or source-review into that directory. The archived recall 5/9, the historical recall 8/9 and 9/9, and the existing export bytes MUST remain unchanged.

#### Scenario: Raising the identity does not reuse the old plan directory
- **WHEN** the repair run is invoked
- **THEN** its snapshots are under the caller-selected repair directory
- **AND** the archived `m4_next_small_batch` plan, observation, live-run, source-review, and exports keep their bytes

### Requirement: The successor is scored by rereading the same frozen reports
The repair MUST use the frozen `600007.SH` and `600010.SH` reports and cutoff `2026-09-17`. Directed tests of the revenue and commodity boundaries MUST pass before the controlled run, query, and export. After both companies are delivered, an independent reading MUST compute recall, accuracy, and critical numeric errors. The three commodity groups MUST be expanded into the per-name checklist. The new score MUST NOT be prefilled. A remaining miss MUST stay a miss. `production_authorization` MUST remain `not_authorized` and `scale_quality_claim_allowed` MUST remain false.

#### Scenario: The new score is not copied from 5/9 or 9/9
- **WHEN** the repair source review is written
- **THEN** its denominator is the reread checklist
- **AND** neither 5/9 nor 9/9 is written in as the result
