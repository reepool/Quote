# repair-company-profile-principal-wrap-and-investee-role-binding Specification

## Purpose

The v5 reread of frozen `600008.SH` and `600019.SH` is recall 12/12 and accuracy 14/14, with critical numeric errors 0. The principal answers themselves contain the business: `600019.SH` says it focuses on the steel industry and also processes, distributes, and operates chemical and information-technology businesses; `600008.SH` says its business covers water, solid waste, air, and energy. Page 201 no longer contributes a steel-sales role. The measured run took 4.73 seconds and used 0 tokens. The expansion gate stays false because `600008.SH` has no commodity-role finding to assess. Production stays not authorized and scale quality is not claimed. The v4 12/12 review and older observations stay unchanged.

## Requirements

### Requirement: The repair stays closed until this change is accepted
Before implementation starts, the change MUST only contain its scope documents. It MUST NOT change Python. It MUST NOT rerun the two reports. It MUST NOT modify the recorded 12/12 review or any older source review.

#### Scenario: Writing this change does not start a successor
- **WHEN** the change documents are created
- **THEN** no v5 snapshot exists
- **AND** the v4 review remains recall 12/12 and accuracy 14/15

### Requirement: A wrapped principal sentence is delivered in full
The overview projection MUST reuse the existing soft-wrap join and return the reporting company's full sentence through the next period. For `600019.SH` page 9 that sentence includes “钢铁业” and “同时从事”. A source text that is only “公司专注于” or only “公司业务覆盖” MUST NOT make the principal dimension answered. A third party's business MUST NOT become the answer.

#### Scenario: The steel sentence is complete in the answer
- **WHEN** page 9 wraps after “公司专注于”
- **THEN** the principal excerpt itself contains “钢铁业” and “同时从事”
- **AND** the evidence quote containing the rest of the page is not treated as the answer

### Requirement: Investee business-nature rows stay off the company role
A clause that contains both an associate or joint-venture label and “业务性质” MUST NOT become the company's sales or procurement role. Page 69 related-party rows of the company MUST remain, including steel sales and both directions of energy medium. The page 15 sales-volume row, domestic scrap procurement, and iron-ore procurement MUST remain.

#### Scenario: Page 201 does not create steel sales
- **WHEN** page 201 lists one investee as steel production and another as coal mining and sales
- **THEN** those rows are not a company steel-sales role
- **AND** page 69 still shows the company's steel sales and both energy-medium directions
