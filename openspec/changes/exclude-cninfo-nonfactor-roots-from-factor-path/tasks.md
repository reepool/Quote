## 1. Factor Input Governance

- [x] 1.1 Attach resolution-state and listing-date metadata to CNInfo factor inputs
- [x] 1.2 Map explicit non-effective and scope-mismatched states to auditable no-factor exclusions
- [x] 1.3 Exclude selected effective dates strictly before listing before quote lookup

## 2. Historical Archive Date Recovery

- [x] 2.1 Implement bounded unique TDX date-only matching against CNInfo economic terms
- [x] 2.2 Preserve unmatched or ambiguous archive events as dedicated historical root gaps
- [x] 2.3 Annotate later derived events and factor observations with prior historical-gap status

## 3. Completeness And Reporting

- [x] 3.1 Include historical root gaps in completeness, benchmark, and promotion gates without counting later events as pending
- [x] 3.2 Expose exclusion and historical-gap counts and bounded samples in rebuild results

## 4. Verification

- [x] 4.1 Add unit tests for terminal exclusions, strict pre-listing exclusion, date-only matching, ambiguity, and true pending failures
- [x] 4.2 Run focused CNInfo factor and rebuild tests
- [x] 4.3 Run the full SSE/SZSE dry-run audit and compare root, pending, and historical-gap counts
- [x] 4.4 Review all uncommitted changes and resolve confirmed findings
