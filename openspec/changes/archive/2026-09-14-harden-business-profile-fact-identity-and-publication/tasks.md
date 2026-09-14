## 1. Specification and fixtures

- [x] 1.1 Add identity, unit, temporal, publication, entity, and API regression fixtures for the reported scenarios.
- [x] 1.2 Add a compatibility/diagnostic report for existing records whose identities cannot be reconstructed.

## 2. Fact and relationship integrity

- [x] 2.1 Include source-row/contract/object identity in activity, operating-fact, relationship, and exposure-fact IDs.
- [x] 2.2 Extend temporal policies and approved-as-of reads so independent scopes are never collapsed.
- [x] 2.3 Change directory-missing legal names to unresolved/manual review while preserving existing history.

## 3. Units and exposure publication

- [x] 3.1 Reuse the unit catalog in exposure fact production and fail closed for unknown dimensions.
- [x] 3.2 Route exposure publication through the complete promotion service and block non-executable mappings from approved publication.
- [x] 3.3 Align industry mapping validity with half-open interval semantics.

## 4. API and verification

- [x] 4.1 Make candidate diagnostics opt-in and permission-bounded on profile/exposure endpoints.
- [x] 4.2 Run focused business-profile tests and the existing semantic/promotion regression suites.
- [x] 4.3 Review the final diff for compatibility with existing approved 601088.SH data and do a read-only identity collision scan.
