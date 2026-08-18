## Why

Instrument identities are normalized by several independent suffix maps, while production storage uses `.SH/.SZ/.BJ/.HK/.US` and some public boundaries accept exchange or vendor aliases. Equity write paths can also fall back to heuristic holiday logic, creating a risk of silent empty reads and false gaps.

## What Changes

- Introduce one structured instrument identity boundary with explicit storage, exchange, and vendor renderers.
- Preserve all existing database keys and public aliases without a database-wide migration.
- Make instrument master governance the authoritative owner of equity universes used by maintenance commands.
- Require authoritative `quotes.db` trading-calendar reads for equity writes and gap decisions.
- Make master refresh an explicit command prerequisite and report section instead of adapter-specific behavior.
- Migrate callers incrementally through compatibility adapters and table-driven equivalence tests.

## Capabilities

### New Capabilities

- `instrument-identity-boundary`: Defines canonical identity parsing, rendering, alias compatibility, and use by application boundaries.

### Modified Capabilities

None. Existing instrument-master governance behavior remains the source contract; this change adds an identity/calendar boundary around its callers.

## Impact

- Affects `utils/code_utils.py`, exchange helpers, quote/master application paths, API parsing, research callers, and relevant tests.
- Preserves database paths, instrument primary keys, API-compatible aliases, scheduler jobs, and financial semantics.
- Depends on W1 documentation governance and implements W2, FR-03, FR-04, FR-06, and FR-11.
