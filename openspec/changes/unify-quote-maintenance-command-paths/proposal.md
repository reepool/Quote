## Why

Daily updates mostly converge on `DataManager`, but gap detection and repair still have independent DataManager, scheduler, script, and Telegram subprocess implementations with different universes and write semantics. This creates a realistic concurrent-write and maintenance-consistency risk.

## What Changes

- Define explicit daily, target-date, range, historical, and gap-repair application commands.
- Reuse one identity, master-universe, calendar, source-routing, persistence-verification, and failure contract per command.
- Move gap detection and repair business logic out of scheduler and standalone scripts into one application service.
- Route CLI, API, scheduler, Telegram, and operator scripts through the same services.
- Remove production imports from `scripts/dev_validation` and remove Telegram subprocess production bypasses.
- Include an explicit ownership decision for `database/operations.py` on the quote-storage side; this change does not silently leave that core file outside the program.
- Preserve existing API, CLI, Telegram, job ids, schedules, database schemas, and report compatibility.

## Capabilities

### New Capabilities

- `quote-maintenance-command-path`: Defines authoritative quote maintenance commands, entry-point convergence, write ownership, and compatibility requirements.

### Modified Capabilities

None.

## Impact

- Affects `data_manager.py`, quote/gap scripts, scheduler quote tasks, Telegram handlers, API management routes, and quote maintenance tests.
- Depends on the instrument identity and authoritative equity-calendar boundary from W2.
- Implements W3, FR-01, FR-02, FR-06, FR-12, and FR-16 without expanding market coverage.
