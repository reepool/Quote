## ADDED Requirements

### Requirement: Instrument identity is structured and canonical
The system SHALL parse supported instrument inputs into a structured symbol and exchange identity before rendering any storage, exchange, or vendor identifier.

#### Scenario: Shenzhen alias is provided
- **WHEN** a caller supplies `000001.SZ`, `000001.SZSE`, or `000001.XSHE` in a supported boundary
- **THEN** each input resolves to the same structured identity and canonical storage id `000001.SZ`

### Requirement: Existing storage identifiers remain stable
The identity boundary MUST preserve existing `.SH`, `.SZ`, `.BJ`, `.HK`, and `.US` database keys and SHALL NOT require a database-wide identifier migration.

#### Scenario: BSE and HKEX inputs are rendered
- **WHEN** supported BSE or HKEX aliases are parsed
- **THEN** storage rendering returns the existing `.BJ` or `.HK` key used by local tables and tests

### Requirement: Ambiguous symbols are not guessed silently
A bare symbol SHALL resolve only when the caller supplies an exchange context or an existing authoritative master lookup can determine a unique identity.

#### Scenario: Ambiguous bare symbol has no context
- **WHEN** an input symbol can map to more than one supported exchange and no context is supplied
- **THEN** the boundary returns a validation error instead of selecting a suffix heuristically

### Requirement: Governed master data owns equity universes
Equity maintenance commands SHALL obtain their eligible instrument universe from the shared instrument-master governance result rather than implementing adapter-specific active lists.

#### Scenario: Daily update is triggered from two entry points
- **WHEN** CLI and scheduler execute the same market/date command
- **THEN** both consume the same governed master snapshot and report the same universe identity

### Requirement: Equity writes use authoritative trading calendars
Daily updates, historical maintenance, and gap decisions MUST use the authoritative exchange calendar stored through the quote database/calendar source boundary.

#### Scenario: Public holiday heuristic conflicts with exchange calendar
- **WHEN** a date is open in the authoritative exchange calendar but closed by a generic holiday heuristic
- **THEN** the equity maintenance decision follows the authoritative calendar

### Requirement: Missing authoritative calendar blocks writes
The system MUST NOT fall back silently to weekend or holiday heuristics when an authoritative calendar needed for an equity write is unavailable.

#### Scenario: Calendar lookup fails
- **WHEN** an equity maintenance command cannot obtain the required authoritative calendar range
- **THEN** it reports a blocked or degraded non-writing result with the calendar failure

### Requirement: Identity migration remains backward compatible
Existing conversion functions and public aliases SHALL delegate to the canonical identity boundary until all callers migrate, and their accepted valid inputs SHALL retain equivalent outputs.

#### Scenario: Legacy API alias is used during migration
- **WHEN** a caller submits an alias accepted before the change
- **THEN** the compatibility adapter returns the same canonical local record or the same documented validation result
