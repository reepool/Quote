## ADDED Requirements

### Requirement: Announcement Attachment Layout

The system SHALL store managed announcement attachments under
`data/filings/announcements` using content-addressed SHA-256 paths and SHALL keep
their catalog records in the shared research database.

#### Scenario: Identical bytes are acquired twice

- **WHEN** two source records resolve to identical verified bytes
- **THEN** they reference one physical blob

#### Scenario: Existing managed blob is read

- **WHEN** an API caller requests content
- **THEN** the system verifies the catalog path, length, and hash before streaming
