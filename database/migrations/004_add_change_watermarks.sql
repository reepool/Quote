-- Add local-observed CDC metadata for daily sync watermarks.
-- This migration is additive: it does not rewrite or delete existing rows.

ALTER TABLE daily_quotes ADD COLUMN row_hash VARCHAR(64);
ALTER TABLE daily_quotes ADD COLUMN row_version INTEGER NOT NULL DEFAULT 1;

ALTER TABLE adjustment_factors ADD COLUMN row_hash VARCHAR(64);
ALTER TABLE adjustment_factors ADD COLUMN row_version INTEGER NOT NULL DEFAULT 1;

CREATE TABLE IF NOT EXISTS data_change_log (
    sequence_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    domain VARCHAR(32) NOT NULL,
    dataset VARCHAR(64) NOT NULL,
    change_type VARCHAR(32) NOT NULL,
    business_key_json TEXT NOT NULL,
    instrument_id VARCHAR(32),
    series_id VARCHAR(64),
    observation_date DATETIME,
    period VARCHAR(32),
    old_hash VARCHAR(64),
    new_hash VARCHAR(64),
    row_version INTEGER,
    source VARCHAR(32),
    source_mode VARCHAR(32),
    source_profile VARCHAR(64),
    ingestion_run_id VARCHAR(64),
    batch_id VARCHAR(64),
    changed_at DATETIME NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_daily_quotes_row_hash ON daily_quotes(row_hash);
CREATE INDEX IF NOT EXISTS idx_adj_factor_row_hash ON adjustment_factors(row_hash);
CREATE INDEX IF NOT EXISTS idx_change_log_domain_sequence ON data_change_log(domain, sequence_id);
CREATE INDEX IF NOT EXISTS idx_change_log_dataset_sequence ON data_change_log(dataset, sequence_id);
CREATE INDEX IF NOT EXISTS idx_change_log_domain_dataset_sequence ON data_change_log(domain, dataset, sequence_id);
CREATE INDEX IF NOT EXISTS idx_change_log_instrument_date ON data_change_log(instrument_id, observation_date);
CREATE INDEX IF NOT EXISTS idx_change_log_series_date ON data_change_log(series_id, observation_date);
