CREATE TABLE IF NOT EXISTS adjustment_factor_observations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instrument_id VARCHAR(32) NOT NULL,
    ex_date DATETIME NOT NULL,
    source VARCHAR(32) NOT NULL,
    source_profile VARCHAR(64) NOT NULL DEFAULT 'default',
    provider_factor FLOAT,
    provider_cumulative_factor FLOAT,
    normalized_factor FLOAT,
    normalization_version VARCHAR(64) NOT NULL,
    quality_status VARCHAR(32) NOT NULL DEFAULT 'unvalidated',
    ingestion_run_id VARCHAR(64),
    raw_payload_json TEXT,
    created_at DATETIME,
    updated_at DATETIME,
    FOREIGN KEY (instrument_id) REFERENCES instruments(instrument_id),
    UNIQUE (instrument_id, ex_date, source, source_profile)
);
CREATE INDEX IF NOT EXISTS idx_adj_factor_observation_inst_source_date
ON adjustment_factor_observations(instrument_id, source, ex_date);

CREATE TABLE IF NOT EXISTS adjustment_factors_canonical (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instrument_id VARCHAR(32) NOT NULL,
    ex_date DATETIME NOT NULL,
    series_version VARCHAR(64) NOT NULL,
    factor FLOAT NOT NULL,
    cumulative_factor FLOAT NOT NULL,
    selected_source VARCHAR(32) NOT NULL,
    source_profile VARCHAR(64) NOT NULL DEFAULT 'default',
    quality_status VARCHAR(32) NOT NULL DEFAULT 'unvalidated',
    evidence_count INTEGER NOT NULL DEFAULT 1,
    created_at DATETIME,
    updated_at DATETIME,
    FOREIGN KEY (instrument_id) REFERENCES instruments(instrument_id),
    UNIQUE (instrument_id, ex_date, series_version)
);
CREATE INDEX IF NOT EXISTS idx_adj_factor_canonical_inst_version_date
ON adjustment_factors_canonical(instrument_id, series_version, ex_date);

CREATE TABLE IF NOT EXISTS adjustment_factor_series_status (
    series_version VARCHAR(64) PRIMARY KEY,
    status VARCHAR(32) NOT NULL DEFAULT 'building',
    source_priority_json TEXT,
    start_date DATETIME,
    end_date DATETIME,
    instrument_count INTEGER NOT NULL DEFAULT 0,
    row_count INTEGER NOT NULL DEFAULT 0,
    coverage_ratio FLOAT NOT NULL DEFAULT 0.0,
    conflict_count INTEGER NOT NULL DEFAULT 0,
    max_cumulative_error_pct FLOAT,
    promotion_eligible BOOLEAN NOT NULL DEFAULT 0,
    report_json TEXT,
    created_at DATETIME,
    updated_at DATETIME
);

CREATE TABLE IF NOT EXISTS adjustment_factor_instrument_status (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instrument_id VARCHAR(32) NOT NULL,
    series_version VARCHAR(64) NOT NULL,
    source VARCHAR(32) NOT NULL,
    coverage_status VARCHAR(32) NOT NULL,
    event_count INTEGER NOT NULL DEFAULT 0,
    start_date DATETIME,
    end_date DATETIME,
    ingestion_run_id VARCHAR(64),
    created_at DATETIME,
    updated_at DATETIME,
    FOREIGN KEY (instrument_id) REFERENCES instruments(instrument_id),
    UNIQUE (instrument_id, series_version)
);
CREATE INDEX IF NOT EXISTS idx_adj_factor_instrument_status_version_coverage
ON adjustment_factor_instrument_status(series_version, coverage_status);
