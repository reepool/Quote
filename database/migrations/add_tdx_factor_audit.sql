-- 通达信自研复权因子审计表
-- 独立于生产因子表 adjustment_factors, 仅用于审计和交叉验证
-- 严禁与生产表交叉写入!

CREATE TABLE IF NOT EXISTS adjustment_factors_tdx (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instrument_id VARCHAR(32) NOT NULL,
    ex_date DATETIME NOT NULL,

    -- 核心因子
    factor FLOAT NOT NULL DEFAULT 1.0,
    cumulative_factor FLOAT NOT NULL DEFAULT 1.0,

    -- XDXR 原始事件详情 (每 10 股为单位的原始值)
    pre_close FLOAT DEFAULT 0.0,
    fenhong FLOAT DEFAULT 0.0,
    songzhuangu FLOAT DEFAULT 0.0,
    peigu FLOAT DEFAULT 0.0,
    peigujia FLOAT DEFAULT 0.0,

    -- 验证结果
    validation_result VARCHAR(32),
    ref_factor FLOAT,
    ref_source VARCHAR(32),
    ratio_diff_pct FLOAT,

    -- 元数据
    source VARCHAR(32) DEFAULT 'tdx_xdxr',
    created_at DATETIME,
    updated_at DATETIME,

    -- 约束
    FOREIGN KEY (instrument_id) REFERENCES instruments(instrument_id),
    UNIQUE (instrument_id, ex_date)
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_adj_factor_tdx_inst_date ON adjustment_factors_tdx(instrument_id, ex_date);
CREATE INDEX IF NOT EXISTS idx_adj_factor_tdx_validation ON adjustment_factors_tdx(validation_result);
