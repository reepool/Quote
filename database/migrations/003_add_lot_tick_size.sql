-- REQ-12: 为 instruments 增加交易规格字段 lot_size / tick_size (可空)
-- 主要用于港股; A股/未知时保持 NULL, 由平台默认处理。
-- 注意: SQLite 的 ADD COLUMN 若列已存在会报错。请通过 scripts/apply_migration_003.py
-- 幂等应用 (先 PRAGMA table_info 判断), 或在全新库由 SQLAlchemy create_all 自动建列。
ALTER TABLE instruments ADD COLUMN lot_size INTEGER;
ALTER TABLE instruments ADD COLUMN tick_size FLOAT;
