## 1. M1 面板构建与最小可对接（含阻塞核验）

- [ ] 1.1 归母口径普查（P0-3，阻塞编码）：对 2,970 行 lineage 未标记 net_income
      按 source/source_mode/ingestion_run 与 numeric_facts 长表分类，输出
      attribution_quality 三类占比；抽 5 家公司（茅台/中石化/工行/比亚迪/宁德
      时代）对公开年报，eps/bps 相对误差 ≤1%；结论固化到字段注释、openapi
      description 与需求文档 §6.1。
- [ ] 1.2 publish_date 一致性：抽 10 只 2026Q3 样本对巨潮披露时间核验。
- [ ] 1.3 生命周期应有数校准：对 2024Q1 的 161 只 listed_date 晚于报告期样本逐类
      归因（北交所重编码/吸收合并/字段错误），修正推导口径或出具解释记录。
- [ ] 1.4 新建 `research/financial_pit_panel.py`：表 DDL（`financial_pit_panel` +
      `(available_at)`、`(instrument_id, available_at)` 索引）、coordinated writes
      接入、查询接口骨架。
- [ ] 1.5 一次性全量构建（12 个成规模报告期 + 稀疏行，约 6.5 万行）：基础推导链
      （exact / exact_event / statutory_deadline）+ attribution_quality 标记；
      重复构建幂等（同版本内容 hash 不变）。
- [ ] 1.6 暴露 `GET /api/v1/research/financials/pit/cross-section`（服务端装配，
      YYYYMMDD 统一格式）与 `GET /api/v1/research/financials/pit/panel`。
- [ ] 1.7 暴露 coverage 最小版：实际/应有（生命周期推导）/缺口 + quality 分布 +
      attribution_quality 分布。
- [ ] 1.8 as-of 语义单元测试：available_at ≤ as_of、报告期不越界、未公告自动
      回退上一期（小型 fixture，不依赖实时网络）。

## 2. M2 推导链完整化与行业端点

- [ ] 2.1 实质变更约束：join `data_change_log`（old_hash ≠ new_hash），
      `available_at = max(推导值, 实质变更日)`，落 `last_value_change_at` 与
      `revised_after_publish`。
- [ ] 2.2 `effective_at` 推导（trading_calendar 次一交易日）并暴露于 cross-section
      与 panel 响应。
- [ ] 2.3 延期股特则（`delayed_estimate` + 宽限参数，初值 90 天）。
- [ ] 2.4 延期覆盖率实测：2026Q2/Q3 上 `publish_date > 法定截止日` 公司的
      disclosure_event_state 命中率，数字记入本文件；不足时调整策略后才可继续。
- [ ] 2.5 更正约束影响面实测：实质变更影响行占比与平均回退期数；超阈值
      （>5% 或 >1 期）则实现 `revision_guard` 参数（默认 strict，lenient 响应
      标记 + openapi 风险明示）。
- [ ] 2.6 暴露 `GET /api/v1/research/industries/as-of`（方案 B：批量时点行业，
      基于 industry_classification_history；字段含 effective_from/effective_to）。
- [ ] 2.7 推导链测试：法定回退不早于截止日；实质变更推后且此前不可见；hash 未变
      不推后；延期样本估计可得日前不可见；effective_at 为次一交易日。

## 3. M3 派生指标与增量维护

- [ ] 3.1 派生指标构建：eps/bps/ocfps（分母 0/缺失整体 null）、rev_yoy/np_yoy
      （年报自 2024-12-31、季度自 2025 年对应期）、ttm_revenue/ttm_net_income
      （自 2025-03-31）；边界外一律 null。
- [ ] 3.2 数值缺失规则测试：无 0/""/"-"/"0.00%" 占位（合法 0 须可区分）。
- [ ] 3.3 增量维护挂钩：`financial_statements_sync` 落库成功后刷新受影响行；
      disclosure 事件更新触发 available_at 重推导；bounded ≤30s。
- [ ] 3.4 暴露 `GET /api/v1/research/financials/pit/changes`（sequence_id 单调
      游标，走 (domain, sequence_id) 索引，幂等重放，响应带 next_cursor）。
- [ ] 3.5 cross-section 响应 stats 块：period_lag_distribution 与
      attribution_quality 分布；universe_note 携带完整期状态与
      latest_complete_period。
- [ ] 3.6 增量测试：模拟一次 upsert 后受影响行自动刷新且 changes 流可见。

## 4. M4 测试与回归验证

- [ ] 4.1 金融语义测试全覆盖（需求 §8 验收 1–11：as-of 正确性、延期不前视、
      法定回退、派生边界、无占位、覆盖一致、幂等、增量挂钩、更正防前视、
      延期覆盖率、性能）。
- [ ] 4.2 双源重叠对齐（验收 #12）：2023Q4/2024Q1/2024Q2 与 AKShare 同字段
      对比，>2% 差异股票 <1% 且逐只归因；输出拼接建议文档（§3.5）。
- [ ] 4.3 行业可用性（验收 #14）与完整期标记（#15）、覆盖缺口抽样核对（#16）、
      回退可观测（#17）。
- [ ] 4.4 回归：现有 financial 查询接口行为不变（`get_financial_statement_bundle`
      等不受影响）。
- [ ] 4.5 性能实测：cross-section 热 P95 ≤1s / 冷 ≤3s；panel/coverage/industries
      分页 ≤3s；全量拉取 ≤2 分钟；增量刷新 ≤30s。
- [ ] 4.6 `openspec validate add-financial-pit-panel-cross-section` 通过；相关
      单测全绿。

## 5. M5 清理与交付

- [ ] 5.1 清理开发临时产物（QuoteBak 下），保留必要验收证据至持久目录。
- [ ] 5.2 Review 未提交改动（codex review --uncommitted 或 /review），按 A/B/C
      分类处理。
- [ ] 5.3 更新需求文档状态与 API 使用说明（如有入口文档）。
- [ ] 5.4 提交并推送；本 change 验收后按流程归档。
