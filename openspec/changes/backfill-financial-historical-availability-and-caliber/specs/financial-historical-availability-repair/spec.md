## ADDED Requirements

### Requirement: 可得性日期回填按本地证据优先执行且幂等

可得性日期回填 SHALL 优先使用本地证据源（`financial_source_files.published_at`、披露事件 `announcement_time`、以及本 change 新增的 `financial_statements_raw` / `financial_filing_versions` 本地披露时间与 `announcement_audit` 勘察结果），按 `(instrument_id, report_period)` 取最早观测日填充 `data_available_date`；`publish_date` 仅在证据质量为实测（observed，含勘察结果）时填充，SHALL NOT 以法定截止日兜底值填充 `publish_date`；证据 SHALL 仅接受定期报告类公告，非定期公告（业绩说明会预告、更正等）SHALL NOT 作为可得日依据（防止把早于报告发布的事件当成可得日造成未来函数）。回填 SHALL 仅更新空值字段并在 lineage 记录证据来源与质量，重复执行 SHALL 不改变已填值。

#### Scenario: 本地证据命中

- **WHEN** 某 `(instrument_id, report_period)` 在任一本地证据源存在定期报告披露时间
- **THEN** 回填 SHALL 以最早观测日填充 `data_available_date`，且空缺的 `publish_date` 在证据质量为实测时一并填充，lineage SHALL 记录证据表名与质量标记

#### Scenario: 非定期公告不得作为证据

- **WHEN** 某 instrument 在报告期附近存在业绩说明会预告等非定期报告公告
- **THEN** 该公告时间 SHALL NOT 被用作 `data_available_date` 证据

#### Scenario: 重复执行

- **WHEN** 回填对同一批次重复执行
- **THEN** 已填充的日期 SHALL 保持不变，仅剩余空值行被处理

#### Scenario: 回填不触发变更水位

- **WHEN** 回填更新目标行
- **THEN** SHALL NOT 更新 `row_hash`、SHALL NOT 产生 `data_change_log` 变更记录（`updated_at` 按回填模块既有行为维护）

### Requirement: 历史披露日批量勘察写入审计表且不触碰事件状态

历史披露日勘察 SHALL 复用既有公告采集服务按市场×季度窗口扫描定期报告公告，从公告标题推断报告期，结果 SHALL 写入 `announcement_audit`（专用 `purpose_key`，如 `historical_disclosure_date_survey`）；勘察 SHALL NOT 写入或修改 `financial_disclosure_event_state`。

#### Scenario: 勘察结果落库

- **WHEN** 勘察在某市场×季度窗口识别到定期报告公告
- **THEN** 对应公告 SHALL 以既有 `announcement_audit` 列记录（`instrument_id`、`published_at`、`title` 等），标题推断的报告期 SHALL 记录于该行的 JSON 列（如 `selection_reasons_json`），供可得性回填按 `(instrument_id, 推断报告期, published_at)` 读取

#### Scenario: 事件状态隔离

- **WHEN** 勘察全量运行
- **THEN** `financial_disclosure_event_state` 的行数与内容 SHALL 不因勘察而变化

#### Scenario: 中断续跑

- **WHEN** 勘察在某窗口中断后重启
- **THEN** 已完成窗口 SHALL 不重复请求（按窗口游标/断点续跑），总请求量 SHALL 有界（按市场×季度窗口计数，非逐 instrument×period 请求）

### Requirement: 网络勘察以应用安全的节奏执行

历史披露日勘察 SHALL 以具备完整访问栈（含 headed Chrome）与温和限速的方式在调度器/worker 侧执行，SHALL NOT 在无浏览器访问栈的独立裸进程中执行全量网络抓取。

#### Scenario: 独立裸进程保护

- **WHEN** 运维尝试以 `python -I` 独立进程发起全市场勘察
- **THEN** 勘察入口 SHALL 拒绝启动或降级为明确报错退出，并提示使用调度侧入口（依据：2026-11-03 data20 三交易所 403 事故，根因含 headed Chrome 引导失败后裸 HTTP 批量请求）

#### Scenario: 源站拒绝服务

- **WHEN** 勘察收到连续 403/限流响应
- **THEN** 勘察 SHALL 在当前窗口停止重试并记录失败窗口，等待下一调度窗口续跑，SHALL NOT 无界重试

### Requirement: 缺失可得性行以法定披露截止日保守兜底

经本地证据与勘察后仍缺失可得日的历史行，SHALL 以对应报告期的法定披露截止日（Q1→当年 4/30、H1→当年 8/31、Q3→当年 10/31、年报→次年 4/30）作为 `data_available_date` 兜底，lineage SHALL 记录兜底来源标记 `source='estimated_statutory_deadline'`（`quality='estimated'`，回填模块既有标记，研究侧据此区分实测日与保守上界）；`publish_date` SHALL 保持 NULL（法定截止日是保守上界而非披露事实，不得写入事实字段）；兜底值 SHALL 满足 `data_available_date ≥ 报告期期末`。

#### Scenario: 兜底填充

- **WHEN** 某行在全部证据源均无披露日
- **THEN** 该行 SHALL 按报告期类型取法定截止日填充 `data_available_date` 且 lineage 兜底来源标记为 `source='estimated_statutory_deadline'`，`publish_date` SHALL 保持 NULL

#### Scenario: 实测日可以晚于法定截止日（延期披露）

- **WHEN** 某行实测/勘察披露日晚于法定截止日（公司延期披露属实）
- **THEN** 该行 `data_available_date` SHALL 取实测日（晚于截止日是真实情况，不是校验违例），仅当实测日超过截止日 60 天以上时 SHALL 列入验收报告的异常清单供人工复核

#### Scenario: 兜底不得早于真实可得

- **WHEN** 任一行完成可得日填充
- **THEN** 硬性校验 SHALL 断言 `data_available_date ≥ 报告期期末`；兜底行 SHALL 等于对应法定截止日；违例 SHALL 阻断验收而非静默通过

### Requirement: 历史批次口径差具备全量量化报告与决策门

系统 SHALL 提供一次性对账量化：对全部报告期的 `financial_facts` 与数值层（hot+history）按关键字段（revenue、net_income、total_assets、equity_total，0.5% 相对容差）全量比对，产出分期、分字段的差异率报告并持久化到 `docs/development/`；对差异行是否按现行映射重派生 SHALL 作为独立决策门，SHALL NOT 在未获 owner 明确决策前改动历史数值。

#### Scenario: 量化报告产出

- **WHEN** 对账脚本运行
- **THEN** 报告 SHALL 覆盖全部报告期与四个字段，含差异行数、差异率与典型样本（含科目口径归类），并落盘到 `docs/development/`

#### Scenario: 决策门生效

- **WHEN** 量化报告显示差异率超过预设关注阈值（默认 0.5%）
- **THEN** 后续处理 SHALL 停留在"提交报告并请求 owner 决策"，SHALL NOT 自动重派生历史行

### Requirement: 零星历史缺口有明确处置

金刚光伏(300093) 2026-06-30 缺失 SHALL 通过定向 reconciliation 收编为事件化处置；40 行数值层有值的 `total_assets` NULL 行 SHALL 本地回填；其余 249 行 NULL SHALL 先检查 `financial_statements_raw` 可重解析性，不可重解析则接受并在验收报告记录；宽表 2023Q4 之前的边界 SHALL 仅作文档声明，SHALL NOT 扩产。

#### Scenario: 定向收编

- **WHEN** 定向 reconciliation 以 `target_symbols=['300093']` 运行
- **THEN** 该行 SHALL 获得事件状态（changed / accepted_gap / blocking 之一），不再是无事件缺失

#### Scenario: 本地回填边界

- **WHEN** `total_assets` 回填执行
- **THEN** 仅数值层存在对应值的行被填充，无源行保持 NULL 并计入验收报告

### Requirement: 修复结果通过验收验证

本 change 完成时 SHALL 产出验收报告：全部缺失行（2023Q4–2026Q1 十个季度约 5.4 万行，另含 2026-06-30 残余 388 行与 2026-09-30 残余 2 行）的 `data_available_date` 处置覆盖率按 observed / surveyed / statutory / 仍缺 四类给占比、PIT 边界校验结果、20 家抽样与 cninfo 公告日人工核对一致率、`mapping_catalog_empty` 陈旧 reason 清零确认（滚动窗口内由周日全量 reconciliation 自然刷新；窗口外的 2023-12-31 期 44 条须经一次定向 reconciliation 刷新）。

#### Scenario: 覆盖率报告

- **WHEN** 全部回填完成
- **THEN** 全部历史缺失行的 `data_available_date` 处置覆盖率 SHALL ≥ 99.9%（仍缺行仅允许为数值层与公告均无记录的退市/特殊行，逐行说明）；覆盖率指标以 `data_available_date` 为准，`publish_date` 允许在 statutory 行保持 NULL

#### Scenario: 陈旧 reason 清零含窗口外期间

- **WHEN** 首个周日全量 reconciliation（滚动窗口 2024Q1 起）完成并对其余期间完成定向刷新后
- **THEN** 全部期间（含 2023-12-31）的 `mapping_catalog_empty` 陈旧 reason 事件 SHALL 归零或逐条给出不可刷新的原因说明

#### Scenario: 抽样核对

- **WHEN** 随机抽取 20 家已完成回填的公司
- **THEN** 其 `data_available_date` 与 cninfo 公告披露日一致率 SHALL ≥ 90%（observed/surveyed 类），不一致样本逐家说明原因
