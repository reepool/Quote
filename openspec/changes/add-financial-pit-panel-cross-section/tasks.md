## 1. M1 面板构建与最小可对接（含阻塞核验）

- [x] 1.1 归母口径普查（P0-3，阻塞编码）：lineage 全量普查 + 未标记行分类完成。
      实测（2026-10-31）：equity 有值 100% 归母（2,997/2,999 长表值验证）；
      net_income 95.5%（62,502/65,501）lineage 归母标记；2,970 行未标记集中于
      2026Q3（ingestion run 1374-1416 lineage 记录回归），仅 1,063 行可与长表
      parent 候选精确匹配，其余来源不同 → 按规范标 unverified 不重分类。
      5 家样例（茅台/中石化/工行/比亚迪/宁德时代 2025 年报）对公开披露值
      5/5 吻合，误差 <0.01%（验收 #13 达成）；结论固化于模块 docstring。
- [x] 1.2 publish_date 一致性：2026Q2 分布 07-16~08-31、2026Q3 分布 10-10~10-31，
      与法定披露窗口吻合（需求阶段已抽查 + 构建后逐期分布复核）。
- [x] 1.3 生命周期校准：2024Q1 的 161 只 / 2023Q4 的 274 只 listed_date 晚于报告期
      样本逐类归因——全部为 2024–2026 IPO 新股招股书历史财务（status=active，
      listed_date 为真实上市日），非字段错误。处置：面板保留（真实披露数据），
      coverage 反向差异按 pre-IPO 解释，应有数口径正确排除。
- [x] 1.4 新建 `research/financial_pit_panel.py`：表 DDL + 3 索引 + 查询接口。
      派生设计注记：宽表 shares_outstanding 0% 填充，eps 优先用长表披露值
      `profit_sheet.basic_eps`，bps/ocfps 分母回退 `share_capital_amount`。
- [x] 1.5 一次性全量构建：**65,501 行 / 5,595 只 / 888 秒**；幂等性单测通过。
- [x] 1.6 暴露 cross-section 与 panel 端点（YYYYMMDD 统一格式）。
- [x] 1.7 暴露 coverage：实际/应有/缺口（仅成规模期，稀疏期 sparse 标记）+
      quality/attribution 分布。
- [x] 1.8 as-of 语义单测（16 项，小型 fixture，全部通过）。

## 2. M2 推导链完整化与行业端点

- [x] 2.1 实质变更约束：domain='financial'，datasets financial_facts /
      financial_numeric_facts，old_hash ≠ new_hash；变更索引 5,343 键
      （560 万行扫描 5m14s）。
- [x] 2.2 effective_at（trading_calendar 次一交易日）；实测 000001.SZ
      available_at=20260430（法定截止日）→ effective_at=20260506（五一节后
      首个交易日），日历推导正确；超视界返回 null。
- [x] 2.3 延期股特则（delayed_estimate = max(截止日, 通知日+宽限 90d)）；
      语义注记：延期事件的 announcement_time 是"延期通知公告日"而非财报
      发布日，故 delayed 事件永不走 exact_event（防前视）。
- [x] 2.4 延期覆盖率实测：2026Q2/Q3 窗口内**迟到披露 0 家**（Q2 全部 ≤08-31、
      Q3 截止日为今日）——现有数据无法触发延期特则，法定回退在两窗口精确；
      命中率 N/A 如实记录，规则方向保守（≥截止日）。已知延期样本
      （688121.SH 2025 年报）走 delayed_estimate 路径由单测覆盖。
- [x] 2.5 更正约束影响面实测：**revised_ratio=2.47%**（1,616/65,501）< 5% 且
      平均回退 0.39 期 < 1 期（平均推迟 1.4 天、最大 26 天、无一跨越披露
      窗口边界）→ **阈值未触发，revision_guard 参数按规范不实现**
      （spec 场景"when thresholds are not exceeded the parameter SHALL NOT
      be implemented"）；strict 语义为唯一行为，lenient 分支保留在规范中
      备未来阈值超限时启用。
- [x] 2.6 暴露 `GET /research/industries/as-of`：真库冒烟 0.18s / 5,884 只，
      L1 名称解析正确（000001.SZ→银行、000002.SZ→房地产），契约含
      effective_from/effective_to；当日无生效分类不返回。
- [x] 2.7 推导链测试：法定回退不早于截止日、实质变更推后且此前不可见、
      hash 未变不推后、延期样本估计日前不可见、effective_at 次一交易日
      （均在内置 16 项单测中）。

## 3. M3 派生指标与增量维护

- [x] 3.1 派生指标构建：eps（披露值优先）/bps/ocfps（分母回退 + 0/缺失 null）、
      rev_yoy/np_yoy/ttm_*（边界外 null）；真库样本 000001.SZ 2026Q1
      eps=0.67（披露）、bps=28.04（share_capital_amount 分母）。
- [x] 3.2 数值缺失规则测试：无占位值（合法 0 可区分）。
- [x] 3.3 增量维护挂钩：`financial_statements_sync` 落库后
      `_refresh_pit_panel_row`（best-effort，失败记日志不阻塞同步，变更日志
      保留重建依据）；refresh_pairs 单测覆盖更新与源行删除两分支。
- [x] 3.4 暴露 changes 端点（sequence_id 单调游标，真库游标翻页验证）。
- [x] 3.5 cross-section stats 块（period_lag/quality/attribution 分布）+
      universe_note 携带 latest_complete_period；coverage 完整期标记
      （is_complete 时间判据）。
- [x] 3.6 增量测试：refresh 后行刷新（单测）。

## 4. M4 测试与回归验证

- [x] 4.1 金融语义测试：16 项单测覆盖验收 1/3/4/5/7/9（as-of 正确性、法定
      回退、派生边界、无占位、幂等、更正防前视）+ 20241231 期验收 6 覆盖
      一致（coverage 与实查一致）。
- [x] 4.2 回归：test_financial_statements_sync + vintage integration +
      disclosure_incremental **61/61 通过**（11m47s，含面板挂钩路径）。
      双源重叠校验（验收 #12）需 quantwin 侧 AKShare 数据联调，接口与口径
      已就绪（归母契约 + panel 端点），留待联调窗口执行。
- [x] 4.3 性能实测（真库）：cross-section 冷查询 **0.61s**（目标 ≤3s）；
      panel 2000 行 **0.18s**（≤3s）；coverage **1.63s**；全量 12 期 ≈
      34 页 × 0.2s ≈ 7s（≤2min）；增量刷新单行（refresh_pairs 实测量级
      秒内，≤30s 目标余量大）。
- [x] 4.4 `openspec validate` 通过；py_compile 全部通过。

## 5. M5 清理与交付

- [x] 5.1 开发临时脚本集中在 `data/QuoteBak/pit-panel-dev/`（可清理目录）；
      验收证据已固化至本文件与提交记录，无仅存于临时目录的交付物。
- [x] 5.2 Review：自审 diff 完成，修复 build_row 漏拷贝事实列、exchanges
      字符串迭代、industry CTE 同层别名三处实现缺陷（均已补测试或真库冒烟）。
- [x] 5.3 需求文档 v1.2 与实现一致；实现差异已记录：eps 披露值优先
      （1.4 注记）、revision_guard 未实现（2.5 阈值判定）、is_complete
      截止日当天计 True（时间判据字面语义）。
- [x] 5.4 提交并推送；change 待验收方确认后归档。

## 6. 交付后对照审计（2026-10-31，需求 v1.2 + openspec 逐场景核对）

审计发现 8 项与 spec/需求不符，全部修复并有测试或真库冒烟验证：

1. **revision_guard 参数违反 spec**（P0）：实测阈值未触发（2.47%<5%、0.39<1 期），
   spec 明文"未超阈值不得实现"，但代码实现了 lenient 分支（与 2.5 记录矛盾）。
   已移除参数与分支，strict 为唯一行为，响应保留 `guard: strict` 声明；
   `announcement_available_at` 列保留用于诊断与未来超阈值时再启用。
   测试改为断言 lenient 不可用（TypeError）。
2. **universe_note 缺失于 panel/coverage/changes 端点**（spec："any panel API
   endpoint"）：已补，三端点 + cross-section 全部携带。
3. **最新报告期披露窗口未完状态未声明**（spec R3 子句）：`_universe_note` 在
   newest 期截止日未过时附加 `newest_period=... incomplete`。
4. **coverage 反向差异数缺失**（spec R9 + 需求 §5.3 + 任务 1.3 承诺）：已补
   `reverse_discrepancy_count`；真库 2023Q4=274、2024Q1=161（= pre-IPO 行）。
5. **修复过程中发现并二次修正**：gap 若按计数相减会被反向行抵消（2024Q1 真缺
   80 被掩盖成 0），改为**集合差**计算——`gap_count = |lifecycle_set − panel_set|`，
   真库 2024Q1 gap=80 与 1.3 校准结论精确吻合；2026Q3 gap=642（当期披露中）。
6. **coverage 近 7 天源变更行数缺失**（spec R9 + 需求 §5.3）：已补
   `recent_source_change_count`（真库 2026Q3=4,431，披露季活跃，历史期 0）。
7. **披露事件更新未触发面板重推导**（spec R10 场景）：
   `financial_disclosure_incremental_sync` 三个 upsert 点全部补
   `_refresh_pit_panel_for_event`（best-effort + 实例级 store 缓存）。
8. **industries/as-of 日期格式为 ISO**（spec 场景要求 YYYYMMDD）：已改，
   as_of/effective_from/effective_to 全部 YYYYMMDD，真库冒烟通过。

次要修复：share facts 查询加 `ORDER BY updated_at DESC, rowid DESC`（多源文件
重复事实的确定性）；`is_delisted` 预留声明写入 API docstring（spec R1 场景）；
测试 fixture 长表 DDL 补 updated_at 列对齐真表。

审计后验证：单测 16/16；披露/财报同步回归（含新钩子）通过；真库 coverage
冒烟 2.3s（反向差异/集合差 gap/近源变更/全端点 universe_note 全部可见）。

## 7. 第二轮审计（2026-10-31，自底向上逐行 + 真库行为抽查）

发现 2 个真缺陷并修复：

1. **截止日当天完整性误判（off-by-one）**：三处 `deadline <= today` 把截止日
   当天标记为 is_complete=True / 计入 latest_complete，而需求 §5.3 公式与
   spec 场景均为"评估时点**严格晚于**截止日"。实际影响：今日（2026-10-31）
   恰为 2026Q3 截止日、尚缺 634 只，coverage 却报完整。已改 `<`，真库验证
   2026Q3 is_complete=False、latest_complete=20260630。
2. **单期 coverage 的 latest_complete_period=None**：只在被过滤的 summary 内
   循环查找，未查全表。已统一走 panel 级 `latest_complete_period()`，两种
   模式真库均返回 20260630。

真库行为抽查（通过项）：
- **延期后实际披露股 688121.SH**（2025 年报）：面板 quality=`exact`、
  available_at=2026-08-06（真实披露日优先于延期估计——地面真值语义正确），
  翻页验证该股在 as_of=2026-08-06 精确可见；
- **as-of 边界单调性**：20230101→0（2021 稀疏行 publish_date≥2024-04 不可见）、
  20240630→5,463、20241201→5,476；
- **构建崩溃安全**：derive 先于 DELETE，推导失败时旧面板保持原子完整。

逐行审阅记录（无需修复的注记）：哨兵日期 9999-12-31 理论上可出现在原始面板
导出（实际无 malformed 期间行）；stats 的 expected-period 查询不带过滤参数
（每票预期期与过滤无关，语义成立）；TTM 公式对 Q1 期验证正确
（FY + Q1 − Q1prior）；上年同期为负值时 YoY 照算（标准口径注意事项）。

第二轮后验证：单测 16/16、openspec validate 通过。

## 8. 第三轮审计（2026-10-31，数值单位契约 + 文档-实现漂移）

发现 2 项并修复：

1. **百分比语义列 100 倍口径断裂（P0，直接影响验收 #12）**：
   `financial_indicator_snapshots` 存分数（000001.SZ roe=0.0267），面板原样透传；
   而需求 §5.1 示例（roe 15.6）、quantwin 映射（直接使用）、AKShare yjbb
   （roe 为 %）均为百分数——双源 roe 对比会差 100 倍。修复：面板构建时对
   gross_margin/operating_margin/net_margin/roe/roa/asset_liability_ratio ×100；
   current_ratio/quick_ratio 保持倍数、每股类保持元。真库重建（825s）验证：
   roe=2.669、operating_margin=49.29、asset_liability_ratio=90.98。
   需求文档 §6.2 补**单位契约**清单；单测加 ×100 与倍数不变断言。
2. **文档 §4.1 缺 `announcement_available_at` 列**（实现有、文档无的漂移）：
   已补，注明用途（推导链原始值，诊断与未来 guard 备用）。

另清理死代码 `_DERIVED_COLUMNS`。第三轮后验证：单测 16/16（含单位断言）、
真库重建全绿、REVISION_IMPACT 与上轮一致（2.47%）、openspec validate 通过。

## 9. 审核收口：结构缺口验证（2026-10-31，替代第四轮同型审核）

三项前三轮从未做过的验证：

1. **HTTP 层实测（首次）**：TestClient 走通 4 个 PIT 端点 + 非法输入 400
   （roe=2.669% 穿路由无损、guard=strict、coverage complete=False +
   latest=20260630、changes next_cursor）——接线全部正确。
2. **验收 #12 预跑（原留待 quantwin 联调）**：拉取 AKShare yjbb
   2023Q4/2024Q1/2024Q2 三期与面板全量对比（>2% 相对差异口径）：
   - **eps PASS**（0.26–0.58%，披露值优先策略验证有效）；
   - **bps FAIL（10.4–10.8%，真实缺陷）**：`share_capital_amount` 语义在部分
     来源下不是期末总股本（600398.SH：面板 14.8 亿 vs 实际总股本约 45 亿，
     equity/错误分母=13.19 vs AK 4.06）；指标表 book_value_per_share 0% 填充，
     无披露回退可用 → 需修复卡：股本 fact 语义调查（含 valuation.db 股本
     数据候选）+ 重建 + 复跑 #12；
   - **roe FAIL（25–71%）双因**：①近零值相对差异口径放大（0.01 vs 0.0074
     判 26%）；②定义差：AKShare=加权平均 ROE，面板=期末摊薄——拼接点定义
     一致性需与 quantwin 决策（换定义 or 声明接缝）；
   - **gross_margin（1.0–1.24%）/ ocfps（1.2–1.41%）临界**：疑似营业总收入
     vs 营业收入等口径差，待逐股归因。
3. **今晚生产增量**（2026Q3 截止日）：明日核查面板行刷新与 changes 流动。

**结论**：审核循环到此收口。剩余两项不是"审核发现"而是工作项：
(a) bps 分母修复卡（数据调查+重建+复验）；(b) roe/毛利率口径对齐决策
（需求级，需 quantwin 参与）。教训：对外契约类风险（单位、定义、分母语义）
用对外验证关（#12 双源对比）一次就能抓住，内部审核三轮皆漏——后续同类
交付应在验收前先跑双源对比。
