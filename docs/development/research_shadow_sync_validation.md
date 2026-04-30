# Research Shadow Sync Validation

> 更新日期：2026-04-20
> 关联执行文档：[research_data_engine_execution.md](/home/python/Quote/docs/development/research_data_engine_execution.md)
> 当前主线变更包：`openspec/changes/use-shenwan-third-components-as-current-membership-source/`
> 已完成未归档变更包：
> - `openspec/changes/stabilize-shenwan-official-mapping-cache/`
> - `openspec/changes/add-shenwan-mapping-refresh-task/`
> - `openspec/changes/bound-shenwan-fallback-latency/`
> 已归档基线变更包：`openspec/changes/archive/2026-04-19-add-official-shenwan-membership-source/`

---

## 1. 验证目标

本文件前半部分记录了早期 official-code mapping 方案的真实环境验证结果。该方案已被后续设计取代：current membership 不再依赖 official 六位行业码和 manual override，而是直接由申万三级行业成分股集合生成。

当前 strict Shenwan 验证重点应确认以下链路：

- `industry_standard_sync`
- 三级行业成分股 current membership 对 authoritative membership 的覆盖效果
- `industry/standard-readiness`

本次验证的目的不是全量回填，而是确认：

- 真实源是否能在当前环境跑通
- research 写入模型是否能生成可读结果
- 哪些 strict Shenwan 链路已经具备 shadow rollout 条件
- 当前 production unblock 还剩下哪些阻塞点

---

## 2. 验证范围

### 2.1 运行环境

- 使用临时库：
  - `/tmp/research_industry_chain_direct.db`
  - `/tmp/research_industry_chain_proxy_patch.db`
- 不写入现有 `data/research.db`
- 预算模式：`balanced`
- 代理策略：同时验证 `AkShare direct` 与 `AkShare proxy_patch`
- 当时 hardening 配置：
  - `max_constituent_fetch_seconds = 20.0`
  - `manual_override: 480301 -> 857831.SI`

### 2.2 代表性样本

- `600519.SH` / `贵州茅台` / `SSE`
- `000001.SZ` / `平安银行` / `SZSE`

### 2.3 执行脚本

- `/tmp/validate_industry_standard_live.py`

---

## 3. 验证结果

### 3.1 `industry_official_mapping_refresh`

结果：通过

关键结果：

- `akshare:direct`
  - `status = success`
  - `taxonomy_nodes_written = 413`
  - `mapping_cache_rows_written = 433`
  - `mapped_code_count = 245`
  - `unmapped_code_count = 188`
  - `component_taxonomy_count = 258`
- `akshare:proxy_patch`
  - 与 `direct` 基本一致
  - 同样在 `90s` 窗口内完成

结论：

- `industry_official_mapping_refresh` 已具备作为独立任务进入 weekly shadow rollout 的条件
- `480301` override 使映射缓存中的 mapped code 从 `244` 提升到 `245`

### 3.2 `industry_standard_sync` with cached official mapping (`direct`)

> 历史记录：该路径现已废弃为 current membership 主路径。当前 `industry_standard_sync` 不再读取 cached official mapping。

结果：通过

关键结果：

- `status = success`
- `successful_exchanges = 2 / 2`
- `taxonomy_nodes_written = 413`
- `total_official_classifications_written = 2`
- `total_memberships_written = 2`
- 同步阶段 diagnostics 明确显示：
  - `official_mapping_source = cache`
  - `official_mapping_cache_row_count = 433`
  - `official_mapping_codes_mapped = 245`
  - `official_mapping_codes_unmapped = 188`

代表性输出：

- `600519.SH`
  - official code `340501`
  - authoritative membership 成功写入
  - `sw_l2_code = 801125.SI`
  - `sw_l3_code = 851251.SI`
- `000001.SZ`
  - official code `480301`
  - 已通过配置化 `manual_override` 落到 `857831.SI / 股份制银行Ⅲ`
  - `mapping_source = manual_override`
  - 不再触发 direct fallback

历史结论：

- cached mapping 曾经成功把 `industry_standard_sync` 从“每次同步都 live rebuild”转成“refresh 后复用缓存”
- 后续该结论已被三级成分股 current membership 主链取代

### 3.3 `industry_standard_sync` with cached official mapping (`proxy_patch`)

结果：通过

关键结果：

- `status = success`
- `successful_exchanges = 2 / 2`
- `taxonomy_nodes_written = 413`
- `total_official_classifications_written = 2`
- `total_memberships_written = 2`
- `600519.SH` 与 `000001.SZ` 均 authoritative 成功写入
- 两个样本均不再触发 fallback

历史结论：

- `proxy_patch` 路径当时能稳定完成 official refresh 与 sync
- 后续 current membership 主链不再需要 `480301` override 解锁
- latency guard 仍然应该保留，用于保护 legacy fallback 路径

---

## 4. Rollout Check

### 4.1 `industry_official_mapping_refresh` 启用前检查

- `direct` 或 `proxy_patch` 至少一条链路能稳定写出 `industry_official_code_mappings`
- `mapping_cache_rows_written > 0`
- `mapped_code_count` 足以达到当前缓存门槛
- `ingestion_runs` 状态为 `success`

### 4.2 `industry_standard` 启用前检查

- taxonomy 与 membership 必须同时成功写入
- `industry_memberships` 需在代表性 `SSE / SZSE` 样本上均返回非空结果
- `mapping_status` 必须为 `authoritative`
- `sw_l1_code / sw_l2_code / sw_l3_code` 必须完整
- 若 official code 仍为 `unmapped`，需要先完成以下任一动作：
  - 补齐 six-digit official code 到 taxonomy node 的规则/映射资产
  - 引入另一个 authoritative Shenwan membership 来源
  - 对 unmapped code 建立人工校验与回修流程
- 若 fallback 仍需保留：
  - 必须维持显式 latency guard，避免 sync 长时间悬挂
  - `proxy_patch` 仅能视作 availability hardening，不等于全量 authoritative gap 已被填平

---

## 5. 当前结论

本轮代表性真实环境验证表明：

1. `industry_official_mapping_refresh` 已达到可独立 weekly shadow rollout 的状态
2. `industry_standard_sync` 已能稳定复用 cached mapping，不再默认依赖 live rebuild
3. `480301 -> 857831.SI` 的配置化 override 已使代表性 `SSE / SZSE` 样本都能 authoritative 落表
4. `direct / proxy_patch` 两条代表性链路都已实现 `2/2` 成功
5. 但 strict Shenwan authoritative rollout 仍 **没有** 被完全解除阻塞，因为 cache 中仍有 `188` 个 official code 处于 `unmapped`

当前更准确的结论是：

- strict Shenwan 主线已经从“mapping 构建不稳定 + proxy 整任务超时”推进到：
  - `mapping refresh` 可独立成功
  - `sync` 可稳定消费缓存
  - 高价值 official unmapped code 可通过配置化 override 被审计化解锁
  - `proxy` 备用链在代表样本上也能稳定完成
- 当前剩余的核心阻塞点已经收敛为：
  - 全量 official code 仍存在 `188` 个 `unmapped`
  - override 目前还是少量高价值回修，不是全市场完备字典
  - relative valuation 仍不能仅凭代表样本通过就视作全量 production-ready peer baseline

下一条更优先的技术路线应转向：

- 按业务优先级继续补齐高价值 unmapped official codes 的映射规则或人工校验资产
- 如 override 数量持续增长，再把其从配置升级为独立资产表
- 继续评估是否存在比 `legulegu` constituent scan 更适合补缺的 authoritative membership 辅助源

---

## 6. Shareholders Representative Validation

### 6.1 验证目标

对 shareholders 当前主线做代表性 A 股真实环境验证，重点确认：

- `cninfo:direct` 是否可作为 reference-only fallback 稳定跑通
- `AkShare:proxy_patch` 是否可作为 whole-domain baseline 主链稳定返回 `top10_holders`
- shadow sync 与 raw payload 审计链路是否会因为上游 `date` 类型而出现“落表成功但任务失败”的假失败

### 6.2 验证范围

- 临时库：
  - `/tmp/research_shareholder_validation_direct.db`
  - `/tmp/research_shareholder_validation_proxy_patch.db`
- 预算模式：`availability_first`
- 代表性样本：
  - `600519.SH` / `贵州茅台` / `SSE`
  - `000001.SZ` / `平安银行` / `SZSE`
- 执行脚本：
  - `/tmp/validate_shareholder_live.py`

### 6.3 修复前问题

首次真实环境验证时，`shareholder_snapshots` 实际已经写入临时库，但 sync 结果仍被标记为 `failed / degraded`。

根因：

- shareholder providers 的 raw payload 里仍残留 `date` 对象
- `shareholder_sync._hash_payload()` 与 `store_raw_payload()` 在审计阶段直接 `json.dumps(...)`
- 导致任务在原始 payload 哈希/审计写入阶段抛出 `Object of type date is not JSON serializable`

工程修复：

- shareholder providers 现在会递归把 `date / datetime` 标准化成字符串
- `shareholder_sync` 与 `ResearchStorageManager.store_raw_payload()` 也补了防御性序列化

### 6.4 修复后验证结果

结果：通过

`cninfo:direct`

- `status = success`
- `successful_exchanges = 2 / 2`
- `total_snapshots_written = 2`
- 代表性样本均返回：
  - `holder_count`
  - `reference_only_ownership_clues`
- 未返回 `top10_holders`

代表性输出：

- `600519.SH`
  - `holder_count = 255892`
  - `holder_count_report_date = 2025-12-31`
  - `control_owner_name = 贵州省国有资产监督管理委员会`
- `000001.SZ`
  - `holder_count = 441142`
  - `holder_count_report_date = 2025-12-31`
  - `control_owner_ratio = 50.2`

`AkShare:proxy_patch`

- `status = success`
- `successful_exchanges = 2 / 2`
- `total_snapshots_written = 2`
- 代表性样本均返回：
  - `holder_count`
  - `top10_holders`
  - `reference_only_ownership_clues`

代表性输出：

- `600519.SH`
  - `holder_count = 243159`
  - `holder_count_report_date = 2026-03-31`
  - `top_holders_count = 10`
- `000001.SZ`
  - `holder_count = 462824`
  - `holder_count_report_date = 2026-02-28`
  - `top_holders_count = 10`
  - `control_owner_ratio = 49.56`

### 6.5 当前结论

本轮验证表明：

1. `cninfo:direct` 可以作为 shareholders 的低成本官方 fallback，适合补 `holder_count / ownership_clues`
2. `AkShare:proxy_patch` 仍然是当前更完整的 shareholders 主链，因为它在代表样本上能稳定提供 `top10_holders`
3. raw payload 审计链路的 `date` 序列化缺陷已经修复，当前不会再出现“snapshot 已写入但 sync 任务被误判失败”的问题
4. 这轮 readiness 结果基于代表性样本临时 universe，**不能**直接等同于全市场 rollout readiness

下一步更优先的工作应是：

- 如需继续验证，可对真实股票池做更大样本的 shareholders shadow validation
- 如需继续验证，可统计 `AkShare:proxy_patch` 主链下的全市场覆盖率与失败率
- 评估是否需要把 `cninfo` 拆成 `holder_count / ownership_clues` 子域长期前置，而不是继续作为 whole-domain fallback

### 6.6 扩大样本验证（`SSE 5 + SZSE 5`）

结果：通过

关键结论：

- `cninfo:direct`
  - `10 / 10` 样本都能写出 snapshot
  - 但 `scope_counts` 仅为：
    - `holder_count = 6`
    - `reference_only_ownership_clues = 10`
    - `top10_holders = 0`
  - 因此在 scope-aware readiness 下会返回：
    - `ready_for_paid_high_availability_rollout = false`
    - `blockers = ["required_scope_coverage_incomplete"]`
- `AkShare:proxy_patch`
  - `10 / 10` 样本都能写出 snapshot
  - `scope_counts` 为：
    - `holder_count = 10`
    - `top10_holders = 10`
    - `reference_only_ownership_clues = 10`
  - 在该扩大样本下：
    - `ready_for_paid_high_availability_rollout = true`
    - `blockers = []`

工程意义：

- shareholders rollout readiness 不能只看快照覆盖率
- `cninfo` 更适合作为 `holder_count / ownership_clues` 的 fallback 或拆域来源
- 现阶段 whole-domain baseline 仍应以 `AkShare:proxy_patch` 为主

### 6.7 扩大样本复验（`SSE 20 + SZSE 20`）与 scope-aware merge

结果：通过，但 rollout 结论收紧

本轮新增前提：

- `shareholder_sync` 已升级为“缺失标的 + 缺失 required scope 继续 fallback 并按 scope merge”
- 同一标的会保留 primary `source/source_mode`，同时在 `snapshot.scope_sources` 中记录各 scope 的真实来源

关键结论：

- `direct` 链路（`cninfo:direct -> akshare:direct`）
  - `snapshot_total = 40 / 40`
  - `scope_counts` 为：
    - `holder_count = 40`
    - `reference_only_ownership_clues = 40`
    - `top10_holders = 31`
  - `SSE` 在这轮样本上达到 `20 / 20` required scope 覆盖
  - `SZSE` 仅达到 `11 / 20`
  - 因此 overall readiness 仍为：
    - `ready_for_paid_high_availability_rollout = false`
    - `blockers = ["required_scope_coverage_incomplete"]`

- `proxy-first` 链路（`akshare:proxy_patch -> cninfo:direct -> akshare:direct`）
  - `snapshot_total = 40 / 40`
  - ingestion metadata 已确认：
    - `attempted_sources = ["akshare:proxy_patch", "cninfo:direct", "akshare:direct"]`
    - `successful_sources = ["akshare:proxy_patch", "cninfo:direct", "akshare:direct"]`
  - 但 `scope_counts` 最终仅为：
    - `holder_count = 40`
    - `reference_only_ownership_clues = 40`
    - `top10_holders = 0`
  - 因此 `SSE / SZSE` 两个交易所都仍是：
    - `resolved_instruments = 0`
    - `missing_instruments = 20`
    - `error_message = "Missing required shareholder scope for 20 instruments"`

- 单标的对照 probe（`600000 / 000001 / 600519`）同时确认：
  - `AkShare` 原始 `stock_main_stock_holder` 在 `direct / proxy_patch` 两种 mode 下都能返回完整表
  - `AkShareShareholdersProvider.fetch_shareholder_snapshots()` 在 `direct / proxy_patch` 两种 mode 下都能构造：
    - `coverage_scope = ["holder_count", "top10_holders", "reference_only_ownership_clues"]`
    - `top_holders_count = 10`

工程判断：

- 当前 `proxy-first` 大样本退化不是 routing 顺序错误，也不是 scope merge 覆盖掉了 `top10_holders`
- 更可能是批量运行阶段的上游节流、会话退化或 endpoint 稳定性问题
- 因此 shareholders whole-domain baseline 目前仍不能按 `paid_high_availability` 进入 rollout
- 为下一轮排障，`AkShare` 股东 provider 已新增 `raw_payload.fetch_errors`，用于显式区分“字段为空”和“分字段抓取失败”

### 6.8 Top-Holder Batch Hardening Re-Validation（`2026-04-20`）

本轮代码变更：

- `AkShareShareholdersProvider` 新增可配置参数：
  - `top_holders_request_interval_seconds`
  - `top_holders_retry_attempts`
  - `top_holders_retry_backoff_seconds`
- 当前仓库默认值：
  - `0.2 / 2 / 0.5`
- repeated empty payload 现在也会被记为 `raw_payload.fetch_errors.top_holders`

结果：

- `direct` 大样本复验（`cninfo:direct -> akshare:direct`）
  - `snapshot_total = 40 / 40`
  - `scope_counts` 为：
    - `holder_count = 40`
    - `reference_only_ownership_clues = 40`
    - `top10_holders = 0`
  - `source_counts = {"cninfo": 40}`
  - `raw_payload_audit` 中可见 `38` 条 `akshare:direct` 的：
    - `fetch_errors.top_holders = "No tables found"`
  - 因此当前 direct batch path 已不能视作 whole-domain baseline 候选

- `proxy-first` 大样本复验（`akshare:proxy_patch -> cninfo:direct -> akshare:direct`）
  - `snapshot_total = 40 / 40`
  - `scope_counts` 为：
    - `holder_count = 40`
    - `reference_only_ownership_clues = 40`
    - `top10_holders = 33`
  - 按交易所：
    - `SSE = 20 / 20` required scope ready
    - `SZSE = 13 / 20`
  - `source_counts = {"akshare": 39, "cninfo": 1}`
  - `raw_payload_audit` 中仅剩 `6` 条 `akshare:proxy_patch` 的：
    - `fetch_errors.top_holders = "No tables found"`
  - 另有 `000028.SZ` 退回 `cninfo:direct`

工程判断：

- 新的节流+重试对 `proxy_patch` 主链有效
- 它把大样本 `top10_holders` 覆盖从此前的 `0 / 40` 拉回到 `33 / 40`
- 但 shareholders readiness 仍然是：
  - `ready_for_paid_high_availability_rollout = false`
  - `blockers = ["required_scope_coverage_incomplete"]`
- 因此当前最合理的结论是：
  - `AkShare:proxy_patch` 仍是 whole-domain baseline 的唯一现实候选
  - 但必须继续围绕剩余 `SZSE` 缺口做批量稳定性硬化，不能进入正式 rollout

### 6.9 Same-Source Recovery Validation（`2026-04-20`）

本轮代码变更：

- `shareholder_sync` 新增 bounded same-source recovery pass
  - `same_source_recovery_candidates`
  - `same_source_recovery_batch_size`
  - `same_source_recovery_max_instruments`
- ingestion metadata 新增：
  - `same_source_recovery_runs`
  - `same_source_recovery_attempted_instruments`
  - `same_source_recovery_resolved_instruments`
- 单测已确认 recovery pass 会按 micro-batch 重试，并把 recovery 统计写回 ingestion run

结果一：focused gap probe（上一轮 `SZSE` 剩余 7 个缺口）

- `direct` 链路（`cninfo:direct -> akshare:direct`）
  - `snapshot_total = 7 / 7`
  - `scope_counts.holder_count = 7`
  - `scope_counts.top10_holders = 7`
  - `scope_counts.reference_only_ownership_clues = 7`
  - `same_source_recovery_runs = 0`
- `proxy_patch` 链路（`akshare:proxy_patch`）
  - `snapshot_total = 7 / 7`
  - `scope_counts.holder_count = 7`
  - `scope_counts.top10_holders = 7`
  - `scope_counts.reference_only_ownership_clues = 7`
  - `same_source_recovery_runs = 0`
- 两条链路都只残留 `000028.SZ` 的 `fetch_errors.holder_count = "'NoneType' object is not subscriptable"` 审计记录，但不影响 required scope 完整性

结果二：`proxy_patch` 大样本复验（`SSE 20 + SZSE 20`）

- `snapshot_total = 40 / 40`
- `scope_counts`
  - `holder_count = 40`
  - `top10_holders = 40`
  - `reference_only_ownership_clues = 40`
- 按交易所：
  - `SSE = 20 / 20`
  - `SZSE = 20 / 20`
- `source_counts = {"akshare": 40}`
- readiness：
  - `ready_for_paid_high_availability_rollout = true`
  - `blockers = []`
- ingestion metadata：
  - `same_source_recovery_runs = 0`
  - `same_source_recovery_attempted_instruments = 0`
  - `same_source_recovery_resolved_instruments = 0`
- 审计里仍可见 `2` 条 `akshare:proxy_patch` 的：
  - `fetch_errors.holder_count = "'NoneType' object is not subscriptable"`
  - 对应 `000011.SZ / 000028.SZ`
  - 但两只股票最终 snapshot 仍具备全部 required scope

工程判断：

- 当前环境下，`AkShare:proxy_patch` 主链已经在这轮 `20 + 20` 样本上恢复到 `40 / 40`
- same-source recovery pass 已经落地并被单测验证，但在本轮 live validation 中并未实际触发
- 这说明当前同步主链状态已经比上一轮稳定很多，recovery 更适合作为 batch-phase 退化时的保护网
- 但这仍然是样本级 shadow validation，不能直接等价为全市场 rollout-ready
- 当前项目决策已改为：不再把更大样本或全市场验证作为继续推进前置；后续仅在需要重新评估正式开放时再补做验证

### 6.10 Strict Shenwan Rollout Runner Validation（`2026-04-21`）

执行命令：

```bash
timeout 300s env PYTHONPATH=/home/python/Quote /home/python/miniconda3/envs/Quote/bin/python scripts/research_industry_standard_rollout_validation.py --exchanges SSE,SZSE --limit-per-exchange 2 --budget-mode availability_first --allow-paid-proxy
```

执行结论：

- runner 自身执行成功，exit code 为 `0`
- `industry_official_mapping_refresh` 成功：
  - `source = akshare`
  - `mode = proxy_patch`
  - `taxonomy_nodes_written = 498`
  - `mapping_cache_rows_written = 433`
  - `mapped_code_count = 304`
  - `unmapped_code_count = 129`
  - `component_taxonomy_count = 336`
  - `component_cache_source = live_fetch`
- `industry_standard_sync` 成功：
  - `attempted_sources = ["akshare:proxy_patch"]`
  - `successful_exchanges = 2`
  - `total_official_classifications_written = 4`
  - `total_memberships_written = 3`
  - `SSE` 写入 `1` 条 authoritative membership
  - `SZSE` 写入 `2` 条 authoritative membership
- `industry/standard-readiness` 返回 `not_ready`

当时 readiness blocker：

- `official_classification_coverage_incomplete`
- `authoritative_membership_coverage_incomplete`
- `unmapped_official_code_backlog_impacts_current_classifications`

`2026-04-21` 后续修正：上述 official classification / unmapped official code blocker 已被降级为历史审计观察项。当前行业归属 readiness 不再被 `stock_industry_clf_hist_sw()` 的六位历史行业码阻塞，只按当前 authoritative `industry_memberships` 覆盖率判断。

关键观测：

- 本轮使用了 `--limit-per-exchange 2`，因此 sync 是代表样本验证；但 readiness 仍按当前 research universe 全市场目标计算：
  - `SSE target = 2307`
  - `SZSE target = 2884`
  - `BSE target = 0`
  - `target_instrument_count = 5191`
- 因此 `industry_standard_ready = false` 是预期结果，不能解释为 runner 或 sync 失败
- mapping cache 相比上一轮改善：
  - 旧记录为 `433` 行、`245` mapped、`188` unmapped
  - 本轮为 `433` 行、`304` mapped、`129` unmapped
- 当时仍影响 latest classifications 审计层的 high-impact unmapped code 为：
  - `480101`
  - `best_taxonomy_industry_code = 857821.SI`
  - `sample_instruments = ["600000.SH"]`
- 但本地 taxonomy 显示：
  - `857821.SI = 国有大型银行Ⅲ`
  - `857831.SI = 股份制银行Ⅲ`
  - `600000.SH` 从业务常识看更接近股份制银行，而不是国有大型银行
- `480101` 的 mapping candidate diagnostics 显示候选存在竞争：
  - `857821.SI` overlap `5`
  - `857831.SI` overlap `5`
  - top candidate recall 仅 `0.4167`
- 因此本轮不应直接把 `480101 -> 857821.SI` 写入 `manual_overrides`
- 后续确认 `480101` 不能作为当前行业归属判断依据；`600000.SH` 当前应直接通过三级行业成分股集合落入 `857831.SI`

工程判断：

- 当前严格申万主链已经切换为 `sync -> readiness`，official refresh 仅为审计任务
- 小样本 sync 路径有效，但 full-universe readiness 仍受 current authoritative membership coverage 阻塞
- `480101` 属于历史分类审计层问题，不再作为 current membership 或 relative valuation rollout blocker
- 下一步应优先推进三级行业成分股 current membership 全市场覆盖，而不是继续扩展 official code 审计接口

补充修复：

- 本轮 live validation 结束时曾出现 `Unclosed client session` 日志
- 原因是 runner 初始化了 `DataManager` 和数据源工厂，但 CLI 退出前没有显式调用 `DataManager.close()`
- 已在 `scripts/research_industry_standard_rollout_validation.py` 增加 lifecycle helper：
  - `initialize -> run validation -> close`
  - 成功和异常路径都会执行 `close`
- 单测已覆盖 success/failure 两条 cleanup 路径

### 6.11 Shareholders Full Readiness Validation（`2026-04-30`）

执行命令：

```bash
timeout 120s env PYTHONPATH=/home/python/Quote /home/python/miniconda3/envs/Quote/bin/python scripts/research_shareholder_rollout_validation.py --exchanges SSE,SZSE,BSE --skip-sync --fail-on-not-ready
```

执行结论：

- runner 执行成功，exit code 为 `0`
- 当前 `shareholders` 配置已进入正式本地 API gate：
  - `module_enabled = true`
  - `delivery_mode = paid_high_availability`
  - `snapshot_api_requires_mode = paid_high_availability`
  - `snapshot_api_enabled = true`
- readiness 返回：
  - `status = ready`
  - `ready_for_paid_high_availability_rollout = true`
  - `blockers = []`
  - `target_instrument_count = 5191`
  - `snapshot_total = 5191`
  - `missing_snapshot_count = 0`
- required scope 覆盖：
  - `holder_count = 5191 / 5191`
  - `top10_holders = 5191 / 5191`
  - `reference_only_ownership_clues = 5191 / 5191`
- 数据来源分布：
  - `source_counts.akshare = 5191`
  - `source_mode_counts.proxy_patch = 5191`
- 交易所覆盖：
  - `SSE = 2307 / 2307`
  - `SZSE = 2884 / 2884`
  - `BSE` 当前已有 `300` 条 snapshot，但仍按 optional-empty 口径不计入必达 target

工程判断：

- 股东域本阶段工作已经完成：存储、同步、全量落库、scheduler 周更、readiness、API gate 均已收口。
- 后续不再作为当前阶段 blocker；只保留周更后的 readiness 复核、异常补缺、上游稳定性监控，以及是否将 `BSE` 从 optional-empty 调整为必达 target 的单独决策。
