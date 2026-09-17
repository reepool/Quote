## Why

2026-09-17 有界路径首次真实观察核原文后，4.1 扩大门槛未达到：两家样本官方年报已披露主营、产品/业务线和收入构成，交付却是 `accepted_facts=0`、三维未答。中航成飞（302132.SZ）已定位官方摘录仍因 `provider-unavailable` 不收事实；浦发银行（600000.SH）官方「公司主要业务情况」与同行「经营范围 银行业务；…」存在却报 `chapter_missing`。同样本再 run 会复用这份空交付。不修这些现场缺陷，不得声称规模质量，也不得启动 M4 或生产授权。

## What Changes

- 当官方年报已定位到有主的 overview / segment 摘录，且摘录已写明对应字段时，`company_profile_common_core` 必须在不调用 LLM 的情况下收成 accepted 事实。
- 银行/服务年报的独立标题「公司主要业务情况」必须能定位 overview；公司简介中的「经营范围」必须按字段标签 + 同行值拥有，不能要求它是独立标题或标题后仅有标点。
- `provider=None` 或未配置 LLM 不得把已定位、已可读的官方摘录标成 `provider-unavailable` / `required_coverage_missing` 后空交付。
- 修复必须发布新的 processing identity，对同一两家公司做受控 successor replay；不得复用旧空 completed work，也不得靠删库或手工清结果。
- query 必须命中 successor 上的 accepted facts，不能靠 `work_id` 字典序碰巧更大。
- 不启用生产、DCF、交易或旧 writer；不回填申万行业、不修 finance 分层、不启动 M4。
- 修复后只对同一 2 家样本受控重跑、query 验收并再核原文，不把本次结果外推为规模质量。

## Capabilities

### New Capabilities
- `company-profile-common-core-owned-page-facts`: 官方已定位摘录到三维骨架事实的确定性收口、银行/服务标题与经营范围同行值定位、受控 successor replay，以及 query 对当前 identity 的读取优先级。

### Modified Capabilities
- `company-profile-bounded-semantic-workflow`: 明确官方已定位摘录属于确定性输入；LLM 只处理摘录仍无法落地的字段，不能把这类字段报成 provider 不可用。

## Impact

- Owner 仍是 `research/company_profile/`：`core_evidence_selection.py`、确定性投影/runtime、`operations.py` 唯一任务入口，`company_profile_read_service.v1` 必须按当前 processing identity 选 successor。
- 写入仍走 `company_profile_research_writer.v1`。旧空 JSON 可保留为 predecessor，不得作为 query 权威结果。
- 入口、调度、Telegram 只转发，不另建采集循环。
- 生产授权保持 `not_authorized`。浦发被标成 `other`（`sw_l1_name` 为空）只记观察，不纳入本 change。
