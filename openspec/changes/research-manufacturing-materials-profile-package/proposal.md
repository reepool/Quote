## Why

阶段 2 已建立分行业研究方法，但制造/材料画像仍处于 `not_researched`：当前没有由多家公司正式年报共同支持的字段清单、章节地图、主体/期间/单位规则、确定性与 LLM 分工或 benchmark。若直接设计 schema 或 prompt，仍会围绕宁德时代等单一样本临时发明语义。

## What Changes

- 使用经本地正式公告库确认的四份 2025 年年度报告开展共性研究：`300750.SZ` 宁德时代（SZSE）、`603659.SH` 璞泰来（SSE）、`920015.BJ` 锦华新材（BSE），以及用于主业转型/regime 边界的 `302132.SZ` 中航成飞（SZSE）。前三者分别挑战电池系统完整产销存/产能、多材料与装备/子公司口径、精细化工产品与匿名客户供应商/BSE 模板；第四份报告以正式重组生效证据验证新旧主业不得追溯混用。
- 深读每份报告的主要业务、主营构成、产销存/产能、原材料/采购、客户/供应商、重大变化章节，形成制造/材料独立 requirements、chapter-family map、字段 checklist 和正反例。
- 冻结第一版制造/材料画像义务：总体业务原文、产品/行业/地区收入成本毛利率、适用的产量销量库存、披露的产能、明示原材料/客户/供应商、业务变化与 regime；每个字段明确 required/conditional/optional/not-applicable、主体、期间、source-native 单位和失败语义。
- 定义确定性表格优先以及 LLM `extract/repair/verify` 的行业级输入输出合同，但不实现生产 prompt、schema、selector、writer 或 resolver，不运行旧画像生产链。
- 建立 gold annotations 与 benchmark acceptance，要求所有 required 静默遗漏、销量/销售额或库存量/存货金额混淆、无证据主体归并、单位猜测和清单外产业链推断均为 blocker。

## Capabilities

### New Capabilities

- `manufacturing-materials-profile-research-contract`: 定义制造/材料公司画像的行业边界、真实样本、章节地图、字段义务、主体/期间/单位语义、确定性/LLM 分工、gold 标注与 benchmark 验收。

### Modified Capabilities

<!-- No existing capability requirement changes. This change instantiates the approved stage-2 research method for one industry package. -->

## Impact

- 新增制造/材料行业 requirements、sample manifest、gold annotation、benchmark acceptance 和研究证据文档，并更新总需求行业登记状态。
- 只读使用本地 `effective_annual_reports` 与正式 PDF blob；不修改数据库、原始年报或旧语义数据。
- 不修改 `research/`、`data_manager.py`、scheduler、Telegram、生产配置、生产 schema 或 DCF。
- 本 change 完成只允许阶段 4 另行设计通用模型和新 LLM 合同；不授权生产写入或恢复 backfill。
