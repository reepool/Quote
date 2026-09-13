## Why

现有公司画像长期反复修复的根因不是单一代码缺陷，而是产品目标、行业差异、Activity/Measurement 边界、完备性、章节输入和 LLM 职责没有形成一份权威合同。继续执行旧 10.3 只会在语义目标未确定时重复写入和清理，因此必须先冻结旧链，再以行业共性研究重建产品与抽取合同。

## Completion Boundary

本 change 是阶段 0/1 的冻结与合同 change，完成线仅包括：旧语义生产真实入口被阻断、原始资产与只读能力保留、唯一权威总需求发布、旧文档去权威化，以及跨层一致性合同冻结。`/opsx:apply` 不得据此实现新 schema、行业包、prompt、writer、数据 reset 或生产恢复。

本 change 下的 capability specs 是后续独立 change 必须遵守的规范基线，不表示这些未来态能力已在本 change 中投入生产。制造/材料研究、通用语义模型、制造/材料竖切、旧语义 reset、逐行业扩展和生产恢复分别进入独立审核的后续 change。

## What Changes

- **BREAKING**：冻结旧 `business_profile` rollout、手工 backfill 和 semantic production，禁止旧 `business_profile_llm_report.v2` / `business_profile_atomic_extraction.v6` 产生新生产事实。
- 建立 `company_profile_product_and_industry_semantic_requirements.md`，作为公司画像产品、对象、完备性、来源、章节、LLM、行业包、供应链/商品和 DCF 边界的唯一权威总需求。
- 定义通用公司画像对象，将 BusinessOverview、Segment、Activity、Measurement、Relationship、BusinessRegime、IndustryPackageAssignment、Evidence 和 CoverageResult 分离。
- 定义第一版业务范围：总体业务原文、产品/行业/地区收入成本毛利、产销存及披露产能、明示原材料/客户/供应商和业务变化；完整产业链、价格敏感性、DCF 自动输入和全量财务附注明确延后。
- 定义“确定性解析优先、LLM fallback”的章节级处理方式，LLM 仅有 extract、repair、verify 三类请求，人工复核包由程序生成。
- 定义行业包必须从多家公司年报共性研究产生，宁德时代只作为制造/材料样本之一；每个行业后续拥有独立需求、章节地图、字段、prompt 和 benchmark。
- 定义首个试点由人工批准 manifest 固定行业包，并在后续按报告期业务 regime 启用，覆盖转型、主业变更、重大重组和借壳上市后的主包切换及过渡期组合，禁止按静态行业永久绑定或由开发者临时叠加。
- 定义旧语义数据后续通过独立 reset change 物理删除并重建；保留正式公告、原始 PDF 和可复用原始证据，不建设长期兼容分支。
- 将当前 change 的可执行任务严格收敛到阶段 0/1；阶段 2 以后仅登记为后续 change，不得由当前 `/opsx:apply` 自动展开。

## Capabilities

### New Capabilities

- `company-profile-product-contract`: 定义产品用户、研究问题、第一版范围、通用对象、事实分层、完备性、来源、时态、研究视图和 DCF 边界。
- `company-profile-industry-packages`: 定义行业共性研究、独立行业文档、主包/扩展包组合，以及按报告期业务 regime 处理转型、重组和借壳后的包切换。
- `company-profile-semantic-contract`: 定义章节家族、确定性解析与 LLM 分工、严格 extract/repair/verify 输入输出、指标/动作、主体、单位和覆盖失败语义。
- `company-profile-production-transition`: 定义旧语义生产冻结、权威文档切换、旧合同禁写、原始证据保留和后续可审计 reset/恢复条件。

### Modified Capabilities

<!-- No existing repository-level capability defines the new product and industry semantics. -->

## Impact

- 阶段 0 修改 `config/business_profile_production_rollout.json`、`config/05_scheduler.json` 和 `config/10_research.json` 的旧生产开关，不删除数据。
- 阶段 1 新增唯一权威需求文档、更新 `docs/README.md`，并将旧画像 requirements、benchmark 和 runbook 标为历史/冻结。
- 本 change 不产生新公司画像 schema、行业包、LLM 调用、语义 writer、生产数据删除或生产恢复；这些影响必须由后续独立 change 另行声明和审核。
- 后续阶段将影响公司画像 schema、section selector、prompt、LLM adapter、治理写入、研究视图和行业 benchmark，但本轮不实现这些代码。
- 旧 repair change 的 10.3 及后续生产迁移停止；其已完成的通用证据、审计、幂等和失败清理能力可在新实现中复用，但旧语义结构不再构成兼容约束。
