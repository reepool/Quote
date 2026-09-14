## Context

Stage 5 已有新对象、真实 provider、研究投影和文件 bundle，2026-09-13 已支持按事实导出。旧异步服务有队列、租约、续跑和任务控制，但 stage runner 仍指向旧语义链；新投影的商品暴露仍为占位。不能把旧 backfill 打开作为新链上线。

本次规划合同已统一；本 change 的实现任务尚未完成。唯一产品上位文档是 `company_profile_product_and_industry_semantic_requirements.md`。现行主规范中的政策修订已经同步，delta 表达同一目标，后续归档不得恢复旧整报门。

## Goals / Non-Goals

**Goals:**
- 所有 A 股进入通用骨架任务，行业增强按需启用。
- 实现主营、产品服务、收入来源三维评价；不以记录条数代替业务完整性。
- 单条接受与范围隔离、自动选证据、scope 保存/续跑及查询形成业务闭环。
- 商品标准标识/待映射原名、角色、证据、期间形成基础暴露，不依赖行情已绑定或利润弹性。
- 通过明确 writer/reader 范围切换开放研究发布，再清理失效旧语义。

**Non-Goals:**
- 本次规划修改不调用 LLM、不改变生产开关、不删除数据库。
- 不新建队列、数仓、置信度服务或统一万能 schema。
- 不推导未知原料/客户、净利润方向、套保效果或 DCF 输入。
- 不要求一次做完所有行业增强，不再增加第N轮固定制造业 cohort 作为主线。

## Decisions

### 1. 通用三维优先，增强包不阻塞基础

执行映射固定为 company_profile_common_core_mapping.v1：主营/产品服务/收入模式的叙述由现有 extract_business_overview 的 business_overview_source / explicit_activity 承载；披露分部及收入时由 extract_segment_financials 的 segment_dimension / operating_revenue 补充。三维是程序评价，不新增 ChapterTask 或请求 field_id。每维保留同报告已接受记录、摘录/锚点及答复状态；同一段落存在不等于三维自动成立。

三维骨架为主营说明、主要产品/服务或业务线、收入产生方式。正式财务规模复用已有事实域。没有细分收入金额但有明确单一主营的公司可以形成定性骨架；只有合法空值不算骨架成立。保留原文名称，后续按证据映射标准对象。行业专有指标无法映射时保留候选，不用制造业 metric 承载。

备选“逐行业包全部做完再生产”会阻塞全市场目标，故只对增强字段使用行业研究门。通用基线须以跨行业真实样本验证，不从制造业通过率外推。

### 2. 单一主体和记录级接受

Gold 先检查事实、锚点及主体是否合法，再区分仅旧主体政策不一致的 gold_contract_conflict，最后应用 strictness。合法默认集团不属于 unsupported promotion；数值错误、第三方行为或明确更窄范围被覆盖仍 failed。现有 matcher 还未实现该顺序，必须由任务1.4补齐，不能把当前代码宣称已满足新规范。

局部明确 issuer/subsidiary/segment 优先；明文集团、同报告数值核对分别保留现有 basis；无更窄依据且无冲突采用 report_default_group_scope。冲突不默认。独立 verify 必须接受这一程序约定而非要求再补明文 group。不得把该 basis 换成 direct_source_wording。

事实接受、骨架完整、运行状态、消费者授权分开。失败只影响依赖同一错误事实的输出；未知字段和缺增强不扣住其他事实。跨 scope 合并必须满足对象/期间/metric/义务一致，不按同名字段掩盖漏抽。来源与数值错误不因降门而通过。

### 3. 复用运行底座，接入 Stage 5

通过现有工作队列的 stage_runner 接口调用 `research/company_profile/` 的应用服务；业务循环仍归该 owner。scheduler/CLI 只转发。原始资产由正式年报服务提供，选择范围是全部 A 股有效报告，不使用 ShadowCandidate 固定六行业/年份/OOS 排除规则。

程序生成 Evidence，完整上下文冻结在各任务输入；无需人审每份计划。新增通用准入路径不得扩大历史固定测试 manifest。scope 完成即持久化；租约恢复继续未完成 scope。复用 identity 含报告版本、输入 hash 和实际影响语义的策略版本。金融旧的“存续状态/announcement-only”逻辑不能直接当新完成语义。

### 4. 商品关联先交付，经济敏感性后置

任务3.1先注册 company_profile_commodity_exposure.v1 的派生投影/读写字段，再接writer。字段权威见总需求§20.1及通用模型主规范：来源引用、时态/主体、商品/映射状态、业务角色、可空Measurement引用与行情绑定、版本和不确定性；报告检查状态与空关联分开。当前 ObjectType 没有 CommodityExposure，ResearchBoundary 是占位，不可将新字段直接塞入已有LLM响应。商品角色词表与 Stage 5 field_id 隔离。

原始来源字段不变；程序使用现有商品目录与规则关联 CommodityExposure/有限角色，保存来源 record ID、商品 ID、角色和时效。角色有收入端/原料端/能源端等，不能净额化或直接表述利润方向。未绑定行情不影响已识别商品关联。仅行业常识推导的商品不会成为 reported fact。

旧 `BusinessProfileExposureFactProducer` 的 Activity 数值假设不可直接复用；复用目录和必要持久化功能，按新 Activity/Measurement/Relationship 输入适配。只保留一个新契约 writer 和发布 owner。

### 5. 上线、质量和清理

先实现 M1/M2，再对可解释范围接入 M3。对原始数据、幂等、续跑、读取和记录隔离做针对性验收后，发布研究任务准确入口。用户按预算启动，结果逐家公司交付，不等待50家。新研究发布授权不改变旧 writer 冻结，也不赋予 DCF/交易权限。

来源抽样在运行前约定选择法、分层、主要信息分母和扩大规模阈值，必须实际核原文。源码/结构检查不能报语义正确率。历史 Gold 保持历史预期，与新政策冲突可报告 contract conflict，不能让运行迁就旧标签。

旧数据清理先 dry-run 明确依赖和恢复方式，确认新读取切换后才执行；不先清整个数据库。研究输出与原 PDF 保留，已撤回派生审计删除并记录来源提交。

## Risks / Trade-offs

- 通用骨架可能过浅 → 独立抽样检查三维实质回答及重要业务召回，分行业披露结果。
- 局部成功可能掩盖漏抽 → 数据可交付与骨架完整分开，未知/未读/未披露分开。
- 默认集团被当可随意加总 → 限制仍按对象、期间、分部和行类执行，保留 basis。
- 旧任务能力被误认为新执行链已接通 → 新入口集成测试必须证明调用 Stage 5 且不调用旧 writer。
- 文档/代码处于迁移期 → 未实现项保持 unchecked，生产开关不随文档变更。

## Migration Plan

M1：通用骨架与主体政策 → M2：持久化任务与查询 → M3：基础商品暴露、范围切换和研究发布。M4 行业增强后续独立迭代，不阻塞 M1—M3。

回滚仅停新 writer/消费者，保留正确结果，不重新启用旧错误语义。数据库删除不在本次规划操作内，后续必须提供确切 manifest。

## Open Questions

首次扩大运行的具体数值阈值与预算须在 live plan 中根据用户成本约束预先登记；此为未完成任务，不能用本轮历史结果反向拟合。其余业务方向已获用户批准，不再逐家公司请求选样许可。
