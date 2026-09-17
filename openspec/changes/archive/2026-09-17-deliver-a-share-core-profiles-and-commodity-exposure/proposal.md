## Why

用户目标是全部 A 股自动获得可体现主营、产品服务、收入来源的核心画像，以及有证据的商品暴露。既有 Stage 5 验证了制造/材料抽取路径，但固定样本、整报六章完成门、先 reset 后批量的依赖和延期的商品暴露阻断了产品交付。

## What Changes

- **BREAKING（规划合同）**：全 A 股通用骨架优先，行业包仅增强；不再以制造行业白名单、50 家测试或所有报告消除 hold 作为日常采集前置条件。
- 统一主体规则：明确的 issuer/subsidiary/segment 优先，否则使用 consolidated_group + report_default_group_scope；显式冲突仍 unclear，不伪装为 direct_source_wording。
- 将记录质量、公司骨架完整性、任务执行状态与生产授权分离；合格事实逐条交付，补充信息缺失不封锁整家公司。
- 将商品标识、业务角色、证据及期间构成的基础暴露纳入近期交付；行情绑定和净利润敏感性独立，不用未知弹性阻塞已知关联。
- 复用正式资产、队列/租约/checkpoint、Stage 5 单一语义 owner 和公共 LLM 池，完成自动选证据、scope 续跑、查询、导出、人工抽样和按范围切换。
- 更新总需求和现行主体/验收条款；清理失效审计及已被取代的执行待办。历史 bundle 与 Gold 保持原版本，不用于证明新政策精度。

## Capabilities

### New Capabilities
- `company-profile-a-share-delivery`: 全 A 股核心骨架、基础商品暴露、任务生产与迁移验收。

### Modified Capabilities
- `company-profile-research-acceptance-policy`: 主体默认口径和事实级可用性；不以复核数量或六章全满扣住合格事实。
- `company-profile-bounded-semantic-workflow`: 通用/增强任务激活、主体核验、局部失败与跨 scope 合并。
- `company-profile-common-semantic-model`: 明确更窄主体优先、默认不覆盖冲突；补齐三维评价投影与 M3 CommodityExposure 字段合同，抽取任务词表不变。
- `manufacturing-materials-profile-research-contract`: 制造/材料主体及抵消行合同与新政策对齐。
- `company-profile-oos-semantic-mappings`: 抵消行不再受旧 unclear-only 规则限制。
- `manufacturing-materials-profile-isolated-slice`: 历史四报告边界与新请求主体政策分开。
- `manufacturing-materials-profile-semantic-adjudication`: 历史裁决保持历史，新请求采用默认集团口径。

历史 shadow/第二OOS主规范另加范围说明：固定样本、人工回写和旧评分只约束原试验，不改变其历史运行；不将其作为新交付能力或新重跑要求。

## Impact

规划权威为 `docs/development/company_profile_product_and_industry_semantic_requirements.md`。实现 owner 仍为 `research/company_profile/`，复用现有工作队列、正式年报资产和商品目录；入口只转发，不另建采集循环、置信度平台或数仓。

本轮仅完成规划和规范统一，不等于新功能已实现。tasks 中实现项保持未勾选，不调用 LLM、不修改生产配置、不删除数据库。未来启用范围仅限公司事实和基础商品关联的研究发布；DCF、自动交易、未知价格方向仍不授权。旧 writer 保持禁用，旧语义清理须先列确切目标、校验新读取切换并另行执行。
