## Why

阶段 4 已证明新语义模型和 bounded extract/repair/verify 合同可以在内存中闭环，但尚未证明它能从多份真实制造/材料年报证据稳定生成可核验画像。阶段 5 需要用四份已批准样本完成隔离纵向竖切，在不恢复旧生产链、不混写旧 approved 数据的前提下验证真实报告差异、LLM 合同和研究员输出。

## What Changes

- 为宁德时代、璞泰来、锦华新材和中航成飞 2025 年报建立显式样本运行清单，按已批准章节任务准备真实 PDF 证据并执行阶段 4 单一语义工作流。
- 接入现有公共 LLM gateway 的窄 provider adapter，仅供显式阶段 5 operator 运行；保持 extract、最多一次 typed repair、独立 verify 三类调用，不增加第四类模型调用。
- 增加隔离的阶段 5 run-bundle store，由调用方显式提供目录，保存请求、证据清单、候选、disposition、coverage、人工复核项和研究投影；禁止访问或写入旧画像数据库、approved 表、replay index 和发布路径。
- 从真实年报 Evidence 重建主体依据、Activity actor/source verb 和其他语义字段；Gold adapter 只用于验收对账，禁止用其默认补值作为运行输入。
- 对四份报告逐任务生成研究员可核验画像和 benchmark 结果，required coverage、source-native、主体/期间、legal empty、重组边界及禁止推断任一 blocker 均使该报告保持 `hold`。
- 固定前五名口径：仅合计披露只产生集中度 Measurement 与名称 `not_disclosed`；同字段 coverage 为 `not_disclosed` 时，不展示由同一检查项派生的 Relationship；其他章节明示的独立关系不受影响。
- 保持 `production_authorization=not_authorized`；人工动作 `accept` 在本阶段统一解释为 `accept_for_research_review`，不等于生产 `approved`。
- 不开发新的 PDF/OCR/表格解析平台；只复用现有共享 PDF 能力、批准的样本页/章节证据和已有结构化输出，无法可靠准备的上下文显式失败。
- 不恢复旧 backfill、scheduler、Telegram、API 或生产 LLM；不删除旧语义数据，不实现阶段 6 reset。

## Capabilities

### New Capabilities

- `manufacturing-materials-profile-isolated-slice`: 定义四份制造/材料真实年报的隔离证据准备、bounded LLM 执行、run-bundle 持久化、研究复核和 benchmark 验收合同。

### Modified Capabilities

<!-- No existing capability requirements change. This slice consumes the approved manufacturing/materials research contract and the archived stage-four common model/workflow specs. -->

## Impact

- 在 `research/company_profile/` 增加阶段 5 的窄 provider、样本执行服务和隔离 run-bundle store；复用现有 `CompanyProfileSemanticService`，不建立第二条语义循环。
- 增加一个显式本地 operator 入口和四样本配置/测试；入口只负责参数、凭据和输出目录，业务逻辑仍由阶段 5 应用服务拥有。
- 读取四份既有本地年报 PDF 和阶段 3 的 sample manifest/checklist/Gold benchmark；Gold 不作为语义补值来源。
- 不修改生产数据库 schema、旧 `business_profile_*` 模块、scheduler、Telegram、API、DCF、freeze switches 或旧数据。
