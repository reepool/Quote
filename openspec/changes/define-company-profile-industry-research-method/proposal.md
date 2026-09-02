## Why

公司画像后续必须按行业深读多份年报并形成独立合同，但当前还没有统一的研究方法、文档模板、样本选择、标注和 benchmark 验收格式。若直接进入制造/材料研究，不同研究者仍可能围绕单家公司临时发明字段、章节和 prompt，重新造成语义分裂。

## What Changes

- 建立独立的行业需求文档模板，固定行业边界、研究问题、章节地图、字段清单、义务/覆盖状态、主体/期间/单位、确定性规则、LLM 合同、正反例、组合规则、benchmark 和非目标等必填章节。
- 建立代表性年报样本选择协议：首轮至少三份报告、至少两家公司，并覆盖不同年度、交易所、披露模板、稳定主业与转型/重组情形；单家公司不得定义行业合同。
- 建立章节级阅读与标注协议，将“行业包 × 章节任务”的检查清单、来源证据、合法空值、歧义和抽取失败分别登记，不枚举清单外指标。
- 建立 benchmark manifest、gold annotation 和 acceptance report 模板，要求报告未覆盖边界、样本偏差和待人工决策，禁止只汇报通过样本。
- 建立行业文档登记与评审门：模板、样本清单和标注协议审核通过后，阶段 3 才能开始制造/材料共性研究。
- 本 change 仅交付文档、模板和评审合同；不研究具体行业字段，不运行 LLM，不修改生产 schema、代码、数据库或冻结开关。

## Capabilities

### New Capabilities

- `company-profile-industry-research-method`: 定义公司画像行业包的研究模板、样本选择、章节标注、检查清单、benchmark manifest、验收报告和进入具体行业研究的评审门。

### Modified Capabilities

<!-- No existing main spec owns the stage-2 research artifacts. The completed stage-0/1 contract remains the authoritative product boundary. -->

## Impact

- 新增阶段 2 的行业研究方法文档、行业 requirements 模板、样本清单模板、标注/benchmark manifest 模板和评审清单。
- 更新公司画像总需求的行业文档登记状态和文档索引，但不改变其产品语义。
- 不修改 `research/`、`data_manager.py`、scheduler、Telegram、数据库或生产配置。
- 阶段 3 的 `research-manufacturing-materials-profile-package` 必须使用本 change 产出的模板和门禁，不能自行建立平行格式。
