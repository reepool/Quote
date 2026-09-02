# 公司画像行业 Requirements 模板

> 文档类型 / artifact type：`company_profile_industry_requirements_template`
> 模板版本：`company_profile_industry_requirements_template.v1`
> 状态 / review status：阶段 2 current / `approved`
> 行业边界：实例化时必填；模板本身不包含具体行业结论
> owner：公司画像需求治理
> reviewer：阶段 2 OpenSpec 合同审核
> 上位需求：`company_profile_product_and_industry_semantic_requirements.md`
> 研究方法：`company_profile_industry_research_method.md`

> 复制本模板建立独立行业 requirements。占位项必须由多报告证据填写；示例不是行业结论。本模板不是生产 schema 或 prompt。

## 0. 文档身份与审核

- industry package / boundary version：`<package_id> / <version>`
- schema/version：`<document_schema_version>`
- owner / primary annotator / independent reviewer / acceptance reviewer：`<names>`
- review status：`draft | in_review | approved | held`
- master requirements：`<reference>`
- sample manifest / gold annotation / acceptance report：`<paths + versions>`

## 1. 行业边界与研究问题

说明包含的行业、子行业、商业模式、本包拥有的问题；列出相邻但排除的行业、外部行情/宏观事实、研究假设和后续扩面。研究员问题是否成为完成门，由字段 checklist 的 `requirement_level` 决定。

## 2. 样本与共性依据

引用 sample manifest，说明至少三份年报、两家公司，以及 focus 外至少两份报告如何挑战章节、字段、主体、单位、合法空值和 business regime。列出所有 coverage gaps。

## 3. BusinessRegime 与包启用

定义 primary/extension package 的报告期证据、稳定/转型/重组/借壳样本、主业变化边界、`package_assignment_unclear` 条件。未审核行业包不得由相近包替代。

## 4. 章节地图

| chapter_family | section_task | heading aliases | semantic anchors | table signatures | context/footnotes | allowed outputs | deterministic opportunity | LLM fallback | continuation | failure behavior | sample evidence |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `<family>` | `<task>` | `<aliases>` | `<anchors>` | `<shape>` | `<context>` | `<objects>` | `<rules>` | `<boundary>` | `<rule>` | `<coverage>` | `<report/page>` |

章节编号只能作为 sample evidence，不能成为唯一 selector。

## 5. 字段级检查清单

| field_id | researcher_question | object_type | business_definition | metric/action/relation | logical_slot | package | chapter_task | activation_condition | requirement_level | subject_scope | period_semantics | source_unit_rule | canonical_unit_owner | evidence_requirement | extraction_owner | allowed_coverage_states | blocking_condition | positive_example | prohibited_inference |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `<id>` | `<question>` | `<master object>` | `<definition>` | `<catalog>` | `<slot/n-a>` | `<package>` | `<task>` | `<condition>` | `<master enum>` | `<master scope>` | `<duration/instant/event>` | `<native>` | `program` | `<anchor>` | `<owner>` | `<master statuses>` | `<reason>` | `<example>` | `<ban>` |

checklist 在标注前冻结；coverage 只对 active package × task 输出；`metric_type` 与 `logical_slot` 沿用总需求；表格身份由 `logical_slot + physical_anchor` 区分；不得创建通用指标的第三套同义词；产能和客户义务分别声明。

## 6. 对象、主体、期间、单位

只使用上位需求对象，说明本行业如何细化 `Segment`、`Activity`、`Measurement`、`Relationship`、`BusinessEvent`。主体只用 `consolidated_group/issuer/named_subsidiary/business_segment/unclear`。说明 duration/instant、report period、有效期、知识可得日和重组不可比。记录行业专用/复合单位的 source-native 形式、位置、确定性换算条件和失败行为；程序仍是 canonical 换算 owner。

## 7. 来源与冲突

说明正式年报、更正稿、专项公告、官网、聚合源的角色。正式目标为 `extraction_failed` 时聚合源只能成为交叉校验 candidate。表格/叙述冲突比较主体、期间、维度、单位，无法解释则 `unclear`。

## 8. 确定性提取规则

逐任务定义表格签名、表头/脚注、行列识别、source-native 输出、跨页、停止条件、错误码和 LLM fallback 条件；此处不实现生产代码。

## 9. LLM extract / repair / verify 合同

- extract：限定连续语料、checklist、枚举、正反例、输出和禁止推断；只产候选。
- repair：只按明确错误码和原输入修复，不扩章节或补常识。
- verify：独立核对证据、主体、期间、单位和槽位。
- `human_review_package` 是程序产物，不是第四类 LLM 请求。

## 10. Coverage 与失败

只用总需求枚举：requirement level 为 `required/conditional/optional/not_applicable_by_design`；coverage 为 `observed/not_disclosed/not_applicable/extraction_failed/unclear`；assertion 为 `reported_fact/deterministic_derivation/research_assumption`。列出 required/conditional 的合法空、blocker 和修复路径；空数组不是成功。

## 11. 正例、反例与冲突样例

至少覆盖正常表格、跨页/脚注、合法未披露、不适用、不可读 required 页、主体/单位歧义、清单外推断、事实/推导混淆、reviewer 分歧和 regime 变化，并引用 annotation id。

## 12. 多包组合

说明通用基础包、primary 和 extension 的组合；通用同名字段只抽一次并保留包来源；extension-only required 触发后才启用；冲突进入 `package_assignment_unclear`。

## 13. Benchmark 与验收

定义字段/章节阈值，分别报告 required coverage、source value/unit、subject/period、evidence、legal empty、failure honesty、prohibited inference、uncovered boundaries，以及不能被平均分覆盖的 blockers。

## 14. DCF、研究视图与非目标

说明后续可读取的 approved facts/derivations；商品价格、预测、传导率、套保效果、完整产业链位置不得自动产生。研究文字不得新增批准事实之外的数字、对象、角色或因果。

## 15. 未解决问题与 coverage gaps

| gap_id | dimension | affected requirement | reason | risk | disposition | supplementation trigger | owner | blocking |
|---|---|---|---|---|---|---|---|---|
| `<id>` | `<dimension>` | `<field/task>` | `<reason>` | `<risk>` | `<hold/defer>` | `<condition>` | `<name>` | `true/false` |

## 16. 审核结论

记录 terminology、sample、annotation、benchmark 的 `pass/hold`；最终 research status 为 `approved/held`；production authorization 固定为 `not_authorized`。
