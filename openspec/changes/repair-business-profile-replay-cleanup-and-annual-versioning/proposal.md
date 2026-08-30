## Why

公司画像批量任务仍会重复触发已经暴露过的两类错误：同一报告事实重跑生成新的身份并在发布时发生时序冲突；普通匿名合同关系被误判为匿名集中度，随后又被错误包装成网关拥塞。现有修复没有把旧失败数据清理、`reuse/replace`、异常分类和年度报告版本更新串成一个可验收的闭环，继续批量运行会重复消耗 LLM token 并污染队列。

## What Changes

- 修复活动、经营事实和关系的稳定 occurrence identity，使同一报告同一来源行重跑时复用原记录，不生成重叠重复事实；不同合同、表格行或报告期必须保持独立。
- 明确普通匿名关系与匿名集中度事实的结构类型；匿名普通合同不要求 `disclosed_share`，集中度事实必须显式提供占比。
- 统一语义转换、验证、worker 和队列的错误分类；业务规则、schema、证据和网关故障不得互相包装，确定性业务错误不得触发网关重试。
- 清理不可复用的历史语义 receipt、旧 shadow run、失效工作项、孤儿 checkpoint 及其候选输出；对 `conversion_pending`、旧结构和失败状态按明确规则决定删除或保留，禁止旧垃圾被 `reuse` 或 worker 再次选中。
- 明确新年度报告、同年度更正报告和同报告重跑的版本与写入规则：保留已批准历史，新报告生成新 occurrence，更正报告显式替换或后继，不能因历史记录永久拒绝新结果。
- 增加生产数据库审计、定向重放和批量前门禁，验证清理后不会重复调用无效 LLM 结果。
- 修正报告流事实的 as-of 可见性，避免报告观察区间与知识窗口互相排斥。
- 修正一般场景大小写敏感的 SI 前缀、百分比单位传播和披露舍入容差，禁止确定性数值污染；保留功率目录对 `MW`/`mw`/`mW` 的明确兆瓦兼容例外。
- 明确功率单位目录的兼容语义：`MW`、`mw`、`mW` 均定位为兆瓦并换算为 `10^6 W`，避免将年报功率文本误判为毫瓦。
- 在缺少成本时仍校验归一化毛利率范围；越界或单位冲突必须阻断发布。
- 让 exposure 修复审计能从历史事实反查 action，并以最近一次 hold 审计决定自动恢复权限。
- 使结构化中文修复、单位 pending、exposure action lineage 和复用身份检查真正可达、可观测、可验收。
- 为长运行 worker 增加租约续期和阶段级异常收口，避免重复领取或整轮失控。

## Capabilities

### New Capabilities

- `business-profile-replay-lifecycle`: 定义语义结果清理、复用/替换、同报告重放和年度/更正报告版本生命周期。
- `business-profile-semantic-error-contract`: 定义匿名关系类型、业务校验、schema/证据/网关错误分类及重试边界。
- `business-profile-fact-integrity`: 定义来源行/合同 occurrence identity 的稳定复用和历史批准记录兼容规则。
- `business-profile-publication-boundaries`: 定义发布前的历史清理、候选隔离和失败结果不得污染正式发布的要求。
- `business-profile-execution-integrity`: 定义运行时身份、租约续期、阶段异常收口、页预算锚点和周期口径门禁。

### Modified Capabilities

<!-- No repository-level capability spec exists for these business-profile contracts; they are introduced as new capabilities in this change. -->

## Impact

- 影响 `research/business_profile_semantic_extraction.py`、`research/business_profile_semantic_runtime.py`、`research/business_profile_async_production.py`、`research/business_profile_governance.py`、`research/business_profile_semantic_artifacts.py`、`research/business_profile_semantic_repair.py` 及相关测试。
- 影响画像语义队列、语义 receipt、活动/经营事实/供应链关系写入、checkpoint 生命周期和年度报告回补入口。
- 不改公共 PDF 解析器、LLM provider 或股东信息来源；不删除已批准且属于真实历史的业务事实。
- 需要一次性对现有生产库执行受规则约束的失败数据清理，并在清理后对 `002415.SZ`、`002496.SZ`、`300750.SZ` 做定向重放验收。
