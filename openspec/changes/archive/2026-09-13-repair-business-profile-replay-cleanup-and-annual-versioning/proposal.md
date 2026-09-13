> **Status update (2026-09-02):** this repair change is frozen before its remaining production replay/migration tasks. The authoritative successor is `define-company-profile-product-and-industry-contracts`; old 10.3 and broad replay MUST NOT resume. Completed fixes remain reusable implementation evidence.

## Why

公司画像批量任务仍会重复触发已经暴露过的错误：同一报告事实重跑生成新的身份并在发布时发生时序冲突；普通匿名合同关系被误判为匿名集中度或 catalog proposal；语义持久化在 `reuse` 下跳过记录后仍把不存在的请求 ID 交给 verify。2026-08-31 的三标的回放再次证明，现有修复没有把稳定来源身份、持久化后的权威记录集合、semantic-to-verify 失败清理和 approved 重复记录迁移串成闭环，继续重跑会重复消耗 LLM token 并让失败候选污染下一次运行。

## What Changes

- 将稳定的物理来源 occurrence key 与可变化的语义内容 fingerprint 分离：表格物理键使用源文档、页/表/行/列或合同定位，叙述型键使用规范化引文及有界上下文锚点，不使用数据库 evidence ID、selected artifact hash、抽取字符偏移或模型判断的主体范围；同一来源 occurrence 重跑时复用原记录，不生成重叠重复事实。
- 在持久化事务完成后生成权威 governed record set；`reuse` 跳过新候选时必须返回现有 governed ID 或 typed blocker，verify 不得消费未写入的请求 ID。
- 明确普通匿名关系与匿名集中度事实的结构类型；匿名普通合同不要求 `disclosed_share` 或实体 catalog 命中，集中度事实必须显式提供占比。
- 统一语义转换、验证、worker 和队列的错误分类；业务规则、schema、证据和网关故障不得互相包装，确定性业务错误不得触发网关重试。
- 清理不可复用的历史语义 receipt、旧 shadow run、失效工作项、孤儿 checkpoint 及其候选输出；semantic 持久化成功但 verify 终态失败时，同样必须将该阶段 run/artifact 标为不可复用并删除其未批准后代。
- 为 exposure facts、published exposures 和 value-chain roles 建立独立于祖先 semantic run 的派生/发布 owner manifest；publish 部分失败时清理其候选后代，避免产生悬空 `run_id` 和跨阶段半成品。
- 明确新年度报告、同年度更正报告和同报告重跑的版本与写入规则：保留已批准历史，新报告生成新 occurrence，更正报告显式替换或后继，不能因历史记录永久拒绝新结果。
- 增加生产数据库审计、定向重放和批量前门禁，验证清理后不会重复调用无效 LLM 结果。
- 将批量前门禁按 instrument-scoped 与 global finding 分级，单一标的的生命周期垃圾不得阻断无关标的，全局完整性问题仍阻断整批。
- 修正报告流事实的 as-of 可见性，避免报告观察区间与知识窗口互相排斥。
- 修正一般场景大小写敏感的 SI 前缀、百分比单位传播和披露舍入容差，禁止确定性数值污染；保留功率目录对 `MW`/`mw`/`mW` 的明确兆瓦兼容例外。
- 明确功率单位目录的兼容语义：`MW`、`mw`、`mW` 均定位为兆瓦并换算为 `10^6 W`，避免将年报功率文本误判为毫瓦。
- 在缺少成本时仍校验归一化毛利率范围；越界或单位冲突必须阻断发布。
- 让 exposure 修复审计能从历史事实反查 action，并以最近一次 hold 审计决定自动恢复权限。
- 使结构化中文修复、单位 pending、exposure action lineage 和复用身份检查真正可达、可观测、可验收。
- 为长运行 worker 增加租约续期和阶段级异常收口，避免重复领取或整轮失控。
- 将集中度占比拆分为来源披露值/单位与 canonical fraction，由程序执行唯一一次确定性换算；旧的值与单位混用 payload 不得继续复用。
- 把已接收但在业务转换或持久化阶段失败的语义 artifact 标记为不可复用，并在同一联合抽取作用域内共享失败结果，避免兄弟 field family 重复调用 LLM。
- 年报和年报更正按文档类型确定性写入 `period_basis=period_total`；季度和半年报继续要求显式周期口径。
- 为转换活动保存 source-native 输入/输出对象；同一受治理 evidence 明确披露两侧时允许程序直接形成完整 lineage，模型不得生成数据库 ID，证据不完整或多义时仍进入 machine rework。
- 对已确认由旧结构错误投影或重复回放批准的 occurrence 提供定向、可审计迁移；对保留的 canonical approved 行仅重建物理 identity metadata，保持主键、业务内容和 review 状态不变，并把旧/新 identity 写入不可变审计；只删除精确 manifest 中的错误/重复业务记录及其派生事实，同时保留证据和 review audit。

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

- 影响 `research/business_profile_occurrence.py`、`research/business_profile_semantic_extraction.py`、`research/business_profile_semantic_runtime.py`、`research/business_profile_activity_production.py`、`research/business_profile_exposure_production.py`、`research/business_profile_async_production.py`、`research/business_profile_governance.py`、`research/business_profile_semantic_artifacts.py`、`research/business_profile_semantic_repair.py` 及相关测试。
- 影响画像语义队列、语义 receipt、活动/经营事实/供应链关系写入、checkpoint 生命周期和年度报告回补入口。
- 不改公共 PDF 解析器、LLM provider 或股东信息来源；不删除已批准且属于真实历史的业务事实。
- 需要一次性清理本次失败 run 的未批准后代，并按精确 manifest 迁移 `002415.SZ`、`300750.SZ` 的重复 approved occurrence/派生 exposure；清理后对 `002415.SZ`、`002496.SZ`、`300750.SZ` 做定向重放，验收需覆盖权威 record-ID 映射、semantic-to-verify 失败清理、派生 owner manifest 和按标的隔离的门禁结果。
