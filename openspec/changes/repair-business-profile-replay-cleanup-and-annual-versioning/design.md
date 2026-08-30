## Context

本 change 针对画像生产链路中已经在真实批次复现的两个阻塞问题：同一报告的经营事实重跑生成不同 activity identity 并触发 temporal conflict；普通匿名合同关系在转换时被当作匿名集中度，业务错误又被 worker 包装为 gateway failure。现有代码还保留部分 `conversion_pending` receipt，并允许 `reuse` 看到历史结构或不完整身份，导致错误结果反复进入队列。

画像数据同时需要支持三种时间场景：新年度报告应追加新的报告期事实；同一报告重跑应幂等复用；更正报告应产生明确的替代版本。已批准的真实历史不能被删除或静默覆盖，但确认不可复用的失败结果和候选垃圾必须物理清理。

## Goals / Non-Goals

**Goals:**

- 建立由报告、物理页、证据 span、表格/来源行、合同引用、主体范围和客体组成的稳定 occurrence identity。
- 使同一 occurrence 的重跑在 `reuse` 下复用，`replace` 仅对同一报告流产生显式新版本；不同报告期、合同或表格行不能碰撞。
- 将普通匿名关系与匿名集中度建模为不同关系语义，只有集中度记录要求 `disclosed_share`。
- 让业务规则、schema、证据、单位和网关错误保留原始分类；只有真实 provider congestion 才进入网关重试。
- 提供受规则约束的清理迁移，删除不可复用 receipt、失败工作项和其 candidate 输出，同时保护 approved 历史。
- 用真实生产回放验证 002415.SZ、002496.SZ、300750.SZ，并验证新年度和更正报告不会被历史记录永久阻塞。

**Non-Goals:**

- 不更换 PDF 解析器、LLM 模型或公共网关。
- 不删除真实的 approved 历史事实、证据和审计记录。
- 不新增通用数据治理平台或第二套画像写入 owner。
- 不在本 change 内启用季度/半年度同步；仅定义其启用前必须提供 `period_basis` 的门禁。
- 不引入第二个 LLM 模型或模型分层逻辑；中文修复沿用现有单一模型接口，模型路由由 LLM 模块另行负责。

## Decisions

1. **Occurrence identity 单一生成入口。** 在语义转换层生成规范化 occurrence material，活动、经营事实和关系均使用同一套来源行/合同/客体字段；治理层使用完全相同的字段集合计算 temporal identity。缺字段时记录 `identity_incomplete` 并进入 machine rework，不用随机或活动 ID 作为替代。

2. **三种报告生命周期显式区分。** 新报告期按新的 `report_period + source_document_id` 追加；同报告重跑在内容 hash 和 occurrence material 相同时复用；更正报告使用新的 source revision 并由 `replace` 产生后继版本，保留原 approved 记录和 lineage。`force` 只控制入队，不能绕过 identity 或治理门禁。

3. **匿名关系类型显式化。** 在规范化关系中增加可判定的 `relationship_scope`（`ordinary` 或 `concentration`），或由集中度标签确定该值。`客户 A(1)` 等普通匿名合同属于 `ordinary`，允许 `disclosed_share=null`；`前五名客户/供应商` 属于 `concentration`，必须有有限的 `disclosed_share`。

4. **错误分类沿异常对象传递。** 转换异常必须携带稳定 reason code 和 retryable 标志；worker 不根据字符串把所有 machine rework 改写成 `gateway_failure`。只有 provider 返回 429、超时、传输错误等才触发退避重试，确定性业务错误直接终态 machine rework。

5. **清理先审计后删除。** repair 先生成按 instrument/report/source 的清理清单；删除范围包括 rejected、不可复用 conversion pending、明确标记为 `rollout_phase=structured_shadow` 的旧 receipt/run/work、superseded 或 terminal/machine work item、superseded run manifest 及其 candidate 输出。对没有该 rollout phase 但确需退役的历史执行态，迁移必须写入带非空 reason 和 timestamp 的不可变 `retirement_marker`，后续清理只读取这两个权威条件。`structured_segments` 和 `tabular_operating_facts` 的 family 名称不是退役依据。approved 记录、源证据和 review audit 不删除。清理后 `find_replay` 不得再返回被删 receipt，旧 shadow work 不得继续被 worker 领取。

6. **执行态文件与数据库同一 owner。** repair 删除 work row 时先在数据库事务中记录精确 checkpoint manifest，提交成功后仅删除配置 checkpoint 根目录直属、名称匹配 `bp-work-*.json` 的文件。无任何保留 work row 引用的同类文件视为孤儿并物理删除；越界路径拒绝删除，文件删除失败必须报告失败并可在下一次 repair 幂等重试。enqueue 遇到旧 scope 或孤儿 checkpoint 时使用新的空路径，并在数据库切换成功后删除不再被引用的旧文件，不建立 quarantine/legacy 目录。

7. **批量前门禁。** 11 只样本批量前必须通过 identity collision scan、unusable receipt scan 和 worker error taxonomy scan；任一阻塞项存在时只允许定向重放，不启动全量 LLM 批次。

8. **报告流可见性。** `segments` 和 `operating_facts` 的 `valid_from/valid_to` 表示报告观察期间，不得与 `knowledge_from/knowledge_to` 一起形成互斥的当前可见门禁；approved-as-of 查询必须能在公告日及其后 freshness 窗口内返回已批准报告事实。

9. **数值单位语义。** 一般 SI 前缀必须区分大小写（`M`/`m`、`G`/`g`、`k`），百分比必须在表头或显式单位进入统一 fraction 表示；披露值的有效精度必须参与 reconciliation 容差计算。功率目录是明确的业务兼容例外：`MW`、`mw` 和混合大小写 `mW` 均定位为兆瓦，统一换算为 `10^6 W`；该例外不得扩展到其他维度，且必须由目录规则和回归测试锁定。

10. **结构化失败收口。** 未知单位、非法数值和语言契约错误必须在记录或行级转为 typed diagnostic/machine rework；不得让单页或单文档异常逃出 scope，也不得把确定性错误包装成网关拥塞。

11. **复用和并发收敛。** `reuse` 必须比较当前 runtime identities；长任务必须在 lease 到期前续期；stage 级异常必须进入 worker 报告，不能令兄弟任务脱管。人工 `held` 记录不得被自动 contract recovery 拒绝。

12. **派生 lineage。** exposure 的 action 必须同时存在于事实、发布 payload 和 predecessor/collision 审计读取的记录中；`sells` 与 `produces` 的同商品记录不得因缺失 action 被错误串接。审计读取旧数据时应优先从 `fact_ids` 反查事实 action，反查不到时显式报告 lineage incomplete。

13. **页面选择不丢锚点。** 页预算是画像上下文预算，不是 PDF 解析器限制。预算不足时必须优先保留显式页和结构化表格锚点，再分配上下文页，并在报告中记录被截断的锚点。

14. **Planner classification 必须有单一权威来源。** 若 planner 持久化 document classification，则后续候选构造和文档族选择必须使用该值；若当前流程不需要持久化值，则删除无效的重复比较，避免看似校验但不影响行为的 no-op 分支。该项为 P2，不得阻塞 P0/P1 主路径。

15. **审核持有权以最新决策为准。** contract recovery 判断 held 记录是否可自动处理时，只读取该记录最新一次 hold 审计的 reviewer 和 decision；早期 system hold 不能覆盖后续 human hold。缺失审计或无法确定 owner 时 fail-closed，保持 held 并报告人工处理。

16. **缺成本仍需先做范围校验。** `segment_cost` 缺失只表示无法执行成本对账，不代表毛利率天然有效。归一化后的 reported margin 越界、单位冲突或无法确定单位时，必须生成 publication blocker 并阻止自动晋升。

## Risks / Trade-offs

- [Risk] 更细的 occurrence identity 会使历史候选数量增加。→ 只对可重建的来源行生成新候选，无法重建的记录保持 machine rework，不自动批准。
- [Risk] 清理 pending receipt 可能失去可重放的模型结果。→ 清理清单区分真正可重放的 `unit_rule_*` receipt；只有用户确认的不可复用旧结构/失败结果才删除，删除后由新抽取重建。
- [Risk] 新报告和更正报告可能同时存在重叠记录。→ source revision、report period 和 lineage 纳入唯一性校验，更正只允许显式 replace，不允许 reuse 静默覆盖。
- [Risk] 旧运行进程加载旧代码。→ 运行时报告加入 source revision/runtime identity，部署后先执行进程版本 smoke test，再运行定向回放。

## Migration Plan

1. 部署代码和回归测试，但先不启动批量回补。
2. 对目标样本执行只读扫描，输出 occurrence 冲突、匿名关系分类、receipt 状态和候选依赖；额外统计历史 `mw`/`mW` 功率单位、缺失 action 的 exposure、最新 hold owner 以及 run/receipt 的退役标记。
3. 在事务中清理确认不可复用的失败或旧 shadow receipt/run/work/candidate，提交后删除失效和孤儿 checkpoint；保留 approved、evidence 和 audit，并记录数据库与文件删除报告。
4. 定向重放 002415.SZ、002496.SZ 与 300750.SZ；要求零 identity conflict、零错误 gateway 包装、零无效重试，并验证功率单位 `mW` 按兆瓦规则解析。
5. 验证一份新年度报告和一份同年度更正报告的追加/替代行为。
6. 通过门禁后恢复 11 只股票批量；任一步失败则停止批量，不自动扩大重试范围。

## Open Questions

- 更正报告的 API 是否需要返回原报告与更正报告的显式 lineage 字段；本 change 默认在内部保存 lineage，保持现有查询兼容。
- `unit_rule_superseded` receipt 是否由本 change 统一删除，还是在单位目录修复后保留一次受控本地重放；实现前以清理扫描结果决定，不能静默删除可复用结果。

## Deferred Backlog Register

以下问题已在审核中识别，但不阻塞本 change 的主路径。归档前必须由 task 10.7 更新处置状态，并为每项指定后续 change 或明确 owner：

- `format_checker` 覆盖：格式校验成为生产 gate 时再补专门 change。
- NFKC 归一化：先确定 canonicalization policy，再改存储 identity。
- `structured_sync` 水位：启用增量同步时处理。
- `list_records(limit=10000)`：扩大生产范围前改为有界分页。
- 审批 supersession 落库：对外暴露更正报告 lineage 时补 successor 持久化。
- catalog 版本单调性：启用多个 catalog writer 时增加迁移和门禁。
- 东财 200 行截断：依赖该端点前补分页和完整性校验。
- GBK 编码处理：用代表性 fixture 单独补来源兼容 change。
- `bookmark_title` 死路径：书签选择成为权威路径时删除或接通。
