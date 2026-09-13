> **Status update (2026-09-02):** this design is retained as historical evidence for completed lifecycle fixes. Remaining real-data replay and migration steps are superseded by `define-company-profile-product-and-industry-contracts` and MUST NOT be executed under the legacy semantic contract.

## Context

本 change 针对画像生产链路中已经在真实批次复现的阻塞问题：同一报告的经营事实重跑生成不同 activity identity 并触发 temporal conflict；普通匿名合同关系在转换或晋级时被当作集中度/catalog proposal；业务错误又被 worker 包装为 gateway failure。2026-08-31 的 targeted replay 还暴露出一条新的确定性失败链：`002496.SZ` 的 semantic bundle 请求 9 个 activity ID，事务内实际只写入 3 个，但 run metadata 仍登记全部 9 个，verify 随后读取不存在的 ID 而终态失败；该失败在库中留下 3 条 activity 和 10 条 operating-fact candidate。与此同时，`002415.SZ` 与 `300750.SZ` 的多轮重放已经批准同一来源事实的重复 occurrence，并派生出 27 条 `failed_gate:no_conflicts` publication gaps。

画像数据同时需要支持三种时间场景：新年度报告应追加新的报告期事实；同一报告重跑应幂等复用；更正报告应产生明确的替代版本。已批准的真实历史不能被删除或静默覆盖，但确认不可复用的失败结果和候选垃圾必须物理清理。

## Goals / Non-Goals

**Goals:**

- 建立不依赖运行时 evidence 主键或模型解释的稳定物理来源 occurrence key，并把主体范围、规范化客体和值等变化放入独立 semantic-content fingerprint。
- 使同一 occurrence 的重跑在 `reuse` 下复用，`replace` 仅对同一报告流产生显式新版本；不同报告期、合同或表格行不能碰撞。
- 使持久化后的 authoritative record IDs 成为 verify、promotion 和 durable run manifest 的唯一目标集合；请求 ID 与实际 governed ID 不一致时必须有显式 disposition。
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

1. **物理来源身份与语义内容分层。** 语义转换层生成两个独立材料：`source_occurrence_material` 只包含 instrument、report period、source document revision、物理页、表格/合同、行列/metric slot 或稳定 span locator；`semantic_content_fingerprint` 包含 subject scope、action/relationship、source-native object/value/unit 等业务解释。数据库 evidence ID、selected artifact hash、semantic run ID 和模型生成字段不得进入物理来源键。表格 `source_row_key` 必须由 source document + page + table + row/column/metric slot 生成，不得由 evidence ID 生成。叙述型 locator 必须由当前规范化策略版本下的 source-native 引文哈希、有界规范化前后文锚点及同页重复匹配序号组成；解析 artifact 内的字符起止偏移仅用于核验匹配，不得进入 occurrence identity。缺少足以区分来源 occurrence 的物理定位时记录 `identity_incomplete` 并进入 machine rework。同一物理 occurrence 的 semantic fingerprint 变化时产生 `occurrence_semantic_drift`，不得作为独立 occurrence 自动批准。

2. **三种报告生命周期显式区分。** 新报告期按新的 `report_period + source_document_id` 追加；同报告重跑在内容 hash 和 occurrence material 相同时复用；更正报告使用新的 source revision 并由 `replace` 产生后继版本，保留原 approved 记录和 lineage。`force` 只控制入队，不能绕过 identity 或治理门禁。

3. **匿名关系类型显式化。** 在规范化关系中增加可判定的 `relationship_scope`（`ordinary` 或 `concentration`），或由集中度标签确定该值。`客户 A(1)` 等普通匿名合同属于 `ordinary`，允许 `disclosed_share=null`，并以 `disclosed_name_only` 作为该匿名披露的完整身份状态，不要求实体 catalog 命中，也不得生成 `catalog_proposal` publication gap；`前五名客户/供应商` 属于 `concentration`，必须有有限的 `disclosed_share`。

4. **错误分类沿异常对象传递。** 转换异常必须携带稳定 reason code 和 retryable 标志；worker 不根据字符串把所有 machine rework 改写成 `gateway_failure`。只有 provider 返回 429、超时、传输错误等才触发退避重试，确定性业务错误直接终态 machine rework。

5. **清理先审计后删除。** repair 先生成按 instrument/report/source 的清理清单；删除范围包括 rejected、不可复用 conversion pending、明确标记为 `rollout_phase=structured_shadow` 的旧 receipt/run/work、superseded 或 terminal/machine work item、superseded run manifest 及其 candidate 输出。所有新建 semantic run、durable receipt 和派生/发布阶段 manifest 必须持久化包含 `rollout_phase`、processing identity 和 owner 信息；对没有该信息但确需退役的存量 run/receipt，迁移必须先按关联 work、创建期 phase 或人工确认写入带非空 reason 和 timestamp 的不可变 `retirement_marker`，再执行清理。后续清理只读取这些权威条件。`structured_segments` 和 `tabular_operating_facts` 的 family 名称不是退役依据。approved 记录、源证据和 review audit 不删除。清理后 `find_replay` 不得再返回被删 receipt，旧 shadow work 不得继续被 worker 领取。

6. **派生发布使用独立 owner。** exposure facts、published exposures 和 value-chain roles 的新候选必须绑定当前 derivation/publication manifest；活动或语义 run 的 `run_id` 只作为来源 lineage，不作为新候选的唯一生命周期 owner。manifest 必须在写入候选前登记后代记录，并在 publish 部分失败时执行回滚或终态前确定性清理；即使祖先 semantic run 已删除，repair 仍能通过 manifest 清理非 approved 后代。
7. **执行态文件与数据库同一 owner。** repair 删除 work row 时先在数据库事务中记录精确 checkpoint manifest，提交成功后仅删除配置 checkpoint 根目录直属、名称匹配 `bp-work-*.json` 的文件。`machine_rework` checkpoint 只有在 owner、processing identity 和内容均可恢复时才保留；引用已删除 owner、过期 identity、损坏状态或不可重放 artifact 的 checkpoint 必须物理删除。无任何保留 work row 引用的同类文件视为孤儿并物理删除；越界路径拒绝删除，文件删除失败必须报告失败并可在下一次 repair 幂等重试。enqueue 遇到旧 scope 或孤儿 checkpoint 时使用新的空路径，并在数据库切换成功后删除不再被引用的旧文件，不建立 quarantine/legacy 目录。

7a. **批量前门禁按作用域执行。** 11 只样本批量前必须通过 identity collision scan、unusable receipt scan 和 worker error taxonomy scan。门禁必须区分 instrument-scoped 与 global finding：前者仅阻断受影响标的并允许无关标的继续，后者阻断整批；被阻断标的在清理或修复完成前不得进入 LLM 提取。

8. **报告流可见性。** `segments` 和 `operating_facts` 的 `valid_from/valid_to` 表示报告观察期间，不得与 `knowledge_from/knowledge_to` 一起形成互斥的当前可见门禁；approved-as-of 查询必须能在公告日及其后 freshness 窗口内返回已批准报告事实。

9. **数值单位语义。** 一般 SI 前缀必须区分大小写（`M`/`m`、`G`/`g`、`k`），百分比必须在表头或显式单位进入统一 fraction 表示；披露值的有效精度必须参与 reconciliation 容差计算。功率目录是明确的业务兼容例外：`MW`、`mw` 和混合大小写 `mW` 均定位为兆瓦，统一换算为 `10^6 W`；该例外不得扩展到其他维度，且必须由目录规则和回归测试锁定。

10. **结构化失败收口。** 未知单位、非法数值和语言契约错误必须在记录或行级转为 typed diagnostic/machine rework；不得让单页或单文档异常逃出 scope，也不得把确定性错误包装成网关拥塞。

11. **复用和并发收敛。** `reuse` 必须比较当前 runtime identities；长任务必须在 lease 到期前续期；stage 级异常必须进入 worker 报告，不能令兄弟任务脱管。人工 `held` 记录不得被自动 contract recovery 拒绝。

12. **派生 lineage。** exposure 的 action 必须同时存在于事实、发布 payload 和 predecessor/collision 审计读取的记录中；`sells` 与 `produces` 的同商品记录不得因缺失 action 被错误串接。审计读取旧数据时应优先从 `fact_ids` 反查事实 action，反查不到时显式报告 lineage incomplete。

13. **页面选择不丢锚点。** 页预算是画像上下文预算，不是 PDF 解析器限制。预算不足时必须优先保留显式页和结构化表格锚点，再分配上下文页，并在报告中记录被截断的锚点。

14. **Planner classification 必须有单一权威来源。** 若 planner 持久化 document classification，则后续候选构造和文档族选择必须使用该值；若当前流程不需要持久化值，则删除无效的重复比较，避免看似校验但不影响行为的 no-op 分支。该项为 P2，不得阻塞 P0/P1 主路径。

15. **审核持有权以最新决策为准。** contract recovery 判断 held 记录是否可自动处理时，只读取该记录最新一次 hold 审计的 reviewer 和 decision；早期 system hold 不能覆盖后续 human hold。缺失审计或无法确定 owner 时 fail-closed，保持 held 并报告人工处理。

16. **缺成本仍需先做范围校验。** `segment_cost` 缺失只表示无法执行成本对账，不代表毛利率天然有效。归一化后的 reported margin 越界、单位冲突或无法确定单位时，必须生成 publication blocker 并阻止自动晋升。

17. **占比来源值与规范值分层。** LLM 只负责返回 source-native `disclosed_share_source_value` 与 `disclosed_share_source_unit`，程序是 percent 到 canonical fraction 的唯一转换 owner。`0.5 + %` 明确表示来源披露的 `0.5%` 并转换为 `0.005`；`14.5 + %` 转换为 `0.145`；来源本身为 fraction 时单位必须显式为 fraction 或为空且契约明确。规范值、规范单位、来源值和来源单位都写入 metadata。旧字段中 `disclosed_share` 已声称是 canonical fraction 却同时带 `%` 的 payload 继续 fail closed，不能用兼容猜测掩盖结构版本错误。

18. **失败 artifact 生命周期与联合调用去重。** `received` 只表示 provider 响应通过外层 schema，不代表可被业务转换、验证或发布复用。下游业务转换、单位、identity、持久化或 verify 目标解析失败时，当前 artifact/run 必须转为 `conversion_pending`/`verification_failed` 等不可复用状态并记录稳定 reason code；`find_replay` 不得返回它。相同 document、selected sections、prompt/schema/runtime identity 的联合抽取在一次运行中共享成功或失败结果，失败不得因兄弟 field family 再调用一次模型。只有显式受支持的本地规则重开路径可以复用指定 pending artifact。

19. **年度周期口径由文档类型确定。** `annual_report` 与 `annual_report_correction` 的活动和派生 exposure 确定性写入 `period_basis=period_total`、`period_basis_source=annual_document_type`。这不是数值猜测；它来自文档类型。季度和半年报仍执行 Decision 9.5 的显式 basis 门禁。

20. **转换 lineage 分两阶段建立。** 模型返回 source-native `transformation_input_objects_raw` 和 `transformation_output_objects_raw`，不得返回 governed activity/fact IDs。runtime 优先在同一报告 bundle 内按规范化对象、证据和 occurrence 绑定现有或新建 activity/fact IDs；若同一个 `processes` assertion 的单一 exact-evidence span 已明确同时披露输入和输出，则程序必须将该 assertion 自身作为受治理 transformation occurrence，持久化 raw input/output 与 evidence-backed component lineage，并执行既有的 processor-role 确定性映射，不要求为了满足 ID 形式再调用 LLM 或虚构独立活动。缺少任一侧、两侧证据不一致或存在多义绑定时保持 machine rework，不把只有动作名称的普通 `processes` 无条件解释为 processor。

21. **旧 approved 错误结构和重复 occurrence 采用定向重建。** approved 保护不等于永久保留已确认错误的结构。迁移只接受精确的 instrument、source document/evidence、record ID、物理来源定位与错误形状谓词；先生成 dry-run manifest。对保留下来的 source-verified canonical approved 行，迁移可以在同一事务中把旧 evidence-derived `source_occurrence_material`/occurrence key 重建为当前物理来源 identity，但只能修改 identity metadata 和随之确定性重算的 lineage hash；record ID、业务字段、review 状态、版本和时间语义必须保持不变。迁移必须向不可变 review audit 写入 manifest ID、旧/新 identity material/hash、操作者和时间，且后续 `reuse` 必须通过 disposition 返回该行原有 governed ID。随后只删除 manifest 已证明的重复 approved occurrence、由重复记录派生的 exposure/role 以及未批准后代，同时保留 canonical 行、全部 source evidence 与 review audit。语义内容存在真实分歧时不得自动任选一条，必须 hold 并进入明确修正。迁移不得按“缺少 `source_row_key`”或粗粒度 action/object 泛化删除其他 approved 历史。

22. **失败清理以阶段 owner 为边界。** semantic、verify、derive 和 publish 是不同 work/事务时，不能假设跨阶段原子回滚；每个阶段必须登记自己的 owner manifest。semantic 持久化后的 candidate 在 verify 终态失败时属于失败 semantic/verify 执行，不得因 semantic run 已写成 `completed` 而保留为一般可复用数据。阶段失败时，candidate descendants 和阶段 artifact 必须在进入 terminal/non-reusable 状态前回滚或物理删除，保留 reused/approved、evidence 和 audit。`retry_due` 仅允许由同一 owner、当前 identity、完整 target set 的可恢复 checkpoint 继续，不授权普通 `find_replay` 复用候选。

23. **持久化提交后的记录集合是唯一权威。** repository 必须在所有 temporal/reuse 校验完成后，为每个请求记录返回 `written(actual_id)`、`reused(actual_governed_id)` 或 `blocked(reason_code)` disposition。durable run metadata、extract stage output 和 verify targets 只能从这些 disposition 生成；事务内后来设置的 `skip_write` 不得留下请求 ID。`reused` 的 approved/held 记录记为 unchanged 且不重新验证，`blocked` 必须阻断 family completion，不能通过“保留 raw artifact”假装成功。

## Risks / Trade-offs

- [Risk] 更细的 occurrence identity 会使历史候选数量增加。→ 只对可重建的来源行生成新候选，无法重建的记录保持 machine rework，不自动批准。
- [Risk] 清理 pending receipt 可能失去可重放的模型结果。→ 清理清单区分真正可重放的 `unit_rule_*` receipt；只有用户确认的不可复用旧结构/失败结果才删除，删除后由新抽取重建。
- [Risk] 新报告和更正报告可能同时存在重叠记录。→ source revision、report period 和 lineage 纳入唯一性校验，更正只允许显式 replace，不允许 reuse 静默覆盖。
- [Risk] 旧运行进程加载旧代码。→ 运行时报告加入 source revision/runtime identity，部署后先执行进程版本 smoke test，再运行定向回放。
- [Risk] 派生阶段独立 owner 会增加 manifest 状态。→ manifest 只记录当前阶段的候选后代和 processing identity，不改变源 semantic lineage；失败时按 manifest 清理，成功后保留最小可审计引用。
- [Risk] 按标的隔离门禁可能放过共享故障。→ 只有明确标记为 instrument-scoped 的 finding 才局部放行，数据库、catalog、runtime schema 和共享 source asset 问题继续阻断整批。
- [Risk] 精确清理重复 approved occurrence 可能误删真实多行披露。→ 只接受物理来源定位和完整依赖清单均可证明的 manifest；不同表格行、合同或 semantic fingerprint 分歧一律 hold，不按 action/object 聚合删除。

## Migration Plan

1. 部署代码和回归测试，但先不启动批量回补。
2. 对目标样本执行只读扫描，输出 occurrence 冲突、匿名关系分类、receipt 状态和候选依赖；额外统计历史 `mw`/`mW` 功率单位、缺失 action 的 exposure、最新 hold owner 以及 run/receipt 的退役标记。
3. 在事务中清理确认不可复用的失败或旧 shadow receipt/run/work/candidate，按阶段 owner manifest 删除派生/发布后代；只对 owner、processing identity 和内容均可恢复的 machine-rework checkpoint 保留，其余提交后删除失效和孤儿 checkpoint。保留 approved、evidence 和 audit，并记录数据库与文件删除报告。
4. 在定向重放前扫描本次 `002496.SZ` verify 终态失败所拥有的全部 candidate/run/artifact，并扫描 `002415.SZ`、`300750.SZ` 的重复 approved occurrence 及派生 exposure；由于旧执行没有持久化 disposition，审计必须以 raw artifact 请求清单与实际持久化行重建并标注 `reconstructed` 对账，无法证明的决策不得伪装成当时事实；迁移必须同时输出精确删除 manifest。
5. 清除 verify 失败执行的所有未批准后代；对已确认重复 approved occurrence 保留 canonical 行，在同一事务中仅重建其 identity metadata/lineage hash并写入旧新值不可变审计，再删除精确重复项及其依赖，保护 canonical record ID、业务内容、review 状态、evidence/audit。无法证明 canonical 记录或 identity 重建材料的分组保持 held，不自动删除或任选。
6. 定向重放 002415.SZ、002496.SZ 与 300750.SZ；要求零 missing governed target、零 identity conflict、零重复 approved occurrence、零错误 gateway 包装、零无效重复 LLM 调用，并验证匿名 ordinary 关系、占比 source/canonical、年度 period basis、转换 lineage 与功率单位 `mW`。
7. 验证一份新年度报告和一份同年度更正报告的追加/替代行为。
8. 通过门禁后恢复 11 只股票批量；instrument-scoped 失败只暂停受影响标的，global failure 才停止整批，不自动扩大重试范围。

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
- legacy `BusinessProfileLLMClient` / `llm.py` null 校验死路径：画像生产路径重新启用该客户端时，先补调用链、null 语义和回归测试，再单独清理 legacy 实现。
