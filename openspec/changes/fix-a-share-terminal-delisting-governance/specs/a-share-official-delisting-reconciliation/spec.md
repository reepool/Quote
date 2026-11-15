## ADDED Requirements

### Requirement: Official terminal lists participate in the existing stock governance refresh
每次实际执行 SSE 或 SZSE A 股主数据刷新时，系统 SHALL 通过现有官方 provider 读取对应正式终止上市名单，并在现有 `a_share_stock` policy 内核对本地本任务范围内的证券；SHALL NOT 要求证券简称带“退”，也不得只在人工修复脚本中提供此能力。

#### Scenario: Voluntary delisting has no delisting-period name
- **WHEN** SZSE 正式终止名单确认本地 `000016.SZ / *ST康佳A` 于 2026-10-23 终止上市，且执行业务日期已到
- **THEN** 现有主数据治理 SHALL 确认该股票退市并通过权威 writer 写入实际生效日
- **AND** SHALL NOT 因简称不以“退市”开头或不以“退”结尾而跳过

#### Scenario: Other terminal causes use the same rule
- **WHEN** 本地其他普通简称 A 股因主动退市、吸收合并或其他原因出现在有效官方终止名单，且终态已生效
- **THEN** 系统 SHALL 使用相同证据规则处理，SHALL NOT 硬编码康佳、证券代码或退市原因

#### Scenario: Batch source is reused inside one refresh
- **WHEN** 同一交易所本轮需要核对多个本地证券
- **THEN** 系统 SHALL 复用本轮批量名单结果并完成实际分页，SHALL NOT 对全市场逐股请求终态
- **AND** SHALL NOT 因固定500行或报告样例上限而静默遗漏待处理证券

#### Scenario: XLSX reading yields an unpadded numeric security code
- **WHEN** 原始XLSX读取后证券代码为数值16或未补零字符串"16"，包括读取器对字符串单元格的类型推断
- **THEN** provider SHALL 将其归一为六位symbol `000016` 和canonical ID `000016.SZ`
- **AND** 回归fixture SHALL 保留真实原始单元格形态并走完整XLSX读取链，SHALL NOT 在fixture中预补零以跳过归一化；合成数值单元格变体 SHALL 单独标明

### Requirement: Terminal evidence is matched to the exact in-scope security
系统 SHALL 使用证券代码、交易所、证券类型、来源范围及当前主体／上市生命周期匹配终态，仅修改已有的本任务 A 股当前主体主数据；SHALL NOT 按公司简称、同码前身或同公告中的其他证券代码传播退市状态。核对 SHALL 复用已有已审核lineage及来源明确的主体／上市区间，SHALL NOT 为匹配而修改既有上市日期。

#### Scenario: Historical predecessor shares the code with a current issuer
- **WHEN** 600018.SH的已审核lineage区分上港集箱predecessor与2006-10-26起的上港集团current，而终态输入明确属于前身区间
- **THEN** 系统 SHALL 保留当前主体生命周期，不得将前身终态写成当前主体退市
- **AND** 无法确认终态所归属主体／区间时 SHALL 保持当前状态并报告待核冲突，SHALL NOT 仅凭同码或单一listed_date推断同一主体

#### Scenario: One announcement contains A shares B shares and bonds
- **WHEN** 康佳公告同时列出000016、200016及公司债券代码
- **THEN** A股治理 SHALL 仅对本地本任务 A股 `000016.SZ` 应用相符证据
- **AND** SHALL NOT 创建 `200016.SZ` 作为新 A股，亦不得停用同公司债券或其他类别证券

#### Scenario: B share is absent from an A share current list
- **WHEN** 本地已有B股证券，但当前读取来源只覆盖A股
- **THEN** 系统 SHALL NOT 把该B股缺失作为本次A股退市候选或连带写入对象

#### Scenario: Similar symbol belongs to another market or type
- **WHEN** 证券代码与本地指数、基金、HKEX证券或不同交易所代码相似
- **THEN** 系统 SHALL 保持这些对象及其生命周期不变

### Requirement: List coverage and positive terminal evidence have distinct semantics
系统 SHALL 区分来源成功、合法无匹配、缺页／缺板块、不可解析和访问失败；仅完整覆盖所声明范围的 current 名单才能生成该范围的缺失候选，SHALL NOT 用非空或简单行数阈值替代覆盖检查。

#### Scenario: Current snapshot is empty or incomplete
- **WHEN** current 名单为空、缺已请求板块、缺已知分页、返回错误HTML或发生批量解析失败
- **THEN** 系统 SHALL 报告该范围覆盖不足并保持无独立终态证据证券的既有状态
- **AND** SHALL NOT 仅凭当前名单缺失或简称带退批量写退市

#### Scenario: Independent positive evidence remains usable
- **WHEN** current 名单或终止名单其他页失败，但一个正式终态记录的来源、身份和完成事实可独立验证
- **THEN** 系统 SHALL 允许按该正证据处理其明确证券
- **AND** SHALL 保留来源部分失败的降级结果，不将未取得记录的其他证券视为已核验

#### Scenario: No matching terminal record is found
- **WHEN** 本地active证券在完整current名单中缺失，但本轮没有可靠终态正证据
- **THEN** 系统 SHALL 记录未确认候选并保持其生命周期不变
- **AND** SHALL NOT 把“未取得终态证据”解释为已退市或已证实正常在市

### Requirement: Delisting completion and effective date require explicit evidence
系统 SHALL 区分申请、决定、公告与实际摘牌／终止上市生效时间；SHALL 使用上海执行业务日判断当前运行态，不得以历史或未来任务日期提前修改当前运行态。

#### Scenario: Decision announcement predates actual delisting
- **WHEN** 正式决定日期为2026-10-16、公告日期为10-17、实际摘牌日期为10-23，而执行业务日期为10-17
- **THEN** 系统 SHALL 保持未生效的退市决定为未来计划，SHALL NOT 提前将股票设为delisted
- **AND** 到10-23首次current治理时 SHALL 在股票池解析前使用10-23作为生效日重新判断

#### Scenario: Effective day arrives inside the ordinary freshness window
- **WHEN** 10-22取得10-23生效的可靠正式计划，而10-23首次current治理仍处于普通48小时fresh窗口
- **THEN** 系统 SHALL 在股票池解析前重判并通过同一owner应用已到期计划，或执行必要实际刷新后再解析股票池
- **AND** SHALL NOT 直接复用“尚未生效”的旧active结论，audit模式则 SHALL 只输出预期变更

#### Scenario: Completed delisting has no exact date
- **WHEN** 可靠正式材料明确该证券已完成退市，但未提供可用实际日期
- **THEN** 系统 SHALL 允许确认终态并保留已有实际退市日或NULL
- **AND** SHALL 标明日期未知，SHALL NOT 用公告日、决定日、同步日或最后行情日补造日期

#### Scenario: Future completion has no exact date
- **WHEN** 材料仅说明将终止上市或将摘牌，且尚不能证实已经完成
- **THEN** 系统 SHALL 保留待确认，SHALL NOT 把未知日期视为已经生效

#### Scenario: Risk suspension and commercial delisting are not terminal stock evidence
- **WHEN** 输入仅为ST、停牌、退市风险、申请／受理、股东会通过、事先告知或商业资产挂牌摘牌
- **THEN** 系统 SHALL NOT 据此确认该股票已退市

#### Scenario: BSE announcement dates follow the same financial meaning
- **WHEN** 既有BSE公告确认路径处理公告日不同于实际摘牌日的记录
- **THEN** 系统 SHALL 保留原BSE来源路径并按实际生效日或明确已完成但日期未知规则处理
- **AND** SHALL NOT 无条件将 `published_at` 写为 `delisted_date`

### Requirement: Terminal writes preserve evidence and cannot silently reactivate
正式终态 SHALL 通过现有生命周期owner一致写入 `status=delisted / is_active=false / trading_status=0`，并保留最小必要正式来源证据；普通current名单或元数据刷新 SHALL NOT 擦除已确认终态。

#### Scenario: Evidence and status are written together
- **WHEN** 生产治理成功确认并写入终态
- **THEN** 系统 SHALL 在同库事务或既有等价可恢复机制下保存状态和必要证据引用
- **AND** 证据 SHALL 可定位来源URL或公告ID、证券身份、抓取时间、hash、parser版本及生效日或日期未知
- **AND** 证据保存或生命周期写入失败 SHALL NOT 被报告为完整成功

#### Scenario: Current metadata refresh follows terminal confirmation
- **WHEN** 后续官方或备源current行及其metadata仍称该证券active
- **THEN** 系统 SHALL 保留已确认终态及其证据，并维持三字段一致
- **AND** 此保护 SHALL 同时覆盖已知退市日与NULL退市日终态

#### Scenario: Same evidence is processed again
- **WHEN** 相同证券和正式证据被重复处理，或后续证据没有日期
- **THEN** 系统 SHALL NOT 重复计为新增停用，也不得清空已有退市日期
- **AND** 可靠证据补足原未知日期 SHALL 可与新增停用分别报告

#### Scenario: Fallback terminal fields reach the ordinary production cache
- **WHEN** official来源失败，BaoStock单独返回past outDate或delisted字段，并进入正常生产模式的名单cache/upsert路径
- **THEN** 系统 SHALL 在首次业务写入前应用官方终态证据约束，保留已有证券生命周期，仅将未确认终态作为交叉核对诊断
- **AND** 本地不存在的历史退市候选 SHALL NOT 因raw fallback行被创建为正式delisted或新active
- **AND** SHALL NOT 先提交raw fallback终态，再依赖后置manager核对补救

### Requirement: Official evidence conflicts are visible and do not depend on write order
系统 SHALL 按身份、证据明确性及生效语义处理冲突，SHALL NOT 按来源调用先后覆盖生命周期。

#### Scenario: Stale current list conflicts with effective terminal evidence
- **WHEN** 普通官方current行仍含该证券，但有确属当前主体及同一上市生命周期且已生效的明确正式终态
- **THEN** 系统 SHALL 保持或写入正式终态并报告冲突，SHALL NOT 用current行自动复活

#### Scenario: Two exact effective dates conflict
- **WHEN** 同一证券两个正式证据提供不同实际生效日，且无法确认明确更正关系
- **THEN** 系统 SHALL 保留既有生命周期和双方证据并报告待核冲突
- **AND** 对尚未确认的对象 SHALL NOT 猜日期后写入终态

### Requirement: Reconciliation results remain visible across reports and reuse
现有治理结果 SHALL 按市场展示来源覆盖、核对数、正式确认数、成功新增停用数、日期补足、未确认、未来生效、冲突、来源错误、写入失败及有限样例；SHALL 区分证据结论与实际写入成果。

#### Scenario: Unresolved candidates remain after current list refresh
- **WHEN** current行更新成功但仍存在终态未确认、未覆盖、来源失败、冲突或写入失败
- **THEN** 相应治理范围 SHALL 保持warning或error及原因
- **AND** 业务任务按配置继续时 SHALL 保留治理降级与continuation决定

#### Scenario: Samples are truncated
- **WHEN** 核对证券多于报告样例限额
- **THEN** 系统 SHALL 仅截断样例，完整处理及去重总数 SHALL NOT 被截断
- **AND** 来源或写入失败 SHALL NOT 计入成功新增停用

#### Scenario: Another exchange succeeds
- **WHEN** 一交易所来源失败，而另一交易所具有独立可靠终态证据
- **THEN** 系统 SHALL 独立处理并报告各市场结果，SHALL NOT 以整体成功覆盖失败市场

### Requirement: Future terminal decisions use a fixed durable protocol
未来计划和核验结果 SHALL 仅复用现有 `instrument_master_discrepancies`，reason分别为 `a_share_terminal_future_effective`、`a_share_terminal_future_applied`、`a_share_terminal_verification`；载荷 SHALL 显式标明 `scope=a_share_stock` 与 `mode=production|audit_only`，SHALL NOT 在metadata另建计划副本或新增队列表。计划 SHALL 保存稳定plan_key、当前证券主体／上市区间、effective_date、verified_at及正式来源证据；应用闭合 SHALL 引用相同plan_key及applied_at；核验结果 SHALL 按交易所保存实际verified_at、覆盖／状态及未决／失败摘要，instrument_id为NULL。已确认终态证据 SHALL 继续写受保护的 `metadata.a_share_terminal_evidence`。

#### Scenario: Future decision survives a restart and current metadata refresh
- **WHEN** 生产治理保存未来正式计划后进程重启，或普通current metadata被刷新
- **THEN** fresh gate SHALL 从差异表恢复请求市场的生产未应用计划及最新生产核验结果，在股票池解析前处理已到期计划
- **AND** 读取 SHALL 按scope／mode／交易所过滤，不受报告样例上限影响

#### Scenario: Applied plan receives another observation
- **WHEN** 相同稳定plan_key重复取得future_effective记录，且已有对应生产applied
- **THEN** 系统 SHALL 保持计划已应用，SHALL NOT 重开计划或重复计新增停用
- **AND** plan_key SHALL NOT 依赖抓取时间、行号或整个文件hash

#### Scenario: Applying a plan fails
- **WHEN** 已到期计划的生命周期、终态证据或应用闭合写入失败
- **THEN** 系统 SHALL 让同一quotes.db事务失败而不追加生产applied，保留可重试计划并报告降级
- **AND** SHALL NOT 用自行提交的独立差异表会话冒充原子应用

#### Scenario: Plan reading fails or production verification is absent
- **WHEN** 计划读取／JSON解析失败，或尚无有效生产核验基线
- **THEN** 系统 SHALL 将其与成功读取后的空计划集合区分，SHALL NOT 据空列表返回healthy fresh
- **AND** SHALL 实际刷新或遵循既有continuation显式降级；最新失败核验 SHALL NOT 被较旧成功结果掩盖

### Requirement: Audit uses the same decisions without business lifecycle writes
`audit_only` SHALL 使用同一证据与生效规则输出预期变更，但不得修改instruments、正式终态metadata、财务状态或业务事实。

#### Scenario: Audit identifies Konka delisting
- **WHEN** audit读取康佳正式终态证据
- **THEN** 系统 SHALL 报告would-deactivate及准确生效日，而生产生命周期与财务记录保持不变
- **AND** 允许的审核运行／差异记录 SHALL NOT 冒充生产终态写入或刷新成功证据

#### Scenario: Audit fetches a current list without terminal candidates
- **WHEN** audit通过真实stock policy和source factory读取当前名单，即使没有退市候选
- **THEN** 系统 SHALL 将只读约束传到名单cache及current metadata保存路径，禁止隐式upsert instruments或业务metadata
- **AND** SHALL NOT 仅通过跳过最后一个终态writer来宣称audit无业务写入

#### Scenario: Audit observes a production future plan
- **WHEN** audit读取到期生产计划或保存同类审计记录
- **THEN** 系统 SHALL 只报告预期变更，并将审计记录显式标记 `mode=audit_only`
- **AND** 审计记录 SHALL NOT 闭合生产计划、追加生产applied或成为生产fresh核验基线
