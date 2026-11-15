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

### Requirement: Terminal evidence is matched to the exact in-scope security
系统 SHALL 使用证券代码、交易所、证券类型及来源范围匹配终态，仅修改已有的本任务 A 股主数据；SHALL NOT 按公司简称或同公告中的其他证券代码传播退市状态。

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
- **AND** 到10-23或其后实际刷新时 SHALL 使用10-23作为生效日重新判断

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

### Requirement: Official evidence conflicts are visible and do not depend on write order
系统 SHALL 按身份、证据明确性及生效语义处理冲突，SHALL NOT 按来源调用先后覆盖生命周期。

#### Scenario: Stale current list conflicts with effective terminal evidence
- **WHEN** 普通官方current行仍含该证券，但有同身份且已生效的明确正式终态
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

### Requirement: Audit uses the same decisions without business lifecycle writes
`audit_only` SHALL 使用同一证据与生效规则输出预期变更，但不得修改instruments、正式终态metadata、财务状态或业务事实。

#### Scenario: Audit identifies Konka delisting
- **WHEN** audit读取康佳正式终态证据
- **THEN** 系统 SHALL 报告would-deactivate及准确生效日，而生产生命周期与财务记录保持不变
- **AND** 允许的审核运行／差异记录 SHALL NOT 冒充生产终态写入或刷新成功证据
