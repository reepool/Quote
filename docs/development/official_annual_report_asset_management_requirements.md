# 官方公告资产管理一期需求文档

> 状态：实施中（当前进度以 OpenSpec 任务清单为准）
> 版本：V1.0
> 日期：2026-08-10
> OpenSpec 变更：`establish-shared-announcement-asset-management`

## 1. 文档目的

本文定义一个独立于公司画像、券商风控和其他业务模块的“官方公告资产管理”能力。第一期只正式管理 A 股完整年报及其完整修订版，负责公告发现、元数据登记、附件下载、完整性校验、有效版本选择、文件保存、旧版本删除、历史回补、日常更新、按需获取、备份和审计。

该能力是公共依赖，不是公司画像的子功能。公司画像、券商风控及未来业务只消费共享资产，并继续拥有各自的解析器、派生文件、事实、审核和业务状态。

本文用于统一产品、研发、测试和运维口径；当前变更已经进入实施阶段，但是否完成必须以 OpenSpec 任务、测试和上线证据为准。

## 2. 背景与问题

当前项目已有以下基础：

- `research.announcements` 提供来源中立的公告发现、路由、规范化记录、保守游标和受控附件获取能力。
- 公司画像在 `data/filings/business_profile` 保存正式报告，并通过 `AnnualReportAssetCatalog` 暴露部分复用能力。
- 券商风控在 `data/filings/financial_statements/broker_risk_control` 独立保存年报、半年报，并维护自己的下载和解析状态。
- DataManager 已有 `get_annual_report_assets` 和 `get_annual_report_asset`，但其底层仍投影公司画像 manifest，不是真正业务中立的资产仓库。

现状存在四个结构性问题：

1. 公告原件的所有权依赖公司画像，而公司画像仍在开发和上线准备阶段，不能作为其他业务获取公告的前置条件。
2. 公司画像和券商风控可能重复发现、下载并保存同一份年报，未来新增业务会进一步放大网络、存储和一致性成本。
3. 附件是否有效、某个 parser 是否成功、某类业务事实是否可用被混在业务 manifest 中，无法支持一个原件被多个独立消费者复用。
4. 当前数据库备份不等于附件备份；`data/filings` 重新挂载后容量充足，但仍需独立的文件备份、空间门槛和恢复流程。

## 3. 目标

### 3.1 一期目标

- 建立独立配置、独立存储、独立调度、独立状态和独立 API 的公告资产模块。
- 覆盖当前活跃的上交所、深交所和北交所 A 股股票。
- 历史回补时，只为每只股票保存其最新可得财务年度的一份最有效完整年报。
- 日更时发现新年报和完整修订版，并主动下载当前有效附件。
- 同一股票、同一财务年度最终只保留一份最有效物理附件。
- 发现完整修订版时，在新附件校验和切换完成后删除无其他引用的旧附件。
- 所有业务通过统一的 local-first 接口查找或获取年报，不再自行实现下载和业务私有原件归档。
- 迁移时优先登记和复用 `data/filings` 下已有有效年报，避免重复下载和破坏性搬迁。
- 通过 DataManager、FastAPI 和现有业务 API 响应有效暴露资产可用性、获取进度和来源血缘，供 AI/API 调用方直接使用。
- 建立空间、完整性、重试、备份、删除和恢复治理。

### 3.2 可扩展目标

数据模型和服务边界应支持未来扩展半年报及其他公告类型。新类型可以按策略选择：

- 只同步元数据，业务需要时下载；
- 对特定股票池主动下载；
- 对全市场主动下载；
- 采用与年报不同的有效版本和保留规则。

一期不得为了尚未明确的公告类型削弱年报规则，也不得默认下载所有公告附件。

## 4. 非目标

- 一期不全量下载半年报、季报、临时公告或所有公告附件。
- 不把公司画像 PDF 分页、章节提取、LLM 语义分析、人工审核或事实提升迁入公共资产模块。
- 不把券商风险控制表解析和金融事实写入迁入公共资产模块。
- 不以 PDF 年报替代已经存在的官方结构化财务 JSON、XBRL 或数值事实来源。
- 不根据标题相似或文件 hash 相同，擅自合并两个法律公告身份。
- 一期不保证修订发生前旧 PDF 的本地历史时点重放；旧记录和 hash 保留，但按用户要求删除无引用的旧物理文件。
- 不在应用启动期间自动运行全市场扫描、批量下载、文件迁移或删除。
- 本项目按设立目标仅提供 API，不要求也不依赖独立 Web 前端；本期提供 AI/API 调用方可直接消费的 DataManager/API/状态契约。

## 5. 核心术语与口径

| 术语 | 定义 |
| --- | --- |
| 法律公告 | 由官方来源发布、以 `source + source_announcement_id` 唯一标识的公告记录 |
| 附件观察 | 某一法律公告下的某个附件身份；优先使用官方附件 ID，否则使用规范化来源 URL 的确定性身份 |
| 物理 Blob | 由 SHA-256 标识的一组附件字节；相同字节可以只保存一次 |
| 完整年报 | 公司的主要中文完整版年度报告 PDF，不含摘要、英文版、图解版、审计报告或其他相关材料 |
| 完整修订版 | 针对同一股票和财务年度重新发布的完整、可独立阅读的修订或更正版年报 PDF |
| 修订通知 | 仅说明更正内容、替换页或差异，未附完整修订年报；不能替换有效年报 |
| 财务年度 | 年报所覆盖的会计年度，例如报告期 `2025-12-31` 对应财务年度 2025 |
| 最有效年报 | 同一股票和财务年度中，按本文规则选出的唯一当前有效完整年报 |
| 本地可用 | 资产记录存在，文件可读，PDF 签名、长度和所需 hash 校验通过 |
| 发现完成 | 指定来源、市场、类别和日期窗口内的所有分页均在限制内完整处理，可推进游标 |
| 附件就绪 | 目标有效附件已经下载或复用，并通过完整性校验；与发现完成分别计量 |
| 消费者 | 公司画像、券商风控或未来任何使用公告原件的业务模块 |

跨 provider、共享资产层和业务适配器使用一套正交分类词汇：`document_family=annual_report|semiannual_report|...` 表示报告家族，`variant=original|correction` 表示同一家族内的原版或完整修订版，`is_full_report` 表示附件是否为可独立阅读的完整正文。`correction` 不得作为与 `annual_report` 并列的报告家族；仅含差异说明的修订通知属于 correction evidence，不得伪装为 `variant=correction` 的完整年报。provider 原始类别只通过版本化映射进入上述词汇，不能直接成为业务判断。

## 6. 范围与关键假设

### 6.1 默认股票范围

本文一期所称“A 股全市场”或“所有股票”，明确指任务快照时点仍在市的 SSE、SZSE、BSE 人民币普通股全集，不包含已经退市或其他非活跃证券；后者保留受控按需获取能力。如后续要求覆盖全部历史退市证券，必须另行定义历史证券主数据、来源可查边界和回补规模，不得把该目标隐含在一期完成率中。

一期历史回补和日更覆盖本地主数据在任务启动时判定为：

- `type=stock`；
- `is_active=true`；
- 交易所为 SSE、SZSE 或 BSE；
- 证券为人民币计价 A 股，包含主板、科创板、创业板、北交所、ST/*ST 和停牌但未退市股票；
- 排除沪深 B 股、基金/ETF、债券、指数、存托凭证及其他非 A 股证券类型。

以上规则必须形成可版本化的 eligibility policy。退市、非活跃和范围外证券不进入默认全市场回补，但允许通过受控的按需接口获取指定年报。股票范围必须在每次历史回补启动时生成包含 instrument identity、policy version、主数据版本、主数据最近成功更新时间和快照时间戳的快照，避免执行期间主数据变化造成覆盖分母漂移。

本地主数据自身不能证明“没有漏掉整行证券”。每次正式 bootstrap 和日更 universe refresh 必须取得一个独立、可审计的在市证券全集证据，例如交易所官方证券清单或经批准的等价 census，保存来源、交易所、查询边界、完整性水位/版本、获取时间和原始内容 hash，并与本地主数据按规范化证券身份做差异对账。census 中存在但本地主数据缺失、身份无法映射或资格字段不完整的证券必须进入 `eligibility_indeterminate`，不得静默从分母消失；census 本身不完整、过期或任一目标交易所缺失时不得宣称全市场完成。

主数据或 census 刷新失败时必须保留上一份成功且完整的配对快照，不得用空集或部分结果覆盖。配置必须分别定义最大允许陈旧时间；证券关键资格字段缺失或冲突时进入 `eligibility_indeterminate` 明细而不是静默排除。配对快照超过 freshness 门槛、刷新失败且没有可接受旧快照，或仍有无法分类证券时，市场公告发现可以继续，但不得宣称全市场覆盖完成，readiness 必须显示 `degraded/blocked` 及分母不确定数量。

### 6.2 “最新一期”定义

历史回补所称“最新一期”是该股票在来源覆盖范围内已经正式发布的最新财务年度完整年报，而不是按当前自然年机械推算的报告期。新上市且尚未发布首份年报的股票可以进入 `confirmed_missing`，但必须记录搜索边界和证据。

### 6.3 日更后的保留范围

历史回补只补一个最新财务年度。进入日更后，新财务年度年报会新增保存，因此长期可以为同一股票保留多个财务年度；存在合法可用候选时，每个财务年度始终只有一个最有效物理附件，依法撤回且无替代稿等 no-winner 期间则没有 current 附件。

### 6.4 时间口径

- 公告必须保存官方原始发布时间、规范化发布时间、首次发现时间和最后检查时间。
- 业务回测或时点研究使用公告实际可得时间，不得使用报告期替代公告时间。
- 新修订版只能从其发布时间起成为可得资料；当前有效查询返回修订版，历史知识截止查询如果旧文件已按策略删除，应明确返回“元数据可追溯但原件不在本地”，不得退回当前修订版冒充历史资料。
- 同一公告 ID 下的附件内容更新、撤回或状态回填必须为每次 observation 保存 `version_available_at` 及时间来源/精度。优先使用官方生效时间；来源不提供时使用首次观察时间。知识截止和业务 `data_available_date` 按 observation 的 `version_available_at` 判断，不得沿用父公告较早的发布时间把后来才出现的字节或撤回状态回填到历史。

## 7. 总体架构与职责边界

```text
官方来源 / provider route
        |
        v
research.announcements
发现、规范化、保守游标、受控字节获取
        |
        v
公告资产管理模块
元数据、分类、版本决策、下载、校验、存储、备份、删除审计
        |
        +-------------------+--------------------+
        v                   v                    v
公司画像 parser       券商风控 parser        未来消费者
业务事实/派生物        金融事实/manifest       各自业务状态
```

### 7.1 公告资产模块负责

- 通过现有 provider 契约发现公告和附件。
- 维护法律公告、附件、物理 Blob 和有效年报身份。
- 集中分类完整年报和完整修订版。
- 选择同一股票、财务年度的最有效附件。
- 下载、校验、原子发布、完整性审计和必要时重新获取。
- 管理日更游标、回补进度、下载 lease、重试和 durable operation。
- 管理替换关系、retention pin、旧文件删除和删除审计。
- 提供 local-first 查询与 ensure。
- 提供 DataManager、API、可观测性、空间和备份状态。

### 7.2 `research.announcements` 继续负责

- 具体来源的请求、参数、路由、fallback 和限流治理。
- 公告列表和附件信息规范化。
- provider 能力和分类路由。
- 安全、受控的附件字节获取。

公告资产模块不得重新硬编码 CNInfo 或交易所 URL、栏目、plate、orgId、headers、TLS 或代理规则。

### 7.3 业务消费者继续负责

- 选择需要处理的业务范围。
- PDF 文本、表格、章节、语义和业务事实解析。
- parser 版本、参数 hash、派生文件和处理状态。
- 业务审核、事实批准、提升和对外业务响应。
- 当有效资产发生替换时，按业务策略重跑或标记旧结果失效。

业务 parser 失败不得把共享资产标记为损坏；共享资产损坏也不得被业务重试状态掩盖。消费者未启用、排队失败、parser 失败或 checkpoint 落后只能改变该消费者自己的 continuation、processing 和 readiness，不得回退已经提交的资产发现水位、effective winner、asset operation 终态或资产层 readiness，也不得阻止其他消费者读取同一有效资产。

## 8. 数据模型需求

具体表名可以随现有 `research/storage.py` 约定调整，但语义必须分层。

### 8.1 `official_announcements`

至少包含：

- `announcement_id`：内部稳定 ID；
- `source`、`source_announcement_id`：法律公告唯一键；
- 标题、股票代码、交易所、来源类别；
- 原始和规范化发布时间；
- 首次发现、最后发现、原始 payload hash；
- provider 诊断、状态和创建/更新时间。

同一来源公告重复发现必须幂等更新观察信息，不得产生重复法律公告。

### 8.2 `official_announcement_attachments`

至少包含：

- 内部 attachment ID、所属法律公告 ID；
- 来源附件 ID 或规范化 URL identity；
- 原始 URL、文件名、媒体类型提示、Content-Length 提示；
- 首次/最后观察时间和当前元数据；
- 不直接承载 parser 业务状态。

### 8.3 `official_document_blobs`

至少包含：

- SHA-256 唯一内容身份；
- 内容长度、PDF 签名状态、完整性状态；
- 受控 canonical path；
- 首次下载/采用时间、最后校验时间；
- 可事务查询的 retention-pin 关系，不使用可能漂移的手工引用计数；
- 备份状态和最近验证时间。

同一内容可被多个法律附件引用，但不能因此合并法律公告身份。

### 8.4 `official_attachment_versions`

记录一次附件内容观察与获取证据：

- attachment ID、blob hash、最终 URL；
- 下载尝试、响应证据、长度和 hash；
- observation version、首次/最后观察时间、`version_available_at`、时间来源和时间精度；
- 有效、损坏、缺失、超限或重试状态；
- lease、attempt、next_retry_at、错误摘要；
- 临时路径不得成为有效资产路径。

### 8.5 `effective_annual_reports`

唯一业务键为 `instrument_id + fiscal_year`，至少包含：

- report period、所选公告/附件/version/blob；
- `variant=original|correction`；
- `is_full_report`、classifier version 和理由；
- 当前有效状态、前任 effective asset、激活时间；
- 最后检查时间和版本决策证据。

该表表示当前最有效选择，不替代公告和附件历史表。只有存在合法、完整且可用的候选时才保留一行 current projection；若当前有效公告被依法撤回且没有合法替代稿，必须清除 current projection 或写入显式 no-winner tombstone，并由不可变决策历史保留该期间为何没有 winner 的证据。

### 8.6 `official_asset_operations`

用于历史回补、日更、按需获取、迁移、完整性修复和备份：

- operation ID、类型、scope、幂等键；
- operation status：`queued/running/completed/missing/failed/blocked/cancelled`；`expired` 仅属于 principal/consumer subscription 查询投影，不是内部 durable operation 状态；
- operation stage：`discovering/reconciling/adopting/downloading/validating/activating/deleting/backing_up/restoring/auditing`；阶段枚举必须版本化，未适用阶段不得伪造为 `completed`；
- batch outcome：`success/partial/blocked/failed`，不与单资产 operation status 混用；
- checkpoint、lease、attempt、next_retry_at；
- 请求边界、进度计数、最后心跳、错误摘要和最终结果。

另需有 principal/consumer scoped 的 operation subscription 或等价查询句柄：底层 asset operation 按 normalized scope + policy 全局 single-flight，scheduler、API 和业务消费者可共享同一次实际获取；外部调用者的幂等键、查询权限、脱敏结果和业务 continuation 则绑定自己的 subscription，不得继承其他 principal 的句柄、诊断或处理任务。

### 8.7 删除和消费者审计

- `official_asset_deletion_audit`：追加式保存旧/新 asset、hash、路径、引用判断、删除原因、任务/操作者和时间。
- `official_asset_deletion_intents`：持久化 `planned -> deleting -> deleted|failed` 物理删除状态、lease/generation、重试和最后错误，和追加式审计分离；recovery pin 必须有 `blocks_primary_unlink` 与 `required_set_hold` 转换状态，避免主卷删除门槛与备份 required-set 保留互相矛盾。
- `official_asset_recovery_manifest`：在任何普通修订前任、撤回无替代稿的 predecessor 或 legacy duplicate 从主卷 unlink 前，永久保存 manifest kind、旧 asset/法律 filing/attachment、旧路径、content hash、可为空的 replacement asset、备份对象、已验证 file watermark 与预留 `recovery_pair_id`；无替代稿使用 `manifest_kind=withdrawal_tombstone`，V1 条目不可变且永久 active，作为 required-blob 枚举和恢复依据。
- `official_asset_recovery_pair_closures`（或等价追加式记录）：在包含上述不可变 manifest 的 catalog snapshot 生成后，绑定 `recovery_pair_id`、catalog snapshot identity/hash 与 file watermark。manifest 不得通过回写来伪造闭合；只有 closure 完成才可转换 recovery pin 并授权主卷 unlink。
- `official_asset_backup_recovery_journal`（或等价独立增量介质）：在独立备份故障域保存 paired snapshot 之后、声明 RPO 所需的 catalog/outbox/operation/lineage/audit 增量顺序、完整性 hash、前任/覆盖水位和截断证据，使灾难恢复能够实际重放或证明 write-freeze 后没有增量。
- `official_asset_consumer_processing`：可选公共索引，保存 `asset_id + consumer + parser_version + parameter_hash`，但业务自己的 manifest 仍是派生输出的权威记录。
- 前台业务命令还需持久化 principal-scoped `consumer_request_id`（或等价 opaque handle），关联调用者、consumer、processing fingerprint、pending continuation 和最终 consumer operation；外部状态查询不得暴露内部 consumer operation ID。
- `official_asset_change_events`（或等价 outbox）：资产新增、替换、修复、撤回和删除生成不可变单调事件；有效版本切换事件与 effective decision、replacement edge，以及本次转换适用的 deletion intent 或 same-hash non-unlink audit 同事务提交。
- `official_asset_consumer_checkpoints`（或等价投递状态）：按 consumer 持久化最后已处理事件、幂等键、尝试和错误，使离线消费者可重放且不会漏掉修订失效/重算。

事件“必须持久化并可重放”不等于“必须启动所有业务 parser”。事件需保存 trigger origin 和 dispatch-policy version：generic ensure 永不创建 consumer operation；`added/repaired` 只有显式 consumer continuation 或 scheduler dependency policy 才可触发业务处理；`replaced/withdrawn/deleted` 必须使证据范围被该事件改变的 lineage 失效，但是否自动重跑仍由对应 consumer policy 决定。默认 effective-period 结果随有效资产变化而失效；精确 observation pin 或 knowledge-cutoff 结果只有在事件落入其 selector/cutoff 证据范围时才失效。

## 9. 年报分类与最有效版本规则

### 9.1 分类要求

分类器必须确定性、可版本化、可离线测试并默认 fail closed。provider 类别只用于缩小流量，不能替代本地分类。

可成为有效年报的对象：

- 主要中文完整年度报告；
- 明确针对同一财务年度发布、包含完整正文的更正或修订年度报告。

不能成为有效年报的对象：

- 年报摘要；
- 英文版、图解版、可视化版；
- 审计报告、鉴证报告；
- 年报问询函、回复、说明会材料；
- 仅包含差异、改动说明或替换页的更正公告；
- 半年报、季报；
- 无法可靠识别财务年度或股票身份的附件。

### 9.2 版本优先级

同一股票和财务年度按以下顺序选唯一 winner：

1. 已验证的完整修订版优先于已验证原版；
2. 多个完整修订版之间，以规范化公告发布时间最新者优先；
3. 同一来源、同一法律公告链内发布时间相同且内容一致时，才可使用稳定公告/附件 ID 做确定性排序；
4. 未下载、下载失败、非 PDF、长度或 hash 校验失败的候选不能成为 winner；
5. 仅发现修订通知时继续保留原版有效。

跨来源候选只有在内容 hash 相同或存在经过配置审计的官方镜像映射时，才能自动判为等价。等价候选必须保留完整、排序稳定的 `equivalent_source_filings` 证据集；单值 `canonical_source_filing` 只能由版本化 projection policy 从该集合确定，且不得依赖发现顺序。有效资产和 consumer lineage 必须同时保存 projection policy version 与 evidence-set hash，后续新增等价镜像不得无审计地改变既有处理身份。不同来源对同一股票、同一财务年度给出不同完整 PDF 且无法证明法律优先关系时，必须 `ambiguous/blocked`，不得按 source 名称或 filing ID 字符串任意决胜。

若 provider 不提供可信 content hash 或官方 mirror/precedence 证据，系统可按版本化策略执行有界临时候选字节验证以证明等价或冲突；“只下载 winner”在此指只把 winner 发布并长期保留为 canonical 有效附件，不禁止为确定 winner 读取受限临时候选。非 winner 临时字节不得进入有效 Blob/coverage，必须遵守 mount、大小和 reservation 门槛；其 hash、长度、获取证据、验证 policy 和清理结果写入不可变 attachment-observation/version 证据后，临时字节必须删除或进入受管 quarantine，不能形成同财年的第二份长期附件。

同一来源的两个完整修订版如果规范化发布时间相同但内容 hash 不同，只有 provider 明确的 replacement edge、官方 revision sequence 或其他版本化法律优先证据可以决胜；否则同样必须 `ambiguous/blocked`。系统必须保留原始发布时间和时间精度，不能用 filing/attachment ID 的字典序推断先后。

分类和选择必须落到 attachment 级。一个公告同时包含完整中文报告、摘要、英文版和附录时，只允许完整中文正文参与 winner 选择。来源若撤回、取消公告，或在相同公告 ID 下静默更新附件，系统必须保存新的 observation 并重新评估；撤回/取消只有在 provider 显式状态、被撤公告或附件 ID、官方 replacement/withdrawal relation，或版本化确定性规则能够绑定到具体候选时才生效，仅凭通用标题关键词不得撤销当前资产。目标不确定时只登记 evidence 并进入 `ambiguous`。撤回当前有效修订版时，仅在可以证明前任仍合法、完整且物理可用时安全回退；若没有合法替代稿，必须在同一串行化决策边界提交 `withdrawn_without_replacement`、清除 current projection 或写入 no-winner tombstone，并使该期间进入 `blocked`，不得继续把已撤回资产作为 current 或通过公共内容接口流出。

首次历史回补若发现法律上更新的完整修订版但无法下载或验证，不得把原版宣称为最终 latest-effective。该股票应为 `blocked/retryable`。已经在线服务且原版仍有效时，可继续提供原版，但必须标为 `provisional` 或“存在待验证修订”，直到修订问题收敛。V1 默认禁止为该默认 effective-period 新启 consumer processing，并把既有默认 effective-period consumer result 投影为 `stale`、reason=`pending_correction`，不得进入要求 current 事实的 DCF；精确 observation pin 和 knowledge-cutoff 结果仍按各自证据范围判断。该默认策略及版本必须进入配置指纹，改变策略需要显式版本迁移和回归证据。

### 9.3 修订替换事务

必须按以下顺序执行：

1. 登记新公告和附件元数据；
2. 取得附件级 lease；
3. 下载到 `.part` 临时文件；
4. 校验来源身份、PDF 签名、长度、SHA-256 和目标路径安全性；
5. 原子发布新 Blob；
6. 在同一数据库事务中重新选择 winner，以 `instrument_id + fiscal_year` decision lease 或 row-version CAS 激活修订版、记录替换边并追加不可变、可重放的 change-event outbox；只有 predecessor 与 replacement 是不同物理 hash 时才写入带 recovery-retention pin 和预留 `recovery_pair_id` 的 `planned` 删除意图。同 hash 的不同法律 filing 只更新法律替换/引用并记录 `not_applicable_shared_blob` 等非 unlink 审计结果，不得为仍由 winner 使用的 Blob 创建物理删除意图；并发较旧修订后完成时不得反向降级有效版本或为真实 winner 建立删除意图；
7. 事务提交后投递 outbox 事件，将依赖旧 asset 的业务处理标记为 superseded 或入重跑队列；投递失败必须从持久化 outbox 重试，不能丢失事件；
8. 解除旧有效附件对旧 Blob 的活动引用；
9. backup required-set 必须枚举所有 `planned|deleting` 删除意图中的 predecessor，即使 recovery manifest 尚未提交；在 predecessor 和 replacement 均完成独立故障域备份后，先写入绑定预留 pair ID 和 file watermark 的不可变 `official_asset_recovery_manifest`，再生成包含该 manifest 的 catalog snapshot，最后写入追加式 recovery-pair closure；
10. 仅在所有非 recovery retention pin 释放、替换记录完整、备份/删除前提满足且 recovery-pair closure 已验证时，将 recovery pin 转换为 `required_set_hold`，再把意图置为 `deleting` 并删除旧文件；
11. 成功后写 `deleted`，失败写 `failed` 和可重试诊断；finalize 前必须复核 operation 捕获的同一批准 mount identity/source/read-write 状态并在该挂载上确认路径不存在，不能因 fallback/其他挂载上的路径缺失误报成功；追加式审计在任何阶段都不得先于实际 unlink 宣称已删除。

新附件校验失败，或第 6 步激活、所需删除意图（同 hash 时为 non-unlink audit）、替换边、outbox 中任一事务内写入失败时，整个事务回滚，旧年报必须继续有效和物理存在。第 6 步事务提交后，新修订版是唯一 effective winner；后续事件投递、消费者失效、备份或文件删除失败只能形成可重试的 outbox/`planned/deleting/failed` 清理和消费重算状态，不得把 effective winner 回退到旧版。不得因“发现修订标题”就先删除原件。

依法绑定的撤回也必须使用同一 decision/outbox 事务边界。有合法本地前任时按普通 predecessor/replacement 流程回退；没有合法替代稿时，事务写入 `withdrawn_without_replacement` 决策和 withdrawn outbox、清除 current projection/写入 no-winner tombstone，并创建 replacement identity 为空、预留 recovery pair ID 的单 predecessor 删除意图。该 predecessor 在主卷 unlink 前必须完成独立故障域备份、不可变 `withdrawal_tombstone` manifest、包含它的 no-winner catalog snapshot 和追加式 pair closure；不得伪造 replacement Blob。

## 10. 历史回补需求

### 10.1 业务范围

历史回补是一次可重复、可恢复的 latest-only bootstrap：

- 分母为任务启动时快照中的活跃 SSE/SZSE/BSE 股票；
- 每只股票只目标化其最新可得财务年度；
- 该年度存在完整修订版时只保留最新有效修订版；
- 先复用现有文件，再执行网络下载；
- 回补完成后才进入正式日更阶段。

### 10.2 建议执行阶段

1. 冻结股票范围快照和来源能力版本。
2. 只读盘点现有公司画像和券商年报文件、manifest、hash 和报告期。
3. 采用可验证的现有文件，建立 shadow 资产记录，不移动、不删除。
4. 对当前及上一披露季做按市场、年报类别的日期窗口扫描。
5. 聚合每只股票已发现的最新财务年度和候选修订。
6. 只下载 winner；本地有效则零网络复用。
7. 对未覆盖股票进入轮换式定向修复队列，按有限年度窗口向前搜索。
8. 对每只股票分别给出 latest winner fiscal year、asset availability（`available/confirmed_missing/retryable/blocked`）、expected-period coverage（`not_due/current/overdue_missing/incomplete`）和终态/重试证据。
9. 只有覆盖账本完整且无伪完成窗口时，bootstrap 才可标记完成。

回补必须固定 `as_of` 和来源/查询配置指纹。版本化财年边界策略以 `as_of`、项目时区、财年末、上市日、provider coverage start、有限 lookback 和披露日历为输入，输出 candidate upper year、due year 和 earliest searchable year；日历年 A 股 V1 默认使用次年 4 月 30 日披露边界。未到期的空年度可以继续使用实际已发布的上一年度作为 latest available，但到期仍缺报必须形成显式 overdue coverage gap。上市日晚于报告期末的股票可以形成有证据的 `confirmed_missing`；延期披露、长期停牌或来源部分失败不能据此确认缺失。若较新财务年度窗口扫描不完整，不得降级选择更老财务年度并宣称“最新”。

每只股票必须分开记录 asset availability（例如 `available/confirmed_missing/retryable/blocked`）和 expected-period coverage（`not_due/current/overdue_missing/incomplete`）。来源完整扫描后确认应披露年度仍缺报时，上一年度可以继续作为 `available` 的 latest available asset，但必须同时携带 `overdue_missing`，不能把“有旧资产”误报为“本期覆盖完成”。发现法律上更新但尚不可验证的修订时，原版可以按 provisional 策略保留字节，但该股票 latest-only coverage 必须保持 `retryable|blocked`，不得获得 `available|current` credit。V1 默认允许 discovery-complete 的 bootstrap 结束并启用日更，使延期公告仍能被日更发现；`overdue_missing` 默认使 readiness 降级并持续进入 repair，而不是形成阻止日更自身启动的循环依赖。部署可通过版本化 policy 将其升级为更严格 blocker。

### 10.3 为什么不是逐股全量扫描

约 5,500 只股票逐日逐股查询会产生大量请求和负结果，容易触发限流。历史回补应以市场级、年报类别、日期窗口扫描为主，只对不断缩小的缺失股票集合使用单标的查询。

### 10.4 恢复与幂等

- 窗口、股票覆盖、候选选择和下载分别持久化进度。
- 中断后不得重新下载已经验证的相同附件。
- 相同 scope 的重复任务应复用 operation 或被 lease 阻止。
- `confirmed_missing` 必须记录来源、完整成功的查询边界、上市时间、`as_of`、证据和过期时间，不得仅因一次空响应写入永久缺失。
- 来源暂时失败、页面超限或身份歧义默认只能进入 `incomplete/retryable/blocked`，不能计入该来源游标的成功覆盖。只有版本化 route policy 以审计证据证明 fallback 对完全相同的 source/exchange/category/query scope 构成查询等价替代时，完整 fallback 才可以满足独立的 route-level required-coverage projection；fallback 仍只推进自己的 source-qualified cursor，失败 primary 的 cursor 和诊断 gap 必须保持未完成且可审计。
- 回补只有在不存在 `incomplete/retryable/blocked` 时返回 `success`；仍有这类记录时只能返回 `partial` 或 `blocked`，即使本次运行已经停止。
- `overdue_missing` 只有在 expected-period 搜索完整时才允许与 `success` 并存，并必须使默认 readiness 降级、保留修复资格和逾期证据；未完成的应披露期间仍属于 `incomplete`。

## 11. 每日更新与公告发现机制

### 11.1 独立调度语义

日更任务独立于公司画像和券商风控的开关与运行状态。V1 每日执行：

1. 同步指定窗口内的年报和年报修订元数据；
2. 分类并选出新出现的当前有效完整年报；
3. 对有效年报主动下载、校验和归档附件；
4. 对修订版执行替换和旧附件治理；
5. 发布变更 asset ID 给消费者；
6. 运行小规模缺失股票轮换修复。

因此，V1 不是“只日更元数据、附件一律等业务需要再下载”。元数据和附件阶段在状态上分开，但有效年报附件按本需求主动下载。未来其他公告类型可配置为元数据日更、附件懒下载。

### 11.2 游标维度

发现状态至少按以下 scope 隔离：

- source；
- exchange；
- normalized category=`annual_report`；
- market scope 或 provider route；
- classifier/policy version。

每个 scope 分别保存 provider item cursor、完整时间范围水位 `covered_until` 和 gap；item cursor 不能代替“某个时间范围已完整扫描”的证明。primary/fallback 默认各自保留状态，只有版本化 route policy 明确证明等价替代时，fallback 的完整结果才可补足 primary scope。不能用一个全局游标掩盖某一交易所或来源失败。

bootstrap、daily 和按需发现必须共用同一份版本化 provider capability/route matrix，只生成来源实际支持的 `source + exchange + category` 组合，不得扫描来源与交易所的笛卡尔积并把永久不支持的组合误报为 coverage gap。用于 handoff 的 canonical query-policy fingerprint 必须可由 bootstrap 和 daily 共同计算；如两种作业的执行参数不同，应通过显式、可审计的 handoff mapping 证明覆盖等价，不能依赖手工写入 daily fingerprint。

### 11.3 正常发现窗口

- `window_end` 为本次运行固定 cutoff，任务执行中不得随系统时间移动。
- `window_start` 为上次完整成功提交的 `covered_until` 减 overlap，不使用“最后一条公告发布时间”代替范围水位。
- overlap 默认 3 个自然日，可配置并通过真实 provider 延迟数据验证。
- 重叠窗口内依靠来源公告 ID 和附件身份幂等去重。
- provider item cursor/页游标只允许用于续扫“相同 query fingerprint、相同固定 cutoff、相同父窗口”的未完成工作；开始下一次 overlap 窗口时必须从新的日期范围重新扫描，除非 provider 契约明确证明该游标不会截断 overlap。不得把上一窗口最后一条记录的位置带入新窗口而跳过迟到记录。
- 仅当整个请求窗口的所有分页和必要来源 scope 完整成功，才把 `covered_until` 原子推进到固定 cutoff；完整空窗口也必须推进，避免反复扫描同一空区间。
- 附件失败不回滚已登记的元数据，但创建独立可重试下载 operation。

### 11.4 首次日更和披露季窗口

bootstrap 必须按 source/exchange/category/query fingerprint 保存已完整扫描 scope 的 cutoff 和 `covered_until`。与日更 scope 兼容时，首次日更继承该水位并从 `covered_until - overlap` 开始；不兼容或没有合法水位时，只使用配置的有限当前披露季窗口，不得静默扫描多年历史。建议配置明确的 `initial_lookback_days`，并由回补覆盖更早历史。

### 11.5 自适应窗口拆分

当 provider 页数、结果数或请求限制导致一个日期窗口无法完整扫描时：

1. 保存已取得并校验的元数据；
2. 不推进该窗口之后的游标；
3. 将日期区间二分为更小窗口继续；
4. 最小到单日仍超限时，优先按 provider 支持的板块、证券代码前缀、页区间或其他稳定维度继续分片；
5. provider 支持安全页续扫时，持久化 `next_page`、已见 source ID 和固定 `run_cutoff`，跨运行续扫；
6. 只有所有子分片/页区间完整并集后，父窗口才可完成；
7. 无任何稳定续扫或分片能力时，才写入 `unsplittable_dense_day` blocker，并通过来源能力改造或受控单标的修复解决。

禁止在达到最大页数后把“已读前 N 页”报告为完整。

发布时间统一到项目约定时区并保留原始值。窗口必须明确定义起止端点；相同时间戳跨页、未来时间戳和运行期间新插入记录都不得越过固定 `run_cutoff`。cursor/checkpoint 必须绑定查询配置指纹，来源路由、市场参数或分类策略不兼容变化时不能盲目复用旧 cursor。

内部覆盖水位、运行 cutoff 和 observation 时间使用带时区的精确时间语义；provider adapter 必须根据来源能力把窗口转换成其官方参数格式。例如只接受自然日的接口必须收到项目时区下的 `YYYY-MM-DD`，不得直接收到 ISO datetime。日期降精度后的首尾边界、重复覆盖和结果回过滤必须由 adapter 明确定义并测试，不能让 provider 参数格式反向污染内部 `covered_until` 精度。

### 11.6 缺失修复

市场扫描后维护缺失覆盖队列。日更只处理有界的轮换 cohort，例如按配置限制股票数、请求数和运行时长；不得每天对全市场逐股查询。修复优先级建议为：

1. 已发现元数据但附件未就绪；
2. 当前披露季按上市时间应有年报但缺失；
3. 曾失败且到达 `next_retry_at`；
4. 长时间未复核的 `confirmed_missing`。

重叠扫描再次观察到同一个 attachment observation 时，只更新观察时间和来源证据，不得把 `completed`、尚未到期的 `retryable`、已达最大次数的 `blocked` 或人工处置状态无条件重置为 `queued`。只有发现新的 observation/version、到达既定 `next_retry_at`，或经过审计的 operator repair 才能重新开启附件工作。

### 11.7 迟到修订与存量对账

3 日 overlap 只负责低延迟发现，不能证明 provider 不会迟到索引或回填旧发布时间。系统必须另设面向“已经有有效年报”的轮换式长窗口对账：

- 配置 `reconciliation_lookback_days`、每次 cohort 和最大完整对账周期；
- 保存每只股票/财务年度的 `last_reconciled_at`；
- 重新观察相同公告 ID 的标题、附件 URL、附件身份和内容变化；
- 修订版发布时间早于当前 cursor 但今天才可见时，仍在最大对账周期内被发现；
- 对账与 missing repair 分开计数，已有原版不能阻止股票进入修订对账。

publication lookback 之外还必须维护覆盖所有受管 `instrument_id + fiscal_year` 的 oldest-first 期间队列，持久化 `last_reconciled_at`、checkpoint、失败重试和最大完整轮换周期。某个失败期间不得被错误标为已对账，新期间也不得长期饿死旧期间；多年后才被 provider 索引的旧财年修订必须仍能在最大周期内被发现。

仅含撤回/取消关系、没有可下载完整 PDF 的元数据 observation 也必须触发对应 `instrument_id + fiscal_year` 的事务性重选，不能因为附件队列为空而忽略法律状态变化。同一批同时发现原版和法律上更新的完整修订版时，只把可验证的修订版作为最终下载 winner；修订版无法取得或验证时，bootstrap 对该期间进入 `blocked/retryable`，已在线且原版仍合法可用时仅以 `provisional` 继续服务，不得把新发现但已非最有效的原版误报为最终 winner；daily 对该 scope 返回 batch outcome `partial/blocked`，修订附件保持 `retryable/blocked`，且不得把 provisional/original asset ID 放入传给消费者的 completed affected-asset 集合。该期间不得启动新的默认 effective-period consumer processing；V1 固定把已有默认 effective-period 原版结果投影为 `stale/pending_correction` 并排除出要求 current 事实的 DCF，精确 observation pin/knowledge-cutoff 结果仅在事件落入其证据范围时失效；修订版随后验证生效时，再通过 replacement event 触发声明的 consumer reprocessing。

### 11.8 股票范围生命周期

活跃股票范围必须在每次日更前按版本化 `universe_refresh_cadence` 刷新或重新验证配对的本地主数据/census 快照，并持久化 attempted/effective refresh time 与 effective paired snapshot ID。新上市股票自动进入 coverage 和 repair；退市股票从活跃覆盖率分母移出，但不得因此删除已有资产。刷新失败只能使用仍在两侧 freshness 门槛内的上一份完整配对快照；配对快照过期时市场发现可以按降级策略继续，但不得宣称全市场完成。发现窗口内出现较老财务年度的完整修订版时，应只更新对应旧财务年度，不影响该股票更晚财务年度的有效记录。

## 12. Local-First 按需获取

### 12.1 统一调用契约

所有 API 调用方和后台业务必须调用共享 `ensure_annual_report()` 或对应 DataManager 方法，不得直接调用 provider 下载年报。

请求至少支持两类身份：

- `instrument_id + fiscal_year/report_period`：获取该期间最有效年报；
- `source + source_announcement_id`：获取业务已绑定的精确法律公告；`filing_id` 只能作为版本化兼容别名，归一化为 `source_announcement_id`，两者同时提供时必须一致，否则返回 422；可附带 `attachment_id` 以及 `expected_content_hash` 或 `observation_version` 钉住具体附件 observation。

带版本 pin 的 exact-filing 请求必须精确匹配，不得返回同一 filing 后来静默更新的字节冒充原证据。未提供版本 pin 时，返回该法律 filing 当前有效的 observation，并在响应中明确 observation version、hash 和 `version_available_at`；被钉住的旧 observation 已按 V1 删除时，只返回 metadata + `local_content_unavailable`。

请求策略至少包含：

- `allow_network`；
- `integrity_level`；
- `wait_seconds`（`0` 表示立即返回/排队，正值只能在配置上限内有界等待）；
- 调用 consumer 和幂等键；
- 可选 `knowledge_cutoff`；
- exact-filing 专用的 `attachment_id`、`expected_content_hash` 或 `observation_version` pin。

DataManager 与 OpenAPI 必须固定上述字段、互斥 selector 和返回投影。exact-filing 或 knowledge-cutoff 响应必须返回实际解析到的 observation version、hash、`version_available_at`，以及旧字节未保留时的 `local_content_unavailable`。generic ensure 中的 consumer 只用于调用归因/幂等身份，不得据此启动业务 parser。

### 12.2 决策顺序

已知 exact filing 已被替换或撤回，且其物理附件已按 V1 保留策略删除时，普通 consumer/API ensure 即使 `allow_network=true` 也只能返回元数据和 `local_content_unavailable`，不得重新下载前任或重新形成同财年的第二份物理附件。恢复历史前任原件必须留给未来单独授权的 operator policy。

```text
本地有效资产存在
  -> 校验达到要求 -> local_hit，零网络返回

已选出的候选元数据存在，但尚无通过校验并激活的本地 current 附件
  -> allow_network=true -> 下载候选、校验、事务性重选并激活、归档
  -> allow_network=false -> missing_local

元数据和附件均不存在
  -> allow_network=true -> 有界单标的发现 -> 选择 -> 下载
  -> allow_network=false -> missing

候选歧义、空间不足、网络禁用或权限不足
  -> 明确 ambiguous / blocked / forbidden，不任意替代
```

### 12.3 并发和响应方式

- scheduler、API 和两个消费者同时请求同一附件时，只允许一个下载 lease。
- API 不得为不确定时长的来源发现或大附件下载无限保持 HTTP 连接。
- 缺失资产的 API ensure 创建或复用 durable operation，返回 principal-scoped `asset_request_id` 和当前状态。
- 对外 `asset_request_id` 是调用者可见的 subscription/opaque handle；底层共享 asset operation 可被多个调用者复用，但 internal operation ID 不直接跨 principal 暴露。
- 内部批处理可在配置的短时间内等待已有 lease，超时后返回 queued，而不是启动第二次下载。

## 13. 文件存储、复用与删除

### 13.1 新文件目录

新业务中立根目录采用内容寻址的唯一物理 Blob 池：

```text
data/filings/announcements/
  blobs/{sha256[0:2]}/{sha256}.pdf
```

股票、财务年度、交易所、来源和 filing ID 保存在数据库资产记录中，查询时形成可读投影，不要求为每个法律身份再创建一份物理文件。若运维确需人类可读目录，只能创建受管 alias/hardlink，并把它作为 retention pin 登记；不得把 `st_nlink` 当作数据库引用真相。迁移前必须探测当前 NFS 是否支持所需 hardlink 语义，不支持时使用 copy、校验、同文件系统原子 rename，或完全不创建 alias。

要求：

- 根目录通过项目相对配置指定并强制位于 `data/filings` 下；
- 路径片段必须白名单化，禁止 `..`、绝对输入和路径穿越；
- Blob 文件名使用完整内容 hash，法律来源身份由数据库投影提供；
- 新的年报原件不得写入 business-profile 或 broker 私有根目录；
- `.part`、quarantine 和 canonical 文件状态必须可区分。
- 临时文件必须与最终 Blob 位于同一挂载点，避免跨文件系统 rename；发布后必须重新打开并复核长度/hash。
- 任务启动和每次写入前校验 `data/filings` 的实际 mount source、读写能力和允许的 NFS 身份；NAS 未挂载、只读或退化为本地空目录时 fail closed。

### 13.2 现有目录复用

迁移至少扫描：

```text
data/filings/business_profile/{year}/{exchange}/
data/filings/financial_statements/broker_risk_control/{exchange}/{symbol}/
```

生产 inventory/adoption/cleanup 使用的 legacy roots 必须形成版本化配置和指纹；显式 override 必须同时覆盖上述两个必需根目录。任一必需根缺失、越界、挂载身份不可验证或只配置部分 roots 时，生产 adoption 和 cleanup 必须 fail closed，不能把未扫描目录误判为“无存量文件”。

现有样本表明：

- 公司画像文件名已经包含股票、报告期、公告 ID 和 SHA-256，可作为采用证据；
- 公司画像目录也有 `derived/` 派生内容，公共资产迁移不得把派生文件当原始公告；
- 券商目录同时包含年报和半年报，一期只能采用报告期为年末且通过完整年报分类的正文附件，包括可验证原版和完整修订版；
- 两边可能保存相同附件的不同副本，必须按 source identity、报告期、PDF 签名、长度和 SHA-256 核对。

目录模板捕获值也是身份校验的一部分：business-profile 的 `{fiscal_year}/{exchange}` 必须分别与规范化报告期财年和 canonical exchange 一致；broker 的 `{exchange}/{symbol}` 必须分别与 canonical exchange 和 instrument symbol 一致。目录、文件名、manifest 或规范化身份任一交叉不一致都必须 fail closed 为冲突/范围外，不得仅因目录形状正确而采用。

### 13.3 采用优先于搬迁

迁移初期允许 canonical 数据库记录指向现有已验证路径，以实现零复制和零下载。推荐顺序：

1. 只读 inventory；
2. shadow 登记；
3. 双读对账；
4. 业务切换到共享 asset ID；
5. 文件备份验证；
6. 再选择保留原路径、创建 hardlink 或原子移动到新根目录；
7. 最后删除冗余副本。

不得仅因文件名相同认定内容相同，也不得在 consumer 尚未切换时删除其原路径。

shadow 记录在 source identity、股票/报告期、附件分类、长度/hash、受控路径和 latest-effective 决策完成对账前，不得进入生产 effective 查询、不得满足 bootstrap coverage，也不得被前台或业务 parser 消费。无冲突对账完成后，由独立的 asset-adoption promotion gate 将记录提升为 production-visible，使共享回补和日更可以复用该文件；该 gate 不依赖公司画像或券商是否已切换。消费者自己的 cutover gate 只决定对应业务是否读取共享资产，不得阻止资产层提升、回补或日更。冲突和证据不足记录始终 fail closed。

promotion 后仍引用 legacy path 的文件必须按单文件进入共享资产模块的受控 custody：旧 writer/cleanup 不得覆盖、截断、移动或删除该路径。若无法从文件权限、不可变写入约定或迁移隔离证明这一点，promotion 前必须 copy/link 到受控 canonical path。每次生产读取、备份和完整性审计仍按登记长度/hash 验证；外部修改或删除必须立即使本地资产失效并阻止流式读取，随后仅能从已验证备份或受控 provider 修复，不能信任原 manifest。

inventory 报告的 orphan 默认继续 fail closed，不得仅凭文件名采用。但当文件名/manifest 可提供 announcement ID、股票、报告期或 hash 线索时，工具可以执行“零附件下载”的 orphan reconciliation：只查询官方元数据，或使用经过审计的 operator mapping，闭合 source announcement、attachment、instrument、report period、classification、length 和 hash 后转为 shadow。任何身份不唯一、官方元数据不一致或需要猜测的 orphan 继续隔离，不能为了避免下载而降低证据标准。

任何迁移清理都必须生成逐文件 allowlist，列出 managed path、manifest/asset ID、hash 和删除理由。允许删除的原因仅限：字节完全相同的冗余副本，或被完整修订版替代且已满足删除门槛的同财年旧原件。`derived/`、半年报、其他财务年度、未入 manifest 的孤儿文件和任何冲突文件一律排除。工具默认 dry-run，禁止目录级删除。dry-run 只允许返回/记录有界计划和诊断，不得改变数据库业务状态、写 recovery manifest、创建“已执行”的 durable operation、修改文件或推进 catalog/backup/deletion watermark。

### 13.4 一个财务年度一个有效附件

该约束指共享 canonical archive 中一个有效逻辑选择和一个无冗余物理 Blob：

- 同一股票、财务年度在存在合法可用候选时只有一行当前有效选择；依法撤回且无替代稿时必须为零行 current winner，并由 no-winner tombstone/不可变决策历史解释；
- 若多个法律附件内容完全相同，可共享 Blob，但法律身份保留；
- 修订版生效后解除旧引用；
- 物理保留依据独立的 `retention pin`，包括其他有效附件、受管 legacy alias、尚未切换的消费者、活动文件流/解析 lease 和迁移操作；历史元数据外键本身不永久 pin 文件；
- 任何 `blocks_primary_unlink=true` 的非 recovery retention pin 未释放时不能删除，pin 应由数据库查询/约束计算，不能依赖可能漂移的手工计数字段；activation 创建的 recovery pin 在 recovery manifest 和配对 catalog/file watermark 生效前阻止主卷 unlink，随后以 CAS 方式转换为不阻止主卷 unlink 的 `required_set_hold`，继续永久保护备份 required-set 身份；
- read/processing lease 必须包含 owner、TTL、heartbeat、generation 和安全宽限期；过期 lease 只能由 CAS/reconciler 撤销并重新计算 pin，仍有新 heartbeat 或无法证明 owner 已失效时继续阻止删除。崩溃、长时间解析、续租竞争和过期回收必须有测试，避免陈旧 lease 永久阻塞，也避免活跃 reader 被提前删除原件；
- 删除文件不删除公告、附件、hash、替换链和删除审计。

删除收敛必须有可配置、可审计的 `predecessor_cleanup_warning_age` 和 `predecessor_cleanup_hard_age`。两者必须为正数且 `warning_age < hard_age`，进入正式配置/模板/schema parity、版本化配置指纹和变更审计。`planned|deleting|failed` 的 distinct predecessor 超过 warning age 时资产层 readiness 至少为 `degraded`；超过 hard age 时必须阻止 unique-storage 完成声明并进入 operator repair，但不能回滚已经验证生效的新 winner，也不能阻止其他有效本地资产的只读使用。age 越界只改变 readiness 和 repair routing，不得释放 retention pin、改变 deletion-intent required-set 枚举或授权 unlink。报告必须给出最老未收敛 predecessor 的年龄、状态、最后错误和门槛阻塞原因。

迁移过渡期允许受管 legacy alias 为尚未切换的消费者暂时保留 predecessor 字节；该 alias 不计入 current-effective、latest-only coverage 或共享 canonical Blob 数量，且必须有 owner、consumer、hash、过期/切换条件和 retention pin。消费者切换后应按逐文件 allowlist 清理 alias，不能把迁移兼容副本误报为长期有效年报。

## 14. 当前存储容量评估与门槛

### 14.1 2026-08-10 只读测量基线

| 项目 | 当前观察 |
| --- | ---: |
| `data/filings` NFS 卷总容量 | 约 3.5 TiB |
| `data/filings` NFS 已用 | 约 1.5 TiB |
| `data/filings` NFS 可用 | 约 2.1 TiB |
| `data/filings` 当前体积 | 约 2.5 GiB |
| `data/filings` 当前文件数 | 约 2,240 |
| 当前活跃 A 股股票数 | 约 5,543 |
| 现有 PDF 样本平均/P95/最大 | 约 4.6/12.9/43.8 MiB |
| 一份/股的估算增量 | 约 24-25 GiB |
| PVE-Bak 可用空间 | 约 2.1 TiB |
| QuoteBak 可用空间 | 约 384 GiB |

结论：重新挂载后的 `data/filings` 当前足以支持一期“全市场每股最新一期年报”的历史回补，24-25 GiB 估算约占当前可用空间 1.2%。即使考虑大文件、临时文件、下载重试和未来数年日更，近期容量仍可接受。

该估算不能替代运行时门槛。正式容量 artifact 还必须记录主卷/备份 required-set 实际字节、永久 recovery-manifest 字节、临时与 old+new 峰值、预期年增长、规划期限/headroom 和配置指纹；过期或配置不匹配的 artifact 不能满足 rollout gate。年报分布有长尾，个别文件可能明显大于平均值；未来每年新增一轮年报也会累计约数十 GiB，且 V1 永久 recovery 副本在没有后续 GC 规范前持续增长。

### 14.2 强制空间治理

配置必须支持：

- warning utilization；
- hard-stop utilization；
- absolute free-space reserve；
- 单附件最大字节数；
- 单任务计划和实际最大下载字节数；
- 临时文件预算；
- 基于目标文件系统的全局/数据库 byte reservation，未知 Content-Length 按配置上限预留；
- 可选 operator override，且必须审计。

下载前应使用 Content-Length（若可信）、历史分位数或配置默认值估算计划字节，并原子取得空间 reservation。预算必须计入 `.part`、quarantine 和修订切换时 old+new 并存；失败、完成或 lease 过期后必须释放 reservation。流式下载期间仍需检查实际字节，超过限制必须终止并删除/隔离临时文件。

空间不足时：

- 元数据同步继续；
- scheduled attachment prefetch 停止；
- on-demand 返回明确 storage blocker；
- 不得静默写到其他磁盘；
- readiness 暴露缺失附件和预计空间。

### 14.3 附件大小限制

现有通用 50 MiB 附件限制可能接近年报大文件长尾。一期需要单独配置 annual-report limit，并通过现有全样本 P95/P99/最大值校准，不能简单无限放开。

## 15. 备份与恢复

现有 SQLite online backup 只覆盖 `data/*.db` 时，不能视为 `data/filings` 已备份。公告资产需要独立增量文件备份。

### 15.1 备份要求

- 目标必须是经过身份校验的 NAS mount；
- 备份输入必须从 catalog 的 required-blob 集合枚举，而不是只遍历 canonical blob 目录；仍位于受控 legacy path 的已采用 Blob、V1 recovery manifest 中所有永久 active 的普通修订 predecessor、`withdrawal_tombstone` predecessor 和 legacy duplicate Blob，以及 recovery manifest 尚未提交但已被 `planned|deleting` 删除意图/recovery pin 保留的 predecessor，也必须按登记 hash 读取、复制和验证；
- 按内容 hash 只复制缺失 Blob；
- 复制后校验长度和 SHA-256；
- 记录 destination identity、完成时间、错误和未保护字节；
- NAS 未挂载或路径退化为本地目录时 fail closed；
- 不得把本应写 NAS 的完整副本落到本地 `data` 卷。
- 备份目标必须与主 `data/filings` 处于独立存储故障域，并核对配置的不可混淆 `failure_domain_id`、mount source、服务器、export 和可取得的文件系统标识；不能只凭路径名、主机别名或运维标签判断。同一服务器的不同 export 不计灾备，运行时无法证明独立性时按 non-independent fail closed。
- 当前 `data/filings` 与 `PVE-Bak` 都来自 `192.168.188.88`，因此 PVE-Bak 不能作为允许删除前任文件的唯一灾备；当前位于 `192.168.188.68` 的 QuoteBak 才具备不同主机这一最低隔离条件。
- 备份目标也必须执行 warning、hard reserve、planned-byte、临时文件和 freshness 门槛；目标容量不足时保持本地资产有效，但 readiness 降级并阻止前任删除。
- 缺失 Blob 使用目标端临时文件、flush、长度/hash 校验和同目录原子发布；已有 hash 命名目标必须重新验证，不能只信任文件名。已有目标 hash/长度不符时保持 unprotected，普通备份不得改变其 path、bytes 或 mtime，也不得推进 watermark；quarantine 或 replacement 只能由 operator-authorized、可审计 repair operation 执行并保留原始证据。
- Blob file-manifest 水位必须与包含相应 replacement 事务的可恢复 catalog 数据库快照配对；数据库快照早于 Blob 目录水位且无法证明一致时，不满足删除门槛。
- paired snapshot 后、声明 RPO 所需的 catalog/outbox/operation/lineage/audit 增量必须进入独立故障域的追加式 recovery journal，保存顺序、完整性 hash、前任/覆盖水位和截断证据；否则灾难恢复只能在持久化 write-freeze 水位证明不存在后续增量时开放。
- V1 不自动清理备份端 superseded Blob；未来需要独立、审计过的 retention/GC 需求后才能回收。

daily cron 首次启用前，必须验证独立故障域身份、backup job 可运行、初始 required-set 全量受保护且 paired watermark 新鲜。启用后的运行期 backup 失败时，元数据发现和已验证本地读取继续，任何删除/cleanup 立即停止；新附件下载只允许在配置的 unprotected bytes/age 上限内继续，超过上限后 scheduled attachment write 必须 blocked，避免无界扩大未保护资产。

### 15.2 删除前提

修订替换删除不同 hash 的旧文件时，必须要求新文件本地完整、数据库已激活、所有阻止主卷删除的非 recovery retention pin 已释放、删除审计可写，并且 replacement/predecessor 均已在独立故障域完成长度/hash 验证。普通 predecessor 先写入绑定预留 pair ID 与 file watermark 的不可变 recovery manifest，再生成包含该 manifest 的 recoverable catalog snapshot，最后以追加式 recovery-pair closure 绑定 pair ID、snapshot identity/hash 与 file watermark；不得回写 immutable manifest 形成循环自引用。activation 的 recovery pin 只有在 closure 后才能 CAS 转为永久 backup `required_set_hold`。同 hash 法律替换不创建物理删除意图。任一条件不满足时只可标记待删除，不得 unlink。predecessor 备份仅用于灾备/回滚，不得成为业务可见的第二份有效年报；V1 不自动 GC。

撤回无替代稿时没有 replacement Blob，但仍必须执行两阶段闭合：删除意图预留 pair ID，备份 withdrawn predecessor，写入绑定 decision/outbox 与 file watermark 的不可变 `withdrawal_tombstone` manifest，生成包含它的 no-winner catalog snapshot，再写追加式 pair closure 后才可转换 recovery pin 和 unlink。required-set 必须包含该 predecessor，不得为了满足成对规则而伪造 replacement。

### 15.3 恢复顺序

恢复必须配对数据库和文件版本：

1. 恢复匹配时间点的数据库备份；
2. 按数据库登记的 hash 从附件备份恢复 Blob；
3. 对数据库引用的全部 current-effective、retention-pinned、pending-deletion replacement/predecessor 和 V1 recovery manifest 中所有永久 active 的普通 predecessor、`withdrawal_tombstone` predecessor、legacy duplicate Blob 执行 presence、length 和 SHA-256 全量核对；recovery-only predecessor/duplicate 默认留在备份 required-set 或隔离、不可被消费者读取的恢复区，不得为了校验而重新发布为主卷第二份 canonical 附件；任一必需 Blob 缺失或不匹配时保持 blocked，抽样只用于日常演练，不能作为恢复后开放门槛；
4. 按水位和完整性 hash 顺序重放 paired snapshot 后的 recovery journal，或用持久化 write-freeze 水位证明不存在后续增量；随后校验 current-effective/no-winner projection、不可变 decision/replacement history、deletion intent、change-event outbox、recovery manifest 与 consumer lineage/current-result 是否和恢复的 catalog 一致且无悬空或矛盾引用；
5. 重建可派生的 retention-pin 投影和 readiness；
6. 再开放消费者读取和日更写入。

代码回滚不能代替在物理删除发生后的数据库加附件联合恢复。

## 16. 调度与配置需求

### 16.1 作业

至少提供：

| 作业 | 调用方式 | 默认建议 | 职责 |
| --- | --- | --- | --- |
| `annual_report_asset_latest_backfill` | 手工 | `enabled=false`、`manual_only=true`、不得配置 cron | 一次性/补偿性 latest-only 全市场回补 |
| `annual_report_asset_daily_update` | cron + 手工 | `enabled=false`，验收后启用 | 每日发现、下载、修订替换和缺失修复 |
| `annual_report_asset_integrity_audit` | 手工/低频 | 默认只读 | 文件、hash、引用和备份审计 |
| `annual_report_asset_backup` | cron + 手工 | `enabled=false`，独立启用 | 增量文件备份和验证 |

作业开关不得放在 `business_profile_evidence.enabled` 或 broker 配置之下。

手工、cron 和 operator API/CLI 必须调用同一个 durable command service。`run_id` 直接使用底层 operation ID，不另建进程内任务身份；同 scope 已运行时返回现有 operation。控制面至少支持有界 start、status/history、协作式 stop 和 resume：stop 只在安全 checkpoint 转为 `cancelled` 并停止创建新 child work，不得级联取消仍被其他 principal/consumer 订阅的共享 child acquisition；resume 复用原 operation ID、增加 attempt/resume generation 并保留已完成水位。人工命令要求 operator 权限，cron 使用可审计的 service principal；actor、权限、配置版本、请求指纹、心跳和最终结果必须留痕。

### 16.2 配置字段

配置至少覆盖：

- module enabled、scheduled enabled、dry-run；
- active exchanges、instrument type/status；
- universe master-data freshness limit、eligibility policy version、indeterminate handling 和 overdue-missing readiness policy；
- source routes、normalized categories、classifier version；
- bootstrap filing-season bounds 和 targeted repair 的 request/instrument/elapsed bounds；
- daily cron、timezone、overlap days、initial lookback；
- `reconciliation_lookback_days`、`reconciliation_cohort_size`、`reconciliation_max_cycle_days` 和 `missing_repair_cohort_size`；
- max pages/requests/windows/instruments/bytes/elapsed；
- download concurrency、per-source concurrency、rate limits；
- lease TTL、heartbeat、retry backoff、max attempts；
- archive/temp/quarantine roots、max attachment bytes；
- `.part` 最大年龄、最大实际字节、安全宽限期和未知/损坏 sidecar 的 fail-closed 策略；
- quarantine warning/hard 最大年龄和实际字节、证据元数据保留及 operator cleanup policy；
- warning/stop utilization 和 free-space reserve；
- backup mount、destination、freshness threshold；
- runtime backup failure 的 `max_unprotected_bytes`、`max_unprotected_age`、累计起点以及成功备份后的解除 blocker/清零语义；
- predecessor cleanup warning/hard age、长期 `planned|deleting|failed` 的 readiness 映射和 operator repair policy；
- consumer dependency policy 和 rollout gates。

正式配置与配置模板必须通过同一 schema 加载并逐字段验证上述安全默认值；配置加载或 scheduler 注册测试必须证明 latest backfill 不会产生 cron、所有全市场写作业默认关闭、integrity audit 不携带任何破坏性 action。正式 enablement 应在回补和备份验收后执行。

## 17. DataManager 与服务接口

### 17.1 查询接口

DataManager 至少提供：

- 按股票、财务年度、来源、filing ID、完整性、获取状态列出资产；
- 获取某股票某年度当前最有效年报；
- 查询 active-universe coverage、存储、备份和 scheduler readiness；
- 通过 caller-owned `asset_request_id` 查询共享 acquisition 的授权投影，通过 `consumer_request_id` 查询业务 continuation/processing 的授权投影；internal operation ID 仅允许 operator/service scope 查询；
- 根据 asset ID 和授权上下文获取受控文件流或内部文件 handle；不得接受 caller path，流出前重新核对 effective/superseded 状态、mount identity、长度/hash，并在打开/关闭时取得和释放 read lease。公共 asset-id content route 对 superseded/deleted 资产固定返回 410；只有内部 DataManager/service 调用带 exact-filing observation pin、且字节仍保留并通过授权 integrity policy 时，才可通过非公开受控 handle 读取非当前 observation。

现有 `get_annual_report_assets` 和 `get_annual_report_asset` 在迁移期改为共享仓库 read-through adapter，避免破坏调用方；长期不得继续只筛选公司画像 manifest。

列表、单资产、ensure 和业务 lineage 中的单值 `source/source_announcement_id` 表示版本化 `canonical_source_filing` projection，不表示其他等价法律 filing 被合并或丢弃。存在跨来源等价候选时，内部/DataManager 必须同时提供完整 `equivalent_source_filings`；外部 API 至少提供排序稳定的等价 filing 身份、projection policy version 和 evidence-set hash，且不得因发现顺序改变单值投影。

### 17.2 Ensure 返回状态

Ensure 的即时处置只区分：

- `local_hit`；
- `local_miss`；
- `operation_created`；
- `operation_reused`。

异步 operation 再通过独立字段报告 status、stage 和最终结果来源 `adopted/downloaded/repaired`；`missing/ambiguous/failed/blocked` 属于资产可用性或 operation 结果，不与 ensure disposition 混在同一枚举。

返回应包含 asset ID、股票、财务年度、来源、filing ID、发布时间、是否修订、hash、长度、完整性、是否当前有效、诊断和下一步。仅在创建或复用异步 acquisition 时返回前台 `asset_request_id`；HTTP 200 的 `local_hit` 和网络禁用的 `local_miss` 不创建 subscription，`asset_request_id` 应省略或为 `null`。任何响应都不得向外部客户端暴露 internal operation ID 或任意服务器绝对路径。

## 18. FastAPI 与 AI/API 业务调用整合

### 18.1 建议的增量端点

最终路径应遵循现有 API 命名，语义至少包括：

- `GET /api/v1/research/company/{instrument_id}/annual-reports`：分页查询本地记录，纯读、零网络；
- `GET /api/v1/research/company/{instrument_id}/annual-reports/effective`：按 fiscal year 查询单一有效状态；
- `POST /api/v1/research/company/{instrument_id}/annual-reports/ensure`：执行共享 local-first ensure；只有本地缺失且允许网络时才创建/复用异步 acquisition operation；
- `GET /api/v1/research/annual-report-asset-requests/{asset_request_id}`：通过 principal-scoped opaque handle 轮询授权投影，同时返回独立 `asset_request_status=active|cancelled|expired`、内部 operation status/stage 和 disposition；DELETE/expiry 只能改变 caller request 投影，不能覆盖底层 operation 或其他订阅者状态；
- `DELETE /api/v1/research/annual-report-asset-requests/{asset_request_id}`：幂等解绑 caller subscription；不得修改关联 `consumer_request_id` 或其 continuation；V1 不因最后一个订阅者取消而中止已创建的有界共享 acquisition；pending continuation 只能由 consumer-request DELETE 处理；
- `GET /api/v1/research/annual-report-consumer-requests/{consumer_request_id}`：轮询 caller-scoped 业务 continuation/processing 投影，分别返回 consumer request lifecycle、consumer result freshness、发生 acquisition 时关联的 `asset_request_id`、结果身份和脱敏错误；
- `DELETE /api/v1/research/annual-report-consumer-requests/{consumer_request_id}`：幂等取消尚未启动的 continuation；consumer processing 已开始时按业务受控停止契约处理，不支持时返回明确冲突而不是强杀；该操作不得隐式解绑关联 `asset_request_id` 或停止共享 acquisition，调用方需要时必须分别 DELETE asset request；
- `GET /api/v1/research/annual-report-assets/readiness`：查询覆盖和运行状态；
- `GET /api/v1/research/annual-report-assets/{asset_id}/content`：按 asset ID 安全下载文件。

依赖年报并需要启动业务解析的前台命令必须由业务消费者自己的受保护 POST 命令提供入口。本期至少登记两个业务适配器：`POST /api/v1/research/company/{instrument_id}/business-profile/annual-report-process` 和 `POST /api/v1/research/company/{instrument_id}/broker-risk-control/annual-report-process`。`BusinessAnnualReportProcessRequest` 必须复用 generic ensure 的互斥 selector、身份一致性、`allow_network`、`integrity_level`、有界 `wait_seconds`、`knowledge_cutoff` 和 exact attachment/hash/observation pin 契约；客户端只能指定已注册 processing profile 或 optional expected fingerprint，canonical processing fingerprint 必须由服务端按受控 parser/version/parameters/config 计算，未知 profile 返回 422、期望指纹不符返回 409，不得允许任意 caller 字符串制造 processing identity。请求接受时立即创建或复用 caller-owned `consumer_request_id`：只有 `consumer_request_status=completed` 且现有 consumer result 对规范化 selector、knowledge cutoff、retention policy 与服务端 processing fingerprint 解析到的 observation 仍为 `current` 时返回 HTTP 200；默认 effective-period selector 绑定当前 effective asset ID/hash，exact-filing 或历史 cutoff selector 则绑定其 pin/cutoff 可见 observation。资产本地命中但 parser 尚未完成、或结果 stale 时返回 HTTP 202；资产缺失且同时拥有 domain 和 acquire 权限时返回 HTTP 202、`consumer_request_id`、`asset_request_id` 并置 `pending_asset`；资产有效后沿用同一句柄推进。业务命令的 `Location` 始终指向 consumer request，asset request URL 通过 `links.asset_request` 暴露；只有 generic ensure 的 `Location` 指向 asset request。

业务命令的终态必须确定映射：本地或有界搜索确认未找到、或本地缺失且 `allow_network=false` 时，只要调用者已有 domain processing scope，即可返回 HTTP 200 + terminal `missing` consumer projection 和稳定 reason code；该响应仍必须持久化并返回 `consumer_request_id`，可由 owner 后续 GET 审计，设置 `Location` 指向该 terminal consumer resource，但不得设置 `Retry-After`，且不触网。缺少 domain processing scope 始终返回 HTTP 403（按配置可使用 404 non-disclosure）且不创建 request；资产缺失、`allow_network=true` 但缺少 acquire scope 时同样返回 HTTP 403/404 且不创建 consumer/asset work。已接受的异步请求随后因 provider、mount 或空间进入临时 `blocked` 时，由 consumer request 终态投影稳定报告，首次请求若在创建工作前即可确定基础设施不可用则返回 HTTP 503；候选歧义或当前状态冲突返回 HTTP 409。任何路径都不得借业务命令绕过 acquire scope。generic asset ensure 不得隐式启动任何消费者。

V1 reason code 至少固定为：`annual_report_not_found`、`network_disabled`、`provider_unavailable`、`archive_mount_unavailable`、`storage_reserve_exceeded`、`backup_gate_blocked`、`domain_scope_required`、`asset_acquire_scope_required`、`candidate_ambiguous`、`effective_state_conflict`、`idempotency_conflict` 和 `consumer_processing_stale`。错误 envelope 还必须携带 `retryable`、`next_retry_at`（如适用）和不泄露 provider/path 的安全诊断。

业务命令幂等键绑定 principal、consumer、instrument/selector、processing fingerprint 和规范化请求体：同键同指纹复用同一 `consumer_request_id` 和 continuation，同键不同请求返回 HTTP 409 且不得创建第二个 consumer operation。若具体业务命令未在本变更实现，必须作为对应消费者 cutover 的 enablement gate；不得以 generic ensure 已完成宣称业务链路完成。

实际路由必须避免动态路径冲突，并通过 OpenAPI snapshot 固定。现有公司画像读接口明确为 `GET /api/v1/research/company/{instrument_id}/business-profile`；券商 V1 前台状态由 broker processing POST 与 caller-owned consumer-request 资源提供，除非另行登记并快照一个券商事实 GET，不得泛称已有券商前台已整合。任何 GET 都不得隐式触发公告发现或下载。generic asset ensure 的本地命中或网络禁用缺失返回 HTTP 200；`wait_seconds` 的默认值和上限必须版本化，0 立即返回，正值只等待到上限，超时返回同一 durable handle 的 HTTP 202 而不取消/重复任务。业务命令只有 consumer result current 且 lifecycle completed 时返回 HTTP 200，其余已接受异步工作返回 HTTP 202 和 consumer `Location`/`Retry-After`。

普通 instrument list/effective GET 只能返回聚合的 asset availability、effective-decision state 和 last-checked 等资产级状态，不得暴露任何 internal operation、其他 principal 的 subscription、重试计划或特权诊断；具体 acquisition 进度只能通过调用者自己的 `asset_request_id` 投影查询。

### 18.2 AI/API 调用状态模型

API 契约必须分别暴露六类正交状态，不得把“附件下载完成”误解为“业务结果已更新”：

- asset availability：`local_valid/metadata_only/missing/ambiguous/corrupt/superseded/blocked`；
- asset request lifecycle：`active/cancelled/expired`，只描述 caller subscription；
- operation status：`queued/running/completed/missing/failed/blocked/cancelled`；`expired` 仅表示 caller request/idempotency projection 已过期，不改写内部 operation 或 consumer result；
- operation stage：`discovering/reconciling/adopting/downloading/validating/activating/deleting/backing_up/restoring/auditing`；阶段枚举必须版本化，未适用阶段不得伪造为 `completed`；
- consumer request lifecycle：`pending_asset/not_started/queued/processing/completed/failed/missing/blocked/cancelled/expired`；`expired` 仅表示 caller handle/idempotency projection 已超过保留期，不得改写已经完成的业务结果新鲜度；
- consumer result freshness：`unavailable/current/stale/reprocessing`。请求生命周期与结果新鲜度必须分别持久化、返回和判断；HTTP 200 业务完成要求 `consumer_request_status=completed` 且 `consumer_result_state=current`，不能用 `current/stale` 代替请求状态，也不能用 request cancellation 覆盖已经开始的 parser processing。

`asset_availability=local_valid` 只证明某份 PDF 字节本地完整，不代表默认 effective-period 已具备最终业务处理资格。当前 effective decision 为 `provisional|ambiguous` 时，默认业务命令必须保持 `pending_correction`/stale 门禁且不得启动新解析；只有显式 exact-filing 或 knowledge-cutoff selector 在其证据范围内仍合法时，才可按对应 consumer policy 处理。

ensure 另返回 `disposition=local_hit|local_miss|operation_created|operation_reused`；`local_miss` 表示本地不满足且调用策略未创建网络 operation。asset request 投影明确 terminal 集、是否可重试、reason codes、attempt、`next_retry_at`、创建/开始/心跳/完成时间、stage、进度、最终结果来源和脱敏诊断。consumer request 投影独立返回 `consumer_request_id`、consumer/processing fingerprint、request lifecycle、result freshness、可选关联 `asset_request_id`、结果身份、可重试性和脱敏诊断；本地命中未创建 acquisition 时该关联为空。两类 request 均只允许 owner 或 operator 查询，并遵循统一 404 non-disclosure policy。幂等记录在配置保留期内即使已经 terminal/cancelled 也返回原句柄；过期后 owner 仍得到同一句柄的 `expired` 投影，旧 key 不得静默创建新工作，重新发起必须使用新的 caller idempotency key。

V1 取消 `asset_request_id` 只解绑该 principal 的 asset subscription，不修改关联 `consumer_request_id` 或尚未启动的 consumer continuation；pending continuation 只能通过 consumer-request DELETE 取消。已创建的有界共享 acquisition 即使没有剩余订阅者也继续完成，以避免订阅竞态破坏可复用资产。取消 asset request 不得级联停止已经开始的 consumer processing；后者只能通过该业务自己的受控停止契约处理，不支持时明确拒绝。底层 operation 的 lease 过期和重启恢复语义必须独立定义。

DELETE 是保留审计记录的逻辑解绑/取消，不得物理删除 request handle。尚未启动的 asset subscription 或 consumer continuation 首次和重复 DELETE 均返回 HTTP 200 及同一 `cancelled` projection，后续 owner GET 仍可查询；unknown/cross-owner 遵循 404 non-disclosure。consumer processing 已开始且业务接受协作停止时返回 HTTP 202，否则返回 HTTP 409。`completed|missing|failed|expired`、已有 current result，以及已经启动且不可停止的 blocked request 固定返回 HTTP 409 `request_not_cancellable`；尚未启动的 blocked continuation 可以 HTTP 200 取消但必须保留 blocker/retry/audit 证据。consumer DELETE 不得改变 linked asset request；只有已取消 request 的重复 DELETE 返回 HTTP 200 `cancelled`。

| 状态 | API 调用含义 | 允许操作 |
| --- | --- | --- |
| `local_valid` | 当前选择的附件字节本地完整；是否 final 另看 effective-decision state | 查看来源、下载；仅 final/current 决策可启动默认业务处理，provisional/ambiguous 仅允许符合证据范围的 exact-filing/knowledge-cutoff 处理 |
| `metadata_only` | 已知公告但附件未下载 | 发起获取 |
| `queued` 或 `running + stage` | 正在获取 | 轮询，不重复创建任务 |
| `missing` | 有界搜索未找到 | 显示最后检查时间，可按策略重试 |
| `ambiguous` | 候选不能安全决策 | 展示诊断，等待运维处理 |
| `blocked` | 空间、权限、网络策略或备份门槛阻止 | 展示可执行的 blocker |
| `corrupt/failed` | 文件或获取失败 | 有权限时重试，不把资产交给 parser |

API 调用方不得用“是否有公司画像”推断“是否有年报”。年报状态和业务画像状态必须分别返回或组合，不互相覆盖。

### 18.3 业务触发流程

当 AI/API 调用方通过公司画像或券商风控业务命令发起依赖年报的操作：

1. 后端先调用共享 local-first 查询；
2. `local_valid` 时取得 asset ID 作为后续 consumer operation 输入；
3. 缺失且允许获取时创建 ensure operation；
4. 业务命令在接收时立即持久化或复用 `consumer_request_id` 和 continuation；本地命中时若没有 current consumer result 则返回 202 并创建或复用 consumer operation，资产缺失时若允许网络且具备 acquire scope 则先返回 202 + `pending_asset`，若 `allow_network=false` 则在 domain scope 下持久化 terminal `missing` 而不创建 asset work，若允许网络但没有 acquire scope 则返回 403/404 且不创建 work；在 asset operation 成功后沿用同一 consumer request 创建或复用 consumer operation，generic asset ensure 不隐式启动无关消费者；
5. 调用方分别以 `asset_request_id` 和 `consumer_request_id` 轮询两个 caller-scoped 投影，不能用“附件完成”代替“业务结果 current”；
6. 业务结果保存共享 asset ID、来源公告、报告期、hash 和修订状态；公司画像和券商事实都必须保存 consumer processing status，并明确 `current|stale|reprocessing`，stale/reprocessing 的券商事实不得作为无条件 current 事实或进入依赖“当前事实”的 DCF 输入；
7. 若之后修订版生效，默认 effective-period 业务结果将依赖旧 asset 的结果标记“来源已更新/待重算”，不得静默显示为最新；exact-filing 或 knowledge-cutoff 结果只有在修订 observation 落入其 selector/cutoff 证据范围时才失效，截止日后的修订不能污染或错误失效历史结果。

现有公司画像 GET 保持本地纯读。响应契约必须增加允许为空的 `source_assets/annual_report_asset` 和 `consumer_processing_status` 字段而不破坏现有必填字段；字段在 OpenAPI 中必须登记，即使当前公司没有年报资产或尚未处理时其值可以为空。缺失时由具备权限的调用方显式提交获取命令；同一规范化 selector 的重复提交由幂等键和共享 single-flight 抑制，调用方按响应中的 `Location` 和 `Retry-After` 轮询 caller-owned request。资产响应只在 `asset_availability=local_valid` 时返回非空 `content_url`，其他状态及 `retained_internal_only` 均返回 `null`；已知 superseded/deleted 旧 asset 的公共 asset-id 内容请求固定返回 HTTP 410（可在错误体提供 replacement asset 元数据），API 不得流出旧文件。内部 DataManager/service 的 exact-observation 受控 handle 不属于该公共路由。当前 asset 文件损坏或 hash 不匹配才返回 HTTP 409。

### 18.4 API 安全与边界

- 获取和文件下载需使用本期新增或配置的可信身份与 scoped permission boundary；
- 当前仓库没有完整认证中间件，只有 CORS、限流和并发保护；因此 V1 必须实现可信反向代理身份、管理凭证或等价最小权限边界，否则 acquire、content、取消、修复和 operator/readiness 详细端点默认关闭；零网络的脱敏 readiness 摘要仍可按普通只读 API policy 提供；
- 权限至少区分 `annual_report_assets:acquire`、`annual_report_assets:read_content`、`business_profile:process`、`broker_risk_control:process` 和 operator/admin；asset/consumer request 查询校验 subscription owner 或管理权限，internal operation 只允许 operator/service scope；
- 业务命令必须先校验对应 domain processing scope；本地资产可在只有 domain scope 时处理，本地缺失且 `allow_network=false` 时可在 domain scope 下记录 terminal missing，但只有同时具备 `annual_report_assets:acquire` 才能触发 provider acquisition；
- generic ensure 的纯本地命中或 `allow_network=false` 分支可使用普通可信 read policy；任何 `allow_network=true` 且实际需要创建 discovery/acquisition work 的分支必须要求 `annual_report_assets:acquire`，缺权时返回 403/配置的 404 且保持 operation/provider 零活动；
- ensure 只允许单股票、单期间等有界 scope，不能由普通 API 调用触发全市场回补；
- 使用幂等键和 rate limit 防止重复任务；
- 文件下载仅接受 asset ID，拒绝 caller path；
- 返回安全文件名和 `application/pdf`；
- 下载前再次核对 asset 当前状态和文件完整性；
- 内部路径、provider 敏感诊断和异常堆栈不得返回普通 API 调用方。

若已注册受保护路由但可信身份边界未配置，除普通脱敏 readiness 外，统一在 selector 校验、资源查询和 ownership 判断之前返回 HTTP 503 + `authorization_boundary_unavailable`，且不创建 operation 或接触 provider，避免通过 404/422 差异枚举资源。普通 readiness GET 只能返回脱敏摘要；provider、文件系统、actor 和失败明细属于 operator-only 诊断。

API acquisition 必须落在共享 SQLite durable operation/lease 中，不能复用 FastAPI `BackgroundTasks` 作为状态真相。规范化 scope、财年/精确 filing 和 policy version 构成 single-flight key，同时支持 `Idempotency-Key`。scheduler、API 和业务消费者命中相同 key 时只复用同一 internal operation，各前台调用者仍获得自己的 `asset_request_id` 和可选 `consumer_request_id`。

本项目按设立目标采用 `ai_api_only` 客户端模式，没有独立 Web UI 交付物或外部 UI 仓库门禁。本期必须登记绑定的 backend candidate 与 OpenAPI contract version，并以可复现 API 集成测试证明：授权调用方可显式获取、未授权调用在任何业务工作前被拒绝、相同活跃请求被幂等抑制、调用方沿 `Location`/`Retry-After` 轮询 caller-owned request、仅 `local_valid` 返回非空 `content_url` 且内容端点安全流式输出。API 客户端验收通过不自动放行生产回补、日更、消费者切换或旧文件清理，这些仍由各自 rollout gate 决定。

## 19. 公司画像迁移要求

- `BusinessProfileDocumentArchiveService` 不再拥有正式年报原件下载和最终 archive root。
- 公司画像通过 instrument/report period 或精确 source filing 请求共享资产。
- 现有 PDF 页级 artifact、section、text hash、LLM 结果、审核证据和事实仍归公司画像。
- 现有知识截止查询必须保留时间语义；若旧物理 PDF 已按一期策略删除，应明确降级，不得把新修订版作为旧时点证据。
- `AnnualReportAssetCatalog` 过渡为共享仓库兼容视图。
- 切换前后对相同 asset hash 的公司画像解析结果做回归，不得因路径变化改变业务身份。
- 公司画像关闭、未上线或日更失败时，共享年报日更仍独立运行。

## 20. 券商风控迁移要求

- 正式年报首先由共享资产模块提供，券商不得再自行发现、下载或保存一份年报副本。
- 一期尚未共享管理半年报时，现有半年报路径可在显式 migration gate 下暂时保留。
- 独立《风险控制指标报告》仍作为补充或校验来源，不被年报资产模块吞并。
- 同一券商、报告期和指标若同时存在共享年报与独立《风险控制指标报告》等来源画像，必须保留各自 source asset、parser 和事实 lineage；DCF/当前事实组装只能按版本化配置的来源优先级选择，不能因共享资产切换覆盖或合并来源证据。
- 券商 parser 和 `financial_numeric_facts_hot/history` 写入规则不变。
- parser manifest 增加共享 asset ID，且 parser 失败只影响券商 processing。
- 年报修订版替换后，应自动将使用旧 asset 的券商 processing 标记 superseded 或排队重跑，当前事实不得静默继续绑定已删除原件。
- 迁移回归必须证明共享本地命中时 provider 请求数和新 archive 写入数均为零。
- 券商历史回补只以主数据和券商资格证据均确认的在市券商范围为分母；身份、上市状态或券商资格不确定的条目必须显式 blocked/indeterminate，不得静默纳入或排除，也不得扫描全市场非券商证券。

## 21. 可靠性、并发与失败处理

### 21.1 单飞与 lease

- 唯一活动 lease 至少按附件观察 identity 或精确 acquisition scope 建立。
- lease 包含 owner、TTL、heartbeat 和 attempt。
- 进程崩溃后 lease 到期可恢复，但新 worker 必须先以 owner、generation、heartbeat 和安全宽限期核对遗留 `.part`；只有证明原 owner 已失效后才能删除或接管，不能清理仍在写入的临时文件。
- 数据库唯一约束和文件原子 rename 共同保证不发布重复 Blob。
- `.part` 必须记录 operation/owner/generation/创建时间/计划字节，并按最大年龄和总字节纳入 readiness；quarantine 必须有配置的最大年龄和字节上限，只允许 operator 通过审计命令清理，自动流程不得静默删除取证文件。reservation 释放与实际临时/隔离字节清理必须分别对账，避免磁盘占用脱离账本。

### 21.2 重试分类

可重试：网络超时、限流、暂时 5xx、来源附件暂不可用、NAS 暂时不可用。
不可自动重试或需人工：身份冲突、路径不安全、非 PDF、持续 hash 不匹配、候选歧义、单日窗口无法完整分页。
空间不足：进入 blocked，等待空间变化或审计过的 operator override，不使用高频退避重试。
达到最大重试次数时，`exhausted` 只作为 attachment/discovery retry item 子状态；其所属 durable operation 对外固定映射为 `blocked` + `retry_exhausted`，不得另行扩展 operation/API status 枚举。只有新 observation、明确到期的 repair 或审计过的 operator repair 可重新开启。

### 21.3 原子性

- 下载临时文件不能被查询接口视为可用。
- Blob 发布后才能提交 attachment version 有效状态。
- effective 切换和替换关系必须在数据库事务内完成。
- 文件删除晚于逻辑切换和引用核对。
- 数据库提交后文件删除失败时，保留待清理状态和审计，不回滚到引用已改变但状态不明。

SQLite 事务与 NFS `unlink` 无法组成一个原子事务，因此删除必须使用可恢复状态机：

1. 在激活 replacement 的同一数据库事务中写入 `planned` 删除意图；
2. 事务提交后将意图置为 `deleting` 并执行受管路径 unlink；
3. 成功后写 `deleted` 和实际 path/hash/time，失败写 `failed` 和可重试诊断；
4. reconciler 幂等处理长期 `planned/deleting/failed`；文件已不存在时也只能依据完整证据收敛，不能虚报或回滚新有效资产。

测试必须在数据库提交前、提交后 unlink 前、unlink 后 finalize 前注入崩溃，证明新有效件始终可用、审计不谎报且最终可收敛。

## 22. 可观测性与运维报告

### 22.1 日更报告

至少包含：

- 交易所、来源、请求窗口、overlap、cutoff；
- 请求数、分页数、观察公告数；
- 分类为完整年报、修订版、排除项和歧义的数量；
- 本地命中、采用、下载、失败、重试和写入字节；
- 新增有效、修订替换、解除引用、删除文件数量；
- 缺失修复 cohort、覆盖缺口；
- 游标推进/保留及原因；
- 空间和备份状态；
- 各阶段耗时和按来源错误摘要。

### 22.2 回补报告

至少包含：

- 股票范围快照和目标总数；
- `available/confirmed_missing/retryable/blocked` 数量和明细入口；
- 原版/修订版 winner 数量；
- 现有文件采用、网络下载、重复内容、冲突和损坏数量；
- 完成和未完成窗口、checkpoint 和恢复身份；
- 总字节、剩余空间和未备份字节。

### 22.3 Readiness

至少汇总：

- active-universe 最新一期覆盖率；
- 当前有效附件完整率；
- 待处理窗口、下载和重试；
- integrity failures；
- 存储 warning/stop；
- backup freshness 和 unprotected bytes；
- bootstrap 是否完成；
- asset daily scheduler 是否启用及最近成功；
- 公司画像和券商各自迁移阶段（独立展示，不作为 asset daily 的前置条件）；
- 是否允许停止 legacy writes 和删除重复文件。
- 最老 `planned|deleting|failed` predecessor 的年龄、状态和错误，以及 cleanup warning/hard age 是否越界。

readiness 必须持久化最近运行历史、最近成功 cutoff、heartbeat age、连续失败、cursor lag、最老 retry/backlog age 和告警阈值。普通 API 只返回脱敏摘要；provider route、文件系统路径/mount、actor 和详细错误只允许 operator 查询。

`.part` 或 quarantine 达到 warning 阈值时 readiness 为 `degraded`；达到 hard age/byte 阈值、sidecar 无法验证或无法证明 owner 已失效时，必须阻止新的 scheduled attachment write 和 destructive cleanup，但不阻止元数据发现、已验证本地读取或 operator 审计。只有 lease-generation-safe 的 `.part` 回收或 operator-authorized quarantine cleanup 使指标回落后，相关 blocker 才能解除。

资产层 readiness 与消费者迁移 readiness 必须分开。bootstrap/handoff、发现覆盖、附件完整性、主卷空间和备份配置决定资产日更是否可启用；公司画像或券商尚未切换只能阻止对应 consumer cutover 和 legacy cleanup，不得阻止共享资产回补、日更、local-first 查询或按需获取。

## 23. 测试要求

### 23.1 单元测试

- A 股 eligibility policy 纳入主板/科创板/创业板/北交所、ST 和停牌未退市股票，排除 B 股、基金/ETF、债券、指数和其他非 A 股证券，并固定 policy/master-data snapshot。
- 主数据或独立在市证券 census 刷新失败保留上一份完整配对快照；过期/不完整 census、目标交易所缺失、资格字段缺失以及 census 已有但本地主数据尚未纳入的新上市证券，均不得宣称全市场完成，并暴露 `eligibility_indeterminate` 和差异证据。
- 原版、摘要、英文版、图解版、修订通知、完整修订版分类。
- 财务年度提取、1 月和 4 月 30 日前后边界、报告期末后上市、延期披露、多个修订版排序和发布时间并列。
- 同一来源、相同时间精度但不同 hash 的完整修订只有明确法律优先证据才能决胜，否则 fail closed；撤回公告只有绑定具体候选时才生效。
- 同一来源、同一已证明法律链、相同时间精度且相同 hash 的完整修订使用版本化稳定公告/附件 ID 决胜时，正反发现顺序必须得到相同 winner/projection；不同 hash 不得复用该规则。
- 跨来源相同 hash/mirror 候选在正反发现顺序下得到相同 canonical projection 和 evidence-set hash；后来增加等价镜像不合并法律身份或静默改变既有 consumer processing identity。
- source/announcement/attachment/blob/consumer 四类 identity 不混合。
- local hit 零网络、metadata-only 下载、无记录有界发现、network disabled。
- `.part` 不可见、hash/长度/PDF 校验、超限和原子 rename。
- 两个调用方并发只下载一次。
- 修订失败保留原件；修订成功切换并在零引用时删除原件。
- Blob 多引用时不得物理删除。
- cursor 仅完整窗口推进；分页失败和 dense day 保留游标。
- 单日 1,500 条且单次最多读取 600 条时，跨运行/分片最终得到 1,500 个唯一公告，父窗口完成前 cursor 不越过该日。
- 已有原版时，一份发布时间早于 cursor 7 日但今天才被来源索引的修订版，在最大对账周期内被发现。
- 一份超出 publication lookback、数年后才被索引的受管旧财年修订版，仍由 oldest-first 期间队列在最大周期内发现。
- 同一公告包含完整中文正文、摘要、英文版和附录时只选正文；跨来源同时间不同 hash 时 fail closed。
- 修订撤回、同公告 ID 附件静默更新以及首次回补最新修订不可验证的 provisional/blocked 行为。
- 撤回有合法前任时回退并记录普通 replacement；撤回无合法替代稿时提交 `withdrawn_without_replacement`、零 current winner、nullable replacement deletion intent、withdrawal outbox 和单 predecessor `withdrawal_tombstone` recovery 流程。
- provider 只接受日期时，内部带时区 cutoff 被正确转换为 `YYYY-MM-DD` 并按明确定义的边界回过滤；不把 ISO datetime 直接传入日期参数。
- 旧 item/page cursor 只续扫同一未完成 fixed-cutoff scope；新 overlap 窗口不因复用旧游标漏掉迟到记录。
- overlap 重复观察不会复活已完成或已耗尽的附件重试；只有新 observation、到期 retry 或 operator repair 能重新排队。
- withdrawal-only observation 即使没有可下载附件也会触发期间重选；同批原版和更晚修订版在修订不可验证时遵循 blocked/provisional 规则。
- 同一公告第 10 天静默换 hash 或首次观察撤回时，知识截止第 5 天不得看到新字节或被追溯撤回；带 attachment/hash/observation pin 的 exact filing 不得被当前 observation 替代。
- 回补中断恢复不重新下载。
- 新上市股票自动加入日更 coverage，退市只移出活跃分母不删除资产，旧财年修订不影响新财年记录；显式指定退市/非活跃证券可执行有界 local-first 按需获取，但不得扩大 bootstrap/daily 分母、游标 scope 或触发退市全集扫描。
- 空间 hard stop 仅阻止附件，不阻止元数据。
- 并发下载的 byte reservation 不得合计越过 hard reserve。
- NAS 未挂载时备份 fail closed。
- 同一服务器不同 export、备份目标已有 hash 不符文件、数据库水位不一致时不得满足删除备份门槛；普通备份不得静默覆盖不匹配目标或推进 watermark，显式 repair 后可幂等恢复。
- 删除状态机在 DB commit、unlink 和 finalize 各阶段崩溃后可幂等收敛；同 source/export 但 filesystem identity 改变、同一 identity 变为只读、或 unlink 后切到恰好也不存在目标路径的 fallback mount 时，意图必须保持 `deleting`、readiness blocked，且不得写入 `deleted` 审计。
- 修订激活事务必须同时写入 replacement edge、删除意图和 change-event outbox；在提交前、提交后投递前、投递后 consumer checkpoint 前崩溃时，事件均可幂等重放且不得永久漏掉消费者失效/重算。
- 修订激活事务提交后，即使事件发布、备份或 predecessor unlink 失败，新修订版仍保持 effective，旧版进入可重试 cleanup；激活提交前失败才保留旧版为 effective。
- 准备从主卷删除的 predecessor 及 replacement 均已进入同一独立故障域备份/配对 catalog 水位，且删除后可按 recovery manifest 恢复旧 hash 字节。
- recovery journal 缺失中间 increment、顺序交换、尾部截断或 payload/hash 篡改时，恢复必须 fail closed，且不得重新开放读取、写入、consumer startup 或删除。
- 旧 schema 的 recovery manifest 即使保存过 catalog snapshot watermark/hint，迁移也不得据此自动生成 recovery-pair closure；必须重新完成双向验证后追加 closure。
- adopted Blob 即使仍位于受控 legacy path，也能由 catalog required-set 枚举并按 hash 备份，不要求先移动到 canonical blob 目录。
- shadow adoption 在 asset promotion 前不能满足 effective lookup/bootstrap coverage，promotion 后即使两个业务消费者均未 cutover 也可供资产层回补和日更复用。
- `.part` 的 owner/generation 竞争和 quarantine age/byte gate、operator 审计清理均有崩溃与并发测试。
- promoted legacy path 遭旧 writer 覆盖、截断或 cleanup 删除时立即失效且不流出错误字节；无法证明单文件 custody 时 promotion 必须先收敛到受控 path。
- orphan 只有在零附件下载的官方元数据/operator evidence 完整闭合身份和 hash 后才能转为 shadow；歧义 orphan 保持 fail closed。
- asset request 和 consumer request 在保留期内复用原句柄，过期后原句柄返回 `expired` 且旧幂等键不能创建新工作；consumer handle 过期不得删除或错误失效已完成的业务结果。
- 无 operator scope、身份边界未配置、缺少单项破坏性 action flag 或 target 越界时，scheduler/repair 命令必须保持 operation、provider、文件、数据库和水位零变化；read-only integrity audit 必须保持文件 hash/mtime 和业务状态不变。

### 23.2 迁移测试

- 在真实现有 business-profile 与 financial-fact 表、索引、约束和代表性行上分别做迁移前后快照比较；公告资产 schema 创建/升级不得改名、删列、重建或改变这些既有业务契约，不能用合成占位表代替兼容性证据。
- 采用公司画像现有 Q4 年报并验证 hash。
- 排除公司画像 `derived/` 和 Q2 半年报。
- 采用券商年末报告，暂不迁移半年报。
- 同一 filing 同 hash 双副本不立刻删除。
- manifest 与文件 hash、股票、报告期冲突时 fail closed。
- consumer 切换、备份和引用核对前禁止 cleanup。
- fixture 混放半年报、其他财年、`derived/`、孤儿和冲突文件时，dry-run 和执行均保持这些排除项的路径、字节、hash、mtime 和权限不变；不得 touch、chmod、move、link、quarantine 或重写排除项。
- 两个法律公告共享一个 hash、存在 legacy alias 和在途 reader 时，最后一个 retention pin 释放前不得 unlink。

### 23.3 跨业务集成测试

- 业务 A 首次下载后，业务 B 必须零网络复用相同 asset。
- 公司画像关闭时，回补、日更和券商读取仍工作。
- 券商 parser 失败不影响公司画像读取资产。
- 公司画像 parser 升级不触发 PDF 重下载。
- 修订版生效后两个消费者收到变更并分别重算。
- 旧业务 API 保持兼容，新 API 不泄露文件路径。
- 所有 GET 零网络；重复 POST、API+scheduler+consumer 并发复用一个 internal asset operation 和一次物理写入，各 principal 保持独立 request handle。
- operation 在进程重启后可继续轮询/恢复，并隔离不同调用者权限。
- 两个 principal 请求同一资产时复用一个底层下载 operation，但各自获得独立 subscription/opaque handle、幂等记录和脱敏诊断，不能读取对方 continuation。
- `asset_request_id` 与 `consumer_request_id` 可独立轮询且 owner 隔离；取消最后一个 asset subscription 只 detach，底层有界 acquisition 继续，已启动 consumer processing 不被级联取消。
- 删除 linked `asset_request_id` 后，`consumer_request_id` 仍保持 `pending_asset` 并在底层 acquisition 完成后继续推进；只有 consumer-request DELETE 可取消其 pending continuation。
- 公共 asset-id 文件流拒绝 superseded、missing、hash mismatch 和路径穿越；只有内部 DataManager/service 的授权 exact-filing observation pin 可读取仍保留且精确匹配的 non-effective 字节；knowledge cutoff 不读取截止日后的修订版。
- 已按 V1 删除的 superseded/withdrawn exact filing 只能返回 metadata + local-content-unavailable，普通 ensure 不得重新下载。
- 仍因合法 pin 保留的 non-effective exact observation 只有通过授权的内部 DataManager/service exact-filing observation handle 才可返回其精确 hash 字节，且必须零网络、零新 operation、零写入；公共 API ensure 仅返回 metadata 和 `retained_internal_only|local_content_unavailable`，公共 asset-id content route 固定返回 410；never-downloaded 和已删除 observation 均保持 metadata-only。

### 23.4 有界真实来源验证

分别选取 SSE、SZSE、BSE 的原版、完整修订版、摘要和空结果样本，验证：

- provider 类别和分页；
- 公告时间、股票和报告期规范化；
- 修订分类；
- 附件下载、hash 和 rate limit；
- overlap 重跑幂等；
- primary 失败后，完整 fallback 只推进 fallback 自己 source-qualified 的 cursor/`covered_until`，primary cursor/gap 始终保持未完成；只有经审计证明查询边界等价的 fallback 完整结果才可把独立 route-level required-coverage projection 标记为 satisfied 并允许相应 full-market readiness，非等价 fallback 不得补足该 projection。探针必须分别断言 primary cursor、fallback cursor、route coverage 和 readiness；
- 不进行无界全市场写入。

## 24. 验收标准

### 24.1 功能验收

- 在公司画像和券商功能均关闭时，可独立运行回补、日更、查询和按需获取。
- 历史回补覆盖范围内每只股票都有明确终态，并且只下载最新可得财务年度 winner。
- 日更发现新完整年报后可自动下载并成为本地有效资产。
- 日更发现完整修订版后，新文件校验成功才切换；无引用旧文件按规则删除且有审计。
- 同一股票每个财务年度在存在合法可用候选时只有一个当前有效附件；撤回无替代稿或其他已证明无 winner 的期间必须没有 current 附件，并有显式 no-winner 决策证据。
- API 业务调用本地有文件时零网络复用；本地缺失时可创建有界 operation 获取。
- 已有有效文件在迁移中被采用，不发生无必要下载或删除。

### 24.2 完整性与性能验收

- 任何有效 asset 都能通过 PDF 签名、长度和 SHA-256 校验。
- 并发相同请求只产生一个物理下载。
- 正常日更主要使用市场窗口请求，不对约 5,500 只股票逐日全量查询。
- overlap 重跑不产生重复公告、附件、Blob 或 operation。
- 长窗口轮换对账可在最大周期内发现迟到和回填旧发布时间的修订版。
- 任务达到请求、页数、时间、字节或空间限制时返回 partial/blocked，不虚报 complete。
- 单日超限通过稳定分页续扫/子分片完成；没有安全完成路径时保持 blocker 而非推进 cursor。

### 24.3 数据与运维验收

- 存储预检显示 latest-only 回补计划可容纳，并保留配置的 hard reserve。
- 附件备份独立运行，NAS 不可用时明确失败且不退化到本地。
- 允许普通修订 predecessor 物理删除前，replacement 和 predecessor 均已在独立故障域备份并与包含替换边和 recovery manifest 的 catalog 恢复水位匹配；撤回无替代稿时，withdrawn predecessor 与 no-winner catalog/recovery watermark 必须匹配，且不得伪造 replacement。
- 配对恢复后，全部 current-effective、no-winner projection、retention-pinned、pending-deletion replacement/predecessor 和 V1 recovery manifest 中永久 active 的普通 predecessor、`withdrawal_tombstone` predecessor、legacy duplicate Blob 的 identity、length、hash、不可变决策历史和消费者血缘一致；抽样恢复演练只能作为附加证据，不能替代开放门槛。
- legacy archive 写入只在双读对账、备份和回滚门槛通过后关闭。

### 24.4 AI/API 调用验收

- 可查询年报是否存在、当前财务年度、是否修订、本地完整性和最后检查时间。
- 缺失时可发起获取并看到 queued 到 completed/failed/blocked 的状态变化。
- 业务命令在资产获取期间即可获得并轮询 `consumer_request_id`（`pending_asset`），资产就绪后沿用同一句柄进入 `queued/processing/current`；已有资产但尚无 current consumer result 时返回 HTTP 202，而不是误报业务完成。终态 `missing` 仍返回可审计的 `consumer_request_id` 和 `Location`，不带 `Retry-After`。
- 只有 `local_valid` 资产返回非空 `content_url`，调用方可通过该 asset-id 内容端点安全读取本地 PDF；其他状态返回 `null`。
- 公司画像和券商结果可追溯到共享 asset ID 和 hash。
- 修订发生后，依赖旧原件的业务结果可见“待重算/已过期”状态。
- 未配置可信鉴权边界时，获取、内容流和管理端点保持关闭。
- OpenAPI 契约和 AI/API 状态映射通过快照/集成测试；验收证据绑定 backend candidate 和 OpenAPI contract version，不设置外部 UI 仓库、页面或部署门禁。

## 25. 迁移、上线与回滚

### 25.1 上线阶段

1. 新建 schema、repository、classifier 和配置，所有新作业默认关闭。
2. 运行现有文件只读 inventory，解决身份和 hash 冲突。
3. shadow 采用现有有效文件，建立共享查询但不改变消费者。
4. 实现 local-first、文件生命周期、空间和备份，在临时库/目录验证。
5. 在临时范围完成 latest-only 回补和中断恢复验证。
6. 上线独立手工作业和有界真实来源探针。
7. 完成全市场 asset-adoption 对账、备份和容量检查后运行正式回补。
8. 资产层覆盖、完整性、存储、备份配置和 handoff 门槛通过后启用日更 cron；不得等待公司画像或券商 cutover。
9. 上线 DataManager/API 和共享资产 AI/API 调用状态契约。
10. 券商按自己的 consumer gate 切共享读取，验证事实等价和零重复下载。
11. 公司画像按自己的 consumer gate 切共享读取，验证知识截止、派生物和语义流程兼容。
12. 两个消费者各自完成切换后关闭对应 legacy writes，并按审计计划清理其冗余副本。

### 25.2 回滚

在 legacy writes 未关闭、文件未清理前：单个消费者回滚只关闭该消费者的 shared-read/cutover gate，并恢复其 legacy path；共享资产 backfill、daily、其他消费者和 local-first 服务继续运行。只有共享资产模块自身存在完整性、存储、鉴权或数据安全故障时，operator 才可依据独立 blocker/审计决策停用 daily scheduler；回滚始终保留新增记录和已采用文件。

在重复或旧文件已经物理删除后，普通“消费者回滚”不得直接用旧数据库快照覆盖仍在运行的 live catalog。应在隔离临时根目录中验证匹配的应用版本、catalog 快照和附件/file-manifest 水位，并使用 `official_asset_recovery_manifest` 中不可变、版本化、hash 校验的 `legacy_path -> content_hash/shared_asset/consumer` rollback entry 重建所需 alias/copy，验证 legacy consumer 可读后再发布兼容路径和切换 consumer gate。只有真实数据丢失的灾难恢复才允许覆盖式 paired restore；恢复前必须冻结写入、声明 snapshot RPO，并重放或证明不存在快照后的 outbox、operation、lineage 和审计增量。V1 的 recovery-manifest 条目一经批准即永久 active，不能被自动退休或 GC；后续若需要退休，必须另立需求定义 operator 授权、兼容窗口和恢复义务。只回滚代码不足以恢复旧原件。

任何阶段不得通过删除共享数据库记录来回滚；新增记录和审计可以保留，避免二次迁移丢失证据。

## 26. 已确认取舍与后续议题

### 26.1 已确认取舍

- 公告资产管理独立于公司画像，后者只是消费者之一。
- V1 日更既发现元数据，也主动下载有效完整年报附件。
- 历史回补只补每股最新一期最有效年报。
- 每股每财务年度只保留一份最有效物理附件。
- 完整修订版校验并生效后删除无引用的旧原件，但保留元数据和删除审计。
- 半年报和其他公告类型仅预留架构，不纳入一期全市场主动下载。

### 26.2 需要后续独立决策的议题

- 是否为回测和历史知识截止保留所有修订前 PDF，建立 point-in-time 原件仓库；该目标与一期“删除旧附件”要求存在直接存储策略冲突。
- 半年报何时纳入共享调度，以及是否像年报一样全市场主动下载。
- 其他公告类型的订阅范围、元数据保留期和附件下载策略。
- 若未来另立可视化客户端项目，其页面布局、交互和通知策略应作为独立需求；当前公告资产模块不以该项目为依赖或上线门禁。

## 27. 需求追踪与实现入口

`design.md` 的 `Requirement Traceability` 表只维护稳定的 topic-level 追踪锚点。任务 1.8 必须在后续实现完成声明前，同时为本文每个可独立验收的需求叶子和每个 OpenSpec normative scenario/可独立测试 SHALL 建立不可变唯一 ID，并在 `evidence/traceability_registry.json` 中完成双向的 `requirement leaf <-> scenario/clause <-> exact task <-> owner` 映射。需求叶子必须保存稳定 ID、规范化文本 hash 和可审计 source locator；不能用整份文档 hash、上级章节号或同一 Requirement 下的笛卡尔式任务集合代替逐条映射。任务 11.7 保留这些 ID 并补齐 `test/evidence -> final status` 形成唯一发布依据。修改本文任何强制口径时必须同步更新 topic anchor、OpenSpec 场景、任务和逐条 registry/evidence；不能仅凭 OpenSpec 语法校验、主题矩阵、文件指纹或任务勾选宣称覆盖完整。

详细规范和实现任务位于：

- `openspec/changes/establish-shared-announcement-asset-management/proposal.md`
- `openspec/changes/establish-shared-announcement-asset-management/design.md`
- `openspec/changes/establish-shared-announcement-asset-management/specs/`
- `openspec/changes/establish-shared-announcement-asset-management/tasks.md`

继续开发、验收和上线前，均应完成 OpenSpec 严格校验，并再次确认一期对“删除修订前物理 PDF、不支持旧原件历史时点重放”的取舍；实施完成度只以任务、测试和上线证据为准。
