# 官方公告资产管理一期需求文档

> 状态：待实现
> 版本：V1.0
> 日期：2026-08-09
> OpenSpec 变更：`establish-shared-announcement-asset-management`

## 1. 文档目的

本文定义一个独立于公司画像、券商风控和其他业务模块的“官方公告资产管理”能力。第一期只正式管理 A 股完整年报及其完整修订版，负责公告发现、元数据登记、附件下载、完整性校验、有效版本选择、文件保存、旧版本删除、历史回补、日常更新、按需获取、备份和审计。

该能力是公共依赖，不是公司画像的子功能。公司画像、券商风控及未来业务只消费共享资产，并继续拥有各自的解析器、派生文件、事实、审核和业务状态。

本文用于统一产品、研发、测试和运维口径；不代表本变更已开始实施。

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
- 通过 DataManager、FastAPI 和现有前台业务响应有效暴露资产可用性、获取进度和来源血缘。
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
- 不在本需求中实现仓库外不存在的独立 Web 前端项目；本期提供现有前台业务可消费的 DataManager/API/状态契约。

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

## 6. 范围与关键假设

### 6.1 默认股票范围

一期历史回补和日更覆盖本地主数据在任务启动时判定为：

- `type=stock`；
- `is_active=true`；
- 交易所为 SSE、SZSE 或 BSE。

退市、非活跃和范围外证券不进入默认全市场回补，但允许通过受控的按需接口获取指定年报。股票范围必须在每次历史回补启动时生成带版本和时间戳的快照，避免执行期间主数据变化造成覆盖分母漂移。

### 6.2 “最新一期”定义

历史回补所称“最新一期”是该股票在来源覆盖范围内已经正式发布的最新财务年度完整年报，而不是按当前自然年机械推算的报告期。新上市且尚未发布首份年报的股票可以进入 `confirmed_missing`，但必须记录搜索边界和证据。

### 6.3 日更后的保留范围

历史回补只补一个最新财务年度。进入日更后，新财务年度年报会新增保存，因此长期可以为同一股票保留多个财务年度；但每个财务年度始终只有一个最有效物理附件。

### 6.4 时间口径

- 公告必须保存官方原始发布时间、规范化发布时间、首次发现时间和最后检查时间。
- 业务回测或时点研究使用公告实际可得时间，不得使用报告期替代公告时间。
- 新修订版只能从其发布时间起成为可得资料；当前有效查询返回修订版，历史知识截止查询如果旧文件已按策略删除，应明确返回“元数据可追溯但原件不在本地”，不得退回当前修订版冒充历史资料。

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
- 管理替换关系、引用计数、旧文件删除和删除审计。
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

业务 parser 失败不得把共享资产标记为损坏；共享资产损坏也不得被业务重试状态掩盖。

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
- 当前引用计数或可事务计算的引用关系；
- 备份状态和最近验证时间。

同一内容可被多个法律附件引用，但不能因此合并法律公告身份。

### 8.4 `official_attachment_versions`

记录一次附件内容观察与获取证据：

- attachment ID、blob hash、最终 URL；
- 下载尝试、响应证据、长度和 hash；
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

该表表示当前最有效选择，不替代公告和附件历史表。

### 8.6 `official_asset_operations`

用于历史回补、日更、按需获取、迁移、完整性修复和备份：

- operation ID、类型、scope、幂等键；
- `queued/running/partial/completed/missing/failed/blocked`；
- 细分阶段 `discovering/downloading/validating/activating/backing_up`；
- checkpoint、lease、attempt、next_retry_at；
- 请求边界、进度计数、最后心跳、错误摘要和最终结果。

### 8.7 删除和消费者审计

- `official_asset_deletion_audit`：追加式保存旧/新 asset、hash、路径、引用判断、删除原因、任务/操作者和时间。
- `official_asset_consumer_processing`：可选公共索引，保存 `asset_id + consumer + parser_version + parameter_hash`，但业务自己的 manifest 仍是派生输出的权威记录。
- 资产新增、替换、修复和删除应生成可消费的 change event 或单调 watermark。

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

跨来源候选只有在内容 hash 相同或存在经过配置审计的官方镜像映射时，才能自动判为等价。不同来源对同一股票、同一财务年度给出不同完整 PDF 且无法证明法律优先关系时，必须 `ambiguous/blocked`，不得按 source 名称或 filing ID 字符串任意决胜。

分类和选择必须落到 attachment 级。一个公告同时包含完整中文报告、摘要、英文版和附录时，只允许完整中文正文参与 winner 选择。来源若撤回、取消公告，或在相同公告 ID 下静默更新附件，系统必须保存新的 observation 并重新评估；撤回当前有效修订版时，仅在可以证明前任仍合法、完整且物理可用时安全回退，否则进入 blocked。

首次历史回补若发现法律上更新的完整修订版但无法下载或验证，不得把原版宣称为最终 latest-effective。该股票应为 `blocked/retryable`。已经在线服务且原版仍有效时，可继续提供原版，但必须标为 `provisional` 或“存在待验证修订”，直到修订问题收敛。

### 9.3 修订替换事务

必须按以下顺序执行：

1. 登记新公告和附件元数据；
2. 取得附件级 lease；
3. 下载到 `.part` 临时文件；
4. 校验来源身份、PDF 签名、长度、SHA-256 和目标路径安全性；
5. 原子发布新 Blob；
6. 在数据库事务中激活修订版并记录替换边；
7. 发布资产变更事件，将依赖旧 asset 的业务处理标记为 superseded 或入重跑队列；
8. 解除旧有效附件对旧 Blob 的活动引用；
9. 仅在引用计数为零、替换记录完整、备份/删除前提满足时删除旧文件；
10. 写入追加式删除审计。

任一前置步骤失败时，旧年报必须继续有效和物理存在。不得因“发现修订标题”就先删除原件。

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
8. 对每只股票给出 `available`、`confirmed_missing`、`retryable` 或 `blocked` 之一。
9. 只有覆盖账本完整且无伪完成窗口时，bootstrap 才可标记完成。

回补必须固定 `as_of` 和来源/查询配置指纹，并为每只股票计算期望财务年度和最早搜索边界。上市日晚于报告期末的股票可以形成有证据的 `confirmed_missing`；延期披露、长期停牌或来源部分失败不能据此确认缺失。若较新财务年度窗口扫描不完整，不得降级选择更老财务年度并宣称“最新”。

### 10.3 为什么不是逐股全量扫描

约 5,500 只股票逐日逐股查询会产生大量请求和负结果，容易触发限流。历史回补应以市场级、年报类别、日期窗口扫描为主，只对不断缩小的缺失股票集合使用单标的查询。

### 10.4 恢复与幂等

- 窗口、股票覆盖、候选选择和下载分别持久化进度。
- 中断后不得重新下载已经验证的相同附件。
- 相同 scope 的重复任务应复用 operation 或被 lease 阻止。
- `confirmed_missing` 必须记录来源、完整成功的查询边界、上市时间、`as_of`、证据和过期时间，不得仅因一次空响应写入永久缺失。
- 来源暂时失败、页面超限或身份歧义只能进入 `incomplete/retryable/blocked`，不能计入成功覆盖。
- 回补只有在不存在 `incomplete/blocked` 时返回 `success`；仍有这类记录时只能返回 `partial`，即使本次运行已经停止。

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

不能用一个全局游标掩盖某一交易所或来源失败。

### 11.3 正常发现窗口

- `window_end` 为本次运行固定 cutoff，任务执行中不得随系统时间移动。
- `window_start` 为上次完整成功提交的最大发布时间减 overlap。
- overlap 默认 3 个自然日，可配置并通过真实 provider 延迟数据验证。
- 重叠窗口内依靠来源公告 ID 和附件身份幂等去重。
- 仅当整个请求窗口的所有分页完整成功，才推进 committed cursor。
- 附件失败不回滚已登记的元数据，但创建独立可重试下载 operation。

### 11.4 首次日更和披露季窗口

若 bootstrap 完成但没有合法游标，首次日更只使用配置的有限当前披露季窗口，不得静默扫描多年历史。建议配置明确的 `initial_lookback_days`，并由回补覆盖更早历史。

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

### 11.6 缺失修复

市场扫描后维护缺失覆盖队列。日更只处理有界的轮换 cohort，例如按配置限制股票数、请求数和运行时长；不得每天对全市场逐股查询。修复优先级建议为：

1. 已发现元数据但附件未就绪；
2. 当前披露季按上市时间应有年报但缺失；
3. 曾失败且到达 `next_retry_at`；
4. 长时间未复核的 `confirmed_missing`。

### 11.7 迟到修订与存量对账

3 日 overlap 只负责低延迟发现，不能证明 provider 不会迟到索引或回填旧发布时间。系统必须另设面向“已经有有效年报”的轮换式长窗口对账：

- 配置 `reconciliation_lookback_days`、每次 cohort 和最大完整对账周期；
- 保存每只股票/财务年度的 `last_reconciled_at`；
- 重新观察相同公告 ID 的标题、附件 URL、附件身份和内容变化；
- 修订版发布时间早于当前 cursor 但今天才可见时，仍在最大对账周期内被发现；
- 对账与 missing repair 分开计数，已有原版不能阻止股票进入修订对账。

### 11.8 股票范围生命周期

活跃股票范围应在每次日更前或按固定短周期刷新并生成可审计快照。新上市股票自动进入 coverage 和 repair；退市股票从活跃覆盖率分母移出，但不得因此删除已有资产。发现窗口内出现较老财务年度的完整修订版时，应只更新对应旧财务年度，不影响该股票更晚财务年度的有效记录。

## 12. Local-First 按需获取

### 12.1 统一调用契约

所有前台和后台业务必须调用共享 `ensure_annual_report()` 或对应 DataManager 方法，不得直接调用 provider 下载年报。

请求至少支持两类身份：

- `instrument_id + fiscal_year/report_period`：获取该期间最有效年报；
- `source + source_announcement_id/filing_id`：获取业务已绑定的精确法律公告。

请求策略至少包含：

- `allow_network`；
- 所需完整性级别；
- 同步等待上限或异步排队策略；
- 调用 consumer 和幂等键；
- 可选知识截止时间。

### 12.2 决策顺序

```text
本地有效资产存在
  -> 校验达到要求 -> local_hit，零网络返回

元数据存在但附件缺失
  -> allow_network=true -> 单附件下载、校验、归档
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
- 缺失资产的 API ensure 创建或复用 durable operation，返回 `operation_id` 和当前状态。
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

现有样本表明：

- 公司画像文件名已经包含股票、报告期、公告 ID 和 SHA-256，可作为采用证据；
- 公司画像目录也有 `derived/` 派生内容，公共资产迁移不得把派生文件当原始公告；
- 券商目录同时包含年报和半年报，一期只能采用报告期为年末且通过完整年报分类的原件；
- 两边可能保存相同附件的不同副本，必须按 source identity、报告期、PDF 签名、长度和 SHA-256 核对。

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

任何迁移清理都必须生成逐文件 allowlist，列出 managed path、manifest/asset ID、hash 和删除理由。允许删除的原因仅限：字节完全相同的冗余副本，或被完整修订版替代且已满足删除门槛的同财年旧原件。`derived/`、半年报、其他财务年度、未入 manifest 的孤儿文件和任何冲突文件一律排除。工具默认 dry-run，禁止目录级删除。

### 13.4 一个财务年度一个有效附件

该约束指一个有效逻辑选择和一个无冗余物理 Blob：

- 同一股票、财务年度只有一行当前有效选择；
- 若多个法律附件内容完全相同，可共享 Blob，但法律身份保留；
- 修订版生效后解除旧引用；
- 物理保留依据独立的 `retention pin`，包括其他有效附件、受管 legacy alias、尚未切换的消费者、活动文件流/解析 lease 和迁移操作；历史元数据外键本身不永久 pin 文件；
- 任一 retention pin 未释放时不能删除，pin 应由数据库查询/约束计算，不能依赖可能漂移的手工计数字段；
- 删除文件不删除公告、附件、hash、替换链和删除审计。

## 14. 当前存储容量评估与门槛

### 14.1 2026-08-09 只读测量基线

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

该估算不能替代运行时门槛。年报分布有长尾，个别文件可能明显大于平均值；未来每年新增一轮年报也会累计约数十 GiB。

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
- 按内容 hash 只复制缺失 Blob；
- 复制后校验长度和 SHA-256；
- 记录 destination identity、完成时间、错误和未保护字节；
- NAS 未挂载或路径退化为本地目录时 fail closed；
- 不得把本应写 NAS 的完整副本落到本地 `data` 卷。
- 备份目标必须与主 `data/filings` 处于独立存储故障域，并核对 mount source、服务器和 export 身份；同一服务器的不同 export 不计灾备。
- 当前 `data/filings` 与 `PVE-Bak` 都来自 `192.168.188.88`，因此 PVE-Bak 不能作为允许删除前任文件的唯一灾备；当前位于 `192.168.188.68` 的 QuoteBak 才具备不同主机这一最低隔离条件。
- Blob 备份水位必须能与 catalog 数据库备份或一致性 manifest 配对恢复；数据库快照早于 Blob 目录水位且无法证明一致时，不满足删除门槛。

### 15.2 删除前提

修订替换删除旧文件时，必须要求新文件本地完整、数据库已激活、所有 retention pin 已释放、删除审计可写，并且新文件已经在独立故障域完成长度/hash 验证且存在可配对恢复的 catalog 数据库快照或 manifest 水位。任一条件不满足时只可标记待删除，不得 unlink。

### 15.3 恢复顺序

恢复必须配对数据库和文件版本：

1. 恢复匹配时间点的数据库备份；
2. 按数据库登记的 hash 从附件备份恢复 Blob；
3. 运行全量或抽样完整性核对；
4. 重建可派生的引用计数和 readiness；
5. 再开放消费者读取和日更写入。

代码回滚不能代替在物理删除发生后的数据库加附件联合恢复。

## 16. 调度与配置需求

### 16.1 作业

至少提供：

| 作业 | 调用方式 | 默认建议 | 职责 |
| --- | --- | --- | --- |
| `annual_report_asset_latest_backfill` | 手工 | `manual_only=true` | 一次性/补偿性 latest-only 全市场回补 |
| `annual_report_asset_daily_update` | cron + 手工 | 初始关闭，验收后启用 | 每日发现、下载、修订替换和缺失修复 |
| `annual_report_asset_integrity_audit` | 手工/低频 | 默认只读 | 文件、hash、引用和备份审计 |
| `annual_report_asset_backup` | cron + 手工 | 独立启用 | 增量文件备份和验证 |

作业开关不得放在 `business_profile_evidence.enabled` 或 broker 配置之下。

### 16.2 配置字段

配置至少覆盖：

- module enabled、scheduled enabled、dry-run；
- active exchanges、instrument type/status；
- source routes、normalized categories、classifier version；
- bootstrap filing-season bounds 和 targeted repair bounds；
- daily cron、timezone、overlap days、initial lookback；
- max pages/requests/windows/instruments/bytes/elapsed；
- download concurrency、per-source concurrency、rate limits；
- lease TTL、heartbeat、retry backoff、max attempts；
- archive/temp/quarantine roots、max attachment bytes；
- warning/stop utilization 和 free-space reserve；
- backup mount、destination、freshness threshold；
- consumer dependency policy 和 rollout gates。

配置模板必须同步更新，但正式 enablement 应在回补和备份验收后执行。

## 17. DataManager 与服务接口

### 17.1 查询接口

DataManager 至少提供：

- 按股票、财务年度、来源、filing ID、完整性、获取状态列出资产；
- 获取某股票某年度当前最有效年报；
- 查询 active-universe coverage、存储、备份和 scheduler readiness；
- 查询 durable operation 状态；
- 根据 asset ID 获取受控文件流或内部文件 handle。

现有 `get_annual_report_assets` 和 `get_annual_report_asset` 在迁移期改为共享仓库 read-through adapter，避免破坏调用方；长期不得继续只筛选公司画像 manifest。

### 17.2 Ensure 返回状态

结构化结果至少区分：

- `local_hit`；
- `adopted`；
- `downloaded`；
- `queued`；
- `missing`；
- `ambiguous`；
- `failed`；
- `blocked`。

返回应包含 asset/operation ID、股票、财务年度、来源、filing ID、发布时间、是否修订、hash、长度、完整性、是否当前有效、诊断和下一步，但不向外部客户端暴露任意服务器绝对路径。

## 18. FastAPI 与前台业务整合

### 18.1 建议的增量端点

最终路径应遵循现有 API 命名，语义至少包括：

- `GET /api/v1/research/company/{instrument_id}/annual-reports`：分页查询本地记录，纯读、零网络；
- `GET /api/v1/research/company/{instrument_id}/annual-reports/effective`：按 fiscal year 查询单一有效状态；
- `POST /api/v1/research/company/{instrument_id}/annual-reports/ensure`：只为单股票单财年或精确 filing 创建/复用 operation；
- `GET /api/v1/research/annual-report-operations/{operation_id}`：轮询 durable operation；
- `GET /api/v1/research/annual-report-assets/readiness`：查询覆盖和运行状态；
- `GET /api/v1/research/annual-report-assets/{asset_id}/content`：按 asset ID 安全下载文件。

实际路由必须避免动态路径冲突，并通过 OpenAPI snapshot 固定。任何 GET，包括现有公司画像 GET，都不得隐式触发公告发现或下载。POST 本地命中返回 HTTP 200；创建或复用异步 operation 返回 HTTP 202，并设置 `Location` 和 `Retry-After`。

### 18.2 前台状态模型

前台必须分别呈现三套正交状态，不得把“附件下载完成”误解为“业务结果已更新”：

- asset availability：`local_valid/metadata_only/missing/corrupt/superseded/blocked`；
- operation state：`queued/discovering/downloading/validating/completed/missing/failed/blocked/cancelled/expired`；
- consumer processing：`not_started/queued/processing/current/stale/failed`。

ensure 另返回 `disposition=local_hit|operation_created|operation_reused`。operation 明确 terminal 集、是否可重试、reason codes、attempt、`next_retry_at`、创建/开始/心跳/完成时间、进度和脱敏诊断。若一期不支持取消，必须定义超时、lease 过期和重启恢复语义。

| 状态 | 前台含义 | 允许操作 |
| --- | --- | --- |
| `local_valid` | 当前有效附件本地完整 | 查看来源、下载、启动业务处理 |
| `metadata_only` | 已知公告但附件未下载 | 发起获取 |
| `queued/discovering/downloading/validating` | 正在获取 | 轮询，不重复创建任务 |
| `missing` | 有界搜索未找到 | 显示最后检查时间，可按策略重试 |
| `ambiguous` | 候选不能安全决策 | 展示诊断，等待运维处理 |
| `blocked` | 空间、权限、网络策略或备份门槛阻止 | 展示可执行的 blocker |
| `corrupt/failed` | 文件或获取失败 | 有权限时重试，不把资产交给 parser |

前台不得用“是否有公司画像”推断“是否有年报”。年报状态和业务画像状态分别展示或组合，不互相覆盖。

### 18.3 业务触发流程

当用户在公司画像或券商风控前台发起依赖年报的操作：

1. 后端先调用共享 local-first 查询；
2. `available` 时直接将 asset ID 交给业务 parser；
3. 缺失且允许获取时创建 ensure operation；
4. 前台轮询 operation，完成后再触发或自动排队业务处理；
5. 业务结果保存共享 asset ID、来源公告、报告期、hash 和修订状态；
6. 若之后修订版生效，前台将依赖旧 asset 的业务结果标记“来源已更新/待重算”，不得静默显示为最新。

现有公司画像 GET 保持本地纯读。应增加可选 `source_assets/annual_report_asset` 和 `consumer_processing_status` 字段而不破坏现有必填字段。缺失时由有权限用户显式点击获取，前台禁用重复提交并按 `Retry-After` 轮询。只有 `local_valid` 才显示查看/下载入口；superseded 旧 asset 的内容请求返回明确 409/410 或当前资产提示，不能继续流式输出旧文件。

### 18.4 API 安全与边界

- 获取和文件下载需使用现有授权机制；
- 当前仓库没有完整认证中间件，只有 CORS、限流和并发保护；因此 V1 必须实现可信反向代理身份、管理凭证或等价最小权限边界，否则 acquire、content、readiness 管理端点默认关闭；
- 建议权限至少区分 `annual_report_assets:acquire`、`annual_report_assets:read_content` 和 operator/admin；operation 查询校验创建者或管理权限；
- ensure 只允许单股票、单期间等有界 scope，不能由前台触发全市场回补；
- 使用幂等键和 rate limit 防止重复任务；
- 文件下载仅接受 asset ID，拒绝 caller path；
- 返回安全文件名和 `application/pdf`；
- 下载前再次核对 asset 当前状态和文件完整性；
- 内部路径、provider 敏感诊断和异常堆栈不得直接返回前台。

API operation 必须落在共享 SQLite durable operation/lease 中，不能复用 FastAPI `BackgroundTasks` 作为状态真相。规范化 scope、财年/精确 filing 和 policy version 构成 single-flight key，同时支持 `Idempotency-Key`。scheduler、API 和业务消费者命中相同 key 时复用同一 operation。

本仓库未包含实际 Web 前端源码。本期仓库交付范围是 DataManager、FastAPI、OpenAPI、状态模型和集成测试；真实 UI 所在仓库、负责人和上线版本必须作为外部交付 gate 登记，不能以后端接口完成宣称“前端已完成整合”。

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
- 券商 parser 和 `financial_numeric_facts_hot/history` 写入规则不变。
- parser manifest 增加共享 asset ID，且 parser 失败只影响券商 processing。
- 年报修订版替换后，应自动将使用旧 asset 的券商 processing 标记 superseded 或排队重跑，当前事实不得静默继续绑定已删除原件。
- 迁移回归必须证明共享本地命中时 provider 请求数和新 archive 写入数均为零。

## 21. 可靠性、并发与失败处理

### 21.1 单飞与 lease

- 唯一活动 lease 至少按附件观察 identity 或精确 acquisition scope 建立。
- lease 包含 owner、TTL、heartbeat 和 attempt。
- 进程崩溃后 lease 到期可恢复，但新 worker 必须先处理遗留 `.part`。
- 数据库唯一约束和文件原子 rename 共同保证不发布重复 Blob。

### 21.2 重试分类

可重试：网络超时、限流、暂时 5xx、来源附件暂不可用、NAS 暂时不可用。
不可自动重试或需人工：身份冲突、路径不安全、非 PDF、持续 hash 不匹配、候选歧义、单日窗口无法完整分页。
空间不足：进入 blocked，等待空间变化或审计过的 operator override，不使用高频退避重试。

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
- daily scheduler 是否启用及最近成功；
- 公司画像和券商迁移阶段；
- 是否允许停止 legacy writes 和删除重复文件。

## 23. 测试要求

### 23.1 单元测试

- 原版、摘要、英文版、图解版、修订通知、完整修订版分类。
- 财务年度提取、多个修订版排序和发布时间并列。
- source/announcement/attachment/blob/consumer 四类 identity 不混合。
- local hit 零网络、metadata-only 下载、无记录有界发现、network disabled。
- `.part` 不可见、hash/长度/PDF 校验、超限和原子 rename。
- 两个调用方并发只下载一次。
- 修订失败保留原件；修订成功切换并在零引用时删除原件。
- Blob 多引用时不得物理删除。
- cursor 仅完整窗口推进；分页失败和 dense day 保留游标。
- 单日 1,500 条且单次最多读取 600 条时，跨运行/分片最终得到 1,500 个唯一公告，父窗口完成前 cursor 不越过该日。
- 已有原版时，一份发布时间早于 cursor 7 日但今天才被来源索引的修订版，在最大对账周期内被发现。
- 同一公告包含完整中文正文、摘要、英文版和附录时只选正文；跨来源同时间不同 hash 时 fail closed。
- 修订撤回、同公告 ID 附件静默更新以及首次回补最新修订不可验证的 provisional/blocked 行为。
- 回补中断恢复不重新下载。
- 新上市股票自动加入日更 coverage，退市只移出活跃分母不删除资产，旧财年修订不影响新财年记录。
- 空间 hard stop 仅阻止附件，不阻止元数据。
- 并发下载的 byte reservation 不得合计越过 hard reserve。
- NAS 未挂载时备份 fail closed。
- 同一服务器不同 export、hash 不符、数据库水位不一致时不得满足删除备份门槛。
- 删除状态机在 DB commit、unlink 和 finalize 各阶段崩溃后可幂等收敛。

### 23.2 迁移测试

- 采用公司画像现有 Q4 年报并验证 hash。
- 排除公司画像 `derived/` 和 Q2 半年报。
- 采用券商年末报告，暂不迁移半年报。
- 同一 filing 同 hash 双副本不立刻删除。
- manifest 与文件 hash、股票、报告期冲突时 fail closed。
- consumer 切换、备份和引用核对前禁止 cleanup。
- fixture 混放半年报、其他财年、`derived/`、孤儿和冲突文件时，dry-run 和执行均保持这些排除项的 hash/mtime 不变。
- 两个法律公告共享一个 hash、存在 legacy alias 和在途 reader 时，最后一个 retention pin 释放前不得 unlink。

### 23.3 跨业务集成测试

- 业务 A 首次下载后，业务 B 必须零网络复用相同 asset。
- 公司画像关闭时，回补、日更和券商读取仍工作。
- 券商 parser 失败不影响公司画像读取资产。
- 公司画像 parser 升级不触发 PDF 重下载。
- 修订版生效后两个消费者收到变更并分别重算。
- 旧业务 API 保持兼容，新 API 不泄露文件路径。
- 所有 GET 零网络；重复 POST、API+scheduler+consumer 并发复用一个 operation 和一次物理写入。
- operation 在进程重启后可继续轮询/恢复，并隔离不同调用者权限。
- 文件流拒绝 superseded、missing、hash mismatch 和路径穿越；knowledge cutoff 不读取截止日后的修订版。

### 23.4 有界真实来源验证

分别选取 SSE、SZSE、BSE 的原版、完整修订版、摘要和空结果样本，验证：

- provider 类别和分页；
- 公告时间、股票和报告期规范化；
- 修订分类；
- 附件下载、hash 和 rate limit；
- overlap 重跑幂等；
- 不进行无界全市场写入。

## 24. 验收标准

### 24.1 功能验收

- 在公司画像和券商功能均关闭时，可独立运行回补、日更、查询和按需获取。
- 历史回补覆盖范围内每只股票都有明确终态，并且只下载最新可得财务年度 winner。
- 日更发现新完整年报后可自动下载并成为本地有效资产。
- 日更发现完整修订版后，新文件校验成功才切换；无引用旧文件按规则删除且有审计。
- 同一股票每个财务年度只有一个当前有效附件。
- 前台业务本地有文件时零网络复用；本地缺失时可创建有界 operation 获取。
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
- 允许物理删除前，replacement 已在独立故障域备份并与 catalog 恢复水位匹配。
- 数据库和附件抽样恢复后 asset identity、hash、有效版本和消费者血缘一致。
- legacy archive 写入只在双读对账、备份和回滚门槛通过后关闭。

### 24.4 前台验收

- 可查询年报是否存在、当前财务年度、是否修订、本地完整性和最后检查时间。
- 缺失时可发起获取并看到 queued 到 completed/failed/blocked 的状态变化。
- 可按 asset ID 安全下载本地 PDF。
- 公司画像和券商结果可追溯到共享 asset ID 和 hash。
- 修订发生后，依赖旧原件的业务结果可见“待重算/已过期”状态。
- 未配置可信鉴权边界时，获取、内容流和管理端点保持关闭。
- OpenAPI 契约和前端状态映射通过快照/集成测试；实际 UI 仓库和上线责任作为外部 gate 有记录。

## 25. 迁移、上线与回滚

### 25.1 上线阶段

1. 新建 schema、repository、classifier 和配置，所有新作业默认关闭。
2. 运行现有文件只读 inventory，解决身份和 hash 冲突。
3. shadow 采用现有有效文件，建立共享查询但不改变消费者。
4. 实现 local-first、文件生命周期、空间和备份，在临时库/目录验证。
5. 在临时范围完成 latest-only 回补和中断恢复验证。
6. 上线独立手工作业和有界真实来源探针。
7. 券商先切共享读取，验证事实等价和零重复下载。
8. 公司画像切共享读取，验证知识截止、派生物和语义流程兼容。
9. 上线 DataManager/API 和前台状态整合。
10. 完成全市场 shadow 对账、备份和容量检查后运行正式回补。
11. 覆盖门槛通过后启用日更 cron。
12. 最后关闭 legacy writes，并按审计计划清理冗余副本。

### 25.2 回滚

在 legacy writes 未关闭、文件未清理前：关闭共享 consumer gate 和 daily scheduler，保留新增记录和已采用文件即可。

在重复或旧文件已经物理删除后：必须恢复匹配的应用版本、数据库备份和附件备份。只回滚代码不足以恢复旧原件。

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
- 前端项目独立存在后，具体页面布局、权限和通知交互；本期先稳定 API 和业务状态契约。

## 27. 实现入口

详细规范和实现任务位于：

- `openspec/changes/establish-shared-announcement-asset-management/proposal.md`
- `openspec/changes/establish-shared-announcement-asset-management/design.md`
- `openspec/changes/establish-shared-announcement-asset-management/specs/`
- `openspec/changes/establish-shared-announcement-asset-management/tasks.md`

进入开发阶段前，应先完成 OpenSpec 校验，并再次确认一期对“删除修订前物理 PDF、不支持旧原件历史时点重放”的取舍。
