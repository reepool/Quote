## Context

### 1. 已核实事实与证据边界

调查日期：2026-11-15，业务日期按 `Asia/Shanghai`。以下是调查时点的基线，不承诺生产库后续保持相同计数。

| 证据 | 核实结果 | 用途与限制 |
|---|---|---|
| [康佳 2026-103 摘牌公告](https://static.cninfo.com.cn/finalpage/2026-10-23/1225615607.PDF) | 000016、200016 分别为 A、B 股；决定日 10-16，实际摘牌日 10-23 | 本案退市及日期的正式依据；不能把公司债券一并标退市 |
| [康佳 2026-100 决定公告](https://static.cninfo.com.cn/finalpage/2026-10-17/1225608143.PDF) | 主动终止上市，不设退市整理期；摘牌后披露平台转至全国股转系统 | 解释简称未经过“退”字阶段；未证明 Q3 依法免披露 |
| 深交所官方终止上市 XLSX | 213 行，列为证券代码／证券简称／上市日期／终止上市日期；000016、200016 日期均为 2026-10-23 | 验证现成结构化来源；213 是当次行数，不作为以后验收常量 |
| `quotes.db.instruments` | 000016.SZ 为 `active / is_active=1 / trading_status=1 / delisted_date=NULL`，简称 `*ST康佳A` | 说明本地运行态错误 |
| `instrument_master_discrepancies` | 10-23 19:16 至 11-15 00:16，63 次 `official_current_list_missing_no_terminal_evidence` | 已发现消失但未消费候选；不把重复记录数当作证券数 |
| `financials.db` 运行 1605 | `financial_disclosure_reconciliation_sync`，11-15 09:30 开始，degraded；1284 个候选，1283 个 accepted state，唯一 local gap／blocker 为康佳 Q3 | 0 个公告事件驱动，不能描述成“发现三季报公告后下载失败” |
| 财务事件状态 | `local-gap:000016.SZ:2026-09-30`，`blocking_gap / local_core_gap`，五项事实缺失，event_count=0 | 说明重复产生机制；不证明所有平台均无 Q3 报告 |

官方目录核查未见康佳 Q3，公告明确后续披露转向其他平台。本 change 不扩建退市板块财务采集，也不据此作出法定披露义务结论。

2026-11-15针对外审补充核验：本次实取XLSX的康佳A代码单元格为字符串 `000016`（类型`s`），不能把“本次原始单元格就是数值16”写成已核实事实。但默认 `pandas.read_excel` 会对这类纯数字字符串做类型推断而得到整数16；使用原始字符串单元格的内存工作簿已复现。故测试必须保留真实原始XLSX并走完整读取链，同时添加明确标注为合成的数值单元格16变体，验证读取后16／"16"能归一为000016.SZ；不得先把fixture补零后只测匹配。

### 2. 现有调用链与最小改动点

```text
Scheduler / Telegram / 既有运维入口
  → run_master_governance_for_job(a_share_stock)
  → AShareStockPolicy
  → DataManager.sync_instrument_master()
  → SourceFactory → exchange_official → BaoStock → AkShare
  → 官方终态核对（本 change 补齐）
  → DatabaseOperations 生命周期 owner
  → 当前业务重新读取本地股票池
```

直接相关实现：

- `data_sources/a_share_official_stock_master.py`：沪深只读 current list，当前行统一设 active；SSE 分板块请求，不能以另一板块成功掩盖缺失板块。
- `data_sources/source_factory.py`：官方成功时，备源官方名单外的记录只作诊断；这是防幽灵股保护，不能通过恢复名单并集绕过。
- `instrument_master_governance.py` 的 `AShareStockPolicy.execute()` 当前没有将audit模式传入sync；factory的 `_save_instruments_cache()` 实际调用 `save_instrument_list()` 写 `instruments`，不是纯内存缓存。audit零业务写入需要修接线，不能假定现有模式标签已经提供保护。
- BaoStock给出的past outDate／status也会经上述factory保存，在manager读取来源权威前落库；正常生产缓存路径同样需要前置终态证据约束，不能仅在末端新增正式核对步骤。
- `config/a_share_code_lineage.json` 已有 `600018.SH` 上港集箱前身与上港集团现主体的不同区间，`data_sources/a_share_code_lineage.py` 和既有metadata提供已审核沿革；同码不能直接推定为同一当前主体。
- `data_manager.py`：`_deactivate_official_absent_delisting_prefixed_stocks()` 按名称加消失停用；BSE 有公告确认，沪深无终态读取。旧名称路径必须被替代，而非与新路径双跑。
- `database/operations.py`：`mark_instrument_delisted()` 目前可提前写未来日期，传空日期会覆盖已有日期；普通 upsert 对无日期终态只拦非官方源，且未完整保护三字段。
- `research/financial_disclosure_incremental_sync.py`：当前 active 股票池驱动新公告、pending／accepted 回灌及周度 local-gap；旧 blocking 状态本身不在 pending 回灌列表。
- `scripts/dev_validation/prepare_sina_ths_local_core_import_manifest.py`：当前存在“退市早于披露截止日”接受上一季缺口的启发式，需做本地语义修正。

### 3. 规范依赖与开发约束

`prioritize-a-share-official-stock-master-sources` 已实现官方源优先，但其新规范尚未归档；它还通过 ADDED 添加新条款，主规范中旧的 BaoStock 优先条款仍存在。本 change 对主规范真实存在的旧 requirement 进行完整 MODIFIED，避免新旧合同并存矛盾；不 MODIFIED 尚未落入主规范的标题，不修改前序任务勾选、首次结果或历史合同。归档时先核前序 change 的实际状态和 delta 合并顺序，不擅自代它归档。

这是业务正确性修复，不是框架拆分 change。遵循 `project_development_governance.md`，参考 `framework_refactoring_program.md` 的 W2 单一主数据 owner 与生产不变量；不启动 W2 全面迁移，不新增无关框架任务。开发前核对共享核心文件在途修改和生产任务，可靠隔离本任务代码块。

## Goals / Non-Goals

**Goals:**

| ID | 可观察验收条件 |
|---|---|
| DEL-01 | 实际刷新自动取得沪深正式终止名单，康佳无需名称含退即可确认，普通简称吸收合并等同样适用 |
| DEL-02 | 证券代码、交易所、股票类别、来源范围及当前主体／上市区间一致；前身终态不覆盖同码现主体，B 股、债券、指数、基金不被导入或连带停用 |
| DEL-03 | 空／不完整当前名单不会制造退市；独立有效终态正证据可单独用于其明确对象 |
| DEL-04 | 申请、风险、停牌、决定未生效及商业资产摘牌不冒充已退市；日期明确区分 |
| DEL-05 | 已生效／已完成日期未知的正式终态一致停用；未来计划到期须在当前股票池解析前重判，不能被fresh窗口跳过，不受未来 target_date 提前驱动 |
| DEL-06 | 同次三字段一致、保留证据；重复缺日期不清日期；官方及备源当前名单不能复活终态 |
| DEL-07 | 未确认、冲突、来源失败和写入失败可见，fresh 复用不清零或假报健康；审核模式不写业务状态 |
| DEL-08 | 治理后当前财务各候选来源及新全量清单排除康佳；旧状态与运行事实保留 |
| DEL-09 | Q3 早于退市日，不能仅因退市早于披露截止日而自动接受缺失；显式 readiness 仍报告缺事实 |
| DEL-10 | 历史查询、定向历史修复、PIT 原有限制、冻结 manifest／首次结果保持原合同 |
| DEL-11 | 康佳代表性闭环、重复执行、部分来源失败与多市场回归通过，业务写入和证据可对账 |
| DEL-12 | 保持调度与公共接口，删除本次明确替代的名称停用旁路，完成必要文档与产物清理 |

**Non-Goals:**

- 不实现全市场永久证券生命周期事件库、通用证据引擎、注册中心、评分或新调度平台。
- 不导入完整退市历史证券全集、不扩大 A 股范围到 B 股／退市板块，不做退市财务全历史回填。
- 不修改 `listed_date`、证券代码继承关系、公司主体合并规则，不以公司名传播终态。
- 不重构其他行情／财务 provider，不改变财务事实口径、披露日、可得日或回测历史股票池。
- 不改写历史 manifest、首次成绩、已完成运行的计数、accepted 原因或旧失败记录。
- 不新增公共 inactive 查询／采集模式，不宣称现有 current 任务的 target 参数能绕过 active 限制。

## Decisions

### D1. 沪深终止名单接入现有官方 provider，实际刷新时按市场批量读取

SSE、SZSE 每次实际 stock master refresh 读取对应终止名单，与**本地已有且属于本任务 A 股范围**的证券核对，包括 active 与已退市但缺准确日期的证券。不仅限于简称带退或 current 差集；这样也能识别官方 current 快照滞后仍含终止证券的冲突。相同刷新内复用一次来源结果，不逐股全市场请求。fresh 复用沿用既有时间窗，不为每个财务候选重新联网。

核对必须落实到当前证券主体及其上市生命周期，而非仅按code/exchange/type连接全历史名单。复用既有已审核lineage和来源中明确的主体、上市日期、终止事件区间；已知属于predecessor的终态仅作为该历史区间证据，不能写入当前主体的运行态。名称相似、名称变化或一个 `listed_date` 相等都不能单独代替主体核验；无法消除主体／区间歧义时保持当前状态并报告冲突待核。现有 `listed_date` 可能表示代码首次上市而非现主体IPO，不为通过核对改写它。

已知回归反例为600018.SH：沿革中2000-07-19起的上港集箱是predecessor，2006-10-26起的上港集团是current；若输入前身终止证据，不能停用现主体。实施阶段实取的SSE快照未包含该前身行，此回归依据既有已审核沿革而非伪称当次接口返回。这里复用已有身份边界，不构建新沿革库或重新上市状态机。

已找到的协议：

| 市场 | 官方接口及参数 | 原始关键字段 | 验证状态 |
|---|---|---|---|
| SZSE | `https://www.szse.cn/api/report/ShowReport`；`SHOWTYPE=xlsx, CATALOGID=1793_ssgs, TABKEY=tab2` | 证券代码、证券简称、上市日期、终止上市日期 | 2026-11-15 已实取且命中康佳；tab1 是暂停上市，不能混用 |
| SSE | `https://query.sse.com.cn/commonQuery.do`；`sqlId=COMMON_SSE_CP_GPJCTPZ_GPLB_GP_L, COMPANY_STATUS=3, STOCK_TYPE=1,2,8, type=inParams` | `COMPANY_CODE, COMPANY_ABBR, LIST_DATE, DELIST_DATE` | 2026-11-15实施时已实取161行，日期为YYYYMMDD；分页按官方total继续读取，不固定行数 |

AkShare 的 `stock_info_sh_delist` 将 `DELIST_DATE` 命名为“暂停上市日期”，不能据中文列名误用来源。SSE 实施核对实际字段和分页总量，不硬编码500行上限。优先沿用官方 provider 的 HTTP、限流、配置与 fixture 方式，复用协议，不从生产模块反向 import operator 脚本。

SZSE代码归一必须发生在业务解析中，覆盖XLSX真实数值单元格及表读取器把字符串代码推断成整数的路径。fixture保留单元格原始类型，parser输入若为16／"16"不得事先改成000016；规范化输出应为六位symbol `000016` 和canonical ID `000016.SZ`。数值变体单独标明用途，不改写真实快照来制造来源证据。

正式名单未覆盖的**已知本地候选**才使用现有公告扫描／共享公告资产读取作有界补充；限制市场、日期、页数及目标，不引入付费 LLM、不批量下载全市场正文。无法可靠读取正文与日期就留待确认。公告标题用于发现，不能独自证明证券范围和完成日期。

### D2. 正证据和名单缺失分开，拒绝以行数阈值替代完整性

来源结果必须区分取得成功、合法无匹配、超时／访问失败、结构不可解析及缺页／缺板块。至少核对本次请求范围、必要身份列、已知分页及分板块是否完成；不建设通用校验框架。

- 完整 current 名单可以产生“本地 active 缺失”的待核候选；空表、返回错误HTML、缺主板／科创板、已知缺页、批量解析失败不能生成该范围的消失结论。
- `>50` 行或 parseable 不是完整性证明。仅样例截断，不允许候选处理、分页或汇总总数被500／1000条显示上限静默截断。
- 终止名单中来源明确、身份及终态可独立验证的正记录，不依赖 current 名单完整才有效；其他页失败仍须报告降级，未取到的对象保持未核验，不能据缺行推导状态。
- 一个市场来源失败不改变其他市场独立有效证据；失败市场不得被整体成功覆盖。

### D3. 采用固定的金融语义决策表，不按来源调用顺序覆盖

运行态以**执行当天的上海业务日期**为准；历史 target_date 或未来 target_date 不改变今天的运行态。

| 输入证据 | 运行态动作 | 日期／报告 |
|---|---|---|
| 仅 current 缺失、简称带退或行情长期为空 | 不写终态，保留待核 | unconfirmed，不推断日期 |
| ST／停牌／风险、申请、受理、股东会通过、事先告知 | 不写终态，遵循已有风险／停牌语义 | 不能把 `is_active` 与可交易混同 |
| 正式决定，实际生效日尚未来到 | 不提前停用；按D6只记差异表生产计划 | future_effective，到生效日首次current治理在解析股票池前重判 |
| 正式终态，明确生效日已到 | 写三字段一致终态 | 用实际摘牌／终止上市日 |
| 正式材料明确“已完成退市”，但实际日期未知 | 写终态，日期保留已有值或NULL | 标明 date_unknown；不制造精确日期 |
| “将终止／将摘牌”，日期未知 | 不写终态 | 待确认，未知不等于已完成 |
| 确属当前主体／同一上市生命周期的有效终态，普通 current／备源仍报 active | 保留终态，记录冲突 | 前身终态不得套用；名单再出现也不等于撤销、重新上市 |
| 同一证券两个正式生效日期冲突，无法识别明确更正关系 | 保留已有生命周期，不猜测更新；尚未写入则不新增终态 | conflict，保留双方证据待核 |

申请日、决定日、公告发布日、最后交易日与摘牌日分别保存其语义，不能互代。`2026-10-16` 决定、`2026-10-17` 公告、`2026-10-23` 摘牌应解析出最后一个日期。公告中债券代码、其他公司或资产拍卖“摘牌”不属于该股票终态。

本 change 不实现重新上市／撤销退市的自动状态机；如出现明确新官方纠正材料，记录并交由现有受控核查处理，不用普通 current 行覆盖。

### D4. 单一写入 owner，并使终态与证据可核对

继续使用 `DatabaseOperations` 生命周期 owner；扩展既有方法所需最窄校验与证据参数，保持已有调用兼容。不让 provider、财务任务、报告器或新脚本直接 UPDATE 状态。

必须修复：

1. 拒绝把未来日期作为“已退市”立即写入；已完成但日期未知需有明确终态依据。
2. 缺日期的新证据不清空已有非空 `delisted_date`；可靠新证据补足未知日期。已知日期冲突留待核。
3. 所有路径保持 `status/is_active/trading_status` 一致；无日期正式终态也受普通官方与非官方 current upsert 保护。
4. 终态写入与必要来源证据在同一 `quotes.db` 事务内保存，或复用现有等价可恢复机制；证据保存失败不得宣称完整成功。不得“先承诺已写成功、实际只写一半”。
5. `instrument_master_metadata` 当前快照更新不能覆盖掉已确认终态证据。复用其 JSON 扩展位保存来源URL／公告ID、来源类型、抓取时间、原始hash、parser版本、证券身份、实际日期及日期未知标志。仅保留最小必要摘录与定位，不默认存整份开发库或重复正文。
6. 正常生产模式下，source factory取名单时的cache/upsert也必须在首次写入前执行来源权威与终态证据约束。仅有BaoStock的outDate／status时，可保存交叉核对诊断和获准的非终态字段，但不得改变既有证券生命周期；本地不存在的历史退市候选不得因此创建成正式delisted或伪造为新active。后置manager核对不能撤销已经提交的错误写入，故禁止“先保存raw fallback终态、稍后再校验”。此约束只落实现有官方终态合同，不扩建缓存体系。

重复运行同一证据不重复计为新增停用；只补日期、原已退市保持不变应分别可解释。历史行情、财务事实及旧运行记录完全保留。已确认终态日期未知的保护是本次发现的真实缺陷，不是为未来扩展另建框架。

### D5. 替换名称旁路，BSE 仅修正直接相关公共约束

移除 `_deactivate_official_absent_delisting_prefixed_stocks()` 仅凭名称与名单消失写终态的决定权；简称最多帮助筛候选。现有带退字测试须补正式证据后再预期停用，并增加带退字但无正式证据不停用测试。

BSE 沿用现有当前名单与公告来源，不新增 BSE 历史名单适配。其公告分支不得继续将 `published_at` 无条件作为 `delisted_date`：有实际日期取实际日期；明确已完成但日期未知按D3处理；标题“将摘牌”或正文未能证实已完成则待核。复用现有公告访问与有界读取，保持 BSE 停牌、新股、风险和失败继续合同。

保留既有BSE核验开关、扫描日期窗口及页数上界；关闭时不扫描或写终态，报告disabled／未核验。旧 `bse_delisting` 的enabled、候选、实际确认、风险、更新样例及未确认字段保留，新增统一核验明细放在 `delisting_reconciliation`。附件读取不超过原来源配置的更小字节／重试限制。

旧 `scripts/fix_delisted_status.py` 不是本次自动化入口：它还有独立 ORM writer，未统一清零 trading_status。本次保留该路径作为兼容运维入口，退市修复只转发权威路径；保留已有 `--dry-run` 和Step 3合并主板A股／STAR Market当前名单的诊断合同，移除本次替代的独立状态writer。主规范仍明确要求该路径，本change不删除脚本；未来删除须先在独立变更中给出替代入口并更新对应规范。不得再留第二套终态写入循环，也不为此扩建运维平台。

### D6. 在现有结果中呈现闭环，保留 fresh 与 audit 合同

在既有按交易所治理结果下增加 `delisting_reconciliation`（命名在实现时保持统一）及必要汇总，不改变已有字段类型或把候选数当写入数。最小字段：来源／抓取及覆盖状态、核对证券数、正式确认数、实际新增停用数、日期补足数、未确认数、未来生效数、冲突数、来源错误数、写入失败数、有限样例及证据引用。

计数按证券去重；“正式确认”是证据结论，“新增停用”是本次成功状态转移，后者只是前者的子集；日期补足与已退市确认可重叠，禁止直接把各字段相加当总数。写入失败不能计入新增停用。样例默认不超过20条，样例限额不截断实际处理与总数。

未确认、未覆盖、来源不可用、冲突或写入失败使相应治理范围 warning/error，遵循现有 continuation 配置；未来生效的正常计划单独展示，不等同失败。业务任务可以按已有配置继续，但报告中必须显示治理降级与继续决定。

freshness 不能只看本轮其他current行的最新 `updated_at`。本change将**未来计划及核验结果的唯一持久载体固定为现有 `quotes.db.instrument_master_discrepancies`**；不新增表、队列、索引或通用事件框架，不在metadata再保存另一份未来计划。已确认终态证据仍由D4写入受保护的 `metadata.a_share_terminal_evidence`，普通快照更新保留该命名空间及已有lineage。

该表已有 `id/run_id/exchange/instrument_id/reason/payload_json/created_at`，按以下固定业务协议追加记录。`payload_json` 均显式包含 `scope=a_share_stock` 和 `mode=production|audit_only`；旧reason保持原义。

| reason | 必要载荷与含义 | 写入边界 |
|---|---|---|
| `a_share_terminal_future_effective` | `plan_key`、证券ID及已核主体／上市区间、`effective_date`、`verified_at`、正式来源URL／公告ID、来源类型、hash、parser版本和最小证据定位 | 正式决定已确认但尚未生效时保存；保存失败不能建立成功核验基线 |
| `a_share_terminal_future_applied` | 相同 `plan_key`、`applied_at`、实际生效日及结果引用；表示该计划已成功应用 | 仅生产生命周期owner可写，须与状态及终态证据同一quotes.db事务提交 |
| `a_share_terminal_verification` | `instrument_id=NULL`，按交易所保存实际 `verified_at`、来源覆盖／状态、未决／失败数及有限样例 | 记录本轮成功或失败核验；audit记录不能作为生产fresh基线 |

`plan_key`由canonical证券ID、已核主体／上市区间、实际生效日和稳定正式证据标识确定。公告使用公告ID；名单行使用来源类型及规范化证券／上市日／生效日作为行标识，不用行号、抓取时间或整个文件hash生成计划身份。重复抓取可追加同key的观测，但按key去重；已有生产applied后，新重复future_effective不能重新打开计划。两个正式日期或主体冲突仍按D3待核，不靠最新记录顺序解决。

**固定读取顺序：** stock policy在返回fresh、读取当前股票池前，通过 `DatabaseOperations` 的本业务窄读取方法按scope、生产mode及请求交易所读取上述计划／应用记录和最新核验结果；先过滤审计记录，再按plan_key去重并排除已applied计划。最新核验按实际 `verified_at`（同时间再按id）选择，不能只挑最后一次成功来掩盖后续失败；样例限额不限制计划读取。该查询只服务本次A股治理，不建设通用队列API。

**固定生效与失败边界：** 到期未应用计划使旧未生效结论失效；校验身份、实际日期、证据新鲜度与冲突后，通过同一owner应用或实际刷新。成功时在同一事务写状态、终态metadata与applied；失败则不写applied，保留待处理计划并报告降级，下次治理重试。现有 `save_instrument_master_discrepancies()` 自行开会话提交，不能简单调用它来冒充与生命周期同事务；应在既有owner内使用同一会话完成这组写入。重启后仍从同表恢复，current metadata覆盖不影响计划。

**固定空集与错误语义：** 窄读取必须区分成功空集、缺生产核验基线／表未建立、读取失败及JSON解析失败。现有 `execute_read_query()` 会把异常折成空列表，不能无区分复用为“没有到期计划”。缺基线或读取／解析失败时禁止返回完整healthy fresh，执行必要刷新或按现有continuation显式降级。核验未做、失败或仍未决，不能报告完整健康；有效未过期结论可无网络复用，但保留未决警告。首次部署force建立基线。

未来计划的生效边界优先于普通fresh时长：存在尚未应用的已知生效日时，原“尚未生效”结论最多复用到该日上海时间00:00。生效日首次current治理必须在股票池解析前重新判断，复用仍可靠的正式证据经同一owner应用，或执行实际刷新；只有完成重判才能返回可用结果。不能因48小时尚未到期直接返回旧active状态。若所需证据已过期、冲突或执行失败，按既有continuation报告降级，不能伪报fresh。增加10-22建立计划、10-23仍在48小时窗内的跨日验收；不新增定时任务。

`audit_only` 执行同样证据判断并报告 would-deactivate／would-enrich，**不修改 instruments、终态元数据、财务状态或业务事实**。可以按原合同保存审核运行／差异记录，不能让审核结果成为已完成生产写入或成功刷新终态状态的证据。

必须将只读模式从 `AShareStockPolicy.execute()` 贯穿 `sync_instrument_master()`、source factory的取数／缓存保存及普通current metadata路径；不能只跳过最后的 `mark_instrument_delisted()`。优先复用现有provider读取与最窄的保存开关／决策接线，保持默认生产写入合同，不创建第二套采集循环。审核测试须走真实policy与factory，覆盖“只返回当前名单、没有任何退市候选”的情形，证明名单获取也未upsert生产主数据。到期计划在audit中同样只能报告预期变更。

audit可读取生产计划输出would-deactivate，并按原合同写带 `mode=audit_only` 的审计／核验记录；不得追加生产applied、闭合生产计划或建立生产fresh基线。计划、闭合与核验记录的mode隔离均在读取及写入测试中验证。

### D7. 财务任务改变当前范围，不改写事实或披露义务

当前任务从治理后的 `type=stock && is_active` 集合开始；增加与已生效退市日期、明确正式终态的一致性防线。日期未来本身不触发退市过滤；本来因其他业务原因 inactive 的证券仍遵循原范围规则。

公告映射、pending／risk／expired 回灌、accepted 回灌及周度 active×period 缺口扫描都必须受同一当前集合约束。在当前任务中显式指定 target ID／symbol只缩小集合，不能把康佳强行加回。

既有康佳 Q3 `blocking_gap` 及 pending／accepted 历史原样保留：不删除、不改成功、不延长旧重试窗、不改成法定豁免。新运行不再从它们生成候选、provider尝试或本轮blocker。无需新增 retired 状态、重写跨库历史或迁移所有事件。

同时修正全量清单 helper 的无证据启发式：当 `period_end <= delisted_date <= disclosure_deadline` 时，仅凭这些日期不能将缺失的退市前报告期判为 `post_delisting_or_no_disclosure`。删除该自动接受分支及误导措辞；保留报告期确在上市前／退市后的现有生命周期处理，以及具有真实公告证据的异常解释。相等日不凭日期自动豁免。回归 `2026-09-30 / 2026-10-23`；不为迁就旧测试保留错误业务断言。

历史查询和显式 readiness 按给定 instrument-period 判断事实；缺失仍缺失。已有内部 `FinancialMaintenanceRepairRouter.repair_targets()` 可以接收历史定向目标，本 change 不新增公共历史模式，也不无条件承诺每个 provider 支持退市历史。PIT 的既有限制如实保留，不声称因此完成退市覆盖。

新建 full import manifest 受治理后当前范围约束；已有 resume 会直接复用冻结 manifest，不能承诺自动排除。上线前盘点相关在途／待恢复清单，若其仍包含康佳，按原任务控制停止继续消费、另建当前任务；不静默修改冻结清单、hash或成绩。若原任务已明确授权历史补采，则维持该原合同并说明与当前维护不同。

## Risks / Trade-offs

- **官方接口字段或分页改变** → 使用真实精简fixture与分页／必要列检查；只对可靠正证据写入，失败可见，不据空表大批停用。
- **current 与 terminal 抓取时点不同** → 明确正式终态优先、日期冲突待核；保留抓取时间和hash，不按调用先后决策。
- **新增终态读取延长刷新** → 每市场批量读取、复用本轮结果及既有fresh窗口；不逐股全市场抓取或新增调度。
- **未知日期终态影响历史截断** → 当前可排除，历史精确时间保持未知；不用最后行情日补造，不扩大为历史自动豁免。
- **旧名称停用路径被替换后待核项增多** → 这是证据不足的显式呈现，用终止名单与有界公告补齐，不能通过恢复名称旁路掩盖。
- **共享 writer 影响 BSE／其他现有调用方** → 核对真实调用、保持签名兼容和已生效行为，测试空日期保护及未来日期拒绝；不将沪深来源规则套到HKEX或指数。
- **旧财务全量清单仍可继续采集退市股** → rollout 盘点并按原任务合同处理冻结任务；不误报所有入口均已自动修复。
- **代码回退不能撤销正确退市事实** → 回退只回程序，不把已正式退市股票重新激活；错误数据纠正须有对应证据并经同一owner处理。

## Migration Plan

1. **开发前**：记录Git基线、共享核心文件归属与当前任务；盘点本任务及前序相关QuoteBak制品，仅在确知归属、无并行占用、证据已持久化且可重建后清理。调查未产生专项临时目录，无需按日期扫删他人产物。
2. **最小闭环**：先用康佳官方名单精简fixture，经真实现有治理与临时数据库writer确认退市，再运行财务候选构建，证明Q3不再请求；不要先建设报告平台。
3. **完整核心规则**：补沪深适配、证券范围、日期／未知／冲突、名称旁路替代、writer及证据保护、财务helper修正；再补必要报告、fresh、audit和失败语义。
4. **离线回归**：使用QuoteBak临时小数据库及fixtures，验证DEL-01至DEL-12和下表；不复制整套生产库、不运行全仓无关测试。
5. **受限联网审核**：仅在工具获准联网环境核DNS/TLS及接线，按单次沪深名单和既定少量公告范围audit；核对SSE真实协议、当前待核证券和写入差异。来源失败与业务失败分开，禁止受限沙箱内消耗正式入口。
6. **受控修复**：按届时有效授权，经同一owner执行已核实候选；康佳目标为 `delisted / false / 0 / 2026-10-23`，保留正式证据。审核结果不自动赋予新生产权限；本轮文档生成不执行这一步。
7. **幂等与自然任务观察**：重复治理实际新增停用为0；核日期不被清空、不复活；观察一次后续当前财务任务，康佳不生成候选／请求／新blocker，其他有效股票照常，历史Q3缺失仍可查询。不为验证触发全量付费或大批抓取。
8. **交接**：更新现有 `instrument_master_sync.md`、官方源需求和相关财务运维说明；证据只保留一套必要持久版本（来源引用、精简结果、hash、失败轨迹及命令），清理本人无用临时制品，在现有交接入口记录保留用途和清理时点。实现完成后再按依赖核对规范合并并归档本change。

### 验收矩阵与需求映射

| 场景 | 必须证明的结果 | 需求 |
|---|---|---|
| 康佳A真实字段、10-23执行日 | 无退字也写准确终态，财务Q3零修复请求 | DEL-01/05/08/11 |
| 原始XLSX代码字符串经类型推断得到16／数值单元格变体16 | fixture不预补零，完整读取链仍输出000016.SZ；变体不冒充官方原件 | DEL-01/02/11 |
| 普通简称吸收合并终止上市 | 同一规则生效，不能硬编码000016 | DEL-01 |
| 同公告A/B及公司债，另含指数 | 仅本地本任务A股身份处理，不新增B股、不连带停用 | DEL-02 |
| 600018前身终态与现主体共用代码 | 复用已审核lineage，前身证据不写现主体；身份未知则待核 | DEL-02/06 |
| current空、缺科创板／缺页、错误HTML | 不据消失大批停用；独立终态正记录仍可核对且降级可见 | DEL-03/07 |
| 终止名单超过一页／500条，重复记录 | 不截断，按证券去重，样例上限不影响总数 | DEL-01/03/11 |
| 申请、风险、停牌、商业资产摘牌 | 不确认为股票退市 | DEL-04 |
| 10-17读取决定、10-23摘牌 | 10-17不提前退市，到10-23首次current治理在解析股票池前重判生效 | DEL-04/05 |
| 10-22缓存未来计划、10-23命中48小时fresh | 股票池解析前重判并应用已生效终态，康佳Q3不被旧active重新选中 | DEL-05/07/08 |
| 保存未来计划后进程重启、重复pending、应用事务失败 | 差异表恢复；applied后不重开；失败不闭合且可重试；metadata刷新不丢计划 | DEL-05/06/07 |
| 无到期计划／无生产基线／读取或JSON失败／audit记录 | 成功空集与错误严格区分；错误不伪报fresh，audit不建立生产基线或闭合计划 | DEL-07 |
| 正式已完成但日期未知／仅将退市日期未知 | 前者NULL终态保护，后者待核 | DEL-05/06 |
| 重复缺日期、补足日期、正式日期冲突 | 不清旧日期，可靠补足，不猜冲突 | DEL-06/07 |
| 后续官方／BaoStock／AkShare current active | 已知和未知日期正式终态均不复活，三字段一致 | DEL-06 |
| audit与fresh复用未决结果 | audit零业务写入；fresh不清未决、不伪报健康 | DEL-07 |
| audit真实policy/factory取current名单且无退市候选 | 禁止名单cache隐式upsert及current metadata写入，生产状态保持不变 | DEL-07 |
| 任一市场来源／证据保存／写入失败 | 分市场降级可见，不伪计成功，continuation遵循原配置 | DEL-07 |
| official失败、BaoStock给past outDate／delisted | 既有active生命周期不被cache提前覆盖；新历史候选只作诊断，不伪造active或正式终态 | DEL-03/06/07 |
| inactive有pending/risk/expired/accepted/blocking | 原记录保留，本轮各候选来源均不回灌 | DEL-08/10 |
| Q3期末09-30、退市10-23、无豁免证据 | helper不自动接受；显式readiness仍缺五项事实 | DEL-09 |
| 新manifest与冻结resume | 新当前范围排除；旧清单不变更且运行限制交代清楚 | DEL-08/10 |
| BSE公告日与实际摘牌日不同 | 保留来源路径，按实际生效判断而非发布日期 | DEL-04/05/12 |
| 历史查询、原PIT及定向修复 | 事实、可得日、缺失及原有限制不被当前active过滤改写 | DEL-10 |

## Open Questions

- **SSE实际协议已核验**：初次不完整参数请求HTTP 200但空结果，完整 `type=inParams` 协议实取161行；真实fixture、hash及失败轨迹已保留，不将HTTP成功等同有效名单。
- 未来计划、应用闭合及核验结果的载体、reason、读取顺序和事务边界已在D6确定，不再作为开放设计问题；实施仅按该协议接线与验证，不在差异表和metadata两种方案之间切换。
- **在途full import已盘点**：受控修复前财务运行表无running任务，在已配置的默认全量导入目录未发现包含康佳的冻结manifest；既有resume合同未改。后续恢复其他清单仍须按其原合同检查范围。
- 其他业务规则已在本设计和delta specs中明确，不以待讨论为由搁置康佳主路径。

## Implementation and verification

现有stock policy、provider和数据库owner已接入全部业务规则。新增领域判断位于 `instrument_master_delisting.py`，生命周期事务与严格计划读取位于 `database/instrument_lifecycle.py`；DataManager删去名称停用及旧BSE终态循环，只保留接线。Scheduler只增加报告投影，且与工作区既有依赖升级改动分块隔离。

最终直接相关回归为 **221 passed / 1 deselected**；被排除的证券profile案例已用任务开始基线复现相同失败。额外尝试的旧数据库测试有26个fixture错误（await同步initialize），未扩大为本任务的测试框架改造；新owner已用小型真实SQLite事务和失败注入覆盖。Black、正确性静态检查和OpenSpec strict均通过。唯一持久验收记录为 `verification.json`，原始来源fixture及hash在 `tests/fixtures/a_share_official_master/`。

2026-11-15只经同一owner受控纠正 `000016.SZ` 为 `delisted / false / 0 / 2026-10-23`，重复空日期写入保留实际日期；后续服务级定向对账dry-run零候选、零请求、零新blocker，旧Q3事件状态hash不变、显式readiness仍缺五项事实、Q2历史事实仍可读。真实audit的26项其他历史差异保留待核，未批量覆盖。

本次未主动重启生产进程或触发自然全市场定时任务；新增代码由正常部署／进程加载生效。本任务验证不能冒充自然全市场首次调度成绩。清理仅覆盖本人可重建临时库、验证脚本、基线副本及测试误产物，不删除原始业务数据、共享缓存或他人制品。
