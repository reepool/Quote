## Context

本 change 是 2026-11-09 消费方反馈的修复规划。来源为 `docs/后端数据问题与改造需求_20261109.md`；下表中的复核事实来自本会话对 `http://47.237.204.217:8000/api/v1` 的只读 GET 与本地直接调用链检查，并非原反馈逐字采纳。没有修改线上数据，没有测算全市场受影响行数，也没有测得实际投资损失。短证据保留于此，不复制完整财报或生产库作“证据”。

### 修复评估与取舍

| 项目 | 已核实事实及定位 | 本次交付 / 验收要点 | 优先级 / 难度 / 估计人日 |
|---|---|---|---|
| B-01 | `utils/adjustment.py:forward_adjust/_get_latest_cum_factor` 使用查询行情末日；600519 2020-01-02 单日 qfq=1130、factor=1，扩展至 2026-11-09 后同日 qfq=956.3574、factor=0.846334；技术摘要 qfq 与 none 的 MACD 分别约 44.185426 / 44.158691 | 不作为“qfq 未实现”修复；文档说明查询区间锚定，验证单日、跨除权、分页同区间，保留默认值与算法 | 澄清 / 低 / 并入行情切片 |
| B-02/B-03 | `AdjustmentEngine` 默认只复权 OHLC 和 volume；600519 2026-11-06 hfq close=11146.102、pre_close=1452、factor=7.671096；原始量4284700、复权量558551，amount=6199528448 | pre_close/change 与 OHLC 同基准；原始字段保留；volume 固定真实股数；不重算事件因子 | P0 / 中 / 合计1.5–2.5 |
| B-04 | 日行情 pct_change=-0.5513，技术 pct_change_1d=-0.005513；财务 gross_margin≈0.897619，PIT 同类比例按百分数输出；`financial_pit_panel.py` 明确乘100 | 新增同名小数比例入口及单位声明；保留旧字段值，回归验证不二次缩放；收益计算定义单独声明 | P0（跨接口误算）/ 中 / 1–1.5 |
| B-05 | 600519 23个交易日 turnover=0、volume>0、quality=1；`tdx_source.py` 明写“不提供换手率”但填0，当前质量算法不检查它 | 源缺失写null；历史读投影只识别已知占位零；缺失原因与质量口径清晰；不凭当前股本推算历史 | P0 / 中 / 0.5–1 |
| B-06 | 默认响应270439 bytes，关statements仍183060；facts与indicators各约88–89K；`requested_canonical_facts`仅增加service_layers，不投影原bundle；include_local_core=false是已定义默认值，大小不变是正常行为 | 默认窄摘要<8192 UTF-8 bytes；`view=full`可取旧明细；fields明确只投影业务值，身份/时点/单位元数据常驻 | P1 / 中 / 1–1.5 |
| B-07 | latest读日线；实测含time=2026-11-09零点、symbol为空，无EOD标志；batch_id=20260328_223307在不同日重复。`_load_progress`重用进度身份，不能据此断言行情是3月数据 | EOD/as_of/computed_at/采集记录时间与日历陈旧度；symbol由主数据补齐；原batch_id标为progress_session | P1 / 低至中 / 0.5–1 |
| B-08 | HTTP没有limit参数；`limit=2`返回6205条、675449 bytes，升序从2002-01-04到2026-11-06；series/observation data_as_of为空；`latest`是写入修订槽位 | 新增真实分页/排序与区间边界；不把旧历史的查询时间伪造成抓取时间；未来成功sync带时间 | P1 / 中 / 0.5–1 |
| B-09 | 000002 2016-06至07月：include_suspended=false/true与min_quality_score=.9均41行，其中21停牌低分；min_volume=1为20行；foo=bar返回200；`_apply_quote_filters`漏两个分支 | 已声明过滤真正生效；HTTP未知字段拒绝；过滤先于分页和统计；字段缺失不可静默退回未过滤数据 | P0（已声明过滤）/ 中 / 0.5–1 |
| B-10 | pe_ttm均值66.09475、中位数22.40304、min=-601.89322、max=921.98463，18/0纳入/排除；主spec已要求排除负/零分母，代码及旧单测却接受负PE | 修正与既有Clean Multiples要求冲突；各统计量同一有效样本；raw均值对照；可选正值上限，默认不武断裁掉高PE | P2 / 中 / 0.5–1 |
| B-11 | cross-section只有as_of/report_period/exchange/fields/limit/offset；现有stats是当前页统计（limit=2时exact=2），不是5536行总体统计 | 可选质量白名单与排除开关；选择最新可知期后过滤，不偷偷回退旧报表；新增全体过滤统计且保留旧页stats | P2 / 中 / 0.5–1 |
| B-12 | coverage缺日期为400+detail文本；FastAPI自身422已有loc/type/msg，年报路径另有结构化handler；不是全站“没有结构化机制” | 复用现有错误工具，在限定读端点增加稳定码及field/hint，保留detail，避免改写全站错误合同 | P2 / 低至中 / 并入参数切片 |

合计为约 **7–12 个工程人日**，是熟悉项目者对实现、定向测试和迁移说明的粗估，非已经消耗的工时或交付承诺；不含等待共享文件窗口、生产发布、客户端迁移等待和历史数据回填。优先行情正确性约3–5人日；切片复用验证，不能把表中每项上限机械相加或把12个反馈号当12个独立框架项目。

### 现行 owner 与并行边界

| 业务 | 唯一现行路径 | 实施边界 |
|---|---|---|
| 行情读取/过滤/质量 | API → DataManager现行查询/过滤 → db_ops；复权复用AdjustmentEngine | 修复既有分支；新增HTTP投影进入行情域窄模块，不在routes复制算法；技术计算仍走同引擎 |
| 财务摘要 | API → get_research_financial_statements → storage.get_financial_statement_bundle | summary以明确选列读取标量，跳过facts_json/indicators_json/statement_json解码；full沿用完整bundle；不另建事实来源 |
| 利率 | API →现行利率query/storage；sync → RiskFreeRateSyncService →现行upsert | HTTP有界页不改变DCF内部历史/截止日查询；新sync时间通过现有写owner落库 |
| 相对估值 | ResearchValuationService | 过滤与diagnostics留在现有服务；不重建估值历史或行业分类 |
| PIT横截面 | FinancialPITPanelStore.query_cross_section | 继续单查询owner、strict修订防护；过滤和统计共用同一关系，不另造面板 |

总纲登记为FR-09/W9的业务修复，关联FR-06/W4、FR-07/W5。`add-quote-api-data-capability-improvements`的未归档delta含旧的“默认不变”承诺；本change明确列出默认修复变化，不修改其成绩。财务性能change（`accelerate-financial-readiness-queries`、`cut-financial-disclosure-read-amplification`）与W4/W5/W9整体拆分change在实施前检查同文件占用，复用已合并投影能力。提案可并存，触及受控核心文件的实施窗口串行；不得要求先完成全仓框架迁移才修当前错误。

## Goals / Non-Goals

**Goals:**

- 调用方能直接区分原始与复权值、百分数与小数、缺失与真零、日线交易日与装配时间。
- 已声明过滤可验证，未知参数和非法组合给出明确错误。
- 默认财务和利率响应可以完整消费，所需明细/全历史仍可显式取得。
- 以受控除权、停牌、缺失和质量混合样本验证真实金融语义，而不依赖线上固定行情长期不变。

**Non-Goals:**

- 不改canonical因子选择、历史因子值、qfq查询末日锚定、PIT的available_at/effective_at、strict防护或历史原始成绩。
- 不引入实时源、全库换手率计算、付费调用、历史采集时点伪回填、全平台单位迁移、全站参数门禁/错误框架、ETag缓存平台或新数据库。
- 不按反馈“均值必须接近四分位区间”验收；该结论不是统计不变量。负PB已在当前代码被排除，不能据PE样本推断所有排除逻辑从未启用。
- 财务单报告期包含很多列的膨胀先通过投影解决，不为单对象新增任意行分页；多期取数复用history，HTTP缓存后置。

## Decisions

### D1 行情返回投影保证同一价格口径，技术计算复用现行引擎

保留引擎默认的内部复权量行为，避免无意改变技术指标的量比定义。公共daily输出在复权前保存原始前收、涨跌额、成交量，并把`pre_close/change`列纳入同一复权乘数（可复用引擎已有price_fields参数）；所有价格遵循同一continuity context。不在API入口另写因子公式。输出约定：

- `open/high/low/close/pre_close/change`使用实际price_basis；`raw_pre_close/raw_change`保留输入，缺失仍null。`pre_close`指源端除权参考前收，不擅自改成昨日原始close。
- `volume_raw`是真实成交股数；`volume_adj`是按同一价格乘数反向变换的量；`volume=volume_raw`为迁移兼容别名并在OpenAPI标deprecated。`amount`恒为实际交易货币金额，A股为人民币元；币种由证券主数据声明，不对所有市场硬编码CNY。
- `adjustment_type/price_basis/volume_basis`反映实际执行；none与无需复权品种保持原值。qfq单日factor=1合法，区间最后bar为基准；相同完整查询区间分页不能改变锚点，不承诺不同end_date的价格可直接拼接。
- 统计的总成交量/均量使用真实量；CSV/JSON/pandas输出同样执行口径修复，不因序列化分支遗漏。latest也给真实量和相应单位，仍是非复权EOD。

弃选方案：只加说明保留混用（仍可静默误算）；重写引擎/重算因子（无证据必要）；新建第二套qfq算法（违反唯一owner）。精度验收以原始数值×因子及既有rounding误差为准，不声称从已四舍五入pct_change能恢复无限精度。

### D2 单位显式化且兼容旧数值

daily/latest新增`pct_change_ratio=pct_change/100`；technical summary新增同名别名，值等于其现有`pct_change_1d`。声明`return_basis`：行情为源端参考前收收益，技术为复权close-to-close；普通无公司行为日二者应一致至精度容差，除权日按各自定义验收，不强行改源值使两个接口数值永远相同。

财务与PIT新增`ratios`小数对象，至少含gross_margin/operating_margin/net_margin/roe/roa/asset_liability_ratio；财务从既有小数原值取，PIT从既有百分数字段除100；current_ratio/quick_ratio保持倍数，per_share保持币种/股，不能统统除100。旧pct_change和旧财务比例键保留原值并标兼容，不在原字段无提示改变单位。

`units`是返回字段路径→单位的有限映射（如`data[].pct_change=percent`、`data[].pct_change_ratio=ratio`、`ratios.roe=ratio`），不是通用schema registry。daily/财务/PIT/technical使用现有对象顶层，latest保留list形状、在各item带units，避免包一层导致额外破坏。只声明实际输出字段；未知值仍null。

### D3 换手率缺失只修已知占位，不捏造历史

TDX正常解析将“不提供”的turnover写null；现有nullable存储和唯一写owner不变。读投影对`source=pytdx`且turnover为占位0的行返回null（volume为零也不等于该来源真的提供了零换手）；其他源的有效0保留。输出`missing_fields`与按字段的`missing_reason`，已知缺失时响应quality_score取现有分数与0.9的较小值；缺失turnover不把完整OHLCV bar标为停牌或is_complete=false。源端未来若提供有效turnover，必须去掉“不提供”的适配规则并以测试更新，不能仅因source名称把所有数值清空。

min_quality_score过滤使用这份对外质量投影后的分数，先修正、再过滤、再分页/统计；不批量重写历史库。更精细全字段质量模型不在范围内。用当前流通股本计算历史换手率会引入时点错误，故不采纳。

### D4 真实过滤、未知参数与错误兼容

`include_suspended=false`排除明确tradestatus=0的行；true保留，默认false兑现现有声明。tradestatus与该参数取交集，冲突产生空结果不放宽条件；`min_quality_score`采用修正后的对外分数，未知分数不通过正阈值。相关必需过滤列无法判定时返回结构化数据不可用错误，不继续返回未过滤数据。

未知查询字段只在下列路由适用：quotes/daily、quotes/latest、quotes/coverage、单期和history financial-statements、technical/summary、valuation/relative、risk-free-rate、financials/pit/cross-section。HTTP级依赖复用路由实际解析的查询参数（含Depends、别名、重复合法列表参数），不维护第二份人工白名单，不通过Pydantic extra=forbid误以为已捕获FastAPI丢弃的参数；鉴权header不当query处理。

错误增加`error_code/message/field/hint`，保留原detail类型和已有状态码；多个字段问题附`errors`且field可null。未知字段422+`unknown_query_parameter`；非法数值/枚举422+`invalid_query_parameter`；coverage缺时间范围保留400+`missing_date_range`。不泄露堆栈或源凭证，不替换年报已有错误schema，不改本次范围外的HTTP行为。至少两个实际路由共用才提取窄助手，禁止通用异常平台。

### D5 财务默认摘要，完整载荷显式选择

单报告期新增`view=summary|full`，默认summary；`include_statements`改为可识别“未传”的可选参数，summary不能要求raw statements（显式true时422+hint转view=full），full下未传按旧True，显式false仍省raw statements。history默认完整/既有行为不变，新增可选view=summary及同一fields投影；不让单报告期默认切换悄悄改变现有history。

summary只读取既有权威行的标准标量财务科目、标量indicators与ratios，以及身份、报告期、可得日、source、单位；不读取或解码facts_json/indicators_json/statement_json，不包含完整事实包、details、lineage正文或raw statements。读取选列尽量复用已合并财务性能change的投影接口，不建平行事实源。默认无显式诊断时UTF-8 JSON序列化<8192 bytes；这是一项本change的消费目标，不宣称MCP协议具有固定8K上限。

fields接受逗号分隔的规范科目、`indicators.<标量键>`和`ratios.<键>`；例`fields=revenue,net_income`仅返回这两个业务值和必需身份/报告期/来源/单位元数据，不额外返回全部indicators。未知字段422，不把requested_canonical_facts当投影；后者仍只控制分层来源读取。显式include_local_core/include_industry_facts/allow_remote_extension等沿用旧功能，须注明诊断/远程扩展不在默认8K目标内；未显式允许远程不得联网，fields本身不触发远程。

弃选方案：默认仍返回巨大正文、另加无明显入口的可选摘要（未解除默认消费问题）；新增单期limit/offset（不解决一个大对象的重复JSON）；先做缓存（不降低内联正文大小）。

### D6 EOD与时间元数据只表达可证明事实

latest保留List[DailyQuoteResponse]外形：`quote_type=eod`、`is_intraday=false`、`as_of`为bar交易日的带时区时间，文档说明它是日线日期零点标签而非实时tick或抓取时刻；`computed_at`为响应装配时刻；增加存储行created_at/updated_at作为record时间，不伪称源报价timestamp。symbol缺失时用同一主数据查得。

`staleness_trading_days`计数bar交易日之后、查询时刻之前已完成的交易所交易日；当日盘中前收盘为0、当日收盘后仍昨收为1、周末不增加。只使用权威交易日历和现有session cutoff；缺日历/截止时点依据则null+原因，不猜节假日。batch_id原值保留并声明`batch_id_kind=progress_session`，不改造整条采集流水线或伪造历史新批次。

### D7 利率HTTP有界页与真实抓取时点

HTTP新增`order=desc|asc`（默认desc）、`limit`（默认100，1–2000）、`offset`（默认0）。先应用series_id和含边界日期过滤，算该区间total_available/first_date/last_date，再按唯一observation_date顺序分页；保留`total=len(observations)`兼容语义，新增`returned_records/truncated/next_offset`，末页next_offset=null。不提供隐式无限页；取全历史须遍历，或限定日期。

存储内部方法默认不截断并保留其历史排序合同，HTTP通过显式page查询参数/现行owner委托；不能把HTTP默认值下沉到DCF读取，防止截止日取错数据。metadata的last_date表达最新观测，`data_as_of`定义为该源成功抓取时间，二者不混用。

未来成功sync在fetch完成后给新增/更新观测和series写入带时区抓取时间；失败或empty不更新“成功抓取”时间。旧行有可信持久化时间依据才以明确basis回显，否则继续null并给`data_as_of_quality=unknown_historical`，不能用查询时间或最新观测日期填充。原`revision_id=latest`保留为兼容槽位，同时给`revision_kind=overwrite_slot`，不声称有历史vintage。本次没有历史回填/付费采集授权。

### D8 估值按可解释样本统计，不用阈值包装正确答案

默认所有PE/PB/PS变体排除缺失、非数值、非有限和<=0样本（负PE来自负分母，不是有效可比倍数）；raw peers仍展示原值。`peer_mean/median/min/max/p25/p75/percentile_rank/premium_to_median`使用同一有效集合，metric_exclusions列原因，subject非正值时相关排名/溢价null并说明原因。

新增每metric的`peer_raw_mean`与raw_peer_count（仅对有限数值，保留负值）及统计政策元数据，防止调用方把“排除后低均值”当市场全貌。可选查询`max_pe`只影响PE变体、`max_pb`只影响PB变体，有限且>0，默认无上限；回显阈值和above_maximum原因，不能让新设置改写历史估值或行业样本。公开主推荐是有效样本median与分位数，默认不承诺正极端值已经被winsorize，也不要求mean落在P25–P75。

### D9 PIT先选最新可知期，再做质量筛选

新增`available_at_quality`（逗号白名单：exact/exact_event/statutory_deadline/delayed_estimate）、`exclude_unverified`（默认false，指attribution_quality=unverified）、`exclude_discrepancy`（默认false，指share_validation=discrepancy）。使用明确枚举白名单，不给没有严谨全序的“min quality”发明大小排序。组合条件取交集，未知枚举422；requested字段投影不能让过滤/统计失效。

沿用strict可得日和每instrument最新可知期选择；质量条件施于已选行，而非先过滤历史再选，因此最新行不合格时排除该票，不拿更旧的高质量报表替代。原页stats保持返回页分布，新增`filter_stats`：`scope=selected_cross_section_before_paging`、before_count/after_count/excluded_count、过滤前后availability/attribution/share_validation分布。total为过滤后总数；不同分页的filter_stats相同；无过滤时before=after。保留原universe_note和listed-only限制。

### D10 发布和修改范围

默认变化仅来自本change明确的修复：行情volume别名/前收与change一致性、未知换手率null/质量、声明过滤落地、财务单期summary、利率有界降序和未知参数拒绝。单位旧值和历史存储不动。完整API改动更新info.version/OpenAPI及现有API文档中的迁移示例，不能只加一份独立新接口文档。

受控核心文件只允许当前错误修复/薄参数委托；新增投影进入现行域边界，新增共享函数须有两个实际调用方并删除重复代码。实施若确需扩大旧门面领域逻辑，在修改前按总纲§8.1登记具体例外；本提案不预批准任意新增大段逻辑。无部署、重启或生产修复动作属于文档生成的自动后续。

## Risks / Trade-offs

- [外部客户端依赖旧volume、财务完整默认或升序全利率] → 明确BREAKING、迁移示例、版本变更；保留volume_adj/view=full/order=asc+分页入口；不悄改值。
- [字段加法未穿透Pydantic或CSV] → 从HTTP实际请求验证JSON/CSV和latest list模型，不能只直接调用函数单测。
- [过滤与统计顺序不同] → 受控fixture检查quality修正→filter→stats/page，PIT检查latest-visible→quality→page。
- [换手率历史零有不同来源] → 仅归一已证实的TDX缺失占位；其他源0保留，不按volume>0一概抹除。
- [源抓取时点无历史证据] → 诚实保留null和unknown_historical；本change验收不要求伪造所有历史非null。
- [正极端倍数仍使均值偏离中位数] → 返回raw/filtered对照、政策和median推荐；按需显式max_pe/max_pb，不暗造通用阈值。
- [共享文件存在并行开发] → 实施前检查分支、dirty块、相关change/运行进程；按owner窗口串行，复用已交付验证，不碰既有工作区改动。

## Migration Plan

1. M1行情纵向切片：原始/复权字段、成交量、缺失质量、两个过滤器以及EOD和单位元数据连通，代表性除权/停牌请求可消费；不先做全域基础设施。
2. M2财务消费切片：summary/full/fields和财务/technical/PIT小数比例；复用现行本地权威查询与必要投影，定向回归完整读取和多期接口。
3. M3利率切片：HTTP排序分页和interval metadata、可信抓取时点；验证DCF旧内部查询语义不变。
4. M4研究筛选切片：同行统计有效样本、PIT质量过滤/全体统计，以及限定HTTP参数/错误合同；各切片完成相应HTTP验证。
5. M5交付：当前API文档、OpenAPI版本、定向回归、非阻塞Review分类、只提交本人改动。生产发布须在既有运维授权下单独执行；上线后优先只读冒烟，不对生产库实验。

回滚以各切片代码提交为单位；无schema变更，无全库数据迁移；回滚投影/参数逻辑不改因子和财务原值。已通过正常sync写入的真实利率data_as_of不应回滚成null，TDX后续null换手率仍是诚实缺失。完成所有实施任务和验收后才归档；当前全部保持未勾选。

## Open Questions

没有阻塞生成提案的产品问题；以上默认方案已确定。实施开始仍需核实当前共享文件占用、可复用财务投影接口、session cutoff与历史抓取时间证据；只允许据事实细化实现，不得借此扩大到全市场回填、统一平台或改变业务验收。

## 开发产物与本轮交接

本轮仅生成文档，已确认QuoteBak挂载可写；前一轮只读调查没有落盘临时脚本、响应全文或数据库副本，本轮也未创建此类制品，清理释放0字节。目录中其他公司画像/Chrome/测试产物归属未明，本轮不删除。实施前先核对本change及相关前序制品的引用与活跃占用，再清理已可重建的本人产物；所有临时库/脚本/日志进入`data/QuoteBak/<change>`，交接前删除无用副本，必要短证据汇入本design或实施验收记录。原始反馈是任务开始即存在的未跟踪文件，本次不改写、不暂存、不提交。
