> 当前状态（2026-11-10）：实现及复审补修完成，30/30 任务已验收；已同步 5 份主规范并归档。实现提交 `504ede5d`、补修 `523e8c36`，尚未部署或重启。下文保留各轮原始判断与复验轨迹，当前状态以本说明及文末归档收尾为准。

## Context

本 change 是 2026-11-09 消费方反馈的修复规划。来源为 `docs/后端数据问题与改造需求_20261109.md`；下表的线上数据来自2026-11-09的只读 GET，2026-11-10本轮只复核需求、文档和本地调用链，不将旧样本冒充今日重测。并非原反馈逐字采纳。没有修改线上数据，没有测算全市场受影响行数，也没有测得实际投资损失。短证据保留于此，不复制完整财报或生产库作“证据”。

### 修复评估与取舍

| 项目 | 已核实事实及定位 | 本次交付 / 验收要点 | 优先级 / 难度 / 估计人日 |
|---|---|---|---|
| B-01 | `utils/adjustment.py:forward_adjust/_get_latest_cum_factor` 使用查询行情末日；600519 2020-01-02 单日 qfq=1130、factor=1，扩展至 2026-11-09 后同日 qfq=956.3574、factor=0.846334；技术摘要 qfq 与 none 的 MACD 分别约 44.185426 / 44.158691 | 不作为“qfq 未实现”修复；文档说明查询区间锚定，验证单日、跨除权、分页同区间，保留默认值与算法 | 澄清 / 低 / 并入行情切片 |
| B-02/B-03 | `AdjustmentEngine` 默认只复权 OHLC 和 volume；600519 2026-11-06 hfq close=11146.102、pre_close=1452、factor=7.671096；原始量4284700、复权量558551，amount=6199528448 | pre_close/change 与 OHLC 同基准；原始字段保留；volume 固定真实股数；不重算事件因子 | P0 / 中 / 合计1.5–2.5 |
| B-04 | 日行情 pct_change=-0.5513，技术 pct_change_1d=-0.005513；财务 gross_margin≈0.897619，PIT 同类比例按百分数输出；`financial_pit_panel.py` 明确乘100 | 新增同名小数比例入口及单位声明；保留旧字段值，回归验证不二次缩放；收益计算定义单独声明 | P0（跨接口误算）/ 中 / 1–1.5 |
| B-05 | 600519 23个交易日 turnover=0、volume>0、quality=1；`tdx_source.py` 明写“不提供换手率”但填0，当前质量算法不检查它 | 源缺失写null；历史读投影只识别已知占位零；缺失原因与质量口径清晰；不凭当前股本推算历史 | P0 / 中 / 0.5–1 |
| B-06 | 默认响应270439 bytes，关statements仍183060；facts与indicators各约88–89K；`requested_canonical_facts`仅增加service_layers，不投影原bundle；include_local_core=false是已定义默认值，大小不变是正常行为 | 默认窄摘要<8192 UTF-8 bytes；`view=full`可取旧明细；fields明确只投影业务值，身份/时点/单位元数据常驻 | P1 / 中 / 1–1.5 |
| B-07 | latest读日线；实测含time=2026-11-09零点、symbol为空，无EOD标志；batch_id=20260328_223307在不同日重复。`_load_progress`重用进度身份，不能据此断言行情是3月数据；写入owner亦会保留来源自带batch | EOD/as_of/computed_at/记录时间与日历陈旧度；symbol由主数据补齐；仅对能从owner确认的batch分类，来源自带/未知不误标 | P1 / 低至中 / 0.5–1 |
| B-08 | HTTP没有limit参数；`limit=2`返回6205条、675449 bytes，升序从2002-01-04到2026-11-06；series/observation data_as_of为空；`latest`是写入修订槽位 | 新增真实分页/排序与区间边界；不把旧历史的查询时间伪造成抓取时间；未来成功sync带时间 | P1 / 中 / 0.5–1 |
| B-09 | 000002 2016-06至07月：include_suspended=false/true与min_quality_score=.9均41行，其中21停牌低分；min_volume=1为20行；foo=bar返回200；`_apply_quote_filters`漏两个分支 | 已声明过滤真正生效；HTTP未知字段拒绝；过滤先于分页和统计；字段缺失不可静默退回未过滤数据 | P0（已声明过滤）/ 中 / 0.5–1 |
| B-10 | pe_ttm均值66.09475、中位数22.40304、min=-601.89322、max=921.98463，18/0纳入/排除；主spec要求排除负/零分母，但行级代码接受负PE且不读取其分母 | 按可观察倍数排除非正/非有限样本，有分母数据时另核符号；各统计量同一有效样本，raw均值对照，可选正值上限并记录参数身份 | P2 / 中 / 0.5–1 |
| B-11 | cross-section只有as_of/report_period/exchange/fields/limit/offset；现有stats是当前页统计（limit=2时exact=2），不是5536行总体统计 | 可选质量白名单与排除开关；选择最新可知期后过滤，不偷偷回退旧报表；新增全体过滤统计且保留旧页stats | P2 / 中 / 0.5–1 |
| B-12 | coverage缺日期为400+detail文本；FastAPI自身422已有loc/type/msg，年报路径另有结构化handler；不是全站“没有结构化机制” | 复用现有错误工具，在限定读端点增加稳定码及field/hint，保留detail，避免改写全站错误合同 | P2 / 低至中 / 并入参数切片 |

合计为约 **7–12 个工程人日**，是熟悉项目者对实现、定向测试和迁移说明的粗估，非已经消耗的工时或交付承诺；不含等待共享文件窗口、生产发布、客户端迁移等待和历史数据回填。优先行情正确性约3–5人日；切片复用验证，不能把表中每项上限机械相加或把12个反馈号当12个独立框架项目。

### 原始请求处置边界

主表已覆盖B-01至B-12；下面列出与原稿有区别或暂不实施的子请求，实施验收按修订后的spec执行，不能把这些区别默认为已满足原稿：

| 原请求 | 处置与理由 |
|---|---|
| B-01单日历史qfq必须异于原价，默认可改none | 不采纳：已证实是区间末日归一，非未实现。保留默认和算法；补单日/跨事件及因子错误传播回归，不把完整无事件的factor=1误报为失败 |
| B-04全服务原字段直接改小数，行情与技术任意日必须相等 | 本期交付明确带单位的百分数别名、canonical小数入口和units，含糊旧键deprecated；全端点旧键移除/改义后置，须另有迁移版本。仅同一时点和相同收益定义的值验收相等，除权参考收益与复权close收益不强行等同 |
| B-06单期limit/offset和Cache-Control/条件请求 | 不在本期：单期大对象先以summary/fields解决；多期沿用history窗口，不能声称已提供财务任意分页。缓存不解决正文截断，按需后续评估；8K是本change目标而非MCP通用协议上限 |
| B-07新增实时源、batch_id改真实批次或移除 | 不新增实时源；EOD明确标识。保留原batch_id以兼容，按已知出处标progress_session/source_batch或unknown，不把所有来源统一认定为进度身份 |
| B-08全部历史data_as_of非null，移除latest修订槽 | 新的成功采集持久化真实时间；旧历史无证据保持null+unknown_historical。保留latest并声明overwrite_slot，不制造vintage或历史抓取日 |
| B-10默认剔除正极端值，均值必须在四分位区间附近 | 默认剔除非正/非有限等无效值，正值按可选max_pe/max_pb阈值处理并回显；阈值无当前业务依据不擅定，mean处于IQR不是统计不变量 |
| B-11 min_available_at_quality和过滤前stats | 以显式available_at_quality白名单替代人为质量全序；保留原页stats，新增分页前filter_stats，不误称原stats就是全体分布 |
| §4全端点扫描/字段字典、§6未确认停牌状态及其他四域 | 本期只更新涉及字段的现有API字典和OpenAPI；未测域不扩为开发。已测000002的21个零量bar均非tradestatus=1，不能推广为全库保证；requested_canonical_facts的来源层选择语义已核实并保留 |

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
- `factor_metadata`的canonical dataset、series_version、activation、coverage、fallback原样保留；已有availability_error保持409等原有失败语义，不能因为返回投影把因子不可用变成forward成功。完整无事件与合法区间锚定factor=1仍可正常返回；technical的calc_version/parameter_hash/quote_summary保留，只有真实算法变化才更新计算版本。

弃选方案：只加说明保留混用（仍可静默误算）；重写引擎/重算因子（无证据必要）；新建第二套qfq算法（违反唯一owner）。除逐字段乘数外，必须验收`close/pre_close-1`与源端pct_change/100、`amount/volume_raw`与原始价格区间：对原稿600519受控精度样本前者误差<1e-6、后者约1446.90且位于1436–1457.72。一般样本按源pct_change、价格及乘数声明的小数位计算舍入预算，不声称从已四舍五入pct_change能恢复无限精度。

### D2 单位显式化且兼容旧数值

daily/latest新增`pct_change_pct=pct_change`与`pct_change_ratio=pct_change/100`，旧pct_change保留原值并在实际OpenAPI标deprecated；technical summary新增同名小数别名，值等于其现有`pct_change_1d`。声明`return_basis`：行情为源端参考前收收益，技术为复权close-to-close；普通无公司行为日二者应一致至精度容差，除权日按各自定义验收，不强行改源值使两个接口数值永远相同。

财务与PIT新增`ratios`小数对象，至少含gross_margin/operating_margin/net_margin/roe/roa/asset_liability_ratio；财务从既有小数原值取，PIT从既有百分数字段除100；PIT这六个百分数字段分别提供`<name>_pct`别名，其含糊旧键deprecated。current_ratio/quick_ratio保持倍数，per_share保持币种/股，不能统统除100。财务本来就是小数的indicators键不改值，也不误标为百分数。兼容旧键的移除不在本期；文档列出替代字段及将来移除必须有独立迁移版本。

`units`是返回字段路径→单位的有限映射（如`data[].pct_change=percent`、`data[].pct_change_ratio=ratio`、`ratios.roe=ratio`），不是通用schema registry。daily/财务/PIT/technical使用现有对象顶层，latest保留list形状、在各item带units，避免包一层导致额外破坏。只声明实际输出字段；未知值仍null。

### D3 换手率缺失只修已知占位，不捏造历史

TDX和Tencent正常解析将“不提供”的turnover写null；现有nullable存储和唯一写owner不变。读投影对`source`为pytdx或tencent且turnover为占位0的行返回null（volume为零也不等于该来源真的提供了零换手）；其他源的有效0保留。旧占位零和新采集null走相同缺失标注与质量政策：输出`missing_fields`与按字段的`missing_reason`；现有质量分为有效数值时取它与0.9的较小值，无有效质量分时保守返回0并标记缺失评分，不默认补成满分。缺失turnover不把完整OHLCV bar标为停牌或is_complete=false。源端未来若提供有效turnover，必须去掉“不提供”的适配规则并以测试更新，不能仅因source名称把所有数值清空。

min_quality_score过滤使用这份对外质量投影后的分数，先修正、再过滤、再分页/统计；include_quality=false只控制质量字段展示，不能阻止过滤所需内部评分读取，也不能绕过阈值或改成未过滤成功。不批量重写历史库。更精细全字段质量模型不在范围内。用当前流通股本计算历史换手率会引入时点错误，故不采纳。

### D4 真实过滤、未知参数与错误兼容

`include_suspended=false`排除明确tradestatus=0的行；true保留，默认false兑现现有声明。tradestatus与该参数取交集，冲突产生空结果不放宽条件；`min_quality_score`采用修正后的对外分数，未知分数不通过正阈值。相关必需过滤列无法判定时返回结构化数据不可用错误，不继续返回未过滤数据。

daily的adjust值本身也须校验：保留现有大小写/空白归一及qfq/hfq/none、forward/backward别名，其余值422并指向adjust，不能按旧代码静默走none分支。technical已有手工非法adjust校验仍保留400；两者错误码同为invalid_query_parameter，状态区别在迁移说明中交代。

未知查询字段只在下列路由适用：quotes/daily、quotes/latest、quotes/coverage、单期和history financial-statements、technical/summary、valuation/relative、risk-free-rate、financials/pit/cross-section。HTTP级依赖复用路由实际解析的查询参数（含Depends、别名、重复合法列表参数），不维护第二份人工白名单，不通过Pydantic extra=forbid误以为已捕获FastAPI丢弃的参数；鉴权header不当query处理。

错误增加`error_code/message/field/hint`，保留原detail类型和已有状态码；多个字段问题附`errors`且field可null。未知字段优先返回422+`unknown_query_parameter`；已有FastAPI值校验及本次新增参数的非法值/组合返回422+`invalid_query_parameter`，已存在的手工业务参数校验400继续400（如technical非法adjust），同样增加稳定码；仅含已知参数的coverage缺时间范围保留400+`missing_date_range`。原稿`coverage?limit=3`同时有未知limit和缺日期，修复后应422未知字段；另用不带limit的请求验收原缺日期400，不能要求同一请求兼得两种首要状态。不泄露堆栈或源凭证，不替换年报已有错误schema，不改本次范围外的HTTP行为。至少两个实际路由共用才提取窄助手，禁止通用异常平台。

### D5 财务默认摘要，完整载荷显式选择

单报告期新增`view=summary|full`，默认summary；`include_statements`改为可识别“未传”的可选参数，summary不能要求raw statements（显式true时422+hint转view=full），full下未传按旧True，显式false仍省raw statements。history默认完整/既有行为不变，新增可选view=summary及同一fields投影；不让单报告期默认切换悄悄改变现有history。

summary只读取既有权威行的标准标量财务科目、标量indicators与ratios，以及身份、报告期、可得日、source、单位；不读取或解码facts_json/indicators_json/statement_json，不包含完整事实包、details、lineage正文或raw statements。读取选列尽量复用已合并财务性能change的投影接口，不建平行事实源。默认无显式诊断时UTF-8 JSON序列化<8192 bytes；这是一项本change的消费目标，不宣称MCP协议具有固定8K上限。

fields接受逗号分隔的规范科目、`indicators.<标量键>`和`ratios.<键>`；例`fields=revenue,net_income`仅返回这两个业务值和必需身份/报告期/来源/单位元数据，不额外返回全部indicators。未知字段422，不把requested_canonical_facts当投影；后者仍只控制分层来源读取。显式include_local_core/include_industry_facts/allow_remote_extension等沿用旧功能，须注明诊断/远程扩展不在默认8K目标内；未显式允许远程不得联网，fields本身不触发远程。

参数优先级固定为：无fields时view决定summary/full；有fields时投影优先于view=full的全量展开，省去未请求的facts/details/statements，且未显式include_statements时不继承full的隐式True。fields与显式include_statements=true冲突时422，提示取消fields或取消raw statements。显式请求的service-layer诊断仍属于单独诊断块，非标准科目投影；普通fields示例未请求它就不附加。HTTP模型必须根据view/fields真实省略未请求字段，不能用完整Pydantic模型的默认null/空列表又补回全部科目。

实现增加summary投影响应模型，200 schema用与实际输出相匹配的组合描述full/summary/fields，不能只依赖完整ResearchFinancialStatementsResponse的默认序列化；summary含固定view标识，使其不与full分支混淆。history需对每项执行相同稀疏投影，保留wrapper及报告期元数据。验收同时检查实际JSON键集合、OpenAPI和HTTP响应字节，不能只测manager内部dict。

弃选方案：默认仍返回巨大正文、另加无明显入口的可选摘要（未解除默认消费问题）；新增单期limit/offset（不解决一个大对象的重复JSON）；先做缓存（不降低内联正文大小）。

### D6 EOD与时间元数据只表达可证明事实

latest保留List[DailyQuoteResponse]外形：`quote_type=eod`、`is_intraday=false`、`as_of`为bar交易日的带时区时间，文档说明它是日线日期零点标签而非实时tick或抓取时刻；`computed_at`为响应装配时刻；增加存储行created_at/updated_at作为record时间，不伪称源报价timestamp。symbol缺失时用同一主数据查得。

`staleness_trading_days`计数bar交易日之后、查询时刻之前已完成的交易所交易日；当日盘中前收盘为0、当日收盘后仍昨收为1、周末不增加。只使用权威交易日历和现有session cutoff；缺日历/截止时点依据则null+原因，不猜节假日。batch_id原值保留：有现行写入路径证据的进度身份标`batch_id_kind=progress_session`，源端真实批次标source_batch，无法证明出处标unknown，原值缺失则kind为null。`_prepare_daily_quote_rows`的setdefault会保留源端batch，official_index_source也有自有batch，不能据一个股票样本或日期形状把所有bar断定为进度身份。不改造整条采集流水线或伪造历史新批次。

### D7 利率HTTP有界页与真实抓取时点

HTTP新增`order=desc|asc`（默认desc）、`limit`（默认100，1–2000）、`offset`（默认0）。先应用series_id、既有latest修订槽位和含边界日期过滤，算该区间total_available/first_date/last_date，再按observation_date请求方向、source_profile升序、revision_id升序的稳定业务键分页；响应补source_profile，以区分同日不同来源。现有主键还含source_profile/revision_id，不能声称日期唯一或仅按日期保证不重不漏。保留`total=len(observations)`兼容语义，新增`returned_records/truncated/next_offset`，末页next_offset=null。不提供隐式无限页；取全历史须遍历，或限定日期；分页不重不漏的保证以各页读取期间数据集不变为前提，不新增快照游标体系。

存储内部方法默认不截断并保留其历史排序合同，HTTP通过显式page查询参数/现行owner委托；不能把HTTP默认值下沉到DCF读取，防止截止日取错数据。series.last_observation_date表达整个series最新已存观测，interval的first_date/last_date表达请求过滤区间；`data_as_of`定义为该源成功抓取时间，三者不混用。

sync先fetch；首次同步先创建data_as_of为空的静态series父记录以满足外键，已存在父记录保留原成功时间；完成观测写入后再更新series的带时区成功抓取时间；观测时间戳由同一成功fetch生成。失败或empty不得先用空/新metadata覆盖上次成功时间；empty仍可注册静态series定义，但必须保留之前成功的data_as_of，且不得宣告刷新成功。若分块写入失败，series成功时间不前移；已写观测只可保留它们确实使用的fetch timestamp。旧行有可信持久化时间依据才以明确basis回显，否则继续null并给`data_as_of_quality=unknown_historical`，不能用查询时间或最新观测日期填充。响应显式提供series的last_observation_date、series/observation的data_as_of_quality、observation的source_profile/revision_kind及分页元数据；这些字段必须进入Pydantic/OpenAPI模型，否则FastAPI response_model会剥除。原`revision_id=latest`保留为兼容槽位并说明overwrite_slot，不声称有历史vintage。本次没有历史回填/付费采集授权。

### D8 估值按可解释样本统计，不用阈值包装正确答案

默认所有PE/PB/PS变体排除缺失、非数值、非有限和<=0样本；可得分母元数据为负/零时同样排除，但不凭仅有倍数的行虚构分母检查证据。正市值负PE对应负盈利，不是有效可比倍数；raw peers仍展示原值。`peer_mean/median/min/max/p25/p75/percentile_rank/premium_to_median`使用同一有效集合，metric_exclusions列原因，subject非正值时相关排名/溢价null并说明原因。新增可选阈值的有效参数必须纳入parameter_hash，统计政策变化更新calc_version，不能让不同筛选结果声称使用同一参数身份。

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

回滚以各切片代码提交为单位；无schema变更，无全库数据迁移；回滚投影/参数逻辑不改因子和财务原值。已通过正常sync写入的真实利率data_as_of不应回滚成null，TDX后续null换手率仍是诚实缺失。完成所有实施任务和验收后才归档；实施及本地验收状态见文末记录。

## Open Questions

没有阻塞生成提案的产品问题；以上默认方案已确定。实施开始仍需核实当前共享文件占用、可复用财务投影接口、session cutoff与历史抓取时间证据；只允许据事实细化实现，不得借此扩大到全市场回填、统一平台或改变业务验收。

## 开发产物与本轮交接

本轮仅生成文档，已确认QuoteBak挂载可写；前一轮只读调查没有落盘临时脚本、响应全文或数据库副本，本轮也未创建此类制品，清理释放0字节。目录中其他公司画像/Chrome/测试产物归属未明，本轮不删除。实施前先核对本change及相关前序制品的引用与活跃占用，再清理已可重建的本人产物；所有临时库/脚本/日志进入`data/QuoteBak/<change>`，交接前删除无用副本，必要短证据汇入本design或实施验收记录。原始反馈是任务开始即存在的未跟踪文件，本次不改写、不暂存、不提交。

### 2026-11-10需求对照复核

原版本并非没有问题；本轮按A类阻塞问题修订：B-04缺带单位别名；财务full/fields及HTTP响应模型的投影冲突；B-12已有400与新422的优先级矛盾；新null换手率与include_quality=false下过滤验收遗漏；收益/均价恒等式及因子元数据保护未落入规范；批次出处泛化、利率同日多来源分页键和估值阈值参数身份未明确。proposal/design/specs/tasks同步修订，原反馈有意调整的子请求列在上方处置边界；B类性能/架构建议不扩为新工作，C类既有脏文件不修改。没有实施业务修复，全部实现任务仍未勾选；本轮无新增临时制品，释放0字节。

### 实施前审核缺口收口（2026-11-10）

Tencent是现行A股备用来源，日线/快照也硬编码换手率0，与TDX一起按已知缺失处理。首次利率同步须先确保静态series父记录存在，再写observations，成功时间只在全部写入后更新。行情显式返回currency及amount/price/volume/turnover/factor/quality的单位；金融与PIT单位表包含旧字段和新别名。仅查询停牌需tradestatus=0并显式include_suspended=true。补HTTP验收include_snapshot=false应422、include_local_core=false仍可用、daily非法adjust应422。测试运行使用现有conda Quote环境；未发现本change前序临时产物，不删除归属不明的其他产物。


### 实施验收与交接（2026-11-10）

已完成 M0–M5 共 30 项任务。实现沿用现行存储、复权和同步 owner；新行情与财务读投影位于各自域模块，限定 API 参数/错误适配位于 `api/data_query_contract.py`。API info 为 1.2.0，迁移例子已更新 `docs/api/restful_api.md`。这是本地实现验收，尚未部署或重启，未连接生产数据库执行写入；change 保留待归档状态，不将本轮实现声明为 W4/W5/W9 整体迁移完成。

可复验命令先执行 `source scripts/quote_dev_env.sh`，Python 使用 `/home/python/miniconda3/envs/Quote/bin/python`：

- 行情/源/API：`python -m pytest tests/unit/test_data_sources/test_tdx_source.py tests/unit/test_data_sources/test_tencent_source.py tests/unit/test_api/test_data_consumption_contract.py tests/unit/test_research/test_data_read_repairs.py tests/unit/test_api/test_quote_query_semantics.py tests/unit/test_api/test_quote_capability_improvements.py tests/unit/test_api/test_research_routes.py -k 'not real and not integration' -q` → 201 passed、7 deselected。
- 利率/PIT/估值/技术/查询消费者：`python -m pytest tests/unit/test_research/test_risk_free_rate.py tests/unit/test_research/test_financial_pit_panel.py tests/unit/test_research/test_valuation_service.py tests/unit/test_research/test_technical_service.py tests/unit/test_research/test_query_service.py -k 'not futures_cycle_for_margin_normalization' -q` → 98 passed、1 deselected。
- Review 修复定向复验：`python -m pytest tests/unit/test_research/test_data_read_repairs.py tests/unit/test_api/test_data_consumption_contract.py tests/unit/test_research/test_valuation_service.py -k 'not futures_cycle_for_margin_normalization' -q` → 72 passed、1 deselected；随后行情/PIT 复验（排除已通过的文件句柄压力例）36 passed、1 deselected；最后估值分母与阈值主体检查 2 passed。
- 内部兼容：`python -m pytest tests/unit/test_research/test_data_manager_bootstrap.py -k 'relative_valuation or get_research_financial_statements or risk_free' -q` → 16 passed、87 deselected。
- 新模块/新测试 ruff、全部修改实现的语法编译及 `git diff --check` 通过；`openspec validate fix-market-data-api-consumption-semantics --strict` 通过。沙箱内本地 ASGI 线程唤醒异常时，在获准的沙箱外运行同一组 fixture，未改测试预期来绕过异常，也未发起真实数据采集。

业务证据由小型 fixture 与断言保留：后复权 close/pre_close/change 同因子，收益误差小于 1e-6，原始成交量 4,284,700 股、均价约 1,446.8991 元，金额及原始股数不随复权变化；财务默认实际 HTTP 响应小于 8192 字节，SQL trace 确认摘要不读/解码三类完整 JSON；利率首次写入受 FK 约束，部分 chunk 成功后失败保留旧 series 成功时间，已写行保留真实抓取时间；PIT 最新期不合格不回退旧期，filter_stats 跨页不变；估值全统计使用同一正有限集合，可用分母明细会被检查，显式阈值同时约束主体有效性。

Review 分类：A 类已修复真实 HTTP 稀疏序列化、未知参数与旧 400 错误兼容、首次利率父记录/失败时间保护、估值分母读取与校验、latest 空窗口及可空参考值序列化等；B 类整体路由/存储拆分和未来治理不扩大；C 类既有 DCF 周期测试失败及已有 `data_manager.py` 无关路径的 `source_symbol` 未定义保持原样。DCF 失败在本次估值改动前已复现：`test_valuation_service_dcf_cyclical_uses_futures_cycle_for_margin_normalization` 预期 futures-cycle 调整标记，当前返回 clamped 标记；因此本轮定向回归明确排除此例，不声称全仓全绿。

工作区保护：按任务起始基线核对并只提交本 change 可隔离改动。比对时发现公司画像验收/任务文档、restore-company-profile change 和 requirements.txt 等非本任务文件被并行更新；本任务没有写入或还原这些文件，也不将它们加入提交。原反馈未跟踪文件保持原样。核对测试目录名称、无 pytest 活跃锁且相关进程已结束后，删除本轮 pytest-78/79/80 的可重建 fixture 和本任务基线副本，共释放 27,726,162 字节；另移除本轮显式编译生成的字节码 3,220,370 字节。其他缓存、归属不明的历史日志和并行制品保留。必要证据仅保留本节及对应测试，没有保存生产库副本或完整响应正文。


### 实现对照复审（2026-11-10，审核结论未通过）

针对实现提交 `504ede5d` 重新核对五份 delta spec 和实际 HTTP 链路。上一节 30/30 完成结论偏早；本轮重新打开任务 2.3、3.2、5.1、6.4，当前为 26/30。仅修改审核记录与任务状态，未修改业务实现、暂存、提交、推送或操作生产。

- **P1 / 3.2：分层财务值被摘要/fields 丢失。** 当标准 bundle 不存在、显式 local_core 或 remote_extension 有 canonical facts 时，既有 fallback 把数值置于 `facts`；新投影仅以 `bundle.get(key)` 取顶层标量。受控 HTTP 请求中，`view=full&include_local_core=true` 的 `facts.revenue=100`，同一数据的 `fields=revenue&include_local_core=true` 却返回 `revenue=null`（200）。需要在既有权威 fallback 上将可用规范值接入标量投影，并补实际 HTTP 回归，不能改造成新的事实源。
- **P2 / 5.1：估值排除了非有限比较值，但整个响应仍可能 500。** 有一个 `pe_ttm=Infinity` 同行和一个正常同行时，内部统计有效数已正确为 1，但 `peers` 的 compact 行继续带 Infinity；Pydantic 接受它，JSON 序列化失败，实际 relative endpoint 返回 500。主体有同类值也有相同风险。需要在对外投影中将不可用非有限值表达为 null 并保留诊断，补端到端测试；不修改历史原始值。
- **P2 / 2.3：未知质量分处理错误地依赖换手率缺失。** 对合法 `turnover=2`、`quality_score=null` 的可存储行，normalize 保留 null，latest 的非空 float 模型校验失败；评分键缺失时还会由模型默认值补成 1。应独立归一未知评分为 0 并标注缺失原因，保留“已知缺换手率才封顶 0.9”的规则，验证 daily/latest/隐藏质量时的筛选一致性。

复验：`openspec validate fix-market-data-api-consumption-semantics --strict` 仍通过；`python -m pytest tests/unit/test_api/test_data_consumption_contract.py tests/unit/test_research/test_data_read_repairs.py -q` 为 18 passed。独立补充的 7 个本地复现为 5 failed / 2 passed（5 个失败对应上述 3 个缺陷；空筛选结果与既有数值范围 422 对照例通过）。所有复现均为内存 fixture 和本地 ASGI，没有生产数据写入。严格校验只证明规范格式有效，原有测试通过不能替代遗漏边界的业务验收。

补修用的短复现脚本及本轮 Git 状态基线暂存于 `data/QuoteBak/review-market-data-consumption-20261110/`，仅供下一轮将失败例转为正式回归测试；补修复验完成后删除。该目录为本轮新建，无本 change 前序遗留物可清理；其他并行/历史制品不处理。本节为持久审核证据，不保存完整接口正文或数据库副本。


### 缺陷有效性复核与修复闭环（2026-11-10）

三项均属实，属于既有需求遗漏，不属于过度审核：分层 fallback 是现行可用路径，字段投影必须保留其规范数值；非有限倍数已被规范和原有测试明确纳入异常输入，SQLite REAL 也允许持久化 Infinity；quality_score 在实际模型中 nullable，规范明确要求未知评分保守为 0。这里的异常值 fixture 用于验证既定合同，不表示已经证明生产库存在这些异常行。仅修复直接影响当前结果/HTTP 返回的问题，不扩展治理框架、生产回填、路由拆分或阈值策略。

修复内容：

- `financial_read_projection.py` 只对 owner 标为 `source=service_layers` 的既有 fallback，从已选择的规范 `facts` 取缺失顶层科目；已有标量（包括显式 null）保持权威。full 明细、远程授权和正常 summary 的无完整 JSON 读取合同不变。
- `valuation_service.py` 的 relative 专用 compact 读投影将 NaN/Infinity 等无效数值转为 null，继续由原有 metric diagnostics 报告排除原因。有限负值继续作为 raw 对照，原始行、统计集合、calc_version/parameter_hash、历史估值和 DCF 计算不变。
- `quote_read_semantics.py` 独立处理未知评分，输出 0 及 missing_fields/missing_reason；换手率缺失才适用的 0.9 上限、正常来源有效零值、OHLCV 完整性和停牌状态保持原规则。daily/latest 共用同一投影，隐藏质量不绕过筛选。

验证（所有命令先 `source scripts/quote_dev_env.sh`，Python 为 `/home/python/miniconda3/envs/Quote/bin/python`）：

- 新增 9 个正式回归例在修复前均失败，覆盖 local_core/显式 remote_extension，summary/fields/full 优先级，主体/同行 NaN/Infinity，以及评分 null/缺键/NaN。修复后 `python -m pytest tests/unit/test_research/test_data_read_repairs.py tests/unit/test_api/test_data_consumption_contract.py -q` → **27 passed**；补齐真实远程路径必需的 requested_canonical_facts 后该两项 HTTP 用例再次通过。
- `python -m pytest tests/unit/test_api/test_research_routes.py tests/unit/test_api/test_quote_query_semantics.py tests/unit/test_api/test_quote_capability_improvements.py tests/unit/test_data_sources/test_tdx_source.py tests/unit/test_data_sources/test_tencent_source.py -k 'not real and not integration' -q` → **189 passed / 3 deselected**。
- `python -m pytest tests/unit/test_research/test_valuation_service.py tests/unit/test_research/test_technical_service.py tests/unit/test_research/test_financial_pit_panel.py tests/unit/test_research/test_query_service.py tests/unit/test_research/test_data_manager_bootstrap.py tests/unit/test_research/test_risk_free_rate.py -k 'not futures_cycle_for_margin_normalization and not database_route_state_is_isolated and not interest_reads_ignore_other_thread and not nested_and_failing_database_scopes and not missing_series_does_not_hide' -q` → **190 passed / 6 failed / 6 deselected**。排除项为已记录的既有 DCF 周期断言和上轮已通过、且本轮未修改路径的 5 个慢速数据库路由例。
- 扩展回归的 6 个失败属于当前工作区既有测试失配：`test_data_manager_bootstraps_research_storage_when_enabled` 仍断言直接 initialize（当前 owner 为 ensure_business_profile_storage_ready）；3 个 `test_data_manager_get_research_shareholder_readiness_*` 的旧 mock 不匹配当前股票池查询。这 4 项在独立进程内加载 HEAD 修复前三模块后仍以相同原因失败，未回退或改写共享工作区。另外 `test_research_industry_standard_sync_forces_master_governance_refresh` 和 `test_data_manager_financial_summary_sync_attaches_master_governance` 仍 mock 旧 ensure_instrument_master_fresh，而实际路径已是 run_master_governance_for_job；入口代码未被本 change 修改。首次扩展回归发现这两个旧 fixture 可触及真实上游，因此未重复运行它们。未改变这 6 项测试的预期，不据此声称全仓全绿，也不把无关修复并入本 change。
- 本轮修改的 helper/测试 ruff、语法与 diff 检查通过；OpenSpec strict 校验通过。修后再次审核全部新增差异及调用方，没有发现本轮引入的阻塞缺陷。没有修改数据库结构、调度、远程启用、复权因子、PIT 可得日或技术算法；未执行生产迁移和服务重启。

任务 2.3、3.2、5.1、6.4 已恢复完成，当前 **30/30**；可在实施验收后归档。公司画像相关文档在本轮被并行更新，本人没有写入、还原或暂存这些路径。上一轮短复现已转入正式测试，相关进程结束且无活跃锁后，已清理 `review-market-data-consumption-20261110` 和 `fix-market-data-review-repairs-20261110` 两个本人临时目录；共释放 2,597,346 字节；必要证据保留在本节与正式测试中。


### 归档、文档对齐与清理收尾（2026-11-10）

用户本轮明确授权归档、清理和生成消费方回复。`openspec archive fix-market-data-api-consumption-semantics --yes` 完成归档并同步 5 份主规范：20 项新增要求、1 项估值要求修订；补齐新规范 Purpose，并澄清 history 的完整默认独立于单期摘要默认，避免旧“相同结构”措辞被误读。原 proposal/tasks/delta specs 和 `.openspec.yaml` 保持归档前字节，design 只补当前状态说明与本段收尾，不追改各轮成绩。

更新 API 参考、财务系统说明、总纲状态/归档链接、变更日志及总索引；回复文档位于 `docs/后端数据问题与改造需求_20261109_回复.md`，完整覆盖 B-01～B-12、待复核项、调整后的验收、迁移示例与上线边界。原始用户反馈文件保持原样且不纳入本次提交；历史 1.1.0 回执不追溯改写。代码完成与线上发布分开表述，本轮未部署或重启。

清理盘点确认此前本人相关临时目录均已删除；此前清理的测试库、基线副本与编译制品累计 33,543,878 字节。本轮只产生 2,778 字节的基线 hash 清单，核对并行改动归属后已删除；未新增测试数据库、临时脚本或运行代码。正式回归测试和业务模块是持续验收/运行所需，保留；归属不明及并行任务制品未清理。必要证据仅保留原测试、本文和回复，不保留重复 API 正文或整库副本。

归档后 5 份主规范分别通过 `openspec validate <capability> --type spec --strict`，原任务 30/30、无在途剩余项；归档元数据与 requirements 唯一性、当前文档新增链接和 diff 检查通过。本轮仅规范/文档和归档整理，未改 Python 业务实现，不重复运行上轮已记录的业务测试；既有 6 项失配及 DCF 排除项仍按原验收记录披露。工作区另有公司画像等并行改动，本人未写入或还原，提交只包括本次可隔离整理。
