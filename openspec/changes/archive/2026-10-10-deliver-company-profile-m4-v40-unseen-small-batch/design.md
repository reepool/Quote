## Context

前一change已按A角有限结论归档；本轮沿既有权威选样和交付owner验证新披露，不改变默认身份或模型。

## Goals / Non-Goals

Goals：完整44家排除，独立冻结service/manufacturing各一；先于首次执行的新来源合同，真实首次run/query/export及逐对象动态唯一复核，失败原样保留。
Non-Goals：全年报完整性或规模/生产声明，量价、成本、行业模板、正文精简、性能、去重框架及无关旧失败。

## Decisions

1. CompanyProfileTaskService.freeze_m4_next_batch_plan显式传44家delivered_ids，按原排序与官方有效版本取样，cutoff2026-09-17、v40完整五开关、50000token、新m4_v40_unseen_small_batch目录。重复freeze/read验证同时检查队列未变；本卡不预演。
2. 独立读取绑定官方PDF的完整业务、收入表及必要续页，保存原文本及布局。来源合同按主营、产品/服务、收入机制的实质条件及明示动作固定，保存原主体、期间、状态、表头、本期值和单位；禁止继承205/1135。未知映射允许pending/ambiguous。
3. 首次execute至第二export返回连续计时，包含PDF读取、布局和全部重试；第二家以剩余共享预算运行。既有execute_published_task是唯一写入owner。全部真实事实、已答正文唯一评价，Measurement仅准确率，角色别名/多证据不重复召回，原栏目等额不能替代原来源条件。
4. 独立检查原文与原动作，正文以实质完整及支持事实正确性评价，不凭answered或关键词。实际DB五阶段复用/前驱及查询/导出/磁盘一致性保存。双100%、数字0、≤300秒/≤50000token提交有限审核，否则保存首次失败及具体P0/P1业务缺口，暂停。

## Risks / Trade-offs

新披露可能产生正文遗漏、列错读或主体/动作污染，首轮不预演、不事后改合同，失败证据原样保留。负例只限实际检查页，不推全年报。生产not_authorized、规模false。

## Actual first attempt and business gaps

run/query/export两家首次请求均完成，但联通未入队/无工作项（frontier不存在，官方资产存在）及not_found，不能宣称两家业务闭环跑通。人福实际55事实＋3已答正文，52/58准确率；3个p49宜昌销售主体污染、3正文不完整，16/42收入满足，其余26缺，50具名动作未满足；合法葛店/新疆父类销售及集团药品采购不替代具名角色。联通原36条件无交付全部漏召回，仅恢复入口后才可判断抽取支持情况。正向合计16/131。动态Accuracy只核真实输出对象，未答正文/未交付不造错误事实；全部55事实含合同外与重复不同record_id各一次准确率，同条件多Segment只一次召回。

实际五阶段仅人福有执行并无复用/前驱；联通没有阶段记录，不推定执行成功。负例9中人福6实际通过，联通3不可评估，不能以空交付证明边界。p211表头“取得方式”的阅读顺序调整不改变原行数据，官方逐行核验，未改输出。来源期2024/2025分别核验，来源合同不事后改写。4.0→4.1→4.3→4.2→4.4只限真实业务缺口，禁止平行写入owner/全市场扫描或框架、性能、行业包。


## v41 implementation after A-role authorization

A角已核定4.0–4.4，按4.0→4.1→4.3→4.2→4.4实施。本次局部修复复用共享官方资产精确登记与绑定入队，接入CompanyProfileTaskService现有owner；不得转绑报告或手工补SQL。登记前重新核冻结资产四字段，读取间漂移拒绝写入。v41继承现有五累积开关，默认身份不变。原首轮联通idle/not_found、16/131与52/58及所有制品不改。

原131正向（6正文＋71收入＋54具名动作）及9负例不改变实质条件。后继合同显式保存辅助枚举映射：recognition_timing→revenue_timing；raw_material_procurement→现有raw_material_input投影，但底层Activity必须为purchases且原湖北人福主体成立，不能据该投影声明实际制造投入。角色仅召回一次，Measurement仅计准确率。声明来源之外的新实际输出也须唯一核原文。

局部规则：收入确认与药品销售政策完整接续；具名销售清单按本分支当前肯定销售判断，后续计划不生成新品销售，否定/未来不影响其他成立分支。产销表剂量折算不作为商品名。释义页支持简称到原主体全名且保留证据；展开仅v41开启，旧身份主体不改。p49表头不得成为主体。跨页本期关联服务/销售保留对方、真实负调整、原栏及单位；收入确认交叉表保留空格列位，各单元按表头取列，确认时间使用现有revenue_timing。被投资公司收入须具名证据、原主体限定和原表头同时成立，不改作集团营业收入。

正式复验使用同计划fb964468…、联通2024/人福2025原PDF、cutoff2026-09-17及m4_v41_same_source_repair全新目录。首次execute至第二export计时，含PDF读取/布局解析/重试，共享50000余量。首次结果冻结后独立检查全部接受事实＋已答正文，不预填准确率分母；131/131、动态准确率100%、数字0、9负例实际通过、≤300秒且预算内才提交A角有限审核。此次不归档、不扩批，生产not_authorized，规模false。


## v41 actual failure and remaining scope

v41真实两家完整闭环222.793617秒、0/50000、DB五阶段无复用/前驱且查询/导出/磁盘一致。129/131（六正文、69收入、54动作）与251/255（249事实＋6正文）、数字0及9负例实际通过。剩余两项p10业务收入数字正确但四个配对对象主体unclear；同页母公司利润归属文字过宽影响收入范围。4.2重开、4.4不作为通过关闭，仅为该真实缺口登记5.1/5.2，原首次与v41输出/执行代码不改。前置数字/名称回归没有完整覆盖收入条件主体；新增原合同主体断言并如实保留失败。业务验收不通过，不归档、不扩批；生产not_authorized、规模false。


## Post-formal local correction

v41原正式分数及制品保留后，在已核定4.2范围完成5.1原句主体边界补正：同页归母利润不支配经营收入；真正母公司/单体限定保留。完整页及全部既定条件局部19 passed＋母公司边界2 passed。4.2局部完成，4.4正式证明仍失败，5.2未执行；formal与post-correction代码hash/patch明确区分，不能用局部结果改正式分数。


## v42 successor formal proof after A-role review

A角确认v41失败收口及5.1局部补正，授权5.2同源正式复验与5.3独立逐对象审核。v42仅接续五项累积开关、精确共享资产登记/入队和释义主体展开；默认身份不变。原计划、联通2024/人福2025官方版本、cutoff2026-09-17和131正向＋9负例实质不变，新m4_v42_same_source_repair目录。先完整owner/独立PDF及真实母公司限定、否定计划边界验证，再首次run→query→export；全程计时含PDF读取、布局及全部重试，第二家使用共享余量。实际全部事实和六正文唯一核原来源，准确率动态生成不预填255；联通云/数据中心两配对事实须集团范围正确，业务分类与交叉不累计限制保留。131/131、动态100%、数字0、9负例、≤300秒/共享≤50000才提交A角，5.4等待有限通过；历史失败及补正原字节保护，不归档、不扩批。


## v42 first formal result pending finite A-role review

The successor completes both real owners in 239.367379seconds with 0/50000shared tokens. Actual unique review finds131/131positive conditions,255/255accuracy (249facts plus six bodies),zero numeric errors and nine evaluated negatives passed. Both five-stage records have no scope reuse or predecessor;query/export/disk agree. Native labels, amounts, units, actors/actions and six body text remain correct; two cloud/data-center pairs recover group scope. Ten revenue objects have sentence-bound evidence IDs refreshed by the already reviewed correction, including those four corrected subjects. The preserved v40/v41 failures remain unchanged. 5.2/5.3 are delivered for finite A-role review;5.4 waits for A-role acceptance. No archival or next batch is executed;production remains not_authorized and scale quality false.
