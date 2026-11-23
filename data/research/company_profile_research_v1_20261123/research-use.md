# 20家公司画像：研究使用检视与补读取舍

日期：2026-11-23。读取根：`company_profile_research_v1_20261123`；代码基线：`d7cc82a1`。

本页由A基于既有API可消费内容整理，B核对分级和计划绑定；不是新模型抽取、独立全文复核或投资建议。当前20股633条可读记录、1003个指标不变。输入`annual-api.json`的SHA256为`38e30b0dc41ec5736950847cc85d45a61ea50bb70cb8d03d1575c9768b5eba4b`。

结论：这些画像已经可以用来比较“做什么、卖什么、怎么赚钱”。下一步应围绕具体研究问题补读，而不是要求每个字段齐全。原97.8967%成绩、正式质量门槛及生产权限不改。

## 1. 用现有内容能看出什么

下表是业务导航，不重新制作一套财务指标。公司链接进入同一API投影的人读版；F编号和物理页定位本次采用的既有事实，详细引文、章节路径、报告版本、模型和限制在原画像中。引用页码只定位这份报告，不用于其他公司的选章规则。主要材料集中在各报告的业务介绍、经营模式/管理层讨论；收入构成必要时来自财务附注。

| 公司 | 已能形成的业务理解与赚钱方式 | 既有事实与物理页 |
| --- | --- | --- |
| [德赛电池000049](/home/python/Quote/data/research/company_profile_research_v1_20261123/exports/000049.SZ_2025-12-31.md) | 电源管理、锂电池封装集成，并布局SIP与储能电芯；向终端客户或其指定代工厂直销，以销定产、以产定采。 | F01/F05/F06；10页 |
| [大秦铁路601006](/home/python/Quote/data/research/company_profile_research_v1_20261123/exports/601006.SH_2025-12-31.md) | 铁路货客运输，货运以动力煤为主；依托运输合同取得运输收入，另向其他铁路企业提供服务。不能把煤炭运输理解为煤炭生产。 | F01/F04；8页 |
| [华夏银行600015](/home/python/Quote/data/research/company_profile_research_v1_20261123/exports/600015.SH_2025-12-31.md) | 企业与零售金融服务；收入来源可区分利息净收入、手续费佣金等非利息收入，不能将信用卡/财富业务收入当全行总收入。 | F04/F05/F11/F12；13、20、60页；限用说明一并读 |
| [华阳股份600348](/home/python/Quote/data/research/company_profile_research_v1_20261123/exports/600348.SH_2025-12-31.md) | 煤炭开采、洗选与销售为经营主干，并布局新能源新材料；煤炭产品面向电力、化工、冶金、建材等领域。布局不自动代表已有同等收入贡献。 | F001/F004/F007；11、14、15、22页；部分数值限用 |
| [潍柴动力000338](/home/python/Quote/data/research/company_profile_research_v1_20261123/exports/000338.SZ_2025-12-31.md) | 动力系统、商用车、农业装备、智慧物流、电力能源等协同；收入构成能区分动力总成/整车、智慧物流、农业装备等，不只是一家发动机企业。 | F-BS-01/F-RM-02；12、17页 |
| [王府井600859](/home/python/Quote/data/research/company_profile_research_v1_20261123/exports/600859.SH_2025-12-31.md) | 奥莱、购物中心、百货、免税及专业店等零售业态；主要通过商品销售及租金取得收入。免税门店家数存在来源冲突，不据此拼确定数。 | F11/F32；11、18页 |
| [兴业银行601166](/home/python/Quote/data/research/company_profile_research_v1_20261123/exports/601166.SH_2025-12-31.md) | 以银行为主体的综合金融服务，覆盖企业、零售与多类附属金融业务；利息净收入与非利息净收入分别理解。 | F01/F03/F04/F06；18、76、103–106、108页；部分指标限用 |
| [长江电力600900](/home/python/Quote/data/research/company_profile_research_v1_20261123/exports/600900.SH_2025-12-31.md) | 大型水电运营，管理六座流域梯级电站，主要提供清洁电力；水电业务收入已有明确来源。 | F01/F04/F07；9、18页 |
| [格力电器000651](/home/python/Quote/data/research/company_profile_research_v1_20261123/exports/000651.SZ_2025-12-31.md) | 消费电器和工业两大领域；消费电器是重要收入来源，同时有工业制品、绿色能源和智能装备等业务，不能只列一串空调型号代替业务结构。 | F-BS-01/F-RM-02；10、24页；部分数值限用 |
| [学大教育000526](/home/python/Quote/data/research/company_profile_research_v1_20261123/exports/000526.SZ_2025-12-31.md) | 个性化教育、职业教育、文化阅读、医教融合；收入主要来自教育培训服务费。四个业务名称不等于已经取得四板块独立收入占比。 | F01/F08；10、17页 |
| [兴业证券601377](/home/python/Quote/data/research/company_profile_research_v1_20261123/exports/601377.SH_2025-12-31.md) | 财富管理、机构服务、自营投资、海外四大业务；取得手续费佣金、利息及投资收益。已有会计收入科目，但四个业务板块的收入权重仍未获取成功。 | F01/F06/F08/F09/F10；34、47页 |
| [深振业A000006](/home/python/Quote/data/research/company_profile_research_v1_20261123/exports/000006.SZ_2025-12-31.md) | 房地产开发经营为主，另有租赁、物管及代建旧改等；房产销售与租赁等收入类别已有来源，不把旧改服务合同直接当已确认收入。 | F-BS-01/F-RM-01；9、14页；部分指标限用 |
| [洪都航空600316](/home/python/Quote/data/research/company_profile_research_v1_20261123/exports/600316.SH_2025-12-31.md) | 教练机和防务产品研发制造，并提供维修与保障；收入按教练机、其他航空产品、非航空产品披露。不能因展示了教练机型号，就把“其他航空产品”也解释为教练机。 | F01/F09；9、116页 |
| [华数传媒000156](/home/python/Quote/data/research/company_profile_research_v1_20261123/exports/000156.SZ_2025-12-31.md) | 有线电视、新媒体、宽带/智慧城市及广电5G；有按户收费、内容与运营商分成、集成播控/内容服务、项目建设运维等不同赚钱方式。 | F01/F08/F09/F10/F11；11–13页 |
| [江苏金租600901](/home/python/Quote/data/research/company_profile_research_v1_20261123/exports/600901.SH_2025-12-31.md) | 专注融资租赁，另有经营租赁及资金利息收入；通过租赁物、承租人与厂商协同开展业务，并有厂商风险共担安排，不套用制造企业原料采购模板。 | F04/F06/F28；20、22页 |
| [中国建筑601668](/home/python/Quote/data/research/company_profile_research_v1_20261123/exports/601668.SH_2025-12-31.md) | 房建、基建投资建设、地产开发投资、勘察设计等；主要收入类别可分列，合同额、收入、土地储备不互相替代。 | F01/F10；17、33页 |
| [特瑞斯920014](/home/python/Quote/data/research/company_profile_research_v1_20261123/exports/920014.BJ_2025-12-31.md) | 天然气/氢能领域装备研发制造销售，涵盖调压集成设备、非标撬装系统、核心部件及氢能集成设备；按这些产品类别披露收入。 | F01/F07；12、26、154页 |
| [爱尔眼科300015](/home/python/Quote/data/research/company_profile_research_v1_20261123/exports/300015.SZ_2025-12-31.md) | 眼科连锁医疗，提供诊疗、手术及验光配镜；收入可按屈光、白内障、视光等项目理解，不只是医院数量。 | F-PS-01/F-PS-02/F-RM-01；23、35页 |
| [星昊医药920017](/home/python/Quote/data/research/company_profile_research_v1_20261123/exports/920017.BJ_2025-12-31.md) | 同时开展自主药品制造和CMC/CMO服务；药品销售以经销为主，服务业务按技术服务合同/批次或订单取得加工等费用。 | F01/F10/F12；11、64页 |
| [中信海直000099](/home/python/Quote/data/research/company_profile_research_v1_20261123/exports/000099.SZ_2025-12-31.md) | 以海上油气直升机服务为核心，兼有救援、陆上通航、引航、维修等；通航运输为主要收入来源，但不同业务的收入贡献尚不能安全分开。 | F-BS-01/F-RM-01；14–16页 |

部分存储章节上级标签存在自动恢复异常，例如兴业证券34页被挂在带目录页码的上级标签、江苏金租部分段落被挂在表格行标题。这里保原记录和物理页，不将这些标签当新核实的章节名称；定向补读按当份目录的业务章节重新定位，不沿用错误标签或固定页号。此问题不妨碍阅读已保存原文，本轮不扩成目录引擎改造。

## 2. 框架之外，哪些内容确实帮助理解公司

不要求每家公司都抽同样多的特色。以下是既有内容中值得保留的差异，不是额外必填清单：

- 德赛电池：非标定制为主、储能电芯标准化，解释了不同产品的订单与生产组织差异（F04，产品服务）。
- 大秦铁路：以动力煤外运为主的运输服务角色，不能按煤矿企业理解商品暴露（F04/F33）。
- 长江电力：六座梯级电站运营及调度组织，体现运营资产的业务特点，而非仅罗列发电量（F01/F12）。
- 江苏金租：“租赁物+承租人+厂商”和部分厂商风险共担，解释业务组织方式（F28）。
- 星昊医药：自主药品和CMC/CMO并行，两类业务的客户及收费机制不同（F01/F12/F39）。

供应链在现有材料中已有粗粒度描述，可作为理解业务角色的补充；原料价格传导、供应商名单、客户逐家映射不在本轮追补。

## 3. 哪些值得补，哪些先不补

| 取舍 | 具体问题 | 决策 |
| --- | --- | --- |
| 值得按需补读 | 兴业证券四业务板块的主要收入及权重；当前API明确记录只取得相关表头、未取得金额 | 会影响业务贡献判断；先同一经营章节的分板块表，再视必要性读收入/分部附注，不把会计科目当业务板块 |
| 值得按需补读 | 中信海直通航运输收入及业务构成；现有分行业指标绑定失败，业务细分也未获取 | 会影响对海上油气依赖的判断；先主营业务分行业/分产品表与经营模式，未披露细分则停在定性结论，不借地区或子公司数代替 |
| 有研究问题再补 | 洪都“其他航空产品”具体内容、学大四板块独立收入权重 | 现有名称/金额已经可读；可能披露不足，不为细化自动追加阅读，更不从常识猜产品或权重 |
| 暂不追 | 型号规格、逐家供应商客户、所有地域细行、排名荣誉、供应链精细定量 | 不改变本轮业务理解，保持缺项提示即可 |
| 继续限用 | 华夏已有业务收入指标范围、王府井门店数/日期冲突、部分表格或近约超口径 | 原说明与隔离保持，不为了凑齐字段解禁；不重开6.FB四家窗口 |

前两项的[兴业证券问题计划](/home/python/Quote/data/research/company_profile_research_v1_20261123/reading-plans/6fc-601377-revenue.json)与[中信海直问题计划](/home/python/Quote/data/research/company_profile_research_v1_20261123/reading-plans/6fc-000099-revenue.json)仅绑定当前同报告画像/hash，`state=planned`、`execution_started=false`。它们没有模型/预算授权，不是已执行补读，不直接作为日常任务的execution-plan-file使用。真正补读仍经原TaskService enrich，在另获明确范围和有限资源后启动。

## 4. API使用时怎样避免误读

1. 批量取明确股票清单；`missing_instrument_ids`只表示当前根未找到画像，不能解释为年报没披露。
2. `facts`含历史/隔离记录；只有`fact`非空的内容进入普通描述，仍须连同`state/label/reasons`理解待核或限用状态，非空不等于独立核准。
3. 数值只读`fact.metrics`，并看`metric_gaps`。不要从保留正文重新正则抓回被隔离指标，不把缺口填0，不把“近/超”用于精确加总。
4. `information_gaps`先看`active`（旧数据缺省按真），历史被替代缺项不再次当作当前补读任务。active缺项也可能只是可选细项，不按条数判公司失败。
5. 同份画像中不同读取批次保各自来源与审核；全公司画像有内容，不意味着每个要素完整，更不意味着全文审过。

本次快照中，633条可读记录由566条`reviewed`与67条`limited`组成；`reviewed`是现有研究复核标签，不等于本轮逐条独立核准。另有4条`source_only`、3条`withheld`及740条`superseded`历史记录，不能一起当当前可用事实。121条active缺项和123条失效历史缺项也不是121个必须补满的任务；其中包含规格、可选明细和局部限制理由。本页只优先选择两项影响业务理解的问题，不按数量追齐。

下面只读取现有资料，不构造模型或启动任务：

```python
from research.company_profile.reads import CompanyProfileReadService

reader = CompanyProfileReadService('/home/python/Quote/data/research/company_profile_research_v1_20261123')
batch = reader.research_profiles(['601377.SH', '000099.SZ'])
for profile in batch['profiles']:
    readable = [row for row in profile['facts'] if row.get('fact') is not None]
    active_gaps = [gap for gap in profile['information_gaps'] if gap.get('active', True)]
    print(profile['instrument_id'], readable, active_gaps)
```

## 5. 本轮边界

0次新供应商模型请求，0次联网，0次选章/抽取/复核/补读执行；不产生新的准确率或公司验收成绩。原API、已有导出和writer不改；只增加本页及两份未执行问题计划。不新造接口、质量评分或自动补满循环。临时盘点和脚本在核对后清理，详细操作结果记入现有OpenSpec交接入口。

收尾：20行事实编号/原页/链接、两计划的当前base/hash/report及未启动状态直接校验通过，OpenSpec strict与diff检查通过；无源码变化，未重跑无关测试。A清理5个临时备份/脚本文件2,241,790字节，B清理1份统计快照51,275字节，共2,293,065逻辑字节。两个本人临时根均已删除；正式源、101份原交付、原失败及并行内容保留。
