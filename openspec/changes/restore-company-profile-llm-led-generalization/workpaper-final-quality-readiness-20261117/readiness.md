# 6.DB：四阶段主体合同统一，十二份源复核输入已核齐

这次改的是抽取/初核/修订/最终再核的同一语义契约。之前抽取和初核明确issuer须为母公司，修订/最终核验只泛称“正确主体”；五种枚举在实际字段Schema中也没有含义说明。现在四阶段共享同一说明，原fact和metric字段Schema给出相同定义：集团、明确母公司、具名子公司、业务/产品/行业分项、不明确。metric按自身来源对象判定，不能继承整条fact或合并表的集团主体；金额可定位、相近或加总都不能证明对象等同。

这不是程序识别原文主体的规则，也不自动改任何旧标签。prompt v19/review v10标识新请求；源绑定v5、章节指引v4、JSON字段/存储/完成门槛不变。原稿和DA使用限制保留，供应链仍可选、公司特色仍允许自主选择。

## 本轮已验证与未验证

实际CommonGateway/LlmClient仅替外传输，原stage runtime/writer/query/export/新实例走完非空五阶段。测试核实际发出的抽取、初核、修订、最终核验系统消息；有fact响应的抽取/修订Schema也核到字段说明。review/final的返回是findings/checks，没有伪造不存在的fact响应Schema。新解释未改变旧文件读回和序列化。

相关四文件167项通过35.96秒，排除3个未改的TaskService长测。另两文件兼容检查64项通过，4项仅因仍断言旧v18而失败；按本卡明确的v19更新后，4项单独复验5.37秒通过，其他64项无变更不再重跑。初次新增Schema断言曾错误要求review/final响应也有WorkpaperFact，已改为实际返回事实的两阶段；不是放宽业务结果。命令见下文。

对现有十二份真实稿逐一验证：旧JSON完整形状和序列化字节不变，原query/hash未动；移除新复核输入的旧quality_review/use_review元数据，原事实/数值/作者不改；由既有seed_material按正式PDF版本及所选原页坐标重新核源。共371对象全部原样保留，201个报告物理页仅用于源绑定。每份沿原review_workpaper到首个新review请求捕获后主动停止，没有提供模型响应、生成新回执或执行真实发送。原文件及准备输入的hash、章节/页与首次review内容hash见[冻结计划](review-plan.json)。

初核内容payload为48,370—122,423字节，仅是content的计量，不冒充完整wire预留/容量或真实token；执行前仍由原发送接点做完整准入。该核查是原源复核owner的请求边界，不是十二次TaskService整链成功；原TaskService接seed、磁盘绑定和writer读回的有效直接证明复用，不重复初始化十二个研究数据库。

| 风险 | 程序已直接验证的范围 | 仍须真实模型/独立语义核查 |
| --- | --- | --- |
| 主营/产品 | 必填结构、局部失败保独立正文、原writer读回 | 所有主要业务的主次及实质覆盖；不能用字段非空代替 |
| 收入列/空位/单位 | 改数、量级、明确单位冲突、空栏造零、续行与原文限定的正反检查 | 数字恰在别列/别科目时的归属判断 |
| 主体/期间 | 四阶段同一主体定义；旧内容/hash绑定、错版/陈旧审核拒绝 | 集团/分项/子公司含义和事件发生时点 |
| 购销方向 | 合法源ID/原页与金额限制，错误对象局部退出 | 采购/自产/贸易/销售方向，不设词表语义替身 |
| 关联/租赁 | 核验目标/证据/修订身份一致，失败不恢复坏项 | 出租收入、租入成本、融资还款及关联限定 |
| 金融披露 | 数值保真、期间/来源记录、源参考不外放为标准化事实 | 净收入/总业务收入/余额/AUM及集团和金融子公司区分 |

此表不是六类正式负例全部通过。原133条件及六类源风险至少两家公司对应均保留；真实复核和独立核源才可判断语义效果，不以167项工程测试或本次371个对象计数代替正式质量。S1 0/12、S2及生产未放行，未核范围不默认准确。

## 最短下一步：只接统一版本的源复核

不重付已成功的选章和抽取。6.DC拟复用当前十二份有源真实稿，各一次全新的读源初核→必要一次合并修订→最终再核，全部使用相同v19/v10和现有原TaskService/公共网关/唯一writer。旧初核、旧修订、人工133答案和DA独立处置不送模型；旧稿及旧失败保审计，不把本次后继说成新的完整处理或盲测首发。模型新结论与已存在的独立限制冲突时保旧限制，须有实际原文支持才解除，不能仅凭新模型completed放行。

只针对原十二家2025年报（名单/版本见review-plan.json），Scorpio/Grok grok-4.7 high初核/再核，官方DeepSeek至多一次必要合并修订；仅发送所选公开章节和公开派生稿。每家1800秒/500000保守账本/6物理含最多3恢复（每逻辑1次），最多3逻辑；并发4，整组7200业务秒/6000000账本/72物理。单物理尝试600秒、逻辑620秒且受公司deadline约束，载荷1000000字节/输出12000，沿既有硬限制；不新增请求种类。费用无价unknown、有价每家10元，不承诺总人民币上限。

同核心根因连续两家失败即停止未启动派发，在途按原限收尾；核心污染无法隔离则停止相关发送。无追加窗口/新公司/充值/共享配置修改/生产启用；不为细小比例、所有意见全绿或供应链尾项再申请补窗。取得一次明确有界批准后整卡执行，不逐字段/公司请批，结束给当前内容、实际错误/遗漏、独立原文判定和原账/新增账对照，并按原验收标准作后继质量结论，不自动计S1首发。

当前status=not_authorized_for_new_calls，真实0/12；本地准备不是付费批准，旧各窗口都不恢复。实现/源输入已就绪，收到批准后只需获准联网环境的配置/源/hash/DNS/TLS核对和最终冻结，不再增加准备功能。

## 验证命令、Review与清理

均经scripts/quote_dev_env.sh和Quote Python：

```bash
python -m pytest -q --confcutdir=tests/unit/test_research tests/unit/test_research/test_company_profile_workpaper_features.py tests/unit/test_research/test_company_profile_workpaper_delivery.py tests/unit/test_research/test_company_profile_workpaper_source_cells.py tests/unit/test_research/test_company_profile_workpaper_review.py -k 'not task_service'
python -m pytest -q --confcutdir=tests/unit/test_research tests/unit/test_research/test_company_profile_workpaper.py tests/unit/test_research/test_company_profile_workpaper_format_consumption.py -m 'not integration' -k 'not task_service'
python -m pytest -q --confcutdir=tests/unit/test_research tests/unit/test_research/test_company_profile_workpaper.py::test_workpaper_contract_passes_real_gateway_schema_with_only_transport_replaced
```

第二条首次为64通过/4旧版本断言失败，第三条是修正后的4项，不将第二条原记录写成全通过。相关版本/Schema断言调整有本卡依据，非改预期掩盖业务失败。12份准备由临时操作适配器执行，完整快照保留用于输入/hash复现；输出目录已存在时拒绝覆盖。

Review只处理A类合同漂移和直接回归，B类写作/尾项后置，C类既有配置/骨架和并行不动。开工DA/DB本人无依赖残留0，释放0；测试根finally清而未计量，不虚加空间。临时准备脚本6868字节在快照/hash核齐后删除；未新建研究库、复制PDF或保存十二套重复正文。权威计划/必要验证只一套，旧源、缓存、结果、attempt和既有改动保护。OpenSpec严格校验、本人差异和原hash收尾，不宣称本轮新模型/语义质量或生产成功。
