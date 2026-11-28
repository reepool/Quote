# 6.FY 机械审核输入方法（模型前冻结）

方法身份：`6FY-20261128.audit-bindings.v1`；种子：`6FY-20261128`。
只索引、绑定、抽样、计数；不判断语义、不采用B自评分、不调用模型/HTTP/ASGI，不改app、原件、主库或Git。
正式名单唯一沿6.FX `workpaper-automatic-uncertainty-20261128/b2-final20-metadata.json` 的20个candidates，四类各5；本卡v22/v16，固定20分母。当前卡、验收§4.2/4.3/5、A/B v3为合同。旧四家只作离线shape证明，不进入本卡成绩。

2026-11-28用户接续澄清（原冻结文件保留，由 `b-audit-method-clarification.json` 绑定本次方法/脚本增量）：每公司核心固定3问，每阶段全组核心分母固定60；`expected_source_answer` 是三问的实质业务参考，段落里的每个数字/小项不另立hard条件。没有绑定来源的判定不计正确；核心与实际补充仍分开，补充每公司目标20、不足全查。抽样种子、排序及随机方法不变。

当前保持source-mapping standby。B的operator/plan在本目录，原phases将交各公司 `evidence/first-api.json`、`evidence/automatic-final-api.json` 及原raw roots；运行B提供本目录 `b-audit-inputs.json` 路径清单后才执行正式映射。不读计划当结果，不重跑旧proof或预演新20。

独审分工：Main A审核制造/服务10家，Franklin审核金融/多板块10家；Main A亲审全部20家核心及重大金额疑点。A侧判定不来自B自评分。标签粒度、保真限用、无法统一标准化与真实业务错误分开；不因meta不统一自动生成business-error。`assessment_kind`只搬运A的明确分类，机械助手不推断该分类；标准化可计算覆盖另列。

## 给主A、独审与运行B的路径约定

本目录仅机械助手拥有 `b-audit-bindings.py`、`b-audit-method.md`、`b-audit-method-freeze.json`、`b-audit-shape-proof.json`、`b-audit-binding-<证券代码>.json`、`b-audit-counts.json`；不占用B的operator/result/first-api等文件名。
运行B继续保存自己的原始文件，向脚本传一个既有/自有指针清单 `--inputs <B清单绝对路径>`；不用搬运或复制payload/数据库。路径均可为仓库相对路径或绝对路径：

```json
{"companies":[{"instrument_id":"920022.BJ","state":"not_started",
  "first_api":"<正式first-api.json>",
  "automatic_final_api":"<正式automatic-final-api.json>",
  "human_safe_api":"<如有；没有就省略>",
  "records":["<实际原writer record.json，首读/自动/人工分别保留>"],
  "wires":["<原始request/response文件>"],
  "budget":"<原budget.json>",
  "native_pages":"<可选：page整数转字符串 -> 原生页文本的JSON>",
  "source_reference":"<A在该公司首次发送前冻结的独立参考>"}]}
```

`first_api`/`automatic_final_api`是原reader的单公司profile（或显式包含该profile的profiles批量响应），不是重建结果。`records`必须列实际保留的原始记录，不能去掉UseReview重造自动稿。没有阶段实物就缺项，不用当前人工稿替代；failed/not_started如实保留。wires/budget只索引hash及与原record的调用绑定，不把配置首选模型冒充实际作者。

A/独审可用 `--review-inputs <A自有JSON>` 提供 `companies[].phases.{first,automatic_final,human_safe}`；每阶段的 `objects` 用 reading_work_id、fact_id、consumer_fact_sha256精确定位，`groups`可列 core/important/suspect，`semantic_duplicate_of`必须是同阶段另一对象的上述三项身份，明确 `status`（supported/error/not_assessed/unclear）。A判定应含原文依据；提供完整冻结 `core_conditions`（condition_id、view、supported布尔或null）、`source_reference_sha256`及 `usable`布尔或null。A完成该阶段核心/重要/疑点与语义去重标注后显式 `selection_complete=true`，否则抽样结果仍标pending。核心条件来自源先行合同，不能以输出反定分母；三基础问的支持对象由A选择，不按element存在自动认定。A的疑点和同类扩查对象可随审核追加，原方法/随机规则不变。

## 固定采样

1. 每公司每阶段独立建实际可见fact总体，机械身份为报告/work/fact/consumer hash；同一API重复机械身份只索引一次。文本、指标、证据/页码/章节保留原JSON指针和hash，不拆指标凑对象数。
2. A标注的三基础问支持对象、全部重要数字及全部已发现疑点全审；未完成A标注时selection标pending，不能声称随机部分已最终冻结。为减少漏审，所有实际带metrics对象及投影限用/缺口对象先列为保守的全查候选，是否重要/疑点由A裁定；不据字段判正确或语义同一。
3. 其余实际唯一补充每公司最多20，不足全查。不跨公司填补400，不要求模型增加条数。按实际结构分三层：带metrics（Measurement/收入候选）、无metrics且为Activity的显式对象、其他；无法识别的类型进other，保原element，不由正则猜语义。层内按 `sha256(UTF8(seed + ':' + instrument + ':' + work_id + ':' + fact_id))` 升序，再以完整机械身份排序破同hash；按 measurement/activity/other轮转选满最多20，各层不足跳过。先去A明确语义重复，再排核心/重要/疑点；三组及随机补充按机械身份去重合计。
4. 不把相同字段、数值或相似文字视为语义重复。只有A精确绑定的同阶段duplicate决定可扣除；跨阶段仅作同报告/同fact编号差异索引，不据编号认定语义同一或复用评分。两个阶段分别抽样、分别分母。

## 三层精确绑定与源索引

首读、自动源核后、人工safe分别绑定实际API快照 -> reading_work_id -> 原writer record -> 同报告原fact及官方workpaper_fact_hash -> consumer hash -> metric_source_indices。每个可见fact提供完整文本与metrics指针/内容，证据引文、section_path、physical_pages/source_spans指向原record；原PDF/native权威路径给审核人。复用6.FS的facts/entry_content/hash和原project_workpaper_delivery，不复制reader或语义循环。

坐标“匹配”仅指原native页在[start:end]与quote逐字相等；native未交、无span、越界、quote不同分别计，不能据PDF文件hash、数字相等或引用字段存在宣称坐标已匹配，更不能宣称语义有据。来源document_version与报告hash单独比对。PDF由审核人直接读；本助手不替代原文真值。

实际作者/选章/初核/修订/终核归因取原generation、quality_review.calls及完整保存receipt的request_id/response hash/raw_content hash；wires列路径/hash，无法精确连接则unmatched。首读不得带quality_review/UseReview，自动层不得含人工决定/继承人工限制；存在时明示污染，自动成绩不可据此宣布达标。缺记录、hash/原metric位置不符、无法绑定均列问题，不猜值、不清限制。

## 机械统计（仅消费A独立判定）

逐公司/阶段，以及四主层、交易所、行业分别报实际/唯一/核心/补充/疑点/实审/未评、准确率、核心召回、可用率、失败/未启动。准确率分母是有独立结论的唯一实审对象（supported+error）；unclear/not_assessed/缺判单列，绝不计正确；同时报告被选对象中待核数，未完成审核不宣布组达标。数字标准化非null覆盖另报，不等于准确率或保真可用。

核心召回按每家公司固定3个源合同基础问计算，全组分母固定60；传入不是3项时标形状不符/未评，不变更分母，也不按参考正文拆项。组宏平均固定20，失败/未交付=0，已交付但参考/判定未完成=null，报告已知下界及未评数；同时报微平均及各view。可用率仅计A明确usable=true，分母始终20；首读、自动、人工safe分开。正式门槛18/20、19/20、95%、99%及分层/补救成本由主A裁决，本助手不生成通过/失败语义结论。

## 执行与收尾

所有命令先 `source scripts/quote_dev_env.sh`，Python固定 `/home/python/miniconda3/envs/Quote/bin/python`。必要临时产物仅 `data/QuoteBak/company-profile-6fy-audit-*` 本人专属根；不复制主库。首次先冻结method及合同/复用脚本hash；`shape-proof`只读旧四家原record并用原投影确认shape，不执行旧完整suite。收到正式实物后逐公司 `map --inputs ... --instrument ...` 立即交独审，其他公司不等待；`counts`始终补齐固定20状态。机械结果为每股一套权威索引，重跑只更新本人同名派生结果，不重写B原件/A判定。无依赖临时验证内容在权威证明保存后清除，清理量写在现有机械回执。

```bash
source scripts/quote_dev_env.sh
/home/python/miniconda3/envs/Quote/bin/python openspec/changes/restore-company-profile-llm-led-generalization/workpaper-prospective20-closeout-20261128/b-audit-bindings.py map --inputs <B正式指针清单> --instrument <已完成证券> --review-inputs <可选A独审文件>
/home/python/miniconda3/envs/Quote/bin/python openspec/changes/restore-company-profile-llm-led-generalization/workpaper-prospective20-closeout-20261128/b-audit-bindings.py counts
```

准备阶段的reader在旧四家独立目录读回0家；该目录是缺首读父链的源核后继，不能据空结果声称API验证。shape证明使用其非空原writer→原project_workpaper_delivery，且清楚注明内存mapper夹具不是真实API。原App/TaskService既有证明继续有效，本助手不重跑。每次导入原模块产生的日志在本人专属临时根关闭句柄后清除（QuoteBak为NFS）。
