# B1：6.FN 已结束及零调用修复交接

最新A Review P1已复现并修复：v2会计括号负提案转null后曾绕过明确源表单位校验（源元/回万元错误放行）。现仅对v2程序标记的raw/null沿原字面位置核源表/同行明确单位，不推断数值/符号；源元/回万元拒，源万元/回万元保，旧v1/no-policy及原保存反序列化路径保持。新增8个实际normalize→bind及旧身份用例修前3失败/5通过，修后8全过；当前去重82项全过（30.93秒，54旧format/repair+26新+2旧gateway）。原包再核交行F08及人寿14候选仍源绑定待核、无旧终核继承；363原件/首发freeze hash不变。仅本问题的workpaper.py/新增测试追加修正、正确性ruff/diff-check及有限Review通过，0新增模型/临时脚本/数据库，清理根仍不存在。新源码hash及P1前后证据见b-local-fix.json.A_P1_followup；以下74全过为P1之前记录，当前最终结果以82为准。

最终状态：session8769已exit0、20入口全终态、18流程完成/2零请求metadata入口失败。真实组84物理/0恢复、供应商total和保守账本均2,822,082、组4355.237096秒、费用unknown；首读最高17/20和自动383/391=97.954%、7未评/补充331不足400，A判formal not_passed，后继/新窗/paid为0。本批失败不由可选缺项造成，不因本地修复或A安全保护回算通过。

全部真实发送结束后先核134源码与首发freeze一致，并保存363 RAW/checkpoint文件聚合hash，再实施两类授权本地修复：新显式v2保raw/null程序说明及与明确负decimal一致的完整会计括号；原v1/no-policy和严格WorkpaperMetric行为保留，新默认identity指向v2。修正版operator另存`acceptance-operator-corrected.py`，仅本地helper接收精确股票/年度/期间/公告/公开时间/PDFhash同源的双asset ID，真正换文档/公告/时点/期间仍拒；原dd7f源码与b-plan/a-send/reference/method不变。

本地真实回执普通bind源验证保回交行F08 1候选/8metric位置、人寿14候选/38位置；quality_review/use_review均null，仅源绑定待核未发布，旧调用/拒绝不改。原reader18完整API与自动snapshot逐股相等，共406可读/538指标；未改A安全根（其独立400/533与8对象保护沿A报告），未改旧21。

最终验证去重74项全过（19.56秒、6 warning、0失败）：54 format/repair＋18新v2＋2旧gateway；3过时默认v1身份断言经A单独授权最窄改为新默认v2，业务/来源/原值/拒绝/修订断言全部保留。早期51过3失败记录保在`b-local-fix.json`，不冒称当时全绿。正确性ruff、diff-check和有限Review通过，无新增阻塞性行为问题。完整精确命令/源码hash/回执及资源清单见[b-local-fix.json](b-local-fix.json)，由A统一Review/文档/Git，B未提交推送。

清理：本人QuoteBak根已删除72文件26,047,385字节，含20运行工作库；连前段累计77文件30,861,992逻辑字节。权威原/修正版operator及必要实际wire/attempt/失败/首稿/自动稿363原件保一套；未删正式来源/有效解析缓存、原21、A安全根、未知或并行产物。以下为本轮历史过程，不是当前运行状态。

01:20 A已明确不可逆不通过：光大/国信各原入口seed204零发送失败，另人寿首稿收入基础问未成立，首读最高17/20低于18；`a-stop.json`已写`stop_sends=false`。20入口全已启动，最后4家只在原预算/原流程收尾，不追加任何后继、新窗或paid rerun。保首次/自动稿/原wire供A现有审核，不为400扩素材/无意义全审；源码/driver继续冻结，零模型本地修复须待A后续明确授权，不能改变本批失败。

国信002736只读同源身份对照：原29.681226秒/0物理，末帧同`seed_official_frontier:204`；冻结ID `asset_ccea71fb3a03e7cb62dd95338db20c29`，as-of ID `asset_8cfbc5e555a50a1567377eb1e2c70252`，同股票/公告1225120973/公开时点2026-04-17T16:00:00+00:00/hash `69e640cb99857943fd54921ea0b5f0ac0df00aae288aea93dce39041aa09beb5`。同结构化根因`bootstrap:effective_asset_id_mismatch_same_document`；原result空key不改。与光大之间有正常完成公司，不属于相邻连续失败，但A已依据首读目标独立止损。

2026-11-26 00:21:35已按A_SEND及`a-send-freeze.json`从工具`require_escalated`获准联网环境启动冻结driver `dd7f6ed2609a82047f4be5c26249ae0950801699375f93412142c957ce742fe2`，不热修。00:24首四汇川/招商轮船/交行/中集均完成本股官方metadata seed和选章，正文抽取中，每股物理2笔、无终态失败；组checkpoint的`group-progress.json`与逐股`execution/budget.json`是实时状态，最终结果未出。下文0请求等为A_SEND前准备历史，不能当作当前实发数；准备计划/源码/hash不改，运行结果同卡后补。

00:38只读诊断光大601788的首次失败：35.179340秒、0物理；原`execution/result.json`最后一帧为冻结operator `seed_official_frontier:204`，该行固定消息为`official metadata differs from frozen stock/report`，原完整traceback未保存，不能补造。只读当前同cutoff元数据确认：冻结`asset_9ef66d1c8169474f0d374e4f2bb8dc47`，as-of返回`asset_511fa14eb9498a0b9fea0f250cb66bba`；股票/2025期/公告1225035238/公开时间/文档`aa1f35cc252a1532cd7bf0d0a2f4938c28099b09dc02b4ad64b35e5fe37b3f2d`均相同，仅ID不同。人工诊断结构化根因`bootstrap:effective_asset_id_mismatch_same_document`，用于监测后续同因，不改原空execution_root_key/原失败或热修driver。首四源核均已正常结束，三家with_open按合同接受；最终是否基础可用仍由A独审。

差异来源仅作只读定位：`research/announcement_assets/service.py:1052`当前投影在相同content_hash时保留旧asset_id（容许canonical镜像变化）；`:1356`历史/as-of投影按当时attachment/version重新stable_id。光大当前行version=`ver_772bfb18d6bc4466b50bd07a4fa6abb1`、attachment=`att_f14df29a00c9c05f011249526640bd7c`，同文当前稳定ID与as-of重算ID因此不同；本卡不改服务/seed、不重开光大。后续在途seed继续成功，未证明全体余样本不可运行。

2026-11-26 A预发送审核指出的三处已局部修复并直接证明，供应商请求0：正常接受`completed_with_open_findings`；空工作库通过现有`register_business_profile_shared_annual_report_asset`只登记本股冻结官方年报元数据；连续根因使用原`WorkpaperReviewExecutionFailure`/provider trace的结构化kind/code/profile/channel，不再hash包含股票/request ID的last_error。实际root错误原文仍留诊断文件，不参与同因比较。精确新driver/hash以重新prepare后的`b-plan.json`为准。

本次直接验证全部通过：原两阶段同budget/client/owner，受控3发送后正常`completed_with_open_findings`且两事实保留；真实官方汇川元数据在全新DB中使原repository预览eligible 0→1，只有1条frontier且0语义work items，未读业务正文/旧语义；两股票不同request同结构化deadline码根因一致并停未启动。证据见`b-plan.json.A_pre_send_fix_verification`。真实20root/checkpoint和首次名额仍未用，A ready20仅核hash不读答案；继续等待精确A实发freeze。

本轮模型请求仍0。固定20名单/源版本不变；源码、共享配置、原21研究root及既有dirty未修改，未commit/push。真实20运行root和checkpoint尚未创建。

原owner接线已完成：`execute_published_task(first_read)`→原ReadService精确计划→`execute_published_task(review_batch)`，每公司只构造一个`LlmRunBudget`、一个client/provider，同一实例与绝对deadline贯通两阶段。不调用会另建预算的`execute_configured_task`，不复制语义循环。原provider固定该股/该报告，全部发送使用现有真实transport，只有受控离线核验替换传输。

20个样本按组内位置1—5、制造/服务/金融/多板块轮转；先4家公司各一类，并发最多4。每股A参考hash就绪后才能派发，只核文件hash、不载参考答案；无需等其他公司的模型结果。独立参考侧车可逐股增加，名单、方法、代码/model/source和累计预算则必须全组首调前冻结。共享flat raw20输出，checkpoint按股分开，工作DB只本人QuoteBak。

## 直接验证及网络

- 两个用例均由真实原入口、队列、writer/read执行，仅源页和transport为明确受控fixture。成功：首读2发送/100受控usage→终核3发送/150，两个非空事实保留、review完成、预算/client同实例及deadline不变。预算上限2发送：复核准入拒绝，不增发送、原2事实仍可读；query/plan均0新增发送。不是模型语义/准确率成绩。
- 批量守卫直接检查通过：相同具体执行根因连续两家停未启动，正常在途仍按原预算发送；认证硬停阻止后续发送；单凭无内容不能冒充同一核心根因。AST核一处budget/client构造、两原owner同一base，未调用configured-entry。
- 获准`require_escalated`环境中，官方`api.deepseek.com`与既有`scorpio.reepool.com` DNS及证书验证TLSv1.3均通过，精确profile/model/host和密钥存在性核对通过；无HTTP模型/付费探针，无密钥正文输出。
- operator/离线脚本正确性ruff通过，语法检查通过；本轮未改应用源码，既有工程验证不重复全跑。早期准备的PDF工具接口本地错误没有模型/正式入口消耗。

详情及全部精确hash见[b-plan.json](b-plan.json)的`prepared_driver`、`A_pre_send_fix_verification`、`network_preflight`。新实际两阶段派发函数hash为`1bff7636a44d258238cb0e54ec0f62a786ba72c1839ba53e3b1d9adf39a59e94`；原普通完成/资源不足两项可复用未变路径，本轮另直接核验新增open-findings完成路径。源码/profile未变，旧获准联网DNS/TLS预检复用。

## A可执行冻结格式

直接以`b-plan.json`的`A_freeze_format`为JSON基底，填入A的真实`approval_reference`，保存A自己的freeze文件；不要改名单/限额/hash。`ready_reference_sidecar_format`定义`a-ready-references.json`，先列首四不可变参考文件的绝对路径/hash，余16各自准备好再增加。`runtime_stop_sidecar_format`定义A动态止损；`stop_sends=false`只停派发，认证/不可隔离污染才封后续发送。

实发命令必须在工具获准联网环境：

```bash
source /home/python/Quote/scripts/quote_dev_env.sh
/home/python/miniconda3/envs/Quote/bin/python /home/python/Quote/data/QuoteBak/b1-6fn-prepare-o8fwFycv/acceptance_operator.py run --freeze A_FREEZE_FILE
```

当前driver SHA256：`dd7f6ed2609a82047f4be5c26249ae0950801699375f93412142c957ce742fe2`。134源码文件聚合`2e2a979bc7a78307183c250f8d973dc8bfaf40bca0ab293836bc15954034a660`；profiles聚合`00ff420a14958ca371176fc03847320db3be7fae7935b6463cae90e9783076ad`。实际operator源码已原字节保存[acceptance-operator.py](acceptance-operator.py)，双路径sha256一致；运行仍用本人QuoteBak版本，收尾只保此权威源码。卡/方法/源版本和processing identity hash亦已冻结。任何A freeze或运行源/config不匹配在构造client前拒绝；重复group-started不重开预算。没有明确A实发freeze，不能运行此命令。

## 清理与保留

6.FM A/B1/B2三本人根均已核不存在。前两测试DB已删2文件2,424,832字节，本次open-findings/bootstrap两DB又删2文件2,375,680字节；首阶段另删编译缓存1文件14,095字节。小量受控JSON/脚本待同卡运行收尾删除，必要实际wire/attempt/失败一套保留。最终只保权威持久operator源码/hash，临时运行副本和无用产物结束清理。正式PDF/有效解析缓存、旧21和历史失败及并行目录未动。
