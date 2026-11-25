# 6.FI B交付：表格断行金额字面源绑定

状态：局部实现、直接回归及离线原候选验证完成，源码冻结；A末端独审已确认无新增阻塞，B交接完成。没有真实新终核或候选采纳；不改旧成绩、不启生产。按用户指令不stage、commit、push，Git收口由A负责。

## 完成内容

- 保留前B本卡补丁，在原`_wrapped_table_cells`中识别明确货币表头下、相邻行至少两列同类且各自唯一的金额续片：小数尾数及最后千位分组。完整百分比列保持独立，不拼入金额，不调大坐标容差、不flatten全文、不引入公司或金额规则。
- 有界识别另拒绝数据区断开的新表、尾片旁单位/说明等非数字内容。沿原位置排除已恢复前缀，避免半截金额成为新的完整值。
- 修复A首次独立挑战发现的阻塞：`source-0007`无本页货币表头，不能确认wrapped，原ordinary却把`2,095,400,31`当完整数。新ordinary要求完整千位分组语法；`legacy_fragments=True`保旧读取。其他位置的同值完整数字仍合法，不按数值全局屏蔽。调用方仅为`_check_fact_source`及`_check_explicit_money_unit`。
- 新普通处理身份使用`company_workpaper_source_literals.column-wrapped-amounts.v8`；旧身份/原存盘不迁移。
- 写集仅三个源码/测试文件：[workpaper.py](../../../../research/company_profile/workpaper.py)、[workpaper_llm.py](../../../../research/company_profile/workpaper_llm.py)、[source_cells测试](../../../../tests/unit/test_research/test_company_profile_workpaper_source_cells.py)。当前三文件Git diff包含已明确归属本卡的前B增量；重复补丁副本按A末端要求删除。

## 真实保存候选：仅source-ready

输入：`data/research/company_profile_automation_probe_20261124/company_profile_common_core.v1/bp-work-721e6851edbaa279f67917e5.json`，SHA256仍为`e2e19993aca14fd6a7dd977600ad7c12b042fac190b601c4b8452812848df67c`。

| 原候选 | 正确金额/元 | 原保存状态 | 当前strict source check | 实际采纳 |
| --- | --- | --- | --- | --- |
| review-add:R4 | 3119716159.11 | repair_rejected | 通过 | 无 |
| review-add:R5 | 2095400319.12 | repair_rejected | 通过 | 无 |

两候选均实际调用`_check_fact_source(check_units=True)`；原quote、offset、raw、完整对象及旧reject不改。相应半截值`3119716159.1`和`209540031`在新绑定拒绝，旧读取仍兼容。

原最终复核目标只有`F01/F10/F11/review-add:R6`，不含R4/R5；检查source-ready不会创建final或把它们放入元素/API。回执逐条记录候选hash、原引文hash/坐标、命中位置和半截反例，见[b-evidence.json](b-evidence.json)。A原PDF期望和首次唯一挑战失败仍分别保留于`a-expectations.json`及`a-first-challenge-failure.json`；A后继19项全匹配见`a-challenges.json`，不是B独立语义成绩。

## 验证方式与实际结果

最终：**175 passed / 6 warnings / 59.02秒**，其中source_cells完整157项，必要原review18项。机器回执[b-tests.xml](b-tests.xml)，运行日志[b-tests.log](b-tests.log)。源码freeze hash见[b-evidence.json](b-evidence.json)。`git diff --check`通过。

受控原入口覆盖两种新断行和旧标点恢复，共3种布局×成功、错列、错主体、中断、错final目标5种结果。每种结果均经过原runtime/review/writer，断言query、export、新read实例一致；只有自身匹配final接受才交修订，其他结果不复活错误事实，未受影响主营保留。mock明确为`controlled_fixture`，不宣称其判断等于真实模型语义质量。

执行命令（从仓库根，所有输出均在本卡证据目录或本人QuoteBak根）：

```bash
source scripts/quote_dev_env.sh
/home/python/miniconda3/envs/Quote/bin/python -m pytest \
  tests/unit/test_research/test_company_profile_workpaper_source_cells.py \
  tests/unit/test_research/test_company_profile_workpaper_review.py::test_source_column_repair_preserves_unrelated_business_and_original \
  tests/unit/test_research/test_company_profile_workpaper_review.py::test_invalid_or_unconfirmed_repair_never_revives_bad_fact \
  tests/unit/test_research/test_company_profile_workpaper_review.py::test_retained_stages_reject_changed_candidate_or_response_hash_before_network \
  tests/unit/test_research/test_company_profile_workpaper_review.py::test_explicit_schema_revalidation_preserves_old_rejected_receipt \
  tests/unit/test_research/test_company_profile_workpaper_review.py::test_revalidated_repair_still_fails_before_final_send_for_invalid_history \
  tests/unit/test_research/test_company_profile_workpaper_review.py::test_runtime_seed_review_gateway_writer_and_three_reads \
  -q --color=no --tb=short \
  --basetemp=data/QuoteBak/b-6fi-wrapped-source-amounts-20261125/pytest-final \
  -o cache_dir=data/QuoteBak/b-6fi-wrapped-source-amounts-20261125/cache \
  --junitxml=openspec/changes/restore-company-profile-llm-led-generalization/workpaper-wrapped-source-amounts-20261125/b-tests.xml
git diff --check -- research/company_profile/workpaper.py research/company_profile/workpaper_llm.py tests/unit/test_research/test_company_profile_workpaper_source_cells.py
```

这些重跑命令会产生/更新本卡测试回执，不是只读核验冻结证据；不得覆盖A材料。一次性`b-verify.py`曾在禁止socket联网下核候选、原件hash及final目标，仅写本卡回执，不写研究根；回执验证后按A要求删除，生成器hash及删除理由保在`b-evidence.json`。两个真实候选、半截值和不采纳行为仍可由source_cells中的真实保存输入用例复现，不留下常设审计脚本。

开发失败轨迹：首次新增直接测试6失败/136通过（包括新fixture未采用原货币列标题造成的拒绝，以及新表/残片旁文字边界）；修正fixture为原披露标题并收紧识别后，局部复验1失败/39通过，剩余为数据区空行后的新表边界，已修复。最终175项全过。A另发现的成本半截跨span误放已修复并加入真实R5和无表头ordinary回归；不改A首次失败回执。证据生成脚本曾因既有删除文件和A并行更新管理文档而中断，仅修证据读取/报告，不改业务文件或预期。

## Review、保护与注意事项

- A类阻塞已修：跨span不完整分组误放；新表断开及续片旁文字可能误拼。直接负例、旧`FINANCIAL_TABLE`整数残片负例、同值其他完整位置、旧读取兼容均通过。当前未发现本卡未解决的阻塞；A最终审核另列。
- 未开展风格/架构或历史问题治理。六条测试警告保留日志，不扩修无关依赖。
- 复用A的304文件清单核hash，全部不变，涵盖原20与6.FH三稿保护对象；没有更新这些API/研究root。49既有脏内容中44项hash仍同；另5项A管理的验收/交付/当前change管理文档在并行期间变化，逐路径如实记于回执，B未编辑这些文档。既有删除保持删除，未暂存任何文件。
- A末端独立以新read实例核原20全量API（6.781秒）及6.FH三稿API（1.847秒），均与已存完整JSON相等；304原文件不变，三源码/测试hash与B冻结回执相同。见`a-read-compatibility.json`。A19挑战最终全匹配，未新增阻塞；B未重复此全量API核查。
- 字面命中不证明经济列、主体或期间。`source-0007`无前置单位的续页仍不独立恢复，成本依赖`source-0006`的明确表头；孤立、歧义、跨表头/span/页、不同续片形态及其他未支持版式保限制。不为覆盖所有版式扩大为PDF平台。
- 原始研究root只读，新增网络/真实模型/供应商usage/实际终核/真实候选写入均为0；受控fixture消费与真实质量分列。正式运行0次，活跃开发约25分钟（估计），没有交接等待或新增付费窗口。
- 实际未运行：全仓/历史回归、未选中的review/source_recovery测试、B独立原PDF重读、任何新联网/真实模型/两候选终核、新公司批次及生产。原PDF及完整API兼容由A独立核查，不冒记为B执行；未stage/commit/push。

## 清理与交接

本人临时根：`/home/python/Quote/data/QuoteBak/b-6fi-wrapped-source-amounts-20261125`；开工不存在，未清理未知或并行目录。必要一套持久证据仅为本目录的`b-*`及本文件，不保留整套测试库/过程副本。

已删除本人临时根并确认不存在：**363个常规文件、4,157,033字节**（按文件长度求和，不含目录元数据）。其中含三份开工快照、过程日志、隔离测试库及中间输出，均可重建。另删除已无保留必要的一次性生成器`b-verify.py`（7,906字节）和重复`b-patch.diff`（20,962字节），共2文件/28,868字节；本次合计清理365文件/4,185,901字节。不触碰A的doc-before根、A文件或任何未知/并行临时根。

最终B持久证据仅`b-evidence.json`、`b-tests.xml`、`b-tests.log`及本result；首次失败摘要已在上文保留，A首次失败原件保持。源码写集已释放给A，Git收口由A执行；本轮结束，不新增真实调用或下一任务。

A收口：有限Review确认当前工程范围无剩余阻塞，19独立挑战、两真实候选及原20+3完整API兼容证明与B最终三个源码/test hash一致。OpenSpec strict和diff检查通过；A本人临时根已删除并确认不存在，共11文件、3,077,581逻辑bytes。A/B总计清理376文件、7,263,482逻辑bytes，原PDF/cache/304业务文件及49既有脏内容保留；必要证据只有本目录一套，不保留一次性脚本或重复补丁。A按开工快照仅提交可隔离增量并普通推送，旧拒绝/实际终核/采纳状态不改；本轮只交工程就绪，未进行新的供应商验证。
