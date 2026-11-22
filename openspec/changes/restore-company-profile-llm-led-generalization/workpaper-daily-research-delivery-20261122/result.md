# 6.EW 日常研究交付

已沿原日常CLI完成圆通保存真实稿的初核→一次合并修订→终核，并以原writer交付当前20股独立研究根。未重选章/重抽、未增加公司或付费精读；没有业务代码变更、共享配置修改、真实通知或生产启用。基线d94afa70/master，暂存空；B未暂存/提交/推送，旧脏内容及A管理文档保护。

## 当前使用

唯一current用法：`docs/development/company_profile_task_usage.md`。完整项目解释器为`/home/python/miniconda3/envs/Quote/bin/python`，系统python3缺pandas不作为业务bug处理。

- 研究根：`/home/python/Quote/data/research/company_profile_daily_release_20261122`。
- 当前JSON：`/home/python/Quote/data/research/company_profile_daily_release_20261122/exports/annual_profiles.json`。
- 当前人读稿：`/home/python/Quote/data/research/company_profile_daily_release_20261122/exports/annual_profiles.md`。
- 原writer/ReadService发布与导出；40个writer记录保留20股阅读归属，旧19/20股研究根、原首次/预算/失败、6.EV planned文件未改。没有手拼事实或切换默认API根。

可直接只读消费：

```bash
source /home/python/Quote/scripts/quote_dev_env.sh
cd /home/python/Quote
/home/python/miniconda3/envs/Quote/bin/python - <<'PY'
from research.company_profile.reads import CompanyProfileReadService
reader = CompanyProfileReadService('/home/python/Quote/data/research/company_profile_daily_release_20261122')
stocks = [p['instrument_id'] for p in reader.research_profile_catalog()['profiles']]
batch = reader.research_profiles(stocks)
profile = reader.research_profile('600233.SH')
print(batch['delivered'], profile['readings'][-1]['review_status'])
print(profile['information_gaps'])
PY
```

原HTTP批量`POST /api/v1/research/company/annual-profiles/query`、单股`GET /api/v1/research/company/{instrument_id}/annual-profile`及计划接口继续使用；独立研究进程须显式`QUOTE_COMPANY_PROFILE_RESEARCH_ROOT`指向上述根。读/计划不隐式执行。不存在股票单股404、清单单列missing，不解释为未披露或零业务。首读/复核/问题补读的原操作链与参数模板只维护在唯一用法文档，历史两轮示例已收成链接，旧数据保留。

## 实际内容与有限审核

最终20股620条可读记录、834指标；是内容计数，不是准确率。当前各股末轮源核状态7 completed、13 completed_with_open_findings，说明执行已结束，未宣称意见全闭合。

圆通原30作者事实经原约束留28（F15/F19隔离保持）；本轮初核检查28条，只提出RF01/F23，复现A前轮已知“勾选不适用→未发生经济变化”的错误，**不声称新发现未知错误**。DeepSeek修订仅复述披露项目不适用并明确不确认实际经济变化，Grok终核接受该候选。其余27条逐对象未变，原generation.extraction仍rejected。

实际模型运行先继承F23 withheld；首次回执、`review-change-index.json`及`resources-and-protection.json`保首次内容。随后A重新核正式PDF hash/物理29页，提交精确新F23 hash的`a-corrected-f23-decision.json`。B只在新daily稿用既有UseReview/writer消费此显式独立判断，变为source_checked+limitation（投影limited），不由模型自动解禁。旧错误hash决定及旧release不改。圆通最终28条可读：22模型已核、6限用，仍只覆盖所选章节。

农行新daily副本仅在现有products_services.limitations加入A既有基金/资产管理/子公司综合金融服务获取不足及6.EU独审来源。原事实/数值/模型审核不改，API可见具体缺项，可按问题生成同报告父hash绑定计划；本卡不实际补读。深南电F14源单位限制保持。供应链和公司特色可选，不追满；空缺项列表也不表示全报告完整。

A有限内容/源核及使用判断见`a-review.json`；本轮没有全报告独审或正式95%/99%评估，旧S2/6.EU正式失败不回算，不扩20/100或启生产。

## 执行、资源与回执

唯一真实授权文件`execution-plan.json`，run_id=`6ew-600233-source-review`。实际执行命令（已完成，同文件重复只返回原结果，不续额）：

```bash
/home/python/miniconda3/envs/Quote/bin/python main.py job --job-id company_profile_common_core --action review_batch --execution-plan-file /home/python/Quote/openspec/changes/restore-company-profile-llm-led-generalization/workpaper-daily-research-delivery-20261122/execution-plan.json
```

实际结果/账本/001—003公开请求响应均在`/home/python/Quote/data/checkpoints/company_profile_daily_release_20261122/executions/6ew-600233-source-review/`；此处不重复复制正文。正式PDF仍为既有hash d14fd26f…，只发送圆通公开所选原文和派生稿，不发送A参考。

| 阶段 | 模型 | 物理秒 | 供应商total |
| --- | --- | ---: | ---: |
| 初核 | grok-4.7 high | 249.424 | 44,676 |
| 一次合并修订 | deepseek-flash | 1.203 | 13,399 |
| 终核 | grok-4.7 high | 27.999 | 17,972 |

合计3逻辑/3物理/0恢复，76,047供应商total，账本charge76,047；费用unknown，不冒称0元。物理请求合计278.627秒，入口started.json至保存result.json为294.660秒（不包含CLI import/初始化）；B/A实际活跃开发时间unknown。均在6000秒/1m/6物理含3恢复授权内，无追加窗。

## 必要验证与Review

- `network-preflight.json`：获准require_escalated环境DNS/TLS、已有profiles、凭据存在但未输出值；Grok1200物理/2450逻辑、Flash600，接线已核。
- `offline-readiness.json`：真实main/job在隔离目录沿正式源及seed到初核请求边界，0物理，非成功模型质量证据；输入/实现`launch-freeze.json`后真实期间未热修。
- `consumption-verification.json`：首次模型结果的20股清单+不存在股票HTTP、圆通单股、缺项→补读计划、复核计划状态、JSON/人读版/新实例；不隐式调用模型或改writer。已有首读/复核/补读测试及真实证明复用，未重跑大套。
- `final-consumption-verification.json`：A显式使用决定后的最终圆通单股HTTP/新实例与重导一致，另19投影不变；原混合清单/不存在/计划验证复用，不重复19次单股HTTP。
- `known-gap-display.json`：农行显示缺项唯一变动及A来源，事实/审核不变。
- `final-state.json`：最终40writer/年度制品hash、旧成果保护、当前同run_id返回原result且磁盘字节/mtime和账本不变，无新客户端/请求。
- 用法`git diff --check`及OpenSpec strict结果见`strict-validation.log`。本轮无新增业务代码/接口/框架，不把受控或离线验证计为模型质量。

Review只处理本轮直接交付问题：农行具体缺项未投API（用原limitations显示）、旧版操作示例易混（收成历史链接）、F23须显式独审后才放行（新精确决定限用）。未引入逐公司规则或扩旧问题范围。没有额外付费任务，当前可日常研究消费；正式质量及生产权限不变。

## 清理与交接

前轮6.EV两确切临时根复核不存在。本人本轮唯一临时根`/home/python/Quote/data/QuoteBak/company-profile-6ew-b-AK786IrS`，完成并持久必要证据后删除隔离测试库、脚本、过程日志和被替代导出，确切释放字节见`cleanup.json`。正式PDF/有效缓存、旧根/旧失败/attempt、当前单一20股研究根与真实检查点保留；A无临时根，无并行内容清理。

B不提交/推送，交A有限末审及本人可隔离Git收口；用法是本轮唯一修改的既有受版本管理文件，其余为新卡证据及明确授权的新研究副本。
