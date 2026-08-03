# 公司业务画像语义生产运行手册

## 1. 运行原则

本流程以自动化直通生产为默认路径。人工处理仅用于机器无法可靠消除的
实体歧义、披露冲突、复杂合并范围变化和高风险经济判断，不作为清洁事实的
例行审批步骤。

生产顺序固定为：复用已批准结构化事实、选择最小充分公告、解析原生 PDF
文本和表格、关键词定位小段证据、对未解决字段调用 LLM、独立验证、审计式
系统晋级。任一门禁缺失或版本不一致都失败关闭。

## 2. 默认状态和启用前提

以下配置默认关闭：

- `business_profile_evidence.semantic_production.enabled`
- `business_profile_evidence.semantic_production.promotion_enabled`
- `business_profile_evidence.semantic_production.scheduler_enabled`
- `scheduler_config.jobs.business_profile_semantic_maintenance.enabled`

启用顺序必须是：影子收集、字段族晋级、有限行业批次、增量调度。不得同时
跳过多个阶段。字段族只有在冻结 benchmark、运行身份和 promotion manifest
全部匹配时才允许自动晋级。

启用调度前必须确认：

- 目标字段族 manifest 已通过精度、证据、时点、漂移、成本和异常率门禁。
- 有界生产试点、回滚演练和 kill-switch 演练已通过。
- 全市场容量估算和异常积压策略已通过。
- 调度器参数保持明确的字段族和运行身份范围；公司范围可留空，由系统按公告
  hash、字段族完成状态、运行身份变化和到期机器重做自动发现，不允许全量盲扫。

## 3. 运行和检查点

CLI 支持 `plan`、`select`、`extract`、`verify`、`promote`、`resume`、
`report` 和 `rebuild-publications`。每次运行必须提供：

- 公司范围和字段族范围。
- 知识截止日。
- 文档、选择器、解析器、目录、模型、验证器、规则和政策运行身份。
- 字段族 promotion manifest 完整内容；运行时自行计算并绑定哈希。
- 独立检查点路径。

示例命令：

```bash
/home/python/miniconda3/envs/Quote/bin/python \
  scripts/research_business_profile_semantic_production.py resume \
  --instrument 601088.SH \
  --field-family atomic_activities \
  --knowledge-cutoff 2026-08-03 \
  --identities /tmp/business-profile-identities.json \
  --promotion-manifests /tmp/business-profile-manifests.json \
  --config /tmp/business-profile-semantic-config.json \
  --research-db /tmp/business-profile-shadow.db \
  --artifact-root /tmp/business-profile-semantic-artifacts \
  --checkpoint /tmp/business-profile-semantic.checkpoint.json
```

CLI 不接受调用方伪造的阶段结果或 `--input`。它直接读取研究库中的官方公告
manifest 和本地归档 PDF，实际执行选页、表格解析、语义抽取、验证和晋级。
promotion manifest 文件是按字段族键控的完整对象，或放在 `field_families`
对象下，例如：

```json
{
  "field_families": {
    "atomic_activities": {
      "field_family": "atomic_activities",
      "enabled": true,
      "benchmark_passed": true,
      "identities": {
        "document": "document.v1",
        "section": "section.v1",
        "selector": "selector.v1",
        "parser": "parser.v1",
        "schema": "schema.v1",
        "catalog": "catalog.v1",
        "model": "model.v1",
        "verifier": "verifier.v1",
        "rules": "rules.v1",
        "policy": "policy.v1"
      }
    }
  }
}
```

检查点只可在范围哈希和全部运行身份完全一致时恢复。出现 stale checkpoint
时应重新执行 `plan`，不能修改检查点内容绕过校验。

## 4. Kill Switch

`semantic_production.kill_switches` 提供四个独立开关：

- `all_writes`: 停止候选、异常、审核和发布写入。
- `network_calls`: 停止新的公告获取和 LLM 请求，保留本地确定性处理。
- `promotion`: 停止自动晋级，允许候选收集和机器返工继续。
- `scope_widening`: 停止加入新公司、公告或字段族，允许当前有界范围收尾。

异常输出、冲突、漂移或人工异常积压超过阈值时，系统应在安全检查点自动
停止相应动作。处理事故时优先缩小影响面：先关闭单字段族晋级，再关闭全局
晋级，最后才关闭全部写入。

Kill switch 不删除事实、候选、审核、异常或历史发布版本。

## 5. 漂移响应

漂移门禁按字段族独立执行。出现以下任一情况时，受影响字段族返回影子或
候选模式：

- provider 返回模型、prompt、schema、selector、parser、catalog、verifier、
  rule 或 policy 身份与冻结 manifest 不同。
- 生产抽样的 unsupported output、无效证据、时点错误、冲突率或异常率超限。
- 确定性解析和独立语义验证的结果稳定性超限。

自动响应步骤：

1. 禁用受影响字段族的 promotion，不影响其他健康字段族。
2. 停止该字段族扩大公司或文档范围。
3. 保留候选和失败样本，按稳定 reason code 聚类。
4. 优先修复 parser、selector、catalog 或 schema，并在冻结 benchmark 重跑。
5. 生成新 manifest；旧 manifest 不得原地修改。

只有新身份重新通过门禁后才能恢复自动晋级。

## 6. 机器返工

以下问题默认进入 `machine_rework`：

- 缺少或低质量 OCR。
- 可重试网关、超时或 schema repair 失败。
- 选页缺口或小范围上下文不足。
- 可由本地目录提案聚类解决的未知别名。

机器返工使用有界指数退避和最大重试次数。重试必须复用已有文档和页面制品，
仅在记录 `missing_context` 后按确定性规则扩展窗口。达到重试上限后，根据
剩余问题进入 quick review、deep review 或保持未支持状态，不能降低晋级门禁。
`promotion_enabled=false` 的 shadow 模式仍会持久化机器返工和例外，但不会
批准候选事实；否则调度器无法自动发现到期重试。重试耗尽后的记录不再自动
入队，quick/deep review 也不参与自动重试。

确定性数值解析绑定单位目录 `business_profile_units.2026.2`。其中 `万元`
固定换算为 `CNY * 10000`，`万吨` 固定换算为 `tonne * 10000`；未知单位在
bundle 写入前失败关闭，不允许部分事实落库。

## 7. 例外人工处理

`quick_review` 只用于证据完整、但仍有少量本地候选无法唯一选择的情况，
例如两个合法实体或别名。任务必须携带 exact evidence、reason code、gate
signature 和排序后的本地候选。

`deep_review` 仅用于：

- 相互冲突的官方披露。
- 上市主体、子公司或合并范围不清。
- 重大重组导致业务范围难以确定。
- 商品方向、重要性、价格传导、套保有效性或估值参数需要经济判断。

人工决定通过既有 optimistic transition 和 immutable review audit 写入。
人工 `held`、`approved`、`rejected` 或 `superseded` 决定优先于后续自动处理。

## 8. 重放和恢复

相同文档、页面、字段族和运行身份的重放必须满足：

- 不产生重复事实或审核记录。
- 已完成字段族不重新解析 PDF 或调用 LLM。
- 已解决异常不重新打开，除非运行身份或源事实发生变化。
- 发布输出和 lineage hash 保持一致。

网络中断或主动取消后使用 `resume`。如果目录、模型、规则或 policy 已变化，
应创建新范围并重新 `plan`，不能在旧检查点上混合身份。

## 9. 回滚

回滚是停止新行为并切回先前接受版本，不删除历史数据：

1. 关闭受影响字段族 promotion 和 scheduler gate。
2. 必要时开启 `all_writes`，保留只读诊断。
3. 让估值消费者固定到最后接受的 build policy 和发布版本。
4. 将错误发布通过新的 governed supersession 记录替代，禁止直接更新历史行。
5. 在复制数据库执行迁移和重放验证，再恢复有限批次。

回滚验收必须证明：历史 approved 行、知识时间、审核哈希和历史估值结果没有
被静默修改，候选和异常没有进入 DCF 输入。

## 10. 日常报告

每批报告至少包含字段族分母以及：复用率、选择的公告和页数、确定性完成率、
LLM 调用和 token、成本、延迟、自动晋级、机器返工恢复、quick/deep review、
unsupported output、冲突、漂移、检查点身份和候选估值泄漏数。

人工工作量的目标指标是 quick/deep review 比率，而不是总候选数。异常积压
优先通过 reason-code 聚类推动 parser、selector、catalog 和规则自动修复。
