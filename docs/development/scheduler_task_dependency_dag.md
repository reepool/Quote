# Scheduler Task Dependency DAG

## 背景

此前 `broker_risk_control_incremental_sync` 已作为 `financial_disclosure_incremental_sync` 的后置任务运行，但实现曾是过渡方案：

- 后置开关和后置 job id 来自 `config/05_scheduler.json`。
- 实际编排逻辑写在 `ScheduledTasks.financial_disclosure_incremental_sync()` 内部。
- 如果后续新增其他特殊行业、审计、清理或报表任务，继续在主任务函数中追加分支会造成调度层耦合和不可维护。

当前实现已把任务前置/后置关系提升为调度层通用能力：任务之间的依赖关系由配置声明，调度器按配置执行 DAG，业务任务只负责自身逻辑。

## 目标

- 支持通过配置声明任务前置与后置关系。
- 支持 A 成功后并行启动 B/C。
- 支持 A 成功后按配置顺序串行启动 B 再启动 C。
- 支持前置任务成功作为主任务启动前提。
- 支持 `manual_only=true` 任务作为依赖节点被自动编排触发，但不单独注册 cron。
- 迁移券商专项数据获取：从 `financial_disclosure_incremental_sync` 内部硬编码后置调用，迁移到配置化 `post_success`。
- 任务报告应展示依赖节点状态，而不是只展示主任务状态。

## 非目标

- 不在第一阶段实现通用分布式工作流引擎。
- 不引入外部 DAG 系统或队列依赖。
- 不改变现有 cron 注册、`/run <job_id>` 手工触发和任务参数的基本使用方式。
- 不让业务任务直接感知自己被前置/后置调用。

## 配置位置

依赖配置放在 `config/05_scheduler.json` 的 job 节点内：

```json
{
  "scheduler_config": {
    "jobs": {
      "financial_disclosure_incremental_sync": {
        "dependencies": {
          "post_success": [
            {
              "group_id": "financial_industry_supplements",
              "mode": "parallel",
              "jobs": [
                {
                  "job_id": "broker_risk_control_incremental_sync",
                  "inherit": ["exchanges", "dry_run"],
                  "timeout_seconds": 7200,
                  "failure_policy": "degrade_parent"
                }
              ]
            }
          ]
        }
      }
    }
  }
}
```

串行后置示例：

```json
{
  "dependencies": {
    "post_success": [
      {
        "group_id": "serial_financial_supplements",
        "mode": "serial",
        "jobs": [
          {"job_id": "task_b", "failure_policy": "stop_chain"},
          {"job_id": "task_c", "failure_policy": "degrade_parent"}
        ]
      }
    ]
  }
}
```

前置示例：

```json
{
  "dependencies": {
    "pre_success": [
      {
        "group_id": "preflight",
        "mode": "serial",
        "jobs": [
          {"job_id": "instrument_master_governance", "failure_policy": "fail_parent"}
        ]
      }
    ]
  }
}
```

## 配置字段

| 字段 | 类型 | 必填 | 说明 |
|---|---:|---:|---|
| `dependencies.pre_success` | list | 否 | 主任务运行前执行；前置失败时按失败策略决定是否启动主任务 |
| `dependencies.post_success` | list | 否 | 主任务成功后执行；主任务失败时不执行 |
| `dependencies.post_always` | list | 否 | 主任务无论成功失败都执行，适合清理、审计、汇总报告 |
| `group_id` | string | 是 | 依赖组标识，用于日志和报告 |
| `mode` | string | 是 | `parallel` 或 `serial` |
| `jobs` | list | 是 | 依赖任务列表 |
| `job_id` | string | 是 | 被触发的 scheduler job id |
| `inherit` | list | 否 | 从父任务参数继承的字段，例如 `exchanges`、`dry_run` |
| `parameters` | object | 否 | 覆盖或补充依赖任务参数 |
| `timeout_seconds` | int | 否 | 单个依赖任务超时时间 |
| `failure_policy` | string | 否 | `fail_parent`、`degrade_parent`、`ignore`、`stop_chain` |
| `enabled` | bool | 否 | 默认 true；用于临时关闭某个依赖节点 |

## 执行语义

### 前置任务

- `pre_success` 在主任务启动前执行。
- `pre_success` 全部成功后才启动主任务。
- 并行前置组中任一任务失败时，组失败。
- 串行前置组中某任务失败时，后续任务默认不执行。
- 前置失败后的主任务处理由失败策略决定：
  - `fail_parent`：主任务不运行，父任务结果为 failed。
  - `degrade_parent`：主任务不运行或运行但标记 degraded；第一阶段建议主任务不运行，避免前置约束失效。
  - `ignore`：记录 warning 后继续运行主任务；仅适合非关键前置。

### 后置任务

- `post_success` 只在主任务成功时执行。
- 对本项目，`success` 初始定义为任务返回布尔 `True` 或结果状态在配置的成功集合内。
- 主任务失败时不执行 `post_success`。
- `post_always` 无论主任务成功失败都执行。
- 并行后置组中 B/C 同时启动；组状态由所有节点聚合。
- 串行后置组中 B 成功后才启动 C；B 失败时是否继续由 `failure_policy` 决定。

### 失败策略

| 策略 | 语义 |
|---|---|
| `fail_parent` | 依赖失败会使父任务整体失败 |
| `degrade_parent` | 依赖失败会使父任务整体降级，但主任务自身结果保留 |
| `ignore` | 依赖失败只记录 warning，不影响父任务状态 |
| `stop_chain` | 当前依赖失败后停止同组后续串行任务；父任务是否失败由组级或节点级策略决定 |

第一阶段建议最小实现：

- 前置默认 `fail_parent`。
- 后置默认 `degrade_parent`。
- 串行节点失败默认 `stop_chain`。
- `post_always` 默认 `ignore`，除非显式配置。

## 参数继承与覆盖

依赖任务的最终参数按以下顺序生成：

1. 依赖 job 在 `config/05_scheduler.json` 中的 `parameters`。
2. 父任务运行时参数中被 `inherit` 声明的字段。
3. 依赖节点自身的 `parameters` 覆盖。

示例：父任务手工 `/run financial_disclosure_incremental_sync dry_run=true exchanges=SSE,SZSE`，依赖节点配置 `inherit=["exchanges","dry_run"]`，则 `broker_risk_control_incremental_sync` 自动继承同样的 `dry_run` 和交易所范围。

## 报告结构

调度器执行结果需要新增 `dependency_results`；Telegram 报告以父任务维度追加依赖节点摘要：

```json
{
  "dependency_results": {
    "post_success": [
      {
        "group_id": "financial_industry_supplements",
        "mode": "parallel",
        "status": "success",
        "nodes": [
          {
            "job_id": "broker_risk_control_incremental_sync",
            "status": "success",
            "elapsed_seconds": 82.4,
            "inherited_parameters": {"exchanges": ["SSE", "SZSE", "BSE"], "dry_run": false},
            "summary": {"facts_written": 12, "reports_parsed": 4}
          }
        ]
      }
    ]
  }
}
```

Telegram 和日志至少展示：

- 依赖阶段：`pre_success / post_success / post_always`
- 依赖组：`group_id`
- 节点任务：`job_id`
- 状态、耗时、错误摘要
- 写入数量或关键 counters

## 安全约束

- 调度启动时必须校验 DAG：
  - job id 必须存在。
  - 依赖图不得有环。
  - 同一父任务的同一阶段内 `group_id` 不得重复。
  - `mode`、`failure_policy`、`inherit` 字段必须合法。
- 依赖节点不得绕过 `_active_tasks` 互斥保护。
- 依赖触发必须尊重被依赖 job 的 `enabled=false`，默认跳过并记录 `disabled`；是否影响父任务由失败策略决定。
- `manual_only=true` 只表示不注册 cron，不表示不能被依赖图触发。
- 防止递归 `/run` 无限嵌套：执行器需要维护当前 dependency chain，并在发现回环时失败。

## 迁移券商后置任务

目标配置：

```json
{
  "financial_disclosure_incremental_sync": {
    "parameters": {
      "run_broker_risk_control_post_task": false
    },
    "dependencies": {
      "post_success": [
        {
          "group_id": "financial_industry_supplements",
          "mode": "parallel",
          "jobs": [
            {
              "job_id": "broker_risk_control_incremental_sync",
              "inherit": ["exchanges", "dry_run"],
              "timeout_seconds": 7200,
              "failure_policy": "degrade_parent"
            }
          ]
        }
      ]
    }
  }
}
```

迁移步骤：

1. 新增通用 dependency executor。
2. 将 scheduler cron 和 `/run` 手工入口统一包一层依赖执行器。
3. 保留旧 `run_broker_risk_control_post_task` 参数一个兼容周期，但默认关闭。
4. 在配置中声明券商专项后置任务。
5. 删除 `ScheduledTasks.financial_disclosure_incremental_sync()` 内部硬编码 broker 后置逻辑。
6. 更新 Telegram 报告，显示券商后置节点状态。

## 测试要求

- 配置解析：合法配置、非法 mode、非法 failure policy、未知 job id。
- DAG 校验：直接环、间接环、重复 group id。
- 并行后置：A 成功后 B/C 都运行；A 失败时 B/C 不运行。
- 串行后置：A 成功后 B 成功再 C；B 失败时 C 不运行。
- 前置任务：前置成功才运行主任务；前置失败时主任务不运行。
- 参数继承：父任务运行参数正确传入依赖任务，并允许节点参数覆盖。
- 手工触发：`/run A` 也执行配置依赖，除非显式 `skip_dependencies`。
- 券商迁移回归：`financial_disclosure_incremental_sync` 成功后通过配置触发 `broker_risk_control_incremental_sync`，且原硬编码分支不再需要。

## 开发顺序

1. 配置 schema 和 dataclass/model。
2. DAG 校验器。
3. 依赖执行器，先支持 `serial`，再支持 `parallel`。
4. 接入 scheduler job 执行入口和 `/run` 手工入口。
5. 报告聚合。
6. 迁移券商后置任务配置。
7. 删除券商硬编码后置分支。
8. 补测试和文档。
