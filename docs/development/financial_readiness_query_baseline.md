# 财务就绪查询基线报告（窄口径优化前/后）

> 对应 change：`accelerate-financial-readiness-queries`
>
> 测量脚本：`scripts/research_financial_readiness_perf.py`（baseline / equivalence）
>
> 环境：生产库 `data/financials.db`（77.7GB），只读运行，2026-11-08

## 基线（宽实现，窄化前）

| 指标 | 数值 |
|---|---|
| 每标的每期取回行数 | 573 行 × 35 列（含 JSON/大文本列） |
| 就绪判定实际需要 | ~7 行（5 个核心事实 + 变体），98.8% 取回即丢弃 |
| 窄化前查询（热缓存，肥标的） | 10ms（`idx_..._canonical` 已支撑三列定位） |
| 生产扫描实测速率 | 0.64 秒/候选（2026-11-08 周日运行心跳） |
| 全扫描耗时 | ~5.5 小时 → 超出 18000s 上限，11-08 运行在扫描 90% 处被杀 |

注：0.64 秒/候选包含查询以外的候选构建开销（repair target、profile 解析、审计加载等）；查询层占比由本报告窄化差值与 M1 分解共同界定。

## 窄化后（get_financial_core_fact_readiness_rows）

| 指标 | 数值 |
|---|---|
| 取回行数 | 7 行 × 8 列（canonical IN 下推；去重到胜利行在结果构建层，与宽实现一致） |
| 查询耗时（热缓存，肥标的） | <1ms |
| 端到端 get_financial_local_core_facts（30 标的 × 7 期，生产库） | **宽 38.1ms → 窄 3.9ms/候选（9.7 倍）** |
| 等价性门禁（冻结样本 220 标的 × 7 期） | **1540 次评估，0 不一致** |

## 尚未回收的部分

- 扫描 0.64s/候选中的非查询开销（候选构建、repair target、多次调用叠加）由 tasks 1.1 分解工具继续界定；
- 端到端扫描段目标（≤75 分钟）以 tasks 4.1 副本/夜间 dry-run 实测为准；
- 每候选速率已进入心跳进度（`scan_rate_per_sec`），生产回退可见。

## 使用

```bash
bash scripts/quote_dev_env.sh python scripts/research_financial_readiness_perf.py baseline --count 30
bash scripts/quote_dev_env.sh python scripts/research_financial_readiness_perf.py equivalence
```
