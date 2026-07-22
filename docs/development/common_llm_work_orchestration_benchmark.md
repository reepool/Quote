# 公共 LLM 异步协调器离线基准

## 2026-07-22 基准

执行入口：

```bash
/home/python/miniconda3/envs/Quote/bin/python \
  scripts/dev_validation/benchmark_llm_orchestration.py
```

参数：

- 单请求输入字符：225,000；
- 单请求输出字符：100,000；
- 模拟 provider 等待：0.05 秒；
- 分级并发：10、25、50；
- 无真实网络和 API 调用。

结果：

| 并发 | 峰值 transport 并发 | 总耗时 | Python 峰值分配 | 最大 RSS | FD 变化 | 身份校验 |
|---:|---:|---:|---:|---:|---:|---|
| 10 | 10 | 0.126s | 3.30 MiB | 120.3 MiB | 0 | 通过 |
| 25 | 25 | 0.254s | 8.04 MiB | 125.2 MiB | 0 | 通过 |
| 50 | 50 | 0.374s | 14.01 MiB | 131.6 MiB | 0 | 通过 |

每一级的 request ID、request hash 和 business item key 均全部唯一并一一对应。该结果证明
本机公共协调器可承载 50 个主要处于等待状态的大输入请求，且没有文件描述符增长。

该离线结果不能替代真实 provider/CDN 验证。生产启用仍必须按 10、25、50 逐级观察 429、
5xx、超时、首字时间、总耗时和连接稳定性。
