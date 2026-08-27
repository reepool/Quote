# PDF 原生解析进程隔离交付报告

日期：2026-08-27
提交对象：共享 PDF 开发组
影响模块：`research.document_processing.pdf`、公司画像异步回补调用方

## 1. 结论摘要

2026-08-27 11:31:14，`quote-system.service` 被 `SIGTRAP` 终止。内核日志明确指出：

```text
traps: python[1290136] trap int3 ip:7f8c8b495921 sp:7f8c9fffdbb0 error:0 in libpdfium.so
```

根因不是 LLM 返回错误、业务核验失败或 OOM，而是多个画像解析任务在线程池中并发调用同一进程内的 PDFium native 库，触发 `libpdfium.so` 的 `int3` trap。由于 PDFium 当前运行在 Quote 主进程内，单份 PDF 的 native 崩溃直接终止整个服务。

建议将 PDFium 和 `pypdf` 都纳入共享 PDF 解析进程边界。PDFium 是必须隔离的 native runtime；`pypdf` 虽主要为纯 Python，但纳入同一边界可以统一硬超时、崩溃隔离、日志、回退和资源控制。

## 2. 事故时间线

本次任务为 11 只股票的业务画像回补，使用 `force=true result_policy=reuse`。

关键日志：

```text
11:30:52 business-profile backfill start
11:31:06 recovery requeued=0
11:31:06 discovery start ... category=annual_report
11:31:07 discovery end status=success selected=11
11:31:07 workers start
11:31:07 stage start stage=parse ... concurrency=4
11:31:07 stage start stage=semantic ... concurrency=20
11:31:08 Luna request admitted for 000858.SZ
11:31:10 Luna HTTP/1.1 200 OK
11:31:10-11:31:12 acquire items completed normally
11:31:14 systemd: Main process exited, code=killed, status=5/TRAP
11:31:24 systemd: service restarted
```

崩溃窗口中处于 `parse` running 的工作项：

| 股票 | PDF 资产 | 页数 |
|---|---|---:|
| 603268.SH | `asset_216095acbf593874f923fd9d609ab593` | 232 |
| 002496.SZ | `asset_4678a73d576881f1f73714ace2cd3151` | 210 |

归档路径：

```text
data/filings/announcements/blobs/3a/3a3584ba65a58320e918ffa6f8f604fa2ad070d31f6f325f556a989a8e6e8fee.pdf
data/filings/announcements/blobs/63/635dd0bdb6fd76658403170727c8d60614bdee03db1ddb0356df6ec61e2f4467.pdf
```

## 3. 复现和排除结果

使用当前 Quote 环境 `pypdfium2==5.13.0` 对上述两份 PDF 做独立测试：

- 单线程逐份解析：两份均正常返回，分别为 232 页和 210 页；
- 同一进程使用 4 个线程并行创建 `PdfiumNativeAdapter` 解析：可复现 `Trace/breakpoint trap`，进程直接退出；
- 退出信号与生产日志一致，均为 `SIGTRAP`；
- 内存 cgroup 没有 OOM 记录，服务内存峰值约 828 MB；
- 代码搜索未发现业务主动发送 `SIGTRAP` 的路径；
- LLM 请求在崩溃前已收到 HTTP 200。

因此，问题是 PDFium native runtime 与进程内并发访问组合的稳定性问题，而不是某个字段、LLM 输出或画像核验逻辑本身。

## 4. 当前调用结构和风险

画像异步服务使用全局 `_ASYNC_IO_EXECUTOR` 线程池执行阻塞工作，当前 `parse` 阶段配置为 4 路并发。PDFium native adapter 在这些线程中直接导入并调用 `libpdfium.so`。

风险：

1. native 库触发断点、段错误或 abort 时，整个 Quote 主进程退出；
2. 线程无法可靠实施硬超时，解析卡死时只能等待；
3. 一个异常 PDF 会影响其他公司的回补任务、API、Telegram 和调度器；
4. native 解析失败不能在同一进程内可靠完成 fallback；
5. 仅把线程数从 4 改小不能形成故障隔离，只能降低触发概率。

## 5. 建议的目标架构

建立由共享 PDF 模块维护的持久化解析进程池，默认总并发为 4：

```text
Quote 主进程
    |
    +-- PDF parse worker 1: 报告 A
    +-- PDF parse worker 2: 报告 B
    +-- PDF parse worker 3: 报告 C
    +-- PDF parse worker 4: 报告 D
```

每个 worker 是独立 Python 进程，拥有独立的 PDFium 文档句柄和 native 状态。worker 内部不再使用 PDFium 多线程并发。

### 5.1 启动和并发

- 使用 `multiprocessing` 的 `spawn` 模式；
- worker 持久复用，避免每页启动新进程；
- 一个 worker 任务处理一份 PDF，页面按物理页码升序处理；
- 共享进程池的总并发由一个配置项控制，默认 4；
- 不允许每个业务阶段再创建自己的进程池；
- worker 不直接写数据库、不修改队列和业务事实。

### 5.2 统一隔离 PDFium 和 pypdf

建议 `pypdf` 也通过该进程边界运行：

- PDFium：必须隔离，解决 `libpdfium.so` 崩溃拖垮主进程；
- pypdf：虽然主要是纯 Python，但纳入边界后可获得统一的硬超时、异常 PDF 隔离和资源治理；
- 不应把 PDFium 和 pypdf fallback 放在同一 worker 的不可恢复串联中。

推荐回退顺序：

```text
主进程提交 PDFium worker
    -> worker 成功：返回 PDFium 页结果
    -> worker SIGTRAP/崩溃/超时：父进程记录 typed failure
    -> 主进程提交 pypdf worker：只处理需要回退的页
    -> 仍不可用：进入既有 OCR 或 source_unrecoverable 流程
```

### 5.3 输入和输出

输入应绑定共享年报资产：

- `asset_id` 或只读归档路径；
- PDF content hash；
- 请求的物理页码；
- parser profile、版本和参数 hash；
- 单页、单文档超时和输出大小预算。

输出应只包含可序列化的页级结果：

- 页码和文本；
- 文本 hash；
- extraction method；
- parser/runtime version；
- 页耗时和文档耗时；
- candidate/provenance；
- typed diagnostics；
- worker exit code/signal（若 worker 异常退出）。

结果由主进程统一写入现有 page artifact/cache 和业务队列，保持现有共享资产和画像数据契约不变。

## 6. 故障语义要求

共享 PDF 模块必须把 worker 异常转换为可识别诊断，不能让异常冒泡杀死主进程。至少覆盖：

- `native_worker_crashed`：worker 被信号终止，记录 signal 名称和数字；
- `native_worker_timeout`：超过单页或单文档预算；
- `native_worker_protocol_error`：返回 JSON 无法校验；
- `native_extraction_error`：worker 内部捕获的页级解析错误；
- `fallback_exhausted`：PDFium 和 pypdf 均不可用；
- `source_unrecoverable`：后续 OCR 也不可用或预算耗尽。

无可用候选时必须返回明确不可用状态，不能把乱码或半截文本标记为可供语义分析的结果。

## 7. 日志和可观测性

每个 worker 任务至少记录：

- `request_id`、`asset_id`、`instrument_id`、`work_id`；
- profile、parser version、worker pid；
- requested/returned pages；
- worker start/end、wall time、CPU time（可用时）；
- cache hit/miss；
- exit code 或 signal；
- fallback 次数和最终状态。

日志级别建议：

- `INFO`：任务开始、完成、回退、worker 崩溃、最终状态；
- `DEBUG`：页级耗时、请求参数、诊断详情；
- `WARNING`：超时、崩溃、协议错误和回退；
- `ERROR`：进程池不可用或所有 fallback 均失败。

## 8. 验收标准

### 8.1 崩溃隔离

- 用当前已知复现组合（603268.SH、002496.SZ，4 路并发）运行不少于 20 轮；
- PDFium worker 即使出现 `SIGTRAP`，Quote 主进程也不得退出；
- systemd 不得因该测试增加服务重启次数；
- 对应工作项获得 `native_worker_crashed` 或等价 typed diagnostic，并进入 pypdf/OCR 回退。

### 8.2 正常解析

- 600036.SH 字体映射异常年报仍能通过 PDFium 或既有恢复路径得到正确中文页文本；
- 正常原生年报的页码、文本 hash、表格阅读顺序保持不变；
- 结果按物理页码升序返回，缓存 identity 包含 profile/runtime/parser 参数。

### 8.3 超时和恢复

- 人为制造 worker 阻塞，父进程在预算到期后能终止并回收 worker；
- 单个 worker 异常不影响其他 worker；
- pypdf fallback 只处理失败页，不重新解析无关页；
- 重试有上限，不能形成无限重启或无限回退。

### 8.4 并发边界

- 进程池总并发默认值为 4；
- 单 worker 内部 PDFium 和 pypdf 均串行；
- 不出现“业务配置 4 路、内部再创建额外线程池”的隐式放大；
- 解析并发与 LLM 并发、数据库写入并发分别受各自 owner 控制。

### 8.5 兼容性

- 保持现有 `PdfParseRequest`、页级 artifact、共享年报资产和画像调用方契约兼容，或提供明确迁移版本；
- 不把 GPU Paddle/PaddleOCR runtime 导入 Quote 主进程；
- worker 可报告准确的 PDFium/pypdf 版本和运行环境；
- 不改变原始 PDF 文件和数据库写入 owner。

## 9. 交付物请求

请 PDF 开发组评估并交付：

1. 共享 PDF 解析进程池及 worker 协议；
2. PDFium、pypdf 两种 profile 的隔离实现；
3. worker 崩溃、信号、超时和协议错误的 typed diagnostics；
4. 父进程 fallback 和页级结果合并逻辑；
5. 进程池并发、超时、回收和启动恢复测试；
6. 上述 603268.SH、002496.SZ 崩溃复现回归报告；
7. 对 600036.SH、正常原生年报和混合/扫描页的兼容性报告；
8. 部署说明，包括 worker 启动方式、日志、资源预算和 systemd 环境要求。

本报告只要求共享 PDF 模块解决解析进程隔离和技术恢复边界。章节选择、字段族映射、LLM 语义提取、业务核验和画像入库仍由画像业务层负责。
