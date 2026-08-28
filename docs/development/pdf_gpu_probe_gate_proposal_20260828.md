# GPU OCR 探活不应阻断原生 PDF 解析

日期：2026-08-28  
提交对象：共享 PDF 解析器开发组  
影响模块：`research.document_processing.pdf`（`resolve_profile` / `build_router` / `PdfParseRequest` / `PdfRouter.parse`）  
业务触发方：港股生命周期（HKEXnews 长期停牌月报）；同类风险覆盖公司画像 PDF 资产、年报资产首页纠偏、券商风控 PDF、官方指数公告 PDF  
相关缓解：`8eac643`（成功探活进程内缓存、探活超时 20s→60s、失败写 diagnostic）。该提交**不是**根因修复，公共层修好后也**不要回退**。

## 1. 请共享层解决的问题

生产 `QUOTE_PDF_ENGINE_PROFILE=pdfium_paddleocr_gpu` 时，共享解析器在**还不知道这份 PDF 会不会走 OCR** 的时候，就对隔离 GPU worker 做 `--probe`。探活失败则直接 `ValueError`，整次 `parse` 进不去。

默认请求是 `ocr_mode="none"`、`recovery_policy="native_first"`，OCR **根本不会执行**。大量业务只抽原生文本，却被 GPU 冷启动或瞬时探活失败拖死。

请把「选用 GPU canary profile」和「GPU worker 必须此刻健康」拆开：

- 原生抽字只依赖 native 引擎；
- GPU `--probe` 只在**真正要把页交给 OCR** 时发生；
- OCR 探活失败不得抹掉已经成功的原生结果。

## 2. 共享 profile 是什么

`QUOTE_PDF_ENGINE_PROFILE` 选中的是一份命名配置（`DEFAULT_PROFILES`），不是某个业务自己的解析器。它规定：

- native 引擎顺序（生产 GPU profile 仍是 `pypdfium2` → `pypdf`）；
- 是否挂 OCR、OCR 走 CPU 还是 GPU worker；
- 超时、并发、canary 门闩。

`PdfParseRequest` 不传 `profile` 时，经 `_resolve_default_profile()` → `resolve_profile()` 继承全局选择。业务侧通常只说「把这份 PDF 变成按页文本」。

**公告清单扫描不走这套。** 财务披露增量、港股短暂停牌标题扫描只看元数据/标题，不 `resolve_profile`，也不探活。只有已经拿到 PDF 字节、要抽字或看首页时才会进来。

当前会继承全局 GPU profile 的抽字路径：

| 调用方 | 请求形态 | 实际是否 OCR | 探活失败后果 |
|---|---|---|---|
| `HKEXSuspensionReportProvider.parse_pdf` | `resolve_profile(self.profile_name)`，配置空则全局 GPU；默认 `ocr_mode=none` / `native_first` | 否。月报是文本 PDF | 整份 PDF 放弃 |
| `BusinessProfilePdfArtifactExtractor` | `engine_profile is None` → `resolve_profile()` | 仅低文本页才可能 recovery | 抽字失败 |
| `announcement_assets.classifier._pdf_first_page_is_annual_summary` | 默认 `PdfParseRequest`，只看第 1 页 | 否 | 捕获后当「不是摘要」，分类不修正 |
| `broker_risk_control._extract_text` | 默认 `PdfParseRequest` | 通常否 | 空文本，`text_extraction=failed` |
| `official_index_source.extract_pdf_text` | 默认 `PdfParseRequest` | 否 | 抽字失败 |
| 巨潮公司行动 PDF | 显式 `pypdf_corporate_action` | 否 | **不探 GPU**（对照：显式 native profile 可避开） |

业务方不应为了躲探活再各自钉死 `pdfium_native`。港股停牌月报以后若变成扫描件，仍需要同一套 GPU OCR 恢复，而不是业务私有解析器。

## 3. 现行门闩（问题代码）

`resolve_profile()` 在 `ocr_device` 以 `gpu` 开头时调用 `_require_gpu_canary_approval()`，后者在 canary 文件通过后立刻 `_require_isolated_gpu_runtime()`：对 `QUOTE_PDF_GPU_OCR_WORKER` 做 `subprocess.run(..., "--probe")`。

`build_router()` 对 GPU profile 再走同一条路径。

因此下面任一动作都会探活，即使随后只做 native extract：

```text
PdfParseRequest(content=pdf_bytes)          # default_factory → resolve_profile()
resolve_profile()                           # 或 resolve_profile("")
build_router(gpu_profile)
```

`PdfRouter.parse()` 里真正的 OCR 开关是：

```text
allow_ocr = request.ocr_mode != "none" and request.recovery_policy != "native_first"
```

默认请求 `allow_ocr` 为假。探活发生在这个判断之前。

## 4. 生产证据：2026-08-28 17:30 港股日更

环境：`QUOTE_PDF_ENGINE_PROFILE=pdfium_paddleocr_gpu`（canary，自 2026-08-26）。`instrument_master_sync.pdf_profile` 为空，继承全局。

当晚主板长期停牌 PDF 在 `resolve_profile` / `build_router` 阶段失败：

```text
GPU PDF profile requires a healthy isolated CUDA OCR worker
```

当时探活超时 20s；Quote 主进程自 16:30 起已运行，隔离 GPU worker 随后探活正常。失败异常丢掉了 `probe["diagnostic"]`。

该 PDF 是文本 PDF（约 19 页、5 万+ 字符）。同机 `pypdfium2` 抽字约 0.2s，可得完整停牌表。OCR 既未配置、也不需要。创业板 PDF 同轮成功，说明这是探活门闩，不是「PDF 必须 OCR」。

后果：主板官方停牌名单从约 223 降至 135；半成功仍被当成「官方源可用」，6 只长期停牌被误写回 `active / trading_status=1`。这是业务策略漏洞，由探活误伤触发；业务侧另跟，不在本次解析器范围内。

22:10 同任务重跑（Quote 已重启，`8eac643` 已生效）恢复 223，误复活回写纠正。根因仍在：原生解析不该依赖 GPU worker 此刻健康。

## 5. 建议的目标契约

### 5.1 拆开两道门

| 门闩 | 允许的时机 | 不允许的时机 |
|---|---|---|
| Canary 配置门（`QUOTE_PDF_GPU_CANARY_APPROVED`、approval report、corpus hash） | `resolve_profile("pdfium_paddleocr_gpu")`：选用 GPU profile 仍要批准 | 不要在这里启动 worker |
| GPU worker 运行时探活（`--probe`） | 该请求即将把页交给 `PaddleOcrAdapter` | `ocr_mode="none"` 或 `recovery_policy="native_first"`；或 native 已给出可用页、本页不 recovery |

`resolve_profile()` 只解析命名配置、写入 worker command / cache dir，并做**文件级** canary 批准。不要 `subprocess` 探活。

`build_router()` 可以构造带 OCR adapter 的 router，但构造时不要探活。

`PdfParseRequest` 的 `default_factory` 不得因为全局是 GPU profile 就卡住对象构造。

### 5.2 探活只服务 OCR

推荐把 `_require_isolated_gpu_runtime` 收到 `PaddleOcrAdapter`（或 `PdfRouter` 里 `allow_ocr` 为真且某页将 OCR 的路径）的惰性 `ensure_runtime()`。

失败语义：

- native 已成功：返回原生结果；OCR 以 typed diagnostic 标记不可用（例如 `ocr_runtime_unhealthy`），不要把整份文档打成 `failed`；
- `force_ocr` 或业务明确要求 OCR、且没有可用原生页：这次 OCR 失败，带 diagnostic，不要伪装成原生失败。

不要静默改走未批准的 CPU OCR，除非现有 fallback 合同已经写明且调用方能看见。

### 5.3 探活实现（保留，不要删）

`8eac643` 已在公共层落地，OCR 真走时仍然需要：

1. **成功探活进程内缓存**（key：worker command / runtime / cache dir / device）。失败不缓存，下次 OCR 再试。
2. **冷启动超时足够**（默认 60s，`QUOTE_PDF_GPU_PROBE_TIMEOUT_SEC`）。CUDA import 经常超过旧的 20s。
3. **失败必须保留 `probe["diagnostic"]`**：日志 `ERROR` + 异常/页诊断同一条原因。

可选增强，不阻塞上述契约：

- 进程启动后后台预热，结果只加速首次 OCR，不作为 native `parse` 的前置条件；
- 独立 health/readiness，供运维看 GPU worker，不绑在 `PdfParseRequest` 构造上。

### 5.4 不要用业务钉 profile 代替公共修复

不要要求港股、画像、券商各自写 `pdfium_native`。那会让扫描件/低文本页失去同一套 GPU OCR，并复制路由。显式 native profile 只留给像巨潮公司行动那样**永远不要 OCR** 的路径。

## 6. 验收标准

### 6.1 原生路径不受 GPU worker 影响

在 `QUOTE_PDF_ENGINE_PROFILE=pdfium_paddleocr_gpu` 且 canary 文件有效的前提下：

1. 不配置 `QUOTE_PDF_GPU_OCR_WORKER`，或 `--probe` 故意失败/超时；
2. 用默认 `PdfParseRequest`（`ocr_mode=none` / `native_first`）解析文本 PDF（港股停牌月报或等价 fixture）；
3. 必须得到可用原生页文本，耗时与 `pypdfium2` 抽字同量级，**不得**出现 `--probe` 等待；
4. `resolve_profile()`、`PdfParseRequest(content=...)`、`build_router(profile)` 在 worker 不健康时仍能完成（不抛「healthy isolated CUDA OCR worker」）。

### 6.2 OCR 路径仍然 fail-closed，且可诊断

1. `ocr_mode != none` 且 `recovery_policy` 会真正 OCR 时，才允许 `--probe`；
2. 探活失败：OCR 不可用，diagnostic 进入日志和结果，不导入 CUDA Paddle 进 Quote 主进程；
3. 同一进程内成功探活后，后续 OCR 请求不得再付冷启动秒级成本（沿用现有缓存）。

### 6.3 回归

- 现有 `tests/unit/test_research/document_processing/test_pdf_core.py`、`test_pdf_evaluation.py` 继续通过；
- 增补：GPU profile + 默认 native 请求 + 损坏/缺失 worker → 原生成功；
- 增补：GPU profile + 需要 OCR 的请求 + 损坏 worker → OCR diagnostic，原生页（若有）仍保留。

### 6.4 兼容性

- 不改变 `PdfParseRequest` / 页级 artifact 对外字段，除非 bump 并写迁移；
- 不把 GPU Paddle 导入 Quote 主进程；
- 不改变原始 PDF 文件和各业务写入 owner；
- 不要求调用方为了「只抽字」改请求字段。默认请求必须在 GPU canary 下可原生解析。

## 7. 与 `8eac643` 的关系（给业务方的明确结论）

**公共层按第 5 节修好后，不要回退 `8eac643`。**

| `8eac643` 做了什么 | 公共层修好后还要不要 |
|---|---|
| 成功探活进程内缓存 | 要。首次真 OCR 仍会探活，缓存避免每份扫描件再冷启动 |
| 探活超时 20s→60s | 要。冷 CUDA import 仍然慢 |
| 失败写出 `diagnostic` | 要。OCR 失败必须可运维 |

`8eac643` 只降低「进程里第一份 PDF 误探活」的误伤概率，没有改变门闩位置。公共层把探活挪到 OCR 执行点之后，原生日更、年报首页、券商正文不再等 GPU、不再因探活失败整单失败。两件事叠加，不是互相替代。

若公共层另做后台预热或独立 health，缓存和超时仍应留在 OCR ensure 路径上。

## 8. 非目标

- 不在本提案改港股停牌半成功策略（主板失败仍允许复活）——业务层另跟；
- 不在本提案改财务披露/停牌**标题扫描**（它们不调用 PDF 解析器）；
- 不要求业务方改调用方式才能躲开探活；
- 不把 Quote conda 的 CPU Paddle 当生产 OCR；
- 不扩大成新的 observability 平台或第二套 PDF 解析器。

## 9. 交付物请求

请共享 PDF 组评估并交付：

1. `resolve_profile` / `build_router` / `PdfParseRequest` 默认构造与 GPU `--probe` 解耦；
2. OCR 惰性 `ensure_runtime()`（或等价），保留成功缓存、可配置超时、失败 diagnostic；
3. 第 6 节单元测试与回归；
4. 更新 `docs/development/pdf_ocr_worker_runbook.md`：探活是 OCR 前置，不是 native parse 前置；
5. 简短说明：生产 GPU canary 下，默认 `native_first` 请求在 GPU worker 宕机时的预期行为。

调用方（港股生命周期、公司画像、年报资产、券商风控、指数公告）在公共层落地后应无需再改。若接口必须变，请先给出兼容窗口，不要让各业务私自钉 `pdfium_native`。
