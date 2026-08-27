## MODIFIED Requirements

### Requirement: Explicit recovery policy semantics

共享 PDF 模块 MUST 将 `target_pages` 定义为解析页白名单，并提供显式 `ocr_mode` (`none`, `toc_probe`, `section_extract`, `table_extract`) 与 `recovery_policy` (`native_first`, `selective_recovery`, `force_ocr`)。`native_first` MUST 执行有序 native chain 且不创建 OCR 任务；`selective_recovery` MUST 仅对目标页中所有 native engine 均技术质量失败的页创建 OCR 任务；`force_ocr` MUST 执行首个 native engine 以验证 PDF/page count 并保留诊断候选，MUST 跳过后续 native fallback，且 MUST 对所有有效目标页执行 OCR，不得因 native 文本可用而跳过；其 `target_pages` 为空时 MUST 拒绝请求。

#### Scenario: Native-first never creates OCR work
- **WHEN** 调用方使用 `recovery_policy=native_first`
- **THEN** 模块最多执行配置的有序 native chain，任何异常页都返回诊断而不创建 OCR 任务

#### Scenario: Selective recovery exhausts native engines first
- **WHEN** 调用方使用 `selective_recovery` 且某个目标页被前一个 native engine 判为不可用
- **THEN** 仅当该页所有配置的 native engine 均失败后才允许创建 OCR 任务

#### Scenario: Force OCR requires an explicit page set
- **WHEN** 调用方使用 `recovery_policy=force_ocr` 且 `target_pages` 为空
- **THEN** 模块拒绝请求并返回参数错误，不得隐式全文 OCR

#### Scenario: Force OCR does not short circuit on native text
- **WHEN** 调用方对非空目标页使用 `force_ocr` 且首个 native engine 返回可用文本
- **THEN** 模块仍对该目标页执行 OCR、不得运行后续 native fallback，并保留诊断所需的首个 native 候选

### Requirement: Bounded OCR modes

共享模块 MUST 支持至少 `toc_probe`、`section_extract` 和 `table_extract` 的可区分参数或等价预算配置，包含最大页数、最大文档耗时、DPI、batch size 和并发上限。GPU 与 CPU fallback MUST 共享同一个 effective page/document budget，不得按 runtime 重置；OCR 未配置或失败时 MUST 返回 typed diagnostic，不得静默回退到乱码文本。

#### Scenario: OCR runtime is unavailable
- **WHEN** 调用方请求 OCR 但 GPU worker 缺少 runtime，且 CPU fallback 未启用或不满足策略
- **THEN** 模块返回 `ocr_unavailable` 或 `ocr_failure` 诊断，且异常页保持不可用状态

#### Scenario: Mode and profile budgets are combined
- **WHEN** request 提供 mode budget 且 profile 也有预算
- **THEN** 每项有效预算取 request 与 profile 的较小值；`max_pages` 表示该 mode 允许进入 OCR 的页数，`max_page_seconds` 是每个物理页跨 GPU/CPU attempts 的独立上限

#### Scenario: CPU fallback cannot reset budget
- **WHEN** GPU 失败属于允许回退的类型但该页或文档的原始 effective budget 已耗尽
- **THEN** 模块保留 typed GPU failure，不启动 CPU attempt

### Requirement: Page-level provenance and cache identity

共享模块 MUST 为每个 native/OCR candidate 返回 actual engine、engine/runtime/model version、device、mode、confidence、elapsed、text hash、quality、diagnostics 和 cache identity 所需参数。不同 native chain/order、profile、模式、renderer/version、DPI、runtime/device、模型或 inference config MUST 生成不同缓存身份。

#### Scenario: Parser profile changes
- **WHEN** 同一 PDF 页从旧 `pypdf`/inspector profile 切换到 PDFium-first ordered chain
- **THEN** 缓存键发生变化，调用方不会误复用旧 native artifact

#### Scenario: OCR runtime changes device
- **WHEN** 同一页从 GPU worker 回退到版本匹配的 CPU worker
- **THEN** CPU candidate 使用不同 cache identity，结果保留 GPU failure 和 CPU selection provenance

#### Scenario: Optional cache backend
- **WHEN** 调用方注入实现 `get(cache_identity)` / `put(cache_identity, page_result)` 的 cache backend
- **THEN** 模块校验并复用成功缓存并返回 `cache_hit`；未注入 backend 时明确返回 `cache_miss`，不执行隐式持久化

#### Scenario: No usable candidate
- **WHEN** 所有 native candidates 和 OCR 都不可用或失败
- **THEN** 页面返回 `selected_method=none`、`selected_text=""`、`selected_usable_for_semantic=false`，候选诊断仍保留，禁止将异常 native 或数字残留文本作为选中文本

### Requirement: Capability and performance probe

共享模块 MUST 提供只读能力探测和冻结语料评估接口，至少报告 native/OCR runtime availability、CUDA 状态、模型/inference config、renderer/DPI、warm-up、页吞吐、P50/P95 和质量指标；评估不得下载或修改生产资产。GPU 能力探测 MUST 通过隔离 worker 执行，不得在 Quote 进程 import CUDA Paddle。

#### Scenario: GPU is not available
- **WHEN** 隔离 GPU worker 报告 CUDA 不可用、启动失败、模型不健康或进程崩溃
- **THEN** 探测报告返回 typed GPU unavailable/failed 结果，且仍可运行 native profile 和已配置的 CPU worker 评估

### Requirement: Native parser process isolation and parallel execution

共享 PDF 模块 MUST 将生产 `pypdfium2` 文本抽取、PDFium OCR 光栅化和 `pypdf` native fallback 放入共享模块管理的受监管 native worker 池；Quote 主进程不得直接执行这些 native 操作。worker 池 MUST 支持跨文档的有界多进程并行，但单个 worker 内部 PDFium/pypdf MUST 串行执行，不得再创建嵌套 PDFium 线程池。池宽度、队列上限、启动方式、worker 重启上限和页/文档超时 MUST 可配置；worker signal/退出、超时、协议错误和缺页 MUST 转换为 typed diagnostic，并保留已完成页。

#### Scenario: Native worker crash is contained

- **WHEN** native worker 因 `SIGTRAP`、`SIGSEGV`、`SIGABRT` 或其他非零退出终止
- **THEN** 父进程 MUST 保持运行、回收并有界替换该 worker，返回包含 signal/exit status 的 `native_worker_crashed`（或等价）诊断，并按既有 fallback policy 继续处理

#### Scenario: Native workers process documents in parallel

- **WHEN** 同时提交多个独立 PDF 且配置池宽度大于 1
- **THEN** worker 池 MUST 允许不超过配置宽度的跨文档并行，单个业务调用方不得创建额外 native 池

#### Scenario: OCR rasterization uses the native worker

- **WHEN** 页面进入 OCR
- **THEN** native worker MUST 完成 PDFium 渲染并把图片交给现有 OCR worker；Quote 主进程和 OCR worker 均不得重新打开 PDF

#### Scenario: GPU runtime version differs from approved baseline
- **WHEN** worker 报告的 Paddle、PaddleOCR、model 或 inference config 与 profile-bound approval 不一致
- **THEN** GPU profile fail closed，不能复用旧 approval artifact

## ADDED Requirements

### Requirement: Version-matched isolated GPU and CPU OCR workers

生产 GPU worker MUST 使用 `paddlepaddle-gpu==3.3.1` 与 `paddleocr==3.7.0`；CPU fallback worker MUST 使用 `paddlepaddle==3.3.1` 与相同 PaddleOCR/model/inference configuration。两者 MUST 位于 Quote conda 之外并由现有 `PaddleOcrAdapter` 边界调度。Paddle 2.6.2/OCR 2.7.3 实验环境 MUST NOT 作为生产 fallback 或第二套部署路径。

#### Scenario: GPU startup fails before inference
- **WHEN** GPU worker 启动、能力或模型健康检查失败，fallback policy 允许该 typed failure 且预算仍充足
- **THEN** 现有 adapter 调用版本匹配的隔离 CPU worker，并保留两个 runtime attempts

#### Scenario: GPU page times out by default
- **WHEN** GPU inference 超过页级 deadline 且 timeout 不在显式 fallback allowlist
- **THEN** 页面返回 `ocr_timeout`，不得静默在 CPU 重复执行

#### Scenario: GPU package conflicts with Quote libraries
- **WHEN** 部署或探活需要加载 CUDA Paddle
- **THEN** 加载 MUST 只发生在隔离 worker，避免在 Quote conda 触发已知 `libz`/`inflateReset2` 冲突

#### Scenario: P4 rejects an inference optimization path
- **WHEN** worker inference configuration 不匹配获批配置，或 worker 因 IR/fusion path 触发 `SIGILL`
- **THEN** worker MUST fail closed 并返回 typed crash/config diagnostic，Quote 进程不得崩溃或静默切换未验证的优化配置

### Requirement: PDFium rasterization is authoritative OCR input

共享模块 MUST 使用直接依赖的 `pypdfium2==5.13.0` 在受监管 native worker 边界按物理页渲染 OCR 输入，默认 150 DPI (`scale=dpi/72`) 且 DPI 可由 profile 配置。一次 OCR batch MUST 复用同一 PDFium document；GPU 与 CPU worker MUST 接收等价的已渲染输入，不得自行打开 PDF 或依赖 PyMuPDF；Quote 主进程不得直接执行 PDFium 渲染。

#### Scenario: Same page is compared on GPU and CPU
- **WHEN** GPU/CPU canary 处理同一 PDF 物理页
- **THEN** 两者接收相同 renderer/version、DPI 和 image configuration 的输入，并分别记录 device-specific provenance

#### Scenario: Lab latency uses a different renderer
- **WHEN** 外部 benchmark 使用 PyMuPDF 2.0x 渲染
- **THEN** 其耗时只能作为比较证据，不能直接充当 PDFium 150-DPI 生产 SLA

#### Scenario: PDFium native text is usable in a mixed document
- **WHEN** 文档被分类为 `mixed` 但目标页通过 PDFium native quality gate
- **THEN** `selective_recovery` 不渲染该页，也不调用 GPU 或 CPU OCR
