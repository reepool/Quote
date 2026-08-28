## MODIFIED Requirements

### Requirement: Explicit recovery policy semantics

共享 PDF 模块 MUST 将 `target_pages` 定义为解析页白名单，并提供显式 `ocr_mode` (`none`, `toc_probe`, `section_extract`, `table_extract`) 与 `recovery_policy` (`native_first`, `selective_recovery`, `force_ocr`)。`native_first` MUST 执行有序 native chain 且不创建 OCR 任务或探测 OCR runtime；`selective_recovery` MUST 仅对目标页中所有 native engine 均技术质量失败且未命中有效页缓存的页创建 OCR 任务并惰性探测所选 runtime；`force_ocr` MUST 执行首个 native engine 以验证 PDF/page count 并保留诊断候选，MUST 跳过后续 native fallback，且 MUST 对所有有效目标页执行 OCR，不得因 native 文本可用而跳过；其 `target_pages` 为空时 MUST 拒绝请求。

#### Scenario: Native-first never creates OCR work
- **WHEN** 调用方使用 `recovery_policy=native_first`
- **THEN** 模块最多执行配置的有序 native chain，任何异常页都返回诊断而不创建 OCR 任务、探测 OCR runtime 或等待 OCR worker

#### Scenario: Selective recovery exhausts native engines first
- **WHEN** 调用方使用 `selective_recovery` 且某个目标页被前一个 native engine 判为不可用
- **THEN** 仅当该页所有配置的 native engine 均失败且没有有效 OCR cache entry 后才允许创建 OCR 任务和探测 runtime

#### Scenario: Force OCR requires an explicit page set
- **WHEN** 调用方使用 `recovery_policy=force_ocr` 且 `target_pages` 为空
- **THEN** 模块拒绝请求并返回参数错误，不得隐式全文 OCR 或探测 OCR runtime

#### Scenario: Force OCR does not short circuit on native text
- **WHEN** 调用方对非空目标页使用 `force_ocr` 且首个 native engine 返回可用文本
- **THEN** 模块仍对该目标页执行 OCR、不得运行后续 native fallback，并保留诊断所需的首个 native 候选

### Requirement: Bounded OCR modes

共享模块 MUST 支持至少 `toc_probe`、`section_extract` 和 `table_extract` 的可区分参数或等价预算配置，包含最大页数、最大文档耗时、DPI、batch size 和并发上限。GPU readiness probe、GPU inference 与 CPU fallback MUST 共享同一个 effective document budget，不得按阶段或 runtime 重置；OCR 未配置、探活失败或执行失败时 MUST 返回 typed diagnostic，不得静默回退到乱码文本。

#### Scenario: GPU runtime is unavailable before inference
- **WHEN** 已选定未缓存 OCR 页，但 GPU worker command 缺失，或隔离 probe 报告 runtime/CUDA/model/cache 不可用
- **THEN** 模块 SHALL 产生 `ocr_runtime_unavailable` diagnostic，并在现有 allowlist 与剩余预算允许时调用已配置的版本匹配 CPU fallback
- **AND** 未启用、不允许或失败的 fallback SHALL 使异常页返回 `quality_status=ocr_unavailable` 与 `selected_method=none`，不得误标为 `ocr_empty` 或 native failure

#### Scenario: Native pages survive unavailable OCR recovery
- **WHEN** mixed/selective 请求中的部分页已选中可用 native 文本，而其他页的 GPU probe 与允许的 fallback 均失败
- **THEN** 已选中 native 页及其 hash MUST 保留，失败页 MUST 保留所有 candidates/diagnostics，文档 MUST 返回显式 partial 结果而不是丢弃整份文档

#### Scenario: Force OCR runtime is unavailable
- **WHEN** `force_ocr` 页保留了可用 native 诊断候选但 GPU probe 与允许的 fallback 均失败
- **THEN** 该 native candidate MUST 保留但不得重新成为 selected text，页面 MUST 返回 `selected_method=none` 和 typed OCR unavailable diagnostic

#### Scenario: Mode and profile budgets are combined
- **WHEN** request 提供 mode budget 且 profile 也有预算
- **THEN** 每项有效预算取 request 与 profile 的较小值；`max_pages` 表示该 mode 允许进入 OCR 的页数，`max_page_seconds` 是每个物理页跨 GPU/CPU attempts 的独立上限

#### Scenario: Probe and CPU fallback cannot reset budget
- **WHEN** GPU readiness 或 inference 失败属于允许回退的类型
- **THEN** probe elapsed、render elapsed、GPU attempt 与 CPU attempt MUST 共享原始 effective document deadline
- **AND** 当该 deadline 已耗尽时模块 MUST 保留 typed GPU/budget failure，不启动 CPU attempt

### Requirement: Page-level provenance and cache identity

共享模块 MUST 为每个 native/OCR candidate 返回 actual engine、engine/runtime/model version、device、mode、confidence、elapsed、text hash、quality、diagnostics 和 cache identity 所需参数。不同 native chain/order、profile、模式、renderer/version、DPI、runtime/device、模型或 inference config MUST 生成不同缓存身份。GPU readiness 失败后发生的配置化 CPU fallback MUST 保留 primary failure、fallback runtime 与 fallback reason，不改变对外页级 schema。

#### Scenario: Parser profile changes
- **WHEN** 同一 PDF 页从旧 `pypdf`/inspector profile 切换到 PDFium-first ordered chain
- **THEN** 缓存键发生变化，调用方不会误复用旧 native artifact

#### Scenario: OCR runtime changes device after lazy probe failure
- **WHEN** 同一页因 GPU `ocr_runtime_unavailable` 回退到版本匹配的 CPU worker
- **THEN** CPU candidate 使用不同 cache identity，结果保留 `ocr_primary_runtime_failed`、GPU diagnostic、`fallback_from_runtime` 与 `fallback_reason`

#### Scenario: Optional cache backend
- **WHEN** 调用方注入实现 `get(cache_identity)` / `put(cache_identity, page_result)` 的 cache backend
- **THEN** 模块校验并复用成功缓存并返回 `cache_hit`；未注入 backend 时明确返回 `cache_miss`，不执行隐式持久化

#### Scenario: Cache hit requires no live worker
- **WHEN** 页面存在匹配 profile、runtime、renderer、model 和 parser configuration 的有效已选中 OCR cache entry
- **THEN** 模块 MUST 在不探测 GPU/CPU worker 的情况下返回该页与原 provenance

#### Scenario: No usable candidate
- **WHEN** 所有 native candidates 和 OCR 都不可用或失败
- **THEN** 页面返回 `selected_method=none`、`selected_text=""`、`selected_usable_for_semantic=false`，候选诊断仍保留，禁止将异常 native 或数字残留文本作为选中文本

### Requirement: Capability and performance probe

共享模块 MUST 提供只读能力探测和冻结语料评估接口，至少报告 native/OCR runtime availability、CUDA 状态、模型/inference config、renderer/DPI、warm-up、页吞吐、P50/P95 和质量指标；评估不得下载或修改生产资产。GPU 能力探测 MUST 通过隔离 worker 显式执行，不得在 Quote 进程 import CUDA Paddle。生产 router 构造不执行 live probe，不得削弱 evaluator/canary 的显式 runtime gate。

#### Scenario: GPU is not available during explicit evaluation
- **WHEN** evaluator 显式探测的隔离 GPU worker 报告 CUDA 不可用、启动失败、模型不健康或进程崩溃
- **THEN** 探测报告返回 typed GPU unavailable/failed 结果且 GPU canary fail closed
- **AND** 仍可运行 native profile 和已配置的 CPU worker 评估

#### Scenario: Candidate GPU profile is evaluated before approval
- **WHEN** evaluator 使用 approval bypass 构造候选 GPU profile/router
- **THEN** bypass MUST 仅跳过尚未生成的静态 approval artifact
- **AND** evaluator MUST 显式执行 runtime capability/model-health probe，不得把 side-effect-free router construction 当成 runtime 通过
