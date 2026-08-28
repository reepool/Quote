## ADDED Requirements

### Requirement: Explicit recovery policy semantics

共享 PDF 模块 MUST 将 `target_pages` 定义为解析页白名单，并提供显式 `ocr_mode` (`none`, `toc_probe`, `section_extract`, `table_extract`) 与 `recovery_policy` (`native_first`, `selective_recovery`, `force_ocr`)。`native_first` 只执行 native 和 alternate-native，不创建 OCR 任务；`selective_recovery` 仅对目标页中技术质量失败的页创建 OCR 任务；`force_ocr` 允许目标页全部进入 OCR。`force_ocr` 在 `target_pages` 为空时 MUST 拒绝请求。

#### Scenario: Native-first never creates OCR work
- **WHEN** 调用方使用 `recovery_policy=native_first`
- **THEN** 模块最多执行 native/alternate-native，任何异常页都返回诊断而不创建 OCR 任务

#### Scenario: Force OCR requires an explicit page set
- **WHEN** 调用方使用 `recovery_policy=force_ocr` 且 `target_pages` 为空
- **THEN** 模块拒绝请求并返回参数错误，不得隐式全文 OCR

### Requirement: Explicit page parse contract

共享 PDF 模块 MUST 提供按页解析接口，调用方可以传入有序、去重的 `target_pages` 和明确 profile；未指定页时仍保持原生默认行为。返回结果 MUST 保留页码、文本、text hash、extraction method、quality status 和 diagnostics。

#### Scenario: Consumer requests selected pages
- **WHEN** 调用方传入第 2、38 页
- **THEN** 模块只返回这两页的解析结果，并保留原始文档 page_count；结果按物理页码升序返回

#### Scenario: Invalid and out-of-range pages
- **WHEN** `target_pages` 含非正整数或超过 page_count 的页
- **THEN** 非正整数请求被拒绝；越界页返回 `page_out_of_range` typed diagnostic，有效页继续处理，`requested_pages` 保留调用方去重前的顺序，`returned_pages` 只包含实际返回的物理页码

### Requirement: Bounded OCR modes

共享模块 MUST 支持至少 `toc_probe`、`section_extract` 和 `table_extract` 的可区分参数或等价预算配置，包含最大页数、最大文档耗时、DPI、batch size 和并发上限。OCR 未配置或失败时 MUST 返回 typed diagnostic，不得静默回退到乱码文本。

#### Scenario: OCR runtime is unavailable
- **WHEN** 调用方请求 OCR 但 worker 缺少 OCR runtime
- **THEN** 模块返回 `ocr_unavailable` 或 `ocr_failure` 诊断，且异常页保持不可用状态

#### Scenario: Mode and profile budgets are combined
- **WHEN** request 提供 mode budget 且 profile 也有预算
- **THEN** 每项有效预算取 request 与 profile 的较小值；`max_pages` 表示该 mode 允许进入 OCR 的页数，`max_page_seconds` 是每个物理页的独立上限

### Requirement: Page-level provenance and cache identity

共享模块 MUST 为每个 OCR 页返回 engine、model、model version、mode、confidence、elapsed 和 cache identity 所需参数。不同 profile、模式、DPI 或模型版本 MUST 生成不同缓存身份。

#### Scenario: Parser profile changes
- **WHEN** 同一 PDF 页从 native profile 切换到 OCR profile
- **THEN** 缓存键发生变化，调用方不会误复用旧 native artifact

#### Scenario: Optional cache backend
- **WHEN** 调用方注入实现 `get(cache_identity)` / `put(cache_identity, page_result)` 的 cache backend
- **THEN** 模块校验并复用成功缓存并返回 `cache_hit`；未注入 backend 时明确返回 `cache_miss`，不执行隐式持久化

#### Scenario: No usable candidate
- **WHEN** native、alternate-native 和 OCR 都不可用或失败
- **THEN** 页面返回 `selected_method=none`、`selected_text=""`、`selected_usable_for_semantic=false`，候选诊断仍保留，禁止将异常 native 文本作为选中文本

### Requirement: Optional structured table payload

`table_extract` MUST 至少返回与其他 mode 相同的选中文本和页级 provenance，并为未来结构化输出预留可选 `structured_payload` 与 `structured_format` (`markdown`, `html`, `json`) 字段。首轮实现可以为空；文本质量和业务表格门禁仍由画像层负责。

#### Scenario: Text-only table extraction
- **WHEN** 首轮 table adapter 未产生结构化载荷
- **THEN** 模块返回 `structured_payload=null`，但不影响文本、hash、方法、诊断和缓存身份

### Requirement: Capability and performance probe

共享模块 MUST 提供只读能力探测和冻结语料评估接口，至少报告 native/OCR runtime availability、CUDA 状态、模型 warm-up、页吞吐、P50/P95 和质量指标；评估不得下载或修改生产资产。

#### Scenario: GPU is not available
- **WHEN** CUDA 不可用
- **THEN** 探测报告明确返回 CPU-only，且仍可运行 native profile 的评估
