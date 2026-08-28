## ADDED Requirements

### Requirement: Business-owned recovery decisions

公司画像业务层 MUST 负责目录别名、字段族到章节的映射、印刷页码到物理页码的校正、章节边界扩展和业务质量门禁；共享 PDF 模块不得内置这些业务规则。目录失败时，画像可以自动安排有限的下一轮候选页或固定页范围重试，但不得默认全文 OCR。

#### Scenario: Business layer supplies a known page range
- **WHEN** 画像规则已确定某字段族的物理页范围
- **THEN** 画像以 `target_pages` 和明确 mode/policy 调用共享模块，并记录页码来源和边界扩展原因

#### Scenario: Unresolved TOC is retried within bounds
- **WHEN** 首轮目录探测返回 `toc_unresolved`
- **THEN** 画像可自动使用书签、page label、模板锚点或有限固定页范围重试；预算耗尽后转为 `source_unrecoverable`，不得提交乱码给 LLM

### Requirement: Native-first recovery state machine

公司画像 PDF 恢复流程 MUST 先使用原生解析和质量检测；仅当页面为空、低文本、字体映射异常或替代原生解析失败时，才进入后续恢复阶段。流程 MUST 返回可持久化的状态 `native_ready`、`alternate_native_ready`、`toc_probe_required`、`toc_unresolved`、`section_ocr_required`、`partial_ocr` 或 `source_unrecoverable`。

#### Scenario: Native report needs no OCR
- **WHEN** 原生解析识别出可用中文文本并能定位业务章节
- **THEN** 画像直接使用原生章节页，且 OCR 调用数为零

#### Scenario: Mapping-corrupt report uses alternate native before OCR
- **WHEN** 页面存在合法 Unicode 但质量检测判定字体映射异常
- **THEN** 系统先尝试已配置的替代原生解析器，只有替代结果仍不可用时才生成 OCR 任务

### Requirement: Bounded table-of-contents probing

画像 MUST 先检查书签/页标签和有限的前置候选页。目录 OCR MUST 使用独立的页数、分辨率和总时限预算；识别到目录标题及至少两个目标章节后 MUST 停止探测，不得继续全文 OCR。

#### Scenario: TOC is found early
- **WHEN** 第 1--5 页中的某页识别出目录且命中至少两个业务章节
- **THEN** 系统返回目录页和章节定位结果，并不再 OCR 其他目录候选页

#### Scenario: TOC cannot be found within budget
- **WHEN** 候选页耗尽或目录探测超时
- **THEN** 系统返回 `toc_unresolved`，不把整份报告加入 OCR 队列

### Requirement: Business-section selective OCR

画像 MUST 根据字段族和目录页码选择业务章节、供应链关系和商品暴露所需页面，并可对章节边界增加有限的前后页。只有选定页才允许进入 `section_extract` 或 `table_extract` OCR；财务报表、审计报告和治理章节 MUST NOT 因原生异常自动进入 OCR。

#### Scenario: Business sections are located
- **WHEN** 目录包含管理层讨论与分析、主营业务或分部经营业绩
- **THEN** 系统创建去重后的目标页集合并只为该集合创建 OCR work items

#### Scenario: Section boundary is uncertain
- **WHEN** 目录页码与正文页码存在偏移
- **THEN** 系统只扩展配置的边界页范围，并记录扩展原因和最终页集合

### Requirement: Quality-gated semantic input

LLM MUST only receive pages whose text has a valid page number, content hash, extraction method, and quality/provenance metadata. `native_text`、`alternate_native` 和 `ocr` 可在同一章节包中共存，但 MUST 保留逐页来源；乱码、空文本、OCR 失败或超预算的页面 MUST NOT 被静默替换为可用证据。

#### Scenario: OCR section passes quality gate
- **WHEN** 目标章节所有必需页 OCR 成功且置信度和文本质量达到配置门槛
- **THEN** 章节包可提交给 LLM，并携带完整页级 evidence metadata

#### Scenario: OCR is partial
- **WHEN** 任意必需页超时、失败或预算耗尽
- **THEN** 章节包状态为 `partial_ocr` 或 `source_unrecoverable`，语义阶段不提交该不完整章节

### Requirement: Resumable page cache and budgets

系统 MUST 使用内容 hash、页码、profile、OCR mode、DPI、模型版本和 parser config 生成页级缓存身份。OCR MUST 遵守每页、每份报告和队列预算；已完成页可续传复用，失败页保留诊断，不重复下载原始 PDF。

#### Scenario: Cached OCR page is reused
- **WHEN** 同一 PDF 页的 OCR 参数身份未变化且已有成功缓存
- **THEN** 系统直接读取缓存，不重新初始化模型或调用 OCR

#### Scenario: OCR budget is exceeded
- **WHEN** 页数或总时限预算耗尽
- **THEN** 系统停止新增 OCR work item，持久化已完成页和预算诊断，并返回 `partial_ocr`

### Requirement: Runtime capability evidence

系统 MUST 记录 OCR 引擎、模型版本、缓存目录、设备、CUDA 可用性、页级耗时和置信度。GPU profile MUST 只有在同语料质量和延迟门禁通过后才能启用；检测到 GPU 不得自动切换默认 profile。

#### Scenario: CPU-only worker
- **WHEN** PaddlePaddle 未编译 CUDA 或设备不可见
- **THEN** 系统使用 CPU profile，并在评估报告中记录 GPU unavailable，不影响原生解析

#### Scenario: GPU canary passes
- **WHEN** GPU profile 在冻结语料上通过中文、数字、章节命中、P95 和资源门禁
- **THEN** 系统允许对明确配置的 canary 报告启用 GPU OCR，否则保持 CPU profile
