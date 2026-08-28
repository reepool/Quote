## Context

公司画像目前先对年报做整份原生解析，并把原生异常页标记为 `ocr_required`。共享 PDF 模块已经提供 pypdf、pdf-inspector 和 PaddleOCR adapter，但画像侧还没有把这些能力组织成“先恢复、再定位、后选择性 OCR”的业务流程。600036.SH 的实测表明 OCR 可以恢复目录和正文，但 CPU 单页延迟约 29--48 秒；全文 OCR 不可行。

当前 Quote conda 进程使用 CPU 版 PaddlePaddle，不能在进程内加载 CUDA wheel。ComputeR 宿主机实际有可用的 GRID P4；GPU 只能通过版本匹配的隔离 worker、可写模型缓存和同语料 approval 启用。本变更不把 GPU 当作业务正确性的前置条件。

## Goals / Non-Goals

**Goals:**

- 在画像业务层实现原生解析质量判定、替代原生恢复、目录探测、章节定位和选择性 OCR。
- 只把质量通过且带完整 provenance 的目标章节文本交给 LLM。
- 通过页级缓存、预算、超时和可恢复状态控制 OCR 成本。
- 让 600036.SH 这类字体映射异常报告可以自动恢复；扫描型报告在预算内可恢复，超预算则明确停留在部分状态。
- 为 PDF 开发组提供稳定、最小且可测试的共享接口需求。

**Non-Goals:**

- 不把 OCR 设为默认 PDF 解析器。
- 不对全文做 OCR，不由 PDF 模块决定公司画像章节。
- 不在本变更中新增 GPU 基础设施、切换 Paddle 发行版或引入新的重量级 OCR 模型。
- 不把 OCR 文本直接提升为事实；字段抽取、单位换算、实体和商品映射仍由画像逻辑负责。

## Decisions

### 1. 分层恢复而不是 native→OCR 全文回退

画像侧按以下顺序执行：

1. pypdf 原生解析和页级质量检测；
2. 对字体映射异常页尝试 pdf-inspector 等替代原生解析；
3. 检查 PDF 书签/大纲和前 1--5 页低成本文本；
4. 仅对目录候选页运行 `toc_probe` OCR，识别到目录及至少两个业务章节后停止；
5. 用目录页码定位业务章节，扩展前后 1--2 页做边界校正；
6. 对选定页面运行 `section_extract` 或 `table_extract` OCR。

每层都有页数和总时限预算。任何层失败都返回明确状态，不静默把乱码继续送入语义分析。

### 2. 业务层选择页，PDF 模块执行页

画像模块维护章节别名和字段族到章节的映射；共享 PDF 模块只接受 `target_pages`、OCR mode、预算和 profile，返回有序页结果。这样 PDF 模块可被其他消费者复用，也不会把公司画像规则泄漏到技术层。

### 3. OCR 结果按内容和参数缓存

缓存键至少包含 PDF content hash、page number、engine/profile、OCR mode、DPI、模型版本和 parser config。缓存结果必须保存 text hash、extraction method、confidence、elapsed、页码和 engine provenance。已完成页可被后续任务重放，不重复下载或调用 OCR。

### 4. GPU 只做能力探测和可选 profile

启动时记录 Paddle 是否 CUDA compiled、设备、模型版本和缓存目录。只有离线评估满足中文字符、数字、章节命中、P95 延迟和资源门槛，才允许启用 GPU profile；否则继续使用 CPU 或明确失败。不会因为检测到 P4 就自动切换。

### 5. LLM 只消费完整目标章节

目录未解析、关键章节缺页、OCR 超出预算或质量不足时，语义阶段收到 `source_unrecoverable`/`partial_ocr`，不接收乱码或未标识的残缺文本。已有成功的原生页和 OCR 页可以在同一章节包中共存，但每页必须保留 method 和 provenance。

## Risks / Trade-offs

- [OCR 仍然较慢] → 目录探测和章节页严格限额；页面缓存和断点续传；对表格单独限额。
- [目录格式差异导致定位失败] → 优先使用书签、页标签和多组中文别名；失败时明确 `toc_unresolved`，不全文 OCR。
- [替代原生解析器输出不稳定] → 保留原生文本质量和哈希，替代结果也必须通过同一质量检测，不能静默覆盖。
- [GPU 环境差异] → GPU 仅通过独立 profile 和离线评估启用；CPU profile 始终可回滚。
- [OCR 文字识别错误] → 保存页级置信度和原文图像身份；LLM 只做语义归纳，程序继续执行字段、单位和证据门禁。

## Migration Plan

1. PDF 开发组先交付共享契约的兼容实现和能力探测；默认 profile 保持 `pypdf_native`。
2. 画像侧接入恢复状态机，先以 600036.SH 做只读 canary，不写生产事实。
3. 通过 600036、至少一个扫描样本和一个混合样本的质量/延迟门禁后，开启有限报告的选择性 OCR。
4. 失败时切回 `pypdf_native`，已有原始公告资产不变；删除或重建的仅是版本化 derived page artifact。
5. OCR 缓存目录、GPU profile 和预算通过配置发布，不修改原始 PDF。

## Open Questions

- PDF 开发组最终提供的是统一 `toc_probe`/`section_extract` mode，还是由画像侧用统一接口组合参数？本 change 以参数化统一接口为最低兼容要求。
- ComputeR 的 GRID P4 和 Paddle 3.3.1/3.7.0 隔离 worker 已完成能力探测；生产启用仍受新 profile/corpus/renderer-bound approval 和部署设备节点管理约束。
