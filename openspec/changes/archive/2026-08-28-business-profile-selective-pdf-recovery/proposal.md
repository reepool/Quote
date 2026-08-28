## Why

公司画像当前在原生 PDF 文本不可用时缺少一条高效、可追溯的恢复路径。对字体映射损坏或扫描型年报直接逐页 OCR 会产生数十分钟到数小时的延迟，且可能把不完整文本送入语义分析；600036.SH 已证明目录和业务章节可以通过选择性 OCR 恢复，但需要由画像业务层控制范围和停止条件。

本变更建立“原生恢复优先、目录探测、业务章节选择性 OCR”的业务流程，并明确共享 PDF 模块必须提供的最小接口，使非原生年报可以自动恢复，同时避免全文 OCR。

## What Changes

- 为公司画像增加分阶段 PDF 恢复策略：原生质量判定、替代原生解析、目录探测、章节定位和业务章节 OCR。
- 目录探测只处理有限的候选页和有限时间预算；识别到足够章节证据后立即停止。
- 根据目录页码选择业务画像、供应链关系和商品暴露所需章节，不再对整份报告的所有异常页进行 OCR。
- 为 OCR 任务增加页级缓存、内容和参数身份、页码/方法/置信度/耗时 provenance，以及可中断续传的部分完成状态。
- 规定原生文本、替代原生文本和 OCR 文本不得无标识混合；关键章节恢复失败时 fail closed，不把乱码或残缺文本交给 LLM。
- 增加基于真实年报的质量、延迟和资源评估，记录 CPU/GPU 运行时能力；GPU 仅作为经过同语料验证的可选加速路径。
- 明确共享 PDF 模块需支持的接口：指定页解析、OCR 模式/预算、页级缓存身份、诊断和能力探测。PDF 模块不负责公司画像章节选择。

## Capabilities

### New Capabilities

- `business-profile-selective-pdf-recovery`: 公司画像对字体映射异常、低文本和扫描页的自动恢复、目录定位及选择性 OCR。
- `shared-pdf-page-recovery-contract`: 共享 PDF 解析模块向业务消费者提供指定页、OCR 预算、缓存和可审计结果的统一契约。

### Modified Capabilities

- `common-llm-gateway`: 无需求级行为变更；LLM 仍只消费通过质量门禁的章节文本，本变更不修改 LLM 调度协议。

## Impact

- 业务代码：`research/business_profile_pdf_artifacts.py`、画像章节选择和语义运行时，需要增加恢复状态、页选择和缓存消费逻辑。
- 共享 PDF 代码：`research/document_processing/pdf/` 需要支持指定页解析、目录探测/章节 OCR 模式、页级 provenance 和预算结果；保持原生 profile 为默认值。
- 配置：增加画像恢复预算、目录探测页上限、目标章节页扩展范围和 OCR 缓存目录配置；不得把 OCR 设为默认解析器。
- 运行环境：记录 PaddleOCR/PaddlePaddle、PDFium 和 CUDA/GPU 能力。Quote conda 保持 CPU PaddlePaddle；ComputeR 的 GRID P4 通过独立的 Paddle 3.3.1/3.7.0 worker 评估和启用，不能把 GPU wheel 混入 Quote 进程。
- 测试与数据：使用 hash 绑定的 600036.SH 字体映射异常样本及至少一个扫描/混合样本进行只读评估，不修改原始公告资产。
