# PDF 解析器评估（2026-08-25）

## 结论先行

当前项目不是“缺一个更快的 `pypdf`”，而是缺少在原生文本不足时可审计、可按页启用的 OCR adapter。现有画像链路已经把原生文本、页码、文本 hash、低文本页和 `ocr_required` 诊断作为契约；当前实现使用 `pypdf`，扫描页只会被标记，不会实际 OCR（见 `research/business_profile_pdf_artifacts.py` 和 `cninfo_corporate_action_llm_resolution_requirements.md`）。

建议采用两阶段方案：

1. **短期 PoC：`pdf-inspector` 作为原生文本/版面解析候选，并验证其选择性 OCR。** 它的 Rust 核心、Python binding、页级 OCR 路由、Markdown/JSON 输出和单次文档加载，最贴合当前“原生优先、异常页才 OCR”的路径。它发布很新，OCR 依赖 PDFium、ONNX Runtime 和模型文件，不能未经同语料验收就替代当前 `pypdf`。
2. **生产 OCR 候选：PaddleOCR PP-StructureV3 / PaddleOCR-VL，或 Docling 的 OCR 后端。** 若重点是中文年报、扫描表格和复杂版面，PaddleOCR 的识别与表格生态更强；若同时需要 PDF/DOCX/PPTX/XLSX/HTML 等多格式统一输出，Docling 更合适，但两者都明显重于当前 Python 进程。

`OCRmyPDF` 适合作为“先生成可搜索 PDF/A”的离线预处理，不适合作为当前页级结构化抽取器；`pdfplumber`、`pdfminer.six`、PyMuPDF、Marker、MinerU 和 Unstructured 各有价值，但不应被误认为同一种替换方案。

## 当前项目约束

| 约束 | 现状/要求 |
|---|---|
| 原生文本 | `pypdf.PdfReader` 逐页 `extract_text()`；保留页级文本和 hash |
| OCR 触发 | 低文本、抽取异常或字体解码异常时设置 `ocr_required` |
| 质量门禁 | OCR 低质量、引用不精确、缺页等必须 fail closed，不能静默当作无披露 |
| 业务证据 | LLM 只能引用页级文本、页码和 hash；原始 PDF 不被覆盖 |
| 并发 | 文档/OCR 解析预算已有上限，生产链路需要可控 CPU/内存 |
| 依赖 | 当前 `requirements.txt` 只声明 `pypdf>=4.0.0`，没有 OCR runtime |

因此，候选解析器必须能返回：页级文本、页码、提取方式、错误/警告、可选坐标或表格结构、版本和输入 hash。仅输出一个 Markdown 字符串的库不满足生产证据契约。

## 候选分层

评分采用 1（弱）到 5（强），是针对本项目的工程适配度，不是所有 PDF 的绝对准确率。准确性分为原生文本/版面与 OCR 两部分；公开 benchmark 只作为方向性证据，最终仍需用项目自己的中文年报、公告和扫描件冻结集验收。

| 候选 | 原生/版面 | OCR | 速度/效率 | 输出广度 | 维护与部署 | 适配判断 |
|---|---:|---:|---:|---:|---:|---|
| **pdf-inspector 1.15.x** | 4.5 | 3.5 | 5 | 4 | 3 | 最值得先做 PoC；快速 Rust 原生解析 + 选择性 OCR，但项目很新、OCR native runtime 较复杂 |
| **PyMuPDF 1.28.x + PyMuPDF4LLM** | 4.5 | 2 | 5 | 4 | 5 | 成熟、很快、坐标/渲染能力强；OCR 仍需 Tesseract/Paddle 等外部组件；注意 PyMuPDF 的 AGPL/商业授权 |
| **Docling 2.121.x** | 4.5 | 4 | 2.5 | 5 | 4.5 | 统一文档模型和 Markdown/HTML/JSON/DocTags 等输出很强；模型、内存和启动成本高 |
| **PaddleOCR 3.7 / PP-StructureV3 / PaddleOCR-VL** | 4 | 5 | 3（CPU）/4（GPU） | 4.5 | 4.5 | 中文 OCR、表格、公式、图表能力最完整；更像文档 AI/OCR 平台，不是轻量 PDF reader |
| **MinerU 3.4.x** | 4.5 | 4.5 | 2.5 | 4.5 | 4 | 中文 PDF 转 Markdown 很强，适合离线批处理；依赖和模型重，需核对许可证与商业分发边界 |
| **Marker 2.x** | 4.5 | 4.5 | 3.5 | 4 | 4 | 复杂 PDF 转 Markdown 的高质量候选；偏模型流水线，资源和版本耦合较高 |
| **OCRmyPDF 17.x + Tesseract** | 2（不负责结构） | 4 | 3.5（可多核） | 2（输出仍是 PDF/PDF-A） | 5 | 稳定的 OCR 预处理和归档工具；不能直接替代页级表格/版面 parser |
| **pdfplumber + pdfminer.six** | 4 | 1 | 2.5 | 3.5（文本/CSV/JSON/表格） | 4 | 适合可解释的字符坐标和规则表格抽取；官方明确更适合 machine-generated PDF，不含 OCR |
| **pypdf（当前）** | 3 | 1 | 3 | 2.5 | 5 | 轻量、纯 Python、兼容现有契约；不解决扫描件和高级版面 |
| **Unstructured** | 3.5 | 3（后端依赖） | 2.5 | 4 | 4 | 路由/元素抽象层，实际效果取决于后端（pdfminer、OCR、hi_res 等）；不建议作为唯一核心引擎 |

### 1. pdf-inspector（重点评估）

项目地址：<https://github.com/firecrawl/pdf-inspector>

- Rust 核心做 PDF 类型检测、字体/ToUnicode 解码、坐标感知文本、阅读顺序、表格和 Markdown；提供 Python、Node、Rust、WASM 入口。
- 默认纯原生解析，不加载 OCR；可识别 TextBased、Scanned、ImageBased、Mixed，并返回需要 OCR 的页号。
- 1.15.0 的 OCR 路径使用 PDFium 渲染和 PP-OCRv6 Small/ONNX Runtime，只处理被路由的页，返回页级来源、置信度、耗时和 warning。PDFium、ONNX Runtime 和模型缓存仍是外部运行时要求。
- README 自报的 opendataloader-bench（200 个 PDF、无模型 OCR、M4 Pro）结果为：Overall 0.875、Reading Order NID 0.915、Tables TEDS 0.814、速度 0.470 秒；这是上游自测、英文/混合公开语料，不能直接推断中文年报效果。
- 优点是与当前设计的“原生优先 + 受影响页 OCR”高度一致，且单次加载文档避免检测和抽取重复 I/O。
- 风险是项目创建于 2026 年、生产历史短；Python OCR wheel 的跨平台 native 依赖、模型版本和 ABI 需要在本项目部署环境中锁定。它是 parser/router，不应单独承担业务事实的 OCR 质量门禁。

**判断：首选 PoC，不立即切生产主解析器。** 先用同一份冻结语料对比 `pypdf`、pdf-inspector 原生文本、pdf-inspector OCR，重点看中文字符准确率、页码稳定性、表格行列和混合 PDF 的路由误报。

### 2. PyMuPDF 与 PyMuPDF4LLM

PyMuPDF 基于 MuPDF，原生文本、坐标、渲染、页面操作和多种导出都很成熟，通常是 Python 生态中最快的通用 PDF 引擎之一。它本身不是 OCR 引擎；官方安装说明把 Tesseract 作为外部 OCR 依赖，PyMuPDF4LLM 主要改善 Markdown/JSON/RAG 输出。适合做低延迟原生 fallback 或高质量页面渲染，不适合单独填补当前 OCR 缺口。另需评估 AGPL-3.0 或 Artifex 商业授权对部署方式的影响。

### 3. Docling

Docling 提供统一 `DoclingDocument`，支持 PDF、DOCX、PPTX、XLSX、HTML、图片等，并可导出 Markdown、HTML、DocTags、lossless JSON 等；支持 layout、reading order、table、formula 和 OCR，支持本地/离线运行。它适合未来需要“多格式文档理解”的平台场景，但对当前仅处理公告 PDF 的链路过重：模型下载、启动时间、内存峰值和版本迁移都需要单独预算。更适合作为离线深度解析 worker，而不是直接放入 API/同步任务。

### 4. PaddleOCR

PaddleOCR 3.x 的 PP-OCRv6、PP-StructureV3 和 PaddleOCR-VL 覆盖中文/多语言 OCR、表格、公式、图表和 Markdown/JSON；官方宣称 PP-OCRv6 有 CPU/OpenVINO 加速，PaddleOCR-VL 适合复杂文档元素识别。它是本项目最现实的 OCR 后端候选，尤其适用于中文扫描年报和表格，但需要处理模型缓存、Paddle/Paddle Inference 版本、CPU/GPU 资源、页图渲染和结果坐标到业务 artifact 的映射。建议先接 PP-OCR/PP-Structure 的确定性流水线，再评估更重的 VLM。

### 5. MinerU 与 Marker

两者都是面向复杂 PDF 的模型/规则混合转换器，通常可输出 Markdown，并处理版面、表格、公式和 OCR。MinerU 对中文 PDF 和长文档场景很有吸引力；Marker 2 强调速度、CPU 支持和 Surya OCR。它们更适合批量离线“文档转 Markdown/JSON”，不适合直接替换当前细粒度、页级、hash 绑定的 artifact extractor；需要额外编写页级 provenance、警告和 fail-closed 适配层。两者的模型/依赖体积也显著高于当前项目。

### 6. OCRmyPDF、pdfplumber、pdfminer.six、Unstructured

- **OCRmyPDF**：调用 Tesseract，为扫描 PDF 添加可搜索文本层、支持 PDF/A、旋转/去歪、多核和 100+ 语言。输出是新的 PDF，不是结构化表格/版面 JSON；可作为离线预处理或归档增强，不应当作 parser。
- **pdfplumber/pdfminer.six**：字符、线、矩形、坐标和规则表格抽取可解释性好；pdfplumber 文档明确说明更适合 machine-generated、非扫描 PDF。两者都不提供 OCR，速度和复杂表格鲁棒性也不如 Rust/C 引擎。
- **Unstructured**：提供按元素拆分和多后端路由，能接 PDF/OCR/hi-res 方案并输出元素 JSON/Markdown 等；部署和效果受后端与可选依赖影响，且应审查默认 telemetry 配置。它更适合作为编排层，不是本项目需要的单一解析核心。

## 输出格式与当前契约的匹配

| 需求 | 最合适候选 | 备注 |
|---|---|---|
| 页级原生文本 + 坐标 | pdf-inspector、PyMuPDF、pdfplumber | 需自行包装为现有 artifact schema |
| 混合 PDF 只 OCR 必要页 | pdf-inspector、Docling、PaddleOCR 自建路由 | pdf-inspector 的路由模型最贴近当前逻辑 |
| 中文扫描件/表格 | PaddleOCR PP-Structure、MinerU、Docling | 必须保留 OCR confidence 和原图/页码引用 |
| Markdown/JSON | pdf-inspector、Docling、MinerU、Marker、PaddleOCR | Markdown 不能替代页级证据字段 |
| PDF/A 可搜索归档 | OCRmyPDF | 作为预处理，不是结构化解析 |
| 多格式（PDF/DOCX/PPTX/XLSX/HTML） | Docling、Unstructured、PaddleOCR 3.x | 当前需求未要求，不应因此扩大范围 |

## 推荐架构

保持现有 `BusinessProfilePdfArtifactExtractor` 作为业务 owner，增加窄接口而不是在入口复制解析逻辑：

```text
verified PDF bytes/hash
        |
        v
native extractor (pypdf 或 pdf-inspector)
        |
        +--> usable page artifact ----------+
        |                                   |
        +--> ocr_required pages             v
                         OCR adapter (PaddleOCR 或 pdf-inspector OCR)
                                   |
                                   v
                 versioned OCR page artifact + confidence/warnings
```

第一阶段只需支持：检测、页级文本、页号、提取方式、置信度/警告、hash、失败分类和 Markdown/JSON 调试输出；不改变原始 PDF，不让 OCR 结果绕过现有 promotion/fail-closed gate。

## 建议的验收基准

建立不联网也能运行的冻结集，至少包含：

- 10 份可复制中文年报/公告（双栏、脚注、表格、跨页表格）；
- 10 份纯扫描 PDF（中文、数字、印章、低分辨率、倾斜）；
- 10 份 mixed PDF（部分页面原生、部分页面扫描）；
- 5 份字体编码异常或 CID 文本 PDF；
- 5 份加密、空页、损坏或超长 PDF 边界样本。

至少记录：

1. **准确性**：字符/数字 CER，日期和公司代码 exact match，heading 命中，表格单元格/行列 F1，跨页表格连续性，OCR 低置信度召回。
2. **效率**：冷启动、warm run、每页和每文档 P50/P95，CPU 秒、峰值 RSS、模型加载时间、并发 1/4/8 的吞吐和失败率。
3. **证据兼容性**：页码稳定性、文本 hash 可复现、原生/OCR provenance、重跑幂等、异常是否 fail closed。
4. **运维**：Linux wheel/容器可复现性、离线模型缓存、许可证、升级频率、崩溃恢复和外部 native runtime 数量。

建议的切换门槛：原生文本页不低于当前 `pypdf` 的 exact-match 基线；OCR 页数字/日期 CER 至少比 Tesseract 基线降低 30%；P95 资源消耗不超过现有解析预算的 2 倍；所有 OCR 结果都带页级 provenance、confidence 和 warning，不能出现静默空结果。

## 最终建议

- **现在就做**：以 `pdf-inspector` 1.15.x 做原生解析 + 选择性 OCR 的短期 PoC；并行以 PaddleOCR PP-StructureV3 做中文扫描件 OCR 对照。
- **生产首选路径**：若 PoC 通过，采用 `pdf-inspector` 做检测/原生页级抽取，OCR 后端优先选择经基准验证的 PaddleOCR；若 pdf-inspector OCR runtime 在部署上足够稳定，也可先用其一体化路径，但仍保留 PaddleOCR 作为对照/回退。
- **保留**：`pypdf` 作为轻量兼容 fallback、损坏/加密诊断和回归基线；不要因引入新引擎删除它。
- **暂不引入**：Docling、MinerU、Marker 作为生产同步链路默认依赖。只有当业务明确需要多格式统一解析或离线大批量 Markdown/JSON 转换时，再单独建设 worker。

## 补充：项目模块化现状、目标设计与替换工作量

### 1. 当前是否已有独立通用 PDF 模块？

**结论：没有。** 当前是“共享下载/归档 + 业务各自解析”的状态，而不是一个全项目通用 PDF 模块。

已经共享的部分：

- `research.announcements` / `research.announcement_assets` 统一公告查询、附件取回、签名、SHA-256、不可变归档和年报资产访问；
- `data_sources/cninfo_corporate_action_documents.py` 有 `extract_pdf_pages()` 和 `CorporateActionOcrAdapter` 协议，但它只服务 CNInfo 公司行动链；
- `research/business_profile_pdf_artifacts.py` 有较完整的页级 artifact、文本 hash、低文本页、字体解码诊断和 `ocr_required`，但它是公司业务画像域的专用实现；
- `research/business_profile_pdf_benchmark.py` 是画像 parser 的 benchmark，不是全项目合同。

仍然存在的业务内直接解析点至少包括：

| 位置 | 业务用途 | 当前做法 |
|---|---|---|
| `data_sources/cninfo_corporate_action_documents.py` | 上市公司权益/公司行动公告 | `pypdf` 页文本；可注入 OCR，但当前没有生产 adapter |
| `research/business_profile_pdf_artifacts.py` | 年报/公告业务画像 | `pypdf` 页级 artifact 和 OCR-required 诊断 |
| `research/broker_risk_control.py` | 券商风控报告/年报嵌入风控表 | 自己调用 `pypdf` 汇总文本，再做业务行解析 |
| `data_sources/official_index_source.py` | 国证/中证指数生命周期公告 | 模块级 `PdfReader`，全文拼接后按业务关键词解析 |
| `data_sources/hkex_instrument_master.py` | 港交所停牌报告 | 自己读取 PDF 后再解析报告行 |
| `research/announcement_assets/classifier.py` | 年报摘要/全文筛选 | 直接读首个 PDF 页面文本 |

所以，当前不是完全没有复用，而是复用层级停在“附件和归档”以及少量业务局部 seam；文本提取、页质量、OCR 路由、表格/坐标和失败语义没有一个权威 owner。现有文档也明确写着：年报资产共享，但各业务继续拥有自己的 parser、OCR、LLM 和事实逻辑。这种边界能避免业务互相写库，却无法解决 OCR 能力重复建设和解析行为不一致。

### 2. 推荐的 PDF 功能模块设计

建议新增一个**窄的、技术通用但不承载金融语义**的 PDF 处理模块，例如 `research/document_processing/pdf/`。它只负责从已验证的 PDF bytes 中得到可审计的页级结果；公告分类、权益条款、主营业务、风控字段和指数生命周期仍由各自业务 owner 负责。

推荐的最小合同：

```text
PdfParseRequest
  content_hash / bytes
  target_pages (optional)
  native_mode / ocr_mode
  parser_config_version

PdfPageResult
  page_number
  text
  extraction_method: native_text | ocr
  quality_status: usable | low_quality | failed
  confidence (optional)
  blocks / coordinates / tables (optional)
  warnings / diagnostics
  text_hash / page_result_hash

PdfDocumentResult
  page_count
  pages
  status
  native_engine_version
  ocr_engine_version
  input_hash / parameter_hash
  document_diagnostics
```

模块内部只保留三层：

1. **Native adapter**：读取页树、原生文本、坐标和字体/解码诊断；默认先保留 `pypdf`，把 `pdf-inspector` 作为可切换 adapter 做同语料 benchmark。
2. **Router**：根据 native 文本质量、字体异常、页范围和业务要求，只把必要页面送 OCR；不把整份 PDF 无条件 OCR。
3. **OCR adapter**：接收 PDF bytes + 页号，返回页级文本、置信度、耗时、模型版本和 warning；结果不可覆盖原始 PDF，必须绑定输入 hash 和页号。

外部模块建议：

- **第一生产组合**：保留 `pypdf` 作为稳定 native baseline，接入 **PaddleOCR PP-OCR/PP-Structure** 作为中文 OCR 和表格后端。这样替换范围小、中文扫描件能力强，也不把新 Rust 项目直接放进所有生产路径。
- **性能/版面优化组合**：验证通过后增加 **`pdf-inspector`** 作为 native detector/layout adapter；它负责快速识别 TextBased/Scanned/Mixed、位置感知文本和选择性 OCR 路由，但初期 OCR 仍可由 PaddleOCR 承担。这样即使 pdf-inspector 的 OCR runtime 或模型 ABI 变化，也不会影响业务 OCR 合同。
- **保留的 fallback**：`pypdf` 不删除，用于兼容、损坏/加密诊断和回归基线。PyMuPDF 可作为独立渲染 fallback 评估，但 AGPL/商业授权需要先确认，不作为默认依赖。

不建议把 Docling、MinerU、Marker 或 Unstructured 放进这个核心模块的同步默认路径。它们可另建离线 `deep_document_worker`，输出 Markdown/JSON/表格候选，再由业务按页证据合同引用；否则会把模型加载、内存峰值和版本治理强行带入所有公告解析。

业务侧迁移方式应是：

```text
公告/年报资产 owner
  -> shared PdfDocumentResult
  -> business selector / table parser / semantic validator
  -> existing business artifact and fact gates
```

业务模块不能再直接 import `pypdf`，也不能共享业务规则。首批迁移顺序建议为：

1. CNInfo 公司行动（已有 OCR adapter seam，最容易验证端到端）；
2. 公司业务画像（已有最完整 artifact/hash/质量模型）；
3. 券商风控（需保留固定顺序表格 fallback）；
4. 指数生命周期、HKEX 停牌和年报摘要分类（低风险薄迁移）。

### 3. 替换工作量估算

这里的“替换”不是把所有 `PdfReader` 机械改成另一个类，而是建立共享合同、接入 OCR、迁移调用方并验证金融证据语义。以下为一名熟悉项目的工程师、已有测试基础上的粗估：

| 阶段 | 工作内容 | 估算 |
|---|---|---:|
| 现状冻结与基准集 | 盘点 6 个生产解析点，准备原生/扫描/mixed/乱码/加密样本和人工 gold | 2–4 人日 |
| 共享 PDF 合同 | `PdfPageResult`/`PdfDocumentResult`、hash、版本、诊断、兼容序列化 | 3–5 人日 |
| Native adapter | 先封装 pypdf；加入 pdf-inspector 可切换 adapter 和 benchmark | 3–5 人日 |
| OCR adapter | PDF 页渲染、PaddleOCR 模型加载/缓存、中文识别、confidence、失败分类 | 5–8 人日 |
| CNInfo 迁移 | 公司行动页选择、OCR 质量和 LLM evidence 引用回归 | 3–5 人日 |
| 业务画像迁移 | 保留现有 artifact schema，接入共享页结果和 OCR artifact | 3–5 人日 |
| 风控/指数/HKEX/摘要迁移 | 适配表格/全文/首屏等不同调用语义，删除直接 `pypdf` | 3–6 人日 |
| 测试与性能 | CER/日期数字 exact match、表格、P95、并发、幂等和 fail-closed | 4–7 人日 |
| 部署与灰度 | 模型/运行时锁定、离线缓存、资源上限、canary 和回滚 | 3–5 人日 |

因此：

- **只做 PoC（共享接口 + CNInfo OCR + 画像一条链）**：约 **8–15 人日**，通常 1.5–3 周日历时间；不改变其他业务。
- **完成推荐的生产增量改造（pypdf + PaddleOCR，共享模块，迁移全部已发现调用点）**：约 **25–40 人日**，通常 4–7 周日历时间，取决于扫描样本和部署环境。
- **同时把 pdf-inspector 作为正式 native/layout 主引擎，并完成全量回归**：在上述基础上增加 **8–15 人日**；若还要引入 Docling/MinerU/Marker 的离线多格式 worker，应另计 **15–30 人日**，不应混入本次 PDF 核心替换。

主要不确定性不是接口改写，而是：中文扫描件数字/日期准确率、跨页表格坐标映射、Paddle/PDFium/ONNX native runtime 在生产机器上的资源与离线安装、以及现有业务对“空文本/低质量文本”的隐含假设。建议先完成 8–15 人日 PoC，再根据冻结基准决定是否进入 25–40 人日的全量迁移。

## 来源与可复核链接

- 当前项目：`research/business_profile_pdf_artifacts.py`、`docs/development/cninfo_corporate_action_llm_resolution_requirements.md`、`requirements.txt`
- pdf-inspector：<https://github.com/firecrawl/pdf-inspector>、<https://github.com/firecrawl/pdf-inspector/releases/tag/v1.15.0>
- PyMuPDF：<https://github.com/pymupdf/PyMuPDF>
- Docling：<https://github.com/docling-project/docling>
- PaddleOCR：<https://github.com/PaddlePaddle/PaddleOCR>
- MinerU：<https://github.com/opendatalab/MinerU>
- Marker：<https://github.com/VikParuchuri/marker>
- OCRmyPDF：<https://github.com/ocrmypdf/OCRmyPDF>
- pdfplumber：<https://github.com/jsvine/pdfplumber>
- pdfminer.six：<https://github.com/pdfminer/pdfminer.six>
- Unstructured：<https://github.com/Unstructured-IO/unstructured>
- pdf-inspector 上游原生解析 benchmark（README 中的 200-PDF 结果）：<https://github.com/firecrawl/opendataloader-bench/tree/abi/pdf-parser-benchmark-results>
