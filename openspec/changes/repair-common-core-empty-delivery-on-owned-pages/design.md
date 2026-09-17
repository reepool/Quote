## Context

2026-09-17 有界 `company_profile_common_core` 对 302132.SZ、600000.SH 完成了首次真实 run。独立核原文后 `company_profile_source_review.v1` 记录：source recall 0/7，accuracy unassessed，`expansion_gates_met=false`。生产仍为 `not_authorized`。

现场两条缺陷：

1. 中航成飞已定位「报告期内公司从事的主要业务」摘录（p11–12）和分行业收入表，scope 里已有官方原文，但 `provider=None` 时 workflow 写 `provider-unavailable`，`records=[]`。
2. 浦发银行官方有「经营范围 银行业务」和「3.6 公司主要业务情况」，heading index 也打到了 `主要业务`，但 `_owned_heading` 要求去前缀后的整行等于或仅以标题加标点开头；「公司主要业务情况」对不上「主要业务」，于是 `chapter_missing`。

现行 owner 已能选 Evidence，缺的是把已定位摘录收成 SemanticRecord，以及银行/服务标题。

## Goals / Non-Goals

**Goals:**

- 官方已定位、已可读、已写明字段的 overview/segment 摘录，不依赖 LLM 收成 accepted 事实。
- 银行/服务年报的「公司主要业务情况」和公司简介「经营范围」能定位 overview 章节。
- 仍走 `operations.py` → runtime → `company_profile_research_writer.v1` / `company_profile_read_service.v1`。
- 同一 2 家样本重跑后，核原文至少能评 source accuracy，不再因空交付把 recall 打成 0。

**Non-Goals:**

- 不启用生产、DCF、交易、旧 writer。
- 不启动 M4，不回填申万 L1，不把浦发从 `other` 改成 `finance`。
- 不新开 PDF/OCR/版面恢复，不另建采集循环或语义平台。
- 不把本次观察外推为规模质量。

## Decisions

1. **确定性投影挂在已有 Evidence 选择之后，不另建 writer。**  
   `select_core_evidence` 已经产出 owned span 和 PreparedEvidence。新增最小投影：从 excerpt 生成 `BusinessOverview` / `Activity` / `Segment` / `Measurement`，作为 `deterministic_candidates` 进入现有 semantic service。  
   备选是把空交付标成 failed：与「合格事实逐条交付、缺失不封锁」冲突，且会掩盖已定位摘录。

2. **只投影摘录已写明的字段，不猜。**  
   overview 用已有 `_PRINCIPAL_PATTERN` / `_PRODUCT_PATTERN` / 收入句式；segment 只收带标签和金额的分行业/分产品行。投影不出的字段才进 LLM；`provider=None` 时这些字段保持 uncovered，不得写成 provider-unavailable。

3. **标题匹配扩大到银行/服务常用行，不改 TOC 规则。**  
   增加「公司主要业务情况」「公司金融业务」「经营范围」等可拥有标题；去前缀后允许标题作为独立行或后接全角/半角标点。目录页和「…… + 页码」行继续排除。不把正文里偶然出现的「主要业务」当成标题。

4. **验收样本固定为本次观察的两家，不扩样本。**  
   修复证明用 302132.SZ 与 600000.SH 的官方页文本或已落盘 page artifact，不联网、不调 LLM。

## Risks / Trade-offs

- [过宽标题匹配误收目录或无关段] → 保留 TOC 排除；标题必须是独立 title line。  
- [投影把合计行当成收入模型] → 继续排除合计/抵消/无标签总额。  
- [空核心仍标 completed] → 本 change 不改任务完成语义；质量看 accepted facts 和核原文，不靠 completed 标志。  
- [只修两家形态] → 明确不覆盖申万分层；不够再另开 change。

## Migration Plan

研究命名空间就地重跑同一 2 家。不迁库、不改 job id、不改生产开关。回滚即还原确定性投影和标题表，旧空交付仍可读。

## Open Questions

无。申万 `sw_l1_name` 为空导致银行进 `other` 已记录，不在本 change 关闭。
