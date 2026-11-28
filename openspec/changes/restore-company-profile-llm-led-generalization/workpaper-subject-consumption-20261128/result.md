# 6.FW 主体口径契约澄清与研究消费交付

本卡完成：有原文依据的描述可继续读取，不能确认的事实主体标unclear并说明限制；错正文、原指标限制及历史失败不变。没有新增模型调用或新的自动准确率测量。

山东高速恢复的原描述包括2025年“5公里救援圈”“5分钟到岗指挥”及收费站外包服务。原PDF17页明确位于路桥运营板块，不能把标签当全公司口径。现在API返回limited、独立源核依据、`subject_scope_gap`及缺项/建议；主体不用于合并加总或比较。正文中的99.54%有据仍可引用，原标准化指标继续隔离，租赁收入和受托里程的原指标限制也保留。原文“公司运营管理”与“山东高速集团所属”描述不同关系，不将受托资产当自有。

[当前画像](../../../../data/research/company_profile_research_v1_20261123/600350-current-6fw-20261128.md) · [当前JSON](../../../../data/research/company_profile_research_v1_20261123/600350-current-6fw-20261128.json) · [A源判](a-source-adjudication.json) · [发布核对](a-release-result.json) · [B测试及接续修复](b-self-check.json)

原writer新增一份精确父链后继`bp-work-90d584ecf4f680518ae247e7`；原作者事实、证据、生成归因和模型审核均不修改。山东可读内容25→26、可见指标26保持，全39股合计1098/1687，其余38完整投影不变。旧402文件在发布时均保持，之后仅获准更新当前导航。读取和导出不调用模型。

验证与审核：

- 旧39股在新代码下完整投影等于原冻结快照，见[A兼容性](a-legacy-compatibility.json)。原reader/导出/新实例/Markdown完整一致，实际服务见[HTTP核对](a-live-api-audit.json)。
- 新直接测试31项；最终运行`python -m pytest -q tests/unit/test_research/test_company_profile_subject_consumption.py tests/unit/test_research/test_company_profile_metric_use.py tests/unit/test_research/test_company_profile_current_recheck.py`，69/69通过。更早合并回归113过/1失败，失败为既有enrichment查询选择冲突，原HEAD选择函数同样复现，不改旧测试。
- A Review修复两处接续P1：新限制不能在内容变化时消失；转整事实隔离须移除不能重绑的局部字段并保具体原因。旧无新字段的序列化/hash和显示兼容，整事实模型缺陷不能被主体限用放行。无剩余本卡阻塞问题；既有测试冲突另列为C类，不扩大本卡。
- OpenSpec严格校验、局部diff检查、数据/既有工作区保护及收尾见[A收口](a-final-close.json)。必要证据保留一套，删除本人临时库、重复导出、dirty备份和脚本；正式PDF/有效缓存/旧原件及并行改动保持。

这是独立源判支持的研究消费修正，不能称为v15自动理解提升、旧目标通过或总体99%。6.FV的0/2和6.FN/S2正式失败原样保留，95%/99%门槛、S3未启和生产边界不变；当前继续按具体研究需求使用及定向补读，不为单个标签重开付费试跑。
