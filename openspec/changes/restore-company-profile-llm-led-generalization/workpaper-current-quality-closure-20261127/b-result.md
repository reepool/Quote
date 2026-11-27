# 6.FS B交接：当前39股快照、固定抽样与三层映射

本卡B范围完成，A负责独立语义审核、规范和Git。本轮为保存结果的零调用本地核查，没有新生成公司画像、没有发布/修改record，也不重算6.FN、S2或其他历史分数。

## 当前实物与固定抽样

原`CompanyProfileReadService`读取唯一主root的103条记录，确认当前39家、1094条可读内容、1677个可见指标。完整39份只在本人QuoteBak暂存，审核完成已删除。固定抽样为178个唯一work/fact对象、181个可见指标；195个采样槽中17个未取得（15特色、2规模采样槽），未替换样本。规模采样槽缺失不代表公司完全没有规模数字，非新增整公司完成门禁。

抽样及raw/current hash、原指标位置、正文/指标与唯一一套引文在[b-sample-freeze.json](b-sample-freeze.json)。实际schema使用`business_structure/products_services/revenue_model/scale_and_mix/company_features`。冻结后没有修改样本身份、顺序或抽样hash。各股实际内容/缺项计数见[b-current-inventory.json](b-current-inventory.json)。

## 既有独立证据与来源

[b-prior-decision-matches.json](b-prior-decision-matches.json)按同完整report、factID、完整可见内容hash和原证据完全等值，核历史API/runtime字节绑定，得163个精确旧判定候选、15个不匹配。每样本仅留一个权威旧判定文件/hash/JSON Pointer及必要binding，不重复抄整家公司source_findings。163是匹配数，不是自动正确数；S068旧判定为not_assessed，A已独立新核，最终162复用+16新判及178/181支持以A的`a-source-decisions.json`为准，B不自判新准确率。

按同PDF hash核原native cache，提供样本涉及171个物理原页供A核查，未下载或重解析39份PDF。保存引文坐标属于selected-layout转录，不等于native-cache字符偏移；native页以PDF hash/物理页对应，不把quote匹配当语义正确。原PDF/cache及页hash、章节路径和采样页位置见[b-native-source-index.json](b-native-source-index.json)。全页临时副本在A完成后清除。

## 三层与风险层

[b-three-layer-mapping.json](b-three-layer-mapping.json)沿原report/作者generation request+response hash、模型quality calls、当前选择谱系及原投影映射，未清空UseReview、未重选谱系或写临时反事实稿到业务root。

- 原作者层176个有精确保存首读；12个内容与当前不同；2个为初读没有的后增事实，未用其他fact替代。原作者169支持/1错/6未评/2缺失仅引用A的`a-layer-assessment.json`，B不补推判断。
- 模型层178个有精确保存投影，与当前样本可见正文/指标hash均相同。S065依据原实际模型运行记录，只有其他事实带继承A决定，本事实无人工决定；没有清任何限制。结论仅针对本次固定对象及当前选择谱系，非完全无人工影响的端到端整批成绩。
- 当前API层178个均精确对应冻结当前可见对象。模型身份、历史限制及来源不改，三个层次不得混分母。

[b-active-safety-risk-layer.json](b-active-safety-risk-layer.json)另列11家公司全部25个当前有效人工限制性决定：11个指标限制决定涉及20个原metric位置、11个source_only、3个withheld；6个纯正向决定另列。24个限制可沿明确父稿hash链及同当前事实hash抵达既有独立证据；学大F12父链及终端文件hash有效，但内容hash历史变化，保留引用而不当精确当前语义判定复用。`review-recovery-parent:`是原owner虚拟父稿引用，不是丢失文件。

在风险层，11个对象有精确保存模型投影，可机械显示10条整事实与1个指标位置后来被人工额外隔离；其余14个没有精确可重建的人工限制前稿，标未评。这只是消费差异，不把25条限制、20个位置或10+1差异直接当作新漏检/独立错误数量。A的`a-safety-audit.json`独立确认现有限制未外放，未新增限制。

## 实际验证与有限Review

[b-mechanical-result.json](b-mechanical-result.json)保存原read owner的完整39份freeze/read、年度export和fresh实例逐内容相等；948 reviewed+146 limited共1094可读，模型源核basis1077、独立源核basis17；另11 source_only、3 withheld，1439 superseded历史行不当当前内容。当前有效metric gap条目44、信息缺项条目320、建议补充条目61均为API原条目计数，不是独立语义漏项/错误数量。

实际命令（均先source项目dev env）：`python -B b-freeze.py --temp-root <owned-empty-QuoteBak-root>`、`python -B b-match-sources.py`、`python -B b-layers.py`、`python -B b-mechanical.py`，脚本位于本目录；本次原临时路径已在收尾删除，重建不得覆盖冻结证据。机械验证reader24.726秒、export9.378秒、fresh26.396秒，合计62.785秒，不是模型业务采集时长。四个脚本语法编译通过；判定JSON Pointer/hash及178唯一对象/181指标、所有模型当前hash等值直接检查通过。

有限Review未发现本卡应用P1/P2，不改应用代码、不新增测试或审核平台。准备期脚本纠正了历史审计字段名、原生JSON严格装载/namespace过滤及native与layout坐标假设；这些是B局部核查实现修正，不是模型失败或新业务修复。A已做实际8000完整HTTP对比，B没有重复联网/ASGI。397个保护文件在机械前后及最终清理前hash保持，A后续仅更新获准导航不属于B写集。

新增供应商请求、客户端、外传、任务入口、策略激活、配置/服务变更均0；未提交/推送Git。阶段使用范围仍为研究；178个对象抽审不能宣称1094条全部核实或正式95%/99%未见验收通过。A独审及正式阶段裁决以本目录A文件和result.md为准。

## 清理与交接

见[b-cleanup.json](b-cleanup.json)：先清84份hash核对导出及两份重复阅读索引，再依A通知清完整39份副本/native样本页与本人空根，并清本人4个字节码。共92文件、53,589,574逻辑字节，B本人QuoteBak根已不存在。原record、PDF/native缓存、A材料、历史首次失败及既有工作区改动未触碰。仅保当前卡必要的一套身份/源指针、映射、独立判定引用与核验回执；无待清B临时物。B活跃时间未精确计量（约半小时含调查/等待），无等待逐项授权或真实复验，Git由A收口。
