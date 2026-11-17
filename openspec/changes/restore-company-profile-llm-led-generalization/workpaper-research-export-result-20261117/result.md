# 6.DF：十二份画像的直接研究交付与有限验收

原CompanyProfileReadService.export现在随workpaper输出[research_profiles.json](research_profiles.json)，程序可直接读取当前十二份分级画像，无须联结原查询和外部使用决定。可读内容见[原导出总览](profiles.md)，逐家公司继续链接既有权威使用版；[profiles.csv](profiles.csv)保留原摘要/分级计数。

合并JSON包含365条可见事实、524个指标：321项普通已核、44项保真限用（其中4项仍待核），另外4源参考/2隔离/1重复只保状态、原因及引用，fact=null。可见事实中349项依既有独立源核、12项依有效模型源核，4项没有匹配当前内容的核验绑定；分级数量不是准确率。上汽待核的4项为研发投入、前五名客户/供应商和两项股权交易金额，保原限制、不为可选尾项追加模型。

业务结构、产品服务、变现、规模/构成、经营方式、客户渠道及适用驱动/变化、粗粒度供应链、自主特色均保原位置与原名。报告/期间/出版时点、章节/物理页/坐标、原金额字符串/null/单位/主体、阅读范围、实际生成与复核模型/回执身份不变。research JSON不复制原workpaper、original_fact/revised_fact或未采纳候选；完整审计原件仍留原运行目录。

## 如何读取

```python
import json
from pathlib import Path

data = json.loads(Path("research_profiles.json").read_text())
profile = next(p for p in data["profiles"] if p["instrument_id"] == "600054.SH")
visible = [row for row in profile["delivery"]["facts"] if row["fact"] is not None]
for row in visible:
    print(row["state"], row["label"], row["fact"]["text"])
```

使用state、review_basis、reasons和指标自身的qualifier/uncertainty_reason判断使用范围；限用里的待核须看delivery.pending_review_count，不能只看counts.pending。null保持未知，不当0，不跨不同主体/期间/科目加总。source_element_statuses表示原抽取阅读状态，不是正式完整性判定。来源按同一profile中的evidence_ids查evidence，真实作者见generation，复核模型见review.calls。没有历史元数据的字段保持null/空，不虚构新调用。

## 验收结论

本轮的“当前分级研究数据可直接交付”已完成，正式S1尚未通过、S2/生产未放行，详[acceptance.json](acceptance.json)。原133条件的当前后继为123支持/9部分/1缺失，微平均92.48%、宏平均92.16%；10家仅达到90%召回数值参考，不能说正式可用10家。上汽7/10、广州10/12仍低于该参考线；不改冻结分母或因本轮导出重算首发。

正式差距集中在同一当前版本的完整首发/后继证据、适用正反例与已审准确率范围、以及完整入口资源和可核价格。旧CT首发及其三家超过120000、费用/部分补救时间未知保留；DC/DE后段耗时不能替代完整处理成本。这些不是供应链精细度或所有附注明细必须补满的要求。本轮不重抽十二家、不补三家尾项、不把core_complete改true；下一应围正式验证合同与正常资源口径集中裁决，不再逐句修订或先造平台。

## 实际验证与清理

26项分级/新JSON正反与不确定性验证29.49秒通过，另1项旧非workpaper导出兼容132.52秒通过；后者使用原TaskService受控运行，不重复长跑。十二份保存真实稿经原writer/query/export/新实例12/12一致，导出逐项比对原事实、指标、Evidence和模型归因；七个禁用/去重条目无事实载荷、旧DC/DE全套hash不变。十二份交付验证socket禁连，0新真实模型/探针/付费，也不运行TaskService语义。没有把导出接线当新模型质量或泛化成绩。

有限Review修正了持久总览原先指向临时逐公司JSON的链接，使其统一指向合并研究JSON；未改业务内容。已知错项过滤、不确定性及旧导出兼容成立，其他可选增强和既有并行改动不扩。OpenSpec严格校验、差异/链接/hash及本人隔离Git收尾。一套合并研究数据/CSV/总览/验收/必要操作快照后，删除临时写入根、重复逐公司JSON/Markdown和已完成旧导出兼容测试根，共实测13,914,192字节；脚本核快照后清另见cleanup.json。正式PDF、共享有效缓存、历史原件/attempt及既有并行改动保护。
