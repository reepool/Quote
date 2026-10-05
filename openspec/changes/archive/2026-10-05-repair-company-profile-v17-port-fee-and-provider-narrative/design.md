## Context

上港 p12 的经营模式句为"公司经营模式主要为:为客户提供港口及相关服务,收取港口作业包干费、库场使用费和港口其他收费。"——按逗号切分后,"为客户提供港口及相关服务"从句无收费动作、"收取港口作业包干费……"从句无公司主语,`_activity_object_clauses` 逐项投影均不成立,收入记录未生成。万东 p11 的业务叙述为"作为国产医学影像装备与智慧医疗解决方案的核心提供商,公司……",现有 source-delivery 捕获(主要业务是/主要从事/主营业务为/主要经营/作为……上市公司/工业企业……研发制造)均不命中,概览候选未生成。复核侧漏计上港四条 Activity 且负例未写检查页。

## Goals / Non-Goals

**Goals:**

- 上港:接受记录正文、查询答案、导出答案均含 港口服务+包干费、库场使用费、港口其他收费;同句公司主语与服务+收费动作连续关系保留。
- 万东:主营与产品正文呈现 医学影像装备、智慧医疗解决方案 或来源明确的产品类别;完整来源句交付。
- 计划、否定、第三方收费反例与免费附带服务正例全部保持;既有分部行不变。
- v18 隔离重跑与逐对象唯一映射复核,负例补齐检查页。

**Non-Goals:**

- 不回写 v17 未见轮的 24/26、44/44、false。
- 不放宽门槛:双 100%、关键数字错误 0、50000 token、整轮 300 秒。
- 愿景、计划及第三方定位不升格为公司业务;不授权生产,不声明规模质量。

## Decisions

1. 新标记是 `revenue_sentence_repair=v18`,累积 v17 全部修复。
2. 上港收费句在 `_activity_object_clauses` 前整句保形:同句"为客户提供……服务,收取……费"作为提供—收取连续关系整体进入候选,不再按逗号拆断;公司主语取句首 公司。
3. 万东叙述捕获增加 "作为……核心提供商,公司……" 形态;产品答案以叙述与既有分部行共同支撑;愿景(宏伟愿景)、计划(谋篇布局)与第三方定位不升格。
4. 复核逐对象唯一映射:六维答案+全部接受事实(含 Activity)各一条判定;负例判定 id 写明实际扫描页。

## Risks / Trade-offs

- [整句保形后候选变长] → 候选仍受句界与既有接受规则约束;反例回归保持。
- [叙述捕获过宽] → 仅在 source-delivery 路径生效,且必须含 核心提供商 与后续 公司 主语句。

## Migration Plan

先做上港收费句与万东叙述的定向端到端测试,再以 v18 身份隔离重跑两份冻结年报并逐对象复核。失败不回滚 v17 未见轮快照。

## Open Questions

无。

## 2026-10-05 closure amendment

A-role review rejected v18. Tasks 4.1–4.3 supersede its acceptance claim: frozen full-page regression, affirmative reporting-company provider binding, runtime-based reuse reporting, and isolated v19 first execution with unique per-object scoring. The v18 observation and score bytes stay unchanged; its separate restriction is `v18-review-limitations.md`. Archive and unseen-sample expansion remain paused until A-role acceptance.
