# 6.EU 有限Review和独立交付验证

## A的独立业务判断

源参考先于对应公司第一次模型请求冻结，20份共60基础条件，未作为模型输入；四类/交易所/行业及排除集合见manifest。首发固定20分母，17有稿、3失败，18/20不可逆不达标。必要后继只修两处调用前程序阻塞并交两份稿，不重算首发；本卡正式扩量不通过，95%/99%与400补充抽审未完成评估。

八份有限核心/主要金额检查及26个实际采纳修订的原文判断绑定到API快照hash，见a-independent-review.json；另有一条源单位不确定限用。没有将26次修订、592可读记录、模型复核completed或十八个指标当作整批准确率。

## Review分类

- A类当前阻塞：并发PRAGMA/ALTER加row_version导致CLI初始化失败；固定截止源重建被后来的无正文下载失败遮住。均在原owner最小修复，正常锁/其他DDL错误仍报告、已知坏新字节和未来正文不借用，采集诊断不藏失败。
- 直接恢复：复用已有队列force-replay owner，只重开原失败acquire，不新建恢复框架或重做成功业务阶段；原结果/预算/队列失败轨迹持久保留，后继消耗原绝对窗余额。
- 非阻塞限制：原文单位不确定、补充字段/具体客户/供应链细目、个别模型格式隔离及未采纳修订。保持分级读取，不追齐。
- 无关既有改动：开工前脏配置、scheduler、核心骨架/证据选择、市场/依赖等不在写集，不修改、不暂存、不提交。

A已逐行检查3个生产文件的本轮diff及直接回归测试，未发现未解决的当前阻塞；不因纯样式或假想风险增加范围。新测试首次队列事件reason字符串写错，按既有owner真实输出修正后复验；不是放松业务行为断言。

## 验证证据与适用范围

B的实际命令/精确结果见b-execution及status.md：

- owner-fixes-tests.xml：7通过，含并发初始化、非目标错误、历史源/未来源/坏新字节及既有更正源选择。
- row-final-tests.xml：4通过，覆盖同列竞争、锁错误及未确认的重复列错误不吞、非法表名/缺表；与前一组有重叠，不相加成独立用例数。
- source-replay-tests.xml：首次新断言字符串失败及既有force-requeue用例通过；source-replay-final-tests.xml：修正新断言后1通过。失败轨迹未删除。
- 初始未改代码的CLI直验复用上一轮同hash36项，只证明其当时范围，不冒称覆盖后来发现的两处现实失败或新模型语义。
- 真实首发85物理、必要后继10物理；合计95物理/94逻辑/1格式恢复，2,377,610供应商已报告total及账本、费用unknown，业务总墙钟3,664.423秒，原各股/整组限额未突破。模型/来源及无新生产权限见resource-validation.json。

A另行只读运行现有CompanyProfileReadService：

```python
reader = CompanyProfileReadService(
    "/home/python/Quote/data/research/company_profile_prospective20_release_20261122"
)
batch = reader.research_profiles(固定20股票清单)
```

实际新实例返回19画像、仅600233.SH未找到；按股票比对每份完整安全投影与delivery/annual_profiles.json逐内容相同，592个可读记录（非去重事实或准确率）。深南电F14为limited、independent_source_check，原因明确单位尚待核实，production_authorization仍not_authorized。此检查没有模型、排队、重写原研究文件或切换默认研究根。

A另核38个原writer/交付副本对的路径与sha256：37对原样相同，仅深南电叶记录经原writer绑定明确UseReview；原件hash保持。8份独审所引用API快照hash未改变、26实际修订对象数一致。原52个既有writer曾单独核不变，末端保护回执继续见B收尾验证。

OpenSpec严格验证、最终保护hash/HTTP/清理和Git结果在result.md及审核§368汇总；开发临时脚本/测试库/重复输出只保一套必要证据后清理。暂存源码/文档空白检查通过；原始失败JUnit的错误正文自带一行末尾空白，保留原回执，不为差异检查改写历史失败。
