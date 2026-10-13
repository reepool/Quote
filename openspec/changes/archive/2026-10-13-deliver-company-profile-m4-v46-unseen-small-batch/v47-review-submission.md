# v47 同源首次正式复验提交

## 提交结论与有限范围

**v47已达到本轮正式业务门槛，提交A角有限审核；尚不视为A角通过，当前change不归档、不扩批。** 范围限厦门象屿／北京同仁堂原331项正向条件、72项负例及实际检查输出；生产保持`not_authorized`，规模声明`false`。

原104/331、250/301、数字错误3及首次制品/原合同均保留原字节，旧v46有限归档有效。本轮未缩减、重定义分母，未再次选样或重跑失败身份。

## 实际计分

| 维度 | 正式结果 |
| --- | --- |
| 三维正文 | 6/6 |
| 收入条件 | 231/231 |
| 具名动作 | 94/94 |
| 合计召回 | **331/331** |
| 动态唯一准确率 | **661/661＝655事实＋6正文** |
| 关键数字错误 | **0** |
| 原负例 | **72/72实际通过** |
| 实际整轮／共享预算 | **216.56秒／0/50000 token** |

准确率依据全部接受record_id与已答正文动态生成，合同外准确父类/服务/生产输出亦计分；配对Measurement仅准确率，多证据或重复来源不增加召回。97笔本期关联交易共194个Segment/Measurement与独立台账逐对象相等；45笔仅上期交易、3笔仅上期出租无本期事实或造零。

## 修复与业务边界

同仁堂p195对外收入合计未再左移至空抵销格，真实512,028.94万元抵销保留；分部总/主营/对外/内部栏目分别保存。p158/204保留主营地区限定，象屿合并/母公司地区仍为全部营业收入。跨页关联交易按物理页校准年度列，出租多行资产和承租方精确还原。象屿转租按p240元单位接续p241本期单列，托管按具名受托子公司和委托方交付。

六正文按原substance实质复核：专业/产业物流及原主体、订单与实际交付、服务费/规模集拼/价差机制；同仁堂渠道、完整品种/药材、收货验收折扣、系统转出及三种凭据、广告期间、国药商标费收款、控制/注销/境外注册及Ⅱ期临床状态。药品、商品及采购/生产/加工/销售各原方向成立，风险、宣传及枚举尾文不再产生伪动作；采购不推投入，订单/计划/许可不推当前销售。

## 正式执行与独立证据

完整身份：`rules=company_profile_common_core.v1`、`owned_page_facts=v8`、`material_input_facts=v1`、`revenue_sentence_repair=v47`；五累积开关、精确资产入队及主体展开接续，默认身份不变。沿原计划、两份原官方PDF和cutoff `2026-09-17`在`m4_v47_same_source`新目录仅执行一次两家run→query→export，第二家使用剩余共享预算。

整轮计时为首次execute至第二export返回，包含PDF读取、布局及所有重试；实际命中已有PDF解析缓存。两家数据库acquire/parse/semantic/verify/publish均成功，无章节复用或前驱。独立重新读取画像与正式查询／导出／磁盘一致。

124个不同原页文本、32页布局与官方PDF一致。2812项历史、原change41份不可变文件及18份首次正式制品保护通过，38个执行模块从d7579910基线加patch重建一致。两个企业集团BusinessOverview的布局阅读顺序与plain有差异；两者与原审核已确认的准确片段原字节相同，原单位/主体/控制限定保留。

## 验证与Review

新完整页、负例、身份、精确资产入队/拒绝漂移和旧联通单位主体：**9 passed**。旧黄山/尼雅完整页及v44联营章节：**25 passed**。Ruff、格式、当前change及三份主规格严格校验、范围diff检查通过。

本人范围Review未发现剩余业务阻塞。旧人福p233红江店续行测试复现，基线和本次解析输出完全相同，作为范围外既有失败后置，不声明全套测试通过。行业增强、量价成本、正文精简、性能、框架及无关旧问题未扩展。

## 材料入口

- `v47-source-review-evidence.json`：逐对象计分、原条件、正文、五阶段及诊断。
- `v47-actual-object-decisions.json`、`v47-negative-review-evidence.json`：事实/正文判定及原72负例。
- `v47-associated-sales-ledger-validation.json`：194个关联收入对象与97笔原本期台账。
- `v47-source-freeze-receipt.json`、`v47-first-execution-script.txt`及日志：执行前合同/身份/代码冻结和首次业务轮。
- `v47-first-formal-delivery-preservation.json`、`v47-formal-result-preservation.json`：首次交付与首次复核原哈希。
- `v47-actual-evidence-official-source-validation.json`、`v47-delivery-reread-validation.json`、`v47-execution-code-reconstruction.json`、`v47-final-protection-verification.json`：官方页、三路读取、代码重建和保护。
- `v47-verification-receipt.json`、`v47-scoped-review-receipt.json`、两份回归日志及`v47-existing-failure-baseline-validation.json`：验证与既有失败边界。

等待A角有限审核确认；通过后才执行5.1规格同步与有限归档。本轮未归档、扩批或改变生产授权/规模声明；既有工作区改动未触碰。
