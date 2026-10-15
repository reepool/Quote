# v58 广州发展／上汽集团同源修复有限审核入口

B正式自检达标，提交A有限审核；本文件不代表A验收。当前change不归档、不推进下一对，不扩大生产或规模声明。

范围：两份冻结2025年官方年报，cutoff `2026-09-17`；后继457项正向＝6正文＋241收入＋210动作，41项负例。按A裁决，仅将一项进口煤销售的主体改为原文“香港国际公司”；原457合同、首次成绩和制品未改写。

正式结果：**457/457召回、997/997准确率＝991条事实＋6正文、数字错误0、41/41负例实际通过**。全部接受事实及六正文唯一计分，合同外输出包含在准确率中；不是仅核收入或抽样准确率。

| 轮次 | 召回 | 动态唯一准确率 | 数字错误对象 | 负例 | 资源与授权 |
| --- | --- | --- | --- | --- | --- |
| 原首次观察 | 189/457＝0＋143＋46 | 495/612＝606事实＋6正文 | 5 | 39/41 | 230.18秒，0 token；首次1/1 |
| v58首次同源后继 | 457/457＝6＋241＋210 | 997/997＝991事实＋6正文 | 0 | 41/41 | 第二次导出返回279.945381秒；外层监督280.684792秒；0/50000 token；后继1/2 |

正式计时从进程启动（含import、registry和服务构造）开始，包含两份PDF、布局及全部重试至第二次导出返回。已有PDF解析缓存，不能解释为冷缓存上界。不为用完剩余授权重跑。默认身份未变；v58完整接续五累积修复、精确资产入队、主体展开及外层同步限时。

## 修复与来源语义

- 收入：按真实物理表头、栏目、本期列和单位处理合并利润表、合同收入、金融业务、客户、收入确认时间、地区、分部及关联利息／手续费；空位不借额，未来租金和成本不作本期收入。完整跨行商品和具名公司、母公司及参股口径分别保留。
- 动作：原主体业务性质单元格及共享名词／动作列举分别绑定；采购、投入、生产、销售和服务方向、商品／服务／资产／公用事业栏目独立。关系表的续页、合营边界、退出与注册范围限定保留；采购不反推为实际投入。
- 六正文：保留完整主营、产品矩阵、经销／交付、费用／代理及租赁机制；广州储能项目的投产／并网调试分别保留，上汽全固态中试和半固态MG4批量交付分开。正文实质核对见 `v58-body-substance-review.json`，词项只作为回归辅助。
- A来源裁决：广东红海湾发电、广东珠海金湾液化天然气按p234联营，保留p223–224原栏目冲突、原Evidence和独立收入，不算集团合并收入；香港国际公司不推定法律全称。逐项补正见 `source-relationship-A-ruling.json`、`source-actor-A-ruling.json`、`v58-source-scope.json`。

## 验证与Review

完整实际page-source局部接受→查询→导出闭环997/997；正式输出与该局部结果的原生事实语义完全相同。数据库两家五阶段均success、无复用／前驱；只读重新查询、正式查询、导出和磁盘一致。

定向命令：`PYTHONPATH=. python -m pytest -q tests/unit/test_research/test_company_profile_v58_energy_automotive.py tests/unit/test_research/test_company_profile_v55_property_paper.py tests/unit/test_research/test_company_profile_v53_source_conflict.py -k 'not v56'`，**22 passed、2 deselected**。含本对7项、直接旧v57商品／服务和空位回归10项、旧v53来源冲突及正常软件／集成电路业务5项。不声明全套测试通过；v56历史执行专项未重跑。

Ruff、5文件格式、范围diff、当前change严格校验通过；主规格严格校验114项通过。范围Review无已知剩余业务阻塞；曾发现关系上下文页抑制旧采购入口、正文状态／车型缺失及负例误将同名采购当销售，均已局部修正并直接回归，未新增框架或性能任务。见 `v58-scope-review.json`、`v58-targeted-tests.log`。

**顺序限制如实保留：** 代码和实质Review已在正式前完成，但3750项保护哈希核验与Review回执的落盘时间为06:17:49.531685Z，比代码冻结06:17:46.451025Z、正式启动06:17:46.702056Z晚约3秒。原工具调用在核验进程尚未返回完成时继续了后续步骤。最终核验全部通过，代码快照未变化；不将该机械回执描述为启动前已完成，交A判断此限制。后续等待依赖进程明确完成再启动。

现有source-review owner同样记录457/457及997/997，`expansion_gates_met=false`原字段保留。其自动阈值为第二家公司剩余运行115.343654秒，整轮实测279.945381秒；本卡冻结门槛为整轮300秒。本次不改该既有字段或计时框架。

## 证据与保护

- 原文：`independent-source-pages-*.json`（62页）、`independent-source-layouts-*.json`（61页）；与官方PDF逐页重新核验，见 `v58-independent-pdf-verification.json`。
- 原首次：`first-source-review-evidence.json`、`first-negative-review-evidence.json`、`first-formal-delivery-preservation.json`、`first-review-preservation.json`；39模块原执行源码可由 `first-executed-code-snapshot/` 按原哈希重建。
- 正式：`data/checkpoints/company_profile_common_core/reports/m4_v58_same_source_repair/formal_execution.json`；画像在 `data/exports/m4_v58_same_source_repair/`。`v58-formal-delivery-preservation.json` 在全部writer结束后保护22份制品／执行回执，非空保护准确。
- 逐对象：`v58-formal-source-review-evidence.json`、`v58-business-audit-script.txt`；`v58-contract-object-assignment.json` 将457项条件映射到不同实际对象，准确率没有重复计入引用。
- owner回执：`v58-source-review-owner-receipt.json`；重新查询／数据库阶段／来源绑定及保护核验：`v58-readonly-verification-receipt.json`、`v58-requery-receipt.json`。3750项历史、原首次22份、首次复核13份、两个39模块快照及19项既有改动均核验通过。
- 冻结代码：`v58-source-freeze-receipt.json`、`v58-executed-code-snapshot/`。本轮只改四个既有执行／投影模块及直接测试，没有平行业务owner。

## 边界与交接

按A/B流程v1连续完成卡内修复、完整输入闭环、直接回归、Review及一次已授权正式后继。4.1–4.4标记完成；5.1仍以A有限通过为前置。行业增强、量价成本、正文精简、性能、框架和无关旧问题继续后置。

生产`not_authorized`，规模声明`false`。业务轮0 token不代表开发／审核消耗。跨中断的B活跃开发时间无法准确统计，记未知；本次正式后继1次，无重复正式根因，无额外样本。正常提交推送仅包含本任务可隔离改动，既有19项原字节不变；最终提交哈希见本交接提交及对话回执。
