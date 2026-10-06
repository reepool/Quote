# v24 有限验收提交（待A角审核）

4.1–4.4执行完成，v24首次正式轮达到本卡声明来源门槛。当前change保持未归档，下一批暂停，等待A角有限验收；生产not_authorized，规模质量声明false。

- 实质召回62/62：六维正文6/6、原生收入行29/29（华能12＋双鹤17）、销售角色27/27（电力1＋药品26）。完整62项在首次execute前固定，不能以answered状态代替正文实质。
- 准确率104/104：98条实际接受事实＋6个已答正文，逐对象唯一核对；29条Measurement副本仅计准确率。27销售角色各绑定一条底层sells事实，品牌/通用名不重复计分。
- 关键数字错误0。本表金额、元单位、行业/产品/地区/销售模式正确，Segment和Measurement一致。具名销量/收入只证明销售，全部销售Activity数量与价格留空；未知精确映射pending。
- 首次正式201.09549884870648秒（华能103.088秒、双鹤98.007秒），从首次execute至第二export返回，涵盖全部重试；token0/50000，第二家收到剩余50000。五阶段实际reused_scope_ids为空，predecessor_lineage为空。query、export及磁盘JSON相等。
- 616份保护文件哈希不变，包括原593份历史文件、18份首次失败制品，另含原复核JSON/历史限制说明和锁文件。新v24执行/交付快照在复核写入后保持；v23失败19/62、48/57及旧范围轨迹未替换。

华能收入边界从本表开始，不被表前折旧说明截断；真正缺产品续页拒绝。主营保留共享发电项目及开发/建设/运营/管理动作，收入正文包含“公司盈利主要来自发电收入”，产品正文以已接受表格补足实际水电/风电/光伏/储能构成，未来布局仍为原文状态。

双鹤四平台及完整原料药矩阵保持连续原文；p19–20折行产销、p39营销矩阵、p14–15当前业绩披露形成26个销售对象。括号品牌、剂型和API/注射液区分保留。华润紫竹的三个销售对象保留原source_actor及集团范围。客户医院名称没有生成钢铁Activity/exposure，直接公司钢铁销售正例保持。

首次v23的62项是最终独立复核范围，五药披露在首次执行期间补入；这一限制继续保留。v24-source-scope.json合并全部条件并在本次execute前固定。本结论只覆盖声明的六维、29行、27角色与实际接受事实，未宣称全年报完整性；无具名投入判断仍限实际检查页。

验证：202 passed、1项既有失败deselected（test_reads_through_section_and_stops_at_next_heading）；Ruff、OpenSpec严格校验、diff检查通过。未扩展数量细分、成本拆分、行业增强或既有测试治理。未提交用户既有工作区改动。

依据：v24-freeze-receipt.json、v24-source-scope.json、v24-source-review-evidence.json、v24-first-formal-preservation.json、v24-protection-baseline.json、v24-validation-receipt.json；正式制品位于data/checkpoints/company_profile_common_core/reports/m4_v24_hydro_pharma_core_repair、data/research/company_profile_common_core/m4_v24_hydro_pharma_core_repair、data/exports/m4_v24_hydro_pharma_core_repair。
