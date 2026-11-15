# 6.CQ：基础下限与LLM自主特色同链交付

已将“基础框架保证下限、LLM展现公司个性”同步到产品需求和原执行链。八要素不变，新增可选根字段`company_features`，使用同一status/facts/limitations及Fact/Metric/Evidence合同。没有固定特征分类、必填数量或新事实库；事实文本可以解释模型选中的独特机制、资源、品牌或变化，既有要素容得下的信息仍留原处。格式有章法，意义不锁死。

## 已实现

- 发给模型的选章指引、抽取提示与schema都允许自主特色；写作数量仅软目标，不是特色信息的硬上限。以当前来源/输出/资源范围为界，不为凑特色无限扩读。
- 特色走原来源检查和二次审核、一次局部修订/最终再核。复核不因“不在清单”删除有据内容，也能沿原遗漏修订提出一条特色；未获最终接受不能采纳。小错误局部纠正/隔离，独立基础事实及其他特色保留，允许有明确标注的不确定和未决项，不要求所有意见全绿才交研究稿。
- 可选结构格式错误、坏数字/来源或身份冲突不会吞整份基础画像。特色也不能取代缺失的基础字段，不能复用已隔离核心事实的编号混淆身份。
- 原writer持久化和query/export/新读取实例保留特色，Markdown显示“公司特色与自主发现”，JSON/CSV摘要计入相应事实/指标。不改变原trusted/core_complete字段或生产权限。
- 新字段为空时不写入序列化结果，11份原CO工作底稿的形状和canonical workpaper_hash输入完全相同，旧resume依据不被默认null改写。旧原件只读。

“允许小错”不等于将已知错误当可靠事实。非阻塞遗漏/保真歧义可以限用，已发现错数/错主体/无据断言局部退出；阶段正式阈值不因工程示例自动改变。必要的现实局部修复仍可做，不一刀切禁止，也不围单公司追完美。

## 看看特色区可以放什么

[三个有源内容示意](examples.md)取自旧真实模型稿：上汽的自营滚装船运力、同方的CNKI知识服务规模、开创的ALBO品牌表现。文字、数字、主体、期间、引文和事实ID不变，仅在受控工程输入中移动到新容器，其他事实逐对象相同，旧quality_review不继承。

这不是本轮新发现，也不是新模型最终核验或未见泛化。[结构示例](examples.json)与[旧稿兼容核查](legacy-compatibility.json)保留具体来源及hash；原query未覆盖。下一真实运行才观察模型能否自主挑出有价值的特色，不能用容器接通冒充已经达到理想画像。

## 验证与Review

通过项目环境入口执行，Python使用`/home/python/miniconda3/envs/Quote/bin/python`：

```bash
python -m pytest -q --color=no --confcutdir=tests/unit/test_research \
  tests/unit/test_research/test_company_profile_workpaper.py \
  tests/unit/test_research/test_company_profile_workpaper_review.py \
  tests/unit/test_research/test_company_profile_workpaper_local_consumption.py \
  tests/unit/test_research/test_company_profile_workpaper_source_cells.py \
  tests/unit/test_research/test_company_profile_workpaper_features.py \
  -k 'not task_service_review_publishes'
```

最终181 passed、3 deselected、45.84秒。排除三个无入口逻辑变化的长TaskService用例；本轮新五阶段链实际经过CommonGatewaySemanticProvider/真实schema，只替换外部传输，完成选章→抽取→初核→修订→最终确认及原owner三读、JSON/Markdown/CSV展示。它是controlled_fixture，0真实模型请求。首轮新增测试导入了错误辅助模块；修正后又发现受控网关缺少原约定的授权/profile接线，补齐测试构造才通过，没有放宽断言或修改生产授权。前轮104回归及未收口177通过不代替末版181结果。

有限Review A类新增特色不能复用被隔离核心ID、格式隔离说明不能被范围复核擦除的风险已补直接反例并修正；字段空值对旧hash的兼容已有11份旧原件证明。新增范围复核测试最初把“事实不变”误写成“包括范围限制在内整个要素都不变”，已按既有允许更新范围说明的合同改为逐要素事实保持不变并检查处理记录，没有放宽业务断言，末版181通过。B类更丰富特色分组/个性化页面不扩，C类配置/骨架/退市/旧混合块不动。来源约束和模型复核仍不证明全量准确，S1仍0/12、未新评估正式准确率或放行生产。`openspec validate restore-company-profile-llm-led-generalization --strict`与diff检查收尾通过。

开工盘点HEAD28cc0353、空暂存，本人CP/CQ无依赖残留0；必要示例/校准脚本快照一套后清本人脚本5167字节，测试根随TemporaryDirectory退出删除，未复制PDF、整套解析或业务库。保护正式源/有效缓存/原回执/attempt及既有并行，详见cleanup.json。新版prompt v17/guide v4/review v7，schema仍为兼容的v1可选扩展，共享配置/唯一writer不变。
