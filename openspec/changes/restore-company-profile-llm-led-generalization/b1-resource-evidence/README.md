# B1两次离线资源方案证据（2026-10-17）

这是两次失败停止观察，不是208页完整语义闭环、真实token/费用或业务验收。原始PDF hash保持B1卡冻结值；真实模型调用0，正式首次/后继0/0。

`iteration1/`为精简v3＋全文分批；`iteration2/`为同协议＋全页均匀片段地图/选中正文。各目录保存原始binding、source receipt、预算、模拟transport、监督入口、query/export及完整checkpoint；gzip仅压缩，mtime=0。checkpoint包含来源正文及原生事实，不能将其合成条件当作研究结论。两次query/export的profiles逐对象相等。

`remaining-projection.json`只序列化第二方案未发送的合成/核验，沿公共client的真实payload构造方法计算六成员差异。它未构建client实例、未调用transport、未重新准入预算，没有第三次资源方案运行。它采用14个合成unclear条件、7个合成unclear回答、27项协议核验形态；增长超过此形态时不能保证上界不增。重建断言：

```bash
source /home/python/Quote/scripts/quote_dev_env.sh
PYTHONPATH=/home/python/Quote /home/python/miniconda3/envs/Quote/bin/python openspec/changes/restore-company-profile-llm-led-generalization/b1-resource-evidence/project_remaining.py
```

此命令读取当前配置（不读取API密钥）与持久checkpoint，不发送网络请求，也不修改冻结投影。执行依赖hash见各binding；包含任务开始前的脏依赖，因此仅checkout提交不足以完整重建，须保留绑定hash一致的依赖。本次检查全部匹配。

原两次命令均为下式，N分别为1、2，目录已存在不可覆盖；本卡两次上限已用完，不授权重跑第三次：

```bash
source /home/python/Quote/scripts/quote_dev_env.sh
/home/python/miniconda3/envs/Quote/bin/python -m research.company_profile.bounded_entry --binding data/QuoteBak/b1-resource-20261017/iterationN-binding.json --run-root data/QuoteBak/b1-resource-20261017/iterationN
```

`tests-final.log`为57 passed、1 deselected的直接回归。未选中的原owner重型用例本轮没有重跑，已有回执不改写；本轮两次真实资产入口分别完成预算停止、持久化与query/export一致性，但没有完整三purpose交付。`resource-metadata.json`为只读公开资料核查及精确缺项，`scorpio-model-plaza.json`为公开GET原始响应。没有将任何未验证价格或型号上下文写入生产配置。

`manifest.json`绑定全部证据文件的SHA256；执行代码身份见binding，不以测试数替代核心19项/负例6项/准确率审核。
