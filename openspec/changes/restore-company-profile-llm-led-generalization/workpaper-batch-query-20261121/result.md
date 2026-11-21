# 6.EN：按股票清单一次取得完整画像及未找到状态

已新增`POST /api/v1/research/company/annual-profiles/query`，原ReadService.research_profiles提供1—100股票的显式只读入口，去重并保持请求顺序。每份profiles与原单股详情及annual_profiles导出同形，保留多轮源/实际模型、复核后继、信息缺项、建议及局部限用；没有拼第二套画像、语义规则或存储。

```json
{"instrument_ids":["002027.SZ","601111.SH","601881.SH"]}
```

返回`requested_instrument_ids`、`delivered`、完整`profiles`、`missing_instrument_ids`和`missing[].reason=annual_report_profile_not_found`。部分找到为partial，全找到found、全未找到not_found，均200；空/空白/超100输入及额外字段422，不默认全市场。沿已有代码转换兼容SSE/SZSE后缀；不再对已有股票请求重复正文。未找到只表示当前根没有可读年报画像，不诊断其运行是否失败，也不伪装为报告未披露或零业务。

## 实际验证

使用当前company_profile_batch_reviewed_20261121保存的15家公司，加原确认清单中的5个未找到股票，只通过新HTTP读取、没有启动任何公司。返回15画像/5未找到、433可读对象694指标，与现有annual_profiles完整导出逐项一致。分众、国航两份新修订/源参考与6.EM权威API相同；20个原writer文件hash前后不变。名单、逐家内容/指标/缺项计数、末端复核状态及安全响应hash见saved-batch-verification.json，未复制全15份正文作第二套证据。

5个未找到ID为南京银行601009.SH、深圳能源000027.SZ、天健000090.SZ、陆家嘴600663.SH、豫园600655.SH。它们在本次读取中仍无稿，不是新运行/补旧五家或重开S2。已有画像中的缺项和未找到股票独立呈现，不能把433对象/694指标当作去重经济事实、正式质量或全市场覆盖率。

9新增直接用例＋32相关旧回归，共41 passed/0 skipped，31.55秒；覆盖输入边界、别名去重/顺序、缺失/旧非底稿、同报告补读、局部隐藏及原目录/单股/接续计划/导出兼容。保存数据HTTP＋导出核对60.243秒，两者不是开发活跃总耗时。OpenSpec strict、语法/diff通过；Review无剩余A类阻塞，B类全市场分页/进一步采集后置，C类既有配置/核心/市场并行未动，不重跑完整仓库或旧慢集。

本轮0新模型、外传、下载、队列/研究根写入、共享服务重启或生产启用。要使用当前数据，在已有API部署此代码后由研究进程显式设置`QUOTE_COMPANY_PROFILE_RESEARCH_ROOT=/home/python/Quote/data/research/company_profile_batch_reviewed_20261121`；本轮只在本地ASGI进程核对，不切共享配置。完整调用见[接口文档](../../../../docs/api/restful_api.md)。旧S2失败和95%/99%正式门槛不变。

## 清理

开工前核6.EM执行根已不存在，本轮唯一QuoteBak根company-profile-6en-SVVy4tXl。只保验证摘要、测试JUnit及20文件hash引用，不保重复完整导出/测试库；检查后清本人脚本、测试记录、导出及日志，具体量见cleanup.json。正式源/有效缓存、当前及旧研究根、旧失败/attempt及既有并行保护。只隔离本人代码/规格/文档块正常提交推送；没有为国航或分众尾项新增付费卡。
