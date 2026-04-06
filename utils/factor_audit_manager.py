"""
自研复权因子审计管理器 (Factor Audit Manager)

支持全量(Full)、断点续传(Resume)、局部重算等功能，核心职责:
  - 检查完整性 (比较生产标的表与审计记录)
  - 执行审计 (获取 pytdx 因子、校验、入库)
  - 维持断点进度、生成 Markdown 报告
"""
import os
import json
import logging
from datetime import datetime, date
from typing import Any, Dict, List, Optional
from collections import defaultdict

from utils.date_utils import get_shanghai_time
from database import db_ops
from data_manager import data_manager
from data_sources.source_factory import get_data_source_factory

audit_logger = logging.getLogger("tdx_audit_manager")

class FactorAuditManager:
    # 可配置参数 (集中管理，避免码内散落的魔法数)
    PROGRESS_SAVE_INTERVAL: int = 10  # 每处理 N 只股票保存一次进度
    TOP_CONFLICTS_LIMIT: int = 20     # 报告中 Top 差异明细上限
    TDX_SOURCE_NAME: str = "pytdx"   # pytdx 数据源在 factory 中的基名称
    
    def __init__(self):
        self.progress_file = os.path.join(
            data_manager.data_config.get('data_dir', 'data'),
            "audit_progress.json"
        )
        self.reports_dir = os.path.join(
            data_manager.data_config.get('data_dir', 'data'),
            "reports"
        )
        os.makedirs(self.reports_dir, exist_ok=True)
        # 初始化需要的组件
        self.db_ops = data_manager.db_ops
        self.source_factory = data_manager.source_factory
        # 从配置中读取各交易所的默认起始年份
        self._default_start_years = data_manager.data_config.get('default_start_years', {})

    async def initialize(self):
        if not self.source_factory:
            self.source_factory = await get_data_source_factory(self.db_ops)

    def _load_progress(self, exchange: str) -> dict:
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    return data.get(exchange, {})
            except Exception as e:
                audit_logger.warning(f"Failed to load audit progress: {e}")
        return {}

    def _save_progress(self, exchange: str, progress_data: dict):
        data = {}
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception:
                pass
        data[exchange] = progress_data
        try:
            with open(self.progress_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            audit_logger.error(f"Failed to save audit progress: {e}")

    async def check_completeness(self, exchange: str) -> List[str]:
        """检查特定交易所中，尚未有审计记录的品种"""
        # 获取全部品种
        sql_inst = "SELECT instrument_id FROM instruments WHERE exchange = :exchange AND type = 'stock' AND is_active = 1"
        all_insts = await self.db_ops.execute_read_query(sql_inst, {"exchange": exchange})
        all_ids = {instr['instrument_id'] for instr in all_insts}

        # 获取在审计表里有记录的品种 (同样用 instruments 表 JOIN 来筛选归属)
        sql_audited = (
            "SELECT DISTINCT t.instrument_id FROM adjustment_factors_tdx t "
            "INNER JOIN instruments i ON t.instrument_id = i.instrument_id "
            "WHERE i.exchange = :exchange"
        )
        audited_insts = await self.db_ops.execute_read_query(sql_audited, {"exchange": exchange})
        audited_ids = {instr['instrument_id'] for instr in audited_insts}

        missing = list(all_ids - audited_ids)
        return missing

    async def run_audit(self, exchange: str, mode: str = "resume", limit: int = 0) -> str:
        """执行审计
        Args:
            exchange: 交易所(SSE/SZSE/BSE)
            mode: "full" (清空重传) / "resume" (从断点继续 或 补全缺失)
            limit: 用于调试，大于0时限制测试数量
            
        Returns:
            str: 格式化的 Markdown 报告路径
        """
        await self.initialize()

        progress = {}
        if mode == "full":
            audit_logger.info(f"[{exchange}] FULL mode requested. Deleting old audit data...")
            # 删除旧数据: 通过 JOIN instruments 精确定位属于该交易所的审计记录
            async with self.db_ops.get_async_session() as session:
                from sqlalchemy import text
                await session.execute(text(
                    "DELETE FROM adjustment_factors_tdx WHERE instrument_id IN "
                    "(SELECT instrument_id FROM instruments WHERE exchange = :exchange)"
                ), {"exchange": exchange})
                await session.commit()
            progress = {
                "completed": [],
                "results": {"all_pass": 0, "partial": 0, "conflict": 0, "no_overlap": 0},
                "conflict_reasons": {},
                "top_conflicts": []
            }
            target_stocks = await self.db_ops.execute_read_query(
                "SELECT instrument_id, symbol FROM instruments WHERE exchange = :exchange AND type = 'stock' AND is_active = 1",
                {"exchange": exchange}
            )
        else: # resume mode
            missing_ids = await self.check_completeness(exchange)
            audit_logger.info(f"[{exchange}] RESUME mode requested. Missing stocks to audit: {len(missing_ids)}")
            if not missing_ids and not limit:
                audit_logger.info(f"[{exchange}] All stocks audited. Nothing to do.")
            
            target_stocks = []
            if missing_ids:
                # instrument_id 来自自身 DB 查询结果, 无注入风险;
                # SQLAlchemy text() 不支持列表参数绑定, 此处用拼接方式构造 IN 子句
                id_list_str = "','".join(missing_ids)
                target_stocks = await self.db_ops.execute_read_query(
                    f"SELECT instrument_id, symbol FROM instruments WHERE instrument_id IN ('{id_list_str}')", {}
                )
            progress = self._load_progress(exchange)
            if not progress:
                progress = {
                    "completed": [],
                    "results": {"all_pass": 0, "partial": 0, "conflict": 0, "no_overlap": 0},
                    "conflict_reasons": {},
                    "top_conflicts": []
                }

        # 准备任务
        targets = [t for t in target_stocks if t['instrument_id'] not in progress.get('completed', [])]
        if limit > 0:
            targets = targets[:limit]
            
        if not targets:
            report_path = await self.generate_report(exchange, progress)
            return report_path

        total_targets = len(targets)
        audit_logger.info(f"Starting audit for {total_targets} stocks on {exchange}...")
        
        from data_sources.tdx_factor_validator import TdxFactorValidator
        validator = TdxFactorValidator()

        # 开始遍历审计
        for i, stock in enumerate(targets):
            instrument_id = stock['instrument_id']
            symbol = stock['symbol']
            
            # 获取数据源实例
            # 注意: route['validator_instance'] 存的是 TdxFactorEngine (同步计算引擎),
            # 不能直接调用 get_adjustment_factors()。
            # 需要找到完整的 TdxSource 实例, 它才有异步包装方法。
            route = self.source_factory.factor_routes.get(exchange)
            if not route:
                audit_logger.error(f"No factor route for {exchange}")
                break
            
            ref_source = route.get('primary_instance')
            if not ref_source:
                audit_logger.error(f"Missing primary source for {exchange}")
                break
            
            # 获取 pytdx TdxSource 实例 (它包装了 factor_engine 并提供异步接口)
            tdx_source = self.source_factory._find_source_by_base_name(self.TDX_SOURCE_NAME)
            if not tdx_source or not hasattr(tdx_source, 'get_adjustment_factors'):
                audit_logger.error(f"{self.TDX_SOURCE_NAME} TdxSource not available for {exchange}")
                break

            try:
                # 获取 TDX 因子 (通过 TdxSource 的异步包装方法)
                start_year = self._default_start_years.get(exchange, 1990)
                start_date = datetime(start_year, 1, 1)
                tdx_factors = await tdx_source.get_adjustment_factors(
                    instrument_id, symbol, start_date, get_shanghai_time()
                )
                
                # 获取权威源因子
                ref_factors = await ref_source.get_adjustment_factors(
                    instrument_id, symbol, start_date, get_shanghai_time()
                )
                
                # 执行对比
                report = validator.validate(instrument_id, tdx_factors, ref_factors)
                
                if report.result.value in progress["results"]:
                    progress["results"][report.result.value] += 1
                else:
                    progress["results"][report.result.value] = 1
                    
                # 处理异常详情记录与存储
                audit_factors = []
                for f in tdx_factors:
                    ex_date_key = f['ex_date'].strftime('%Y-%m-%d') if isinstance(f['ex_date'], datetime) else str(f['ex_date'])[:10]
                    matching_detail = next(
                        (d for d in report.details if d.ex_date.strftime('%Y-%m-%d') == ex_date_key),
                        None
                    )
                    f['validation_result'] = report.result.value
                    if matching_detail:
                        f['ref_factor'] = matching_detail.ref_factor
                        f['ref_source'] = getattr(ref_source, 'name', str(ref_source))
                        f['ratio_diff_pct'] = matching_detail.ratio_diff_pct
                        f['conflict_reason'] = matching_detail.conflict_reason
                        
                        # 聚集冲突原因统计
                        if not matching_detail.passed and matching_detail.conflict_reason:
                            r = matching_detail.conflict_reason
                            progress["conflict_reasons"][r] = progress["conflict_reasons"].get(r, 0) + 1
                            
                            if len(progress["top_conflicts"]) < self.TOP_CONFLICTS_LIMIT:
                                progress["top_conflicts"].append({
                                    "instrument_id": instrument_id,
                                    "ex_date": ex_date_key,
                                    "tdx_factor": round(matching_detail.tdx_factor, 6),
                                    "ref_factor": round(matching_detail.ref_factor, 6),
                                    "diff_pct": matching_detail.ratio_diff_pct,
                                    "reason": matching_detail.conflict_reason
                                })

                    audit_factors.append(f)
                    
                # 新增孤立的 R4 错位事件写入审计表（它们原来不在 tdx_factors 里，而是在 ref_factors 但被 TDX 漏掉了）
                for detail in report.details:
                    if detail.conflict_reason.startswith("Shift/Missing"):
                        # 如果这段在 TDX 因子列表中不存在，应该强行补一条用于审计
                        ex_date_key = detail.ex_date.strftime('%Y-%m-%d')
                        if not any(f['ex_date'].strftime('%Y-%m-%d') == ex_date_key for f in tdx_factors):
                            audit_factors.append({
                                'instrument_id': instrument_id,
                                'ex_date': detail.ex_date,
                                'factor': detail.tdx_factor,
                                'cumulative_factor': detail.tdx_cumulative,
                                'validation_result': report.result.value,
                                'ref_factor': detail.ref_factor,
                                'ref_source': getattr(ref_source, 'name', str(ref_source)),
                                'ratio_diff_pct': detail.ratio_diff_pct,
                                'conflict_reason': detail.conflict_reason,
                                'source': 'tdx_xdxr'
                            })

                if audit_factors:
                    await self.db_ops.save_tdx_audit_factors(audit_factors)

            except Exception as e:
                audit_logger.error(f"Error auditing {instrument_id}: {e}")
                
            progress["completed"].append(instrument_id)
            if i % self.PROGRESS_SAVE_INTERVAL == 0:
                self._save_progress(exchange, progress)
                audit_logger.info(f"Auditing [{exchange}] {i}/{total_targets} ({(i/total_targets)*100:.1f}%)")

        self._save_progress(exchange, progress)
        audit_logger.info(f"Audit completed for {exchange}. Formatting report...")
        
        report_path = await self.generate_report(exchange, progress)
        
        # 清除满状态进度，允许下次全量重开
        if mode == "full":
            self._save_progress(exchange, {})
            
        return report_path
        
    async def generate_report(self, exchange: str, progress: dict) -> str:
        """生成 Markdown 格式的审计报告"""
        date_str = get_shanghai_time().strftime("%Y%m%d")
        file_path = os.path.join(self.reports_dir, f"tdx_audit_{exchange}_{date_str}.md")
        
        total_completed = len(progress.get("completed", []))
        results = progress.get("results", {})
        
        md_lines = []
        md_lines.append(f"# 自研复权因子审计报告 ({exchange})")
        md_lines.append("")
        md_lines.append("## 基本信息")
        md_lines.append(f"- **交易所**: {exchange}")
        md_lines.append(f"- **生成时间**: {get_shanghai_time().strftime('%Y-%m-%d %H:%M:%S')}")
        md_lines.append(f"- **处理品种数**: {total_completed}")
        md_lines.append("")
        
        md_lines.append("## 验证摘要")
        md_lines.append("| 验证结果 | 数量 | 占比 |")
        md_lines.append("|---|---|---|")
        for k, v in results.items():
            pct = round(v / max(total_completed, 1) * 100, 2)
            icon = "✅" if "all_pass" in k else "⚠️" if "partial" in k else "❌" if "conflict" in k else "ℹ️"
            md_lines.append(f"| {icon} {k} | {v} | {pct}% |")
        md_lines.append("")
        
        md_lines.append("## 差异类型汇总")
        md_lines.append("| 不可接受原因 | 事件数量 |")
        md_lines.append("|---|---|")
        reasons = progress.get("conflict_reasons", {})
        if not reasons:
            md_lines.append("| 无差异事件 | 0 |")
        else:
            for k, v in sorted(reasons.items(), key=lambda x: x[1], reverse=True):
                md_lines.append(f"| {k} | {v} |")
        md_lines.append("")
        
        md_lines.append("## Top 不可接受差异明细")
        top_c = progress.get("top_conflicts", [])
        if not top_c:
            md_lines.append("*未发现严重差异。*")
        else:
            md_lines.append("| 品种 | 除权日 | TDX因子 | 权威因子 | 差异幅度 | 判定结论 |")
            md_lines.append("|---|---|---|---|---|---|")
            for c in top_c[:self.TOP_CONFLICTS_LIMIT]:
                md_lines.append(f"| {c['instrument_id']} | {c['ex_date']} | {c['tdx_factor']} | {c['ref_factor']} | {c['diff_pct']}% | {c['reason']} |")
                
        with open(file_path, "w", encoding="utf-8") as f:
            f.write("\n".join(md_lines))
            
        return file_path
