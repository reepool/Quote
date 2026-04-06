"""
复权因子交叉验证器

对比自研因子(XDXR)与权威源因子(BaoStock/AkShare)的一致性.
核心原则: 比较「事件日期 + 单日因子变动比率」, 不比较累积因子绝对值.

设计:
  - 验证是旁路操作, 不阻塞生产数据流
  - 验证结果写入审计表 + Telegram 告警
  - 容差: 单日因子比率差异 < 0.1%
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

tdx_validator_logger = logging.getLogger("tdx_factor_validator")


class FactorValidationResult(Enum):
    """验证结果枚举"""
    ALL_PASS = "all_pass"          # 所有重叠事件均通过
    PARTIAL = "partial"            # 部分无重叠但无冲突
    CONFLICT = "conflict"          # 发现不一致
    NO_OVERLAP = "no_overlap"      # 无重叠事件可比较
    ERROR = "error"                # 验证过程出错


@dataclass
class ValidationDetail:
    """单个事件的验证详情"""
    ex_date: datetime
    instrument_id: str
    tdx_factor: float
    ref_factor: float
    tdx_cumulative: float
    ref_cumulative: float
    ratio_diff_pct: float  # 单日因子比率差异百分比
    passed: bool
    conflict_reason: str = "" # R1(精度误差), R2(轻微浮动), R3(严重漂移), R4(缺失/错位) 等
    note: str = ""


@dataclass
class ValidationReport:
    """验证报告"""
    instrument_id: str
    result: FactorValidationResult = FactorValidationResult.NO_OVERLAP
    total_tdx_events: int = 0
    total_ref_events: int = 0
    overlap_count: int = 0
    pass_count: int = 0
    conflict_count: int = 0
    tdx_only_count: int = 0  # 仅 tdx 有的事件
    ref_only_count: int = 0  # 仅权威源有的事件
    details: list[ValidationDetail] = field(default_factory=list)
    error_msg: str = ""

    def to_dict(self) -> dict:
        """转为可序列化的 dict"""
        return {
            "instrument_id": self.instrument_id,
            "result": self.result.value,
            "total_tdx_events": self.total_tdx_events,
            "total_ref_events": self.total_ref_events,
            "overlap_count": self.overlap_count,
            "pass_count": self.pass_count,
            "conflict_count": self.conflict_count,
            "tdx_only_count": self.tdx_only_count,
            "ref_only_count": self.ref_only_count,
            "details": [
                {
                    "ex_date": d.ex_date.isoformat(),
                    "tdx_factor": d.tdx_factor,
                    "ref_factor": d.ref_factor,
                    "ratio_diff_pct": d.ratio_diff_pct,
                    "passed": d.passed,
                    "conflict_reason": d.conflict_reason,
                    "note": d.note,
                }
                for d in self.details
            ],
            "error_msg": self.error_msg,
        }

    def summary_text(self) -> str:
        """生成人类可读的摘要"""
        if self.result == FactorValidationResult.ALL_PASS:
            return (
                f"✅ {self.instrument_id}: "
                f"{self.overlap_count} 个事件全部通过"
            )
        elif self.result == FactorValidationResult.CONFLICT:
            conflicts = [d for d in self.details if not d.passed]
            conflict_dates = ", ".join(
                d.ex_date.strftime("%Y-%m-%d") for d in conflicts[:3]
            )
            return (
                f"⚠️ {self.instrument_id}: "
                f"{self.conflict_count}/{self.overlap_count} 个事件不一致 "
                f"({conflict_dates})"
            )
        elif self.result == FactorValidationResult.NO_OVERLAP:
            return (
                f"ℹ️ {self.instrument_id}: "
                f"tdx={self.total_tdx_events} ref={self.total_ref_events} 无重叠"
            )
        elif self.result == FactorValidationResult.ERROR:
            return f"❌ {self.instrument_id}: {self.error_msg}"
        else:
            return (
                f"🔶 {self.instrument_id}: "
                f"部分验证 pass={self.pass_count} conflict={self.conflict_count}"
            )


class TdxFactorValidator:
    """复权因子交叉验证器"""

    def __init__(self, tolerance: float = 0.001, warning_threshold: float = 0.01):
        """
        Args:
            tolerance: 单日因子比率差异容差 (0.001 = 0.1%), 低于此值判定为 Acceptable
            warning_threshold: 轻微浮动上限 (0.01 = 1.0%), 低于此值判定为 Warning, 高于则为 Drift
        """
        self.tolerance = tolerance
        self.warning_threshold = warning_threshold

    def validate(
        self,
        instrument_id: str,
        tdx_factors: list[dict],
        ref_factors: list[dict],
    ) -> ValidationReport:
        """执行交叉验证

        Args:
            instrument_id: 品种 ID
            tdx_factors: 自研因子列表 (含 ex_date, factor, cumulative_factor)
            ref_factors: 权威源因子列表 (同上)

        Returns:
            ValidationReport
        """
        report = ValidationReport(instrument_id=instrument_id)

        try:
            report.total_tdx_events = len(tdx_factors)
            report.total_ref_events = len(ref_factors)

            if not tdx_factors and not ref_factors:
                report.result = FactorValidationResult.ALL_PASS
                return report

            if not tdx_factors or not ref_factors:
                report.result = FactorValidationResult.NO_OVERLAP
                return report

            # 构建日期索引
            tdx_by_date: dict[str, dict] = {}
            for f in tdx_factors:
                ex_date = f.get("ex_date")
                if isinstance(ex_date, datetime):
                    key = ex_date.strftime("%Y-%m-%d")
                else:
                    key = str(ex_date)[:10]
                tdx_by_date[key] = f

            ref_by_date: dict[str, dict] = {}
            for f in ref_factors:
                ex_date = f.get("ex_date")
                if isinstance(ex_date, datetime):
                    key = ex_date.strftime("%Y-%m-%d")
                else:
                    key = str(ex_date)[:10]
                ref_by_date[key] = f

            # 统计
            all_dates = set(tdx_by_date.keys()) | set(ref_by_date.keys())
            overlap_dates = set(tdx_by_date.keys()) & set(ref_by_date.keys())
            report.overlap_count = len(overlap_dates)
            report.tdx_only_count = len(set(tdx_by_date.keys()) - overlap_dates)
            report.ref_only_count = len(set(ref_by_date.keys()) - overlap_dates)

            if not overlap_dates:
                report.result = FactorValidationResult.NO_OVERLAP
                return report

            # 逐事件比较单日因子比率
            for date_key in sorted(overlap_dates):
                tdx_f = tdx_by_date[date_key]
                ref_f = ref_by_date[date_key]

                tdx_day_factor = float(tdx_f.get("factor", 1.0))
                ref_day_factor = float(ref_f.get("factor", 1.0))

                # 计算比率差异
                if ref_day_factor > 0:
                    ratio_diff = abs(tdx_day_factor - ref_day_factor) / ref_day_factor
                else:
                    ratio_diff = abs(tdx_day_factor - ref_day_factor)

                ratio_diff_pct = round(ratio_diff * 100, 4)
                passed = ratio_diff <= self.tolerance
                
                conflict_reason = ""
                if passed:
                    report.pass_count += 1
                    conflict_reason = "Acceptable (精度差异)" if ratio_diff > 0 else ""
                else:
                    report.conflict_count += 1
                    if ratio_diff < self.warning_threshold:
                        conflict_reason = "Warning (轻微浮动)"
                    else:
                        conflict_reason = "Drift (严重漂移)"

                ex_date = tdx_f.get("ex_date", datetime.min)
                if isinstance(ex_date, str):
                    try:
                        ex_date = datetime.strptime(ex_date[:10], "%Y-%m-%d")
                    except ValueError:
                        ex_date = datetime.min

                detail = ValidationDetail(
                    ex_date=ex_date,
                    instrument_id=instrument_id,
                    tdx_factor=tdx_day_factor,
                    ref_factor=ref_day_factor,
                    tdx_cumulative=float(tdx_f.get("cumulative_factor", 0)),
                    ref_cumulative=float(ref_f.get("cumulative_factor", 0)),
                    ratio_diff_pct=ratio_diff_pct,
                    passed=passed,
                    conflict_reason=conflict_reason,
                )
                report.details.append(detail)
                
            # 处理完全错位的事件 (R4)
            only_tdx_dates = set(tdx_by_date.keys()) - overlap_dates
            for date_key in sorted(only_tdx_dates):
                tdx_f = tdx_by_date[date_key]
                ex_date = tdx_f.get("ex_date", datetime.min)
                if isinstance(ex_date, str):
                    try:
                        ex_date = datetime.strptime(ex_date[:10], "%Y-%m-%d")
                    except ValueError: pass
                detail = ValidationDetail(
                    ex_date=ex_date,
                    instrument_id=instrument_id,
                    tdx_factor=float(tdx_f.get("factor", 1.0)),
                    ref_factor=1.0,
                    tdx_cumulative=float(tdx_f.get("cumulative_factor", 0)),
                    ref_cumulative=0.0,
                    ratio_diff_pct=100.0,
                    passed=False,
                    conflict_reason="Shift/Missing (TDX单边事件)"
                )
                report.details.append(detail)
                
            only_ref_dates = set(ref_by_date.keys()) - overlap_dates
            for date_key in sorted(only_ref_dates):
                ref_f = ref_by_date[date_key]
                ex_date = ref_f.get("ex_date", datetime.min)
                if isinstance(ex_date, str):
                    try:
                        ex_date = datetime.strptime(ex_date[:10], "%Y-%m-%d")
                    except ValueError: pass
                detail = ValidationDetail(
                    ex_date=ex_date,
                    instrument_id=instrument_id,
                    tdx_factor=1.0,
                    ref_factor=float(ref_f.get("factor", 1.0)),
                    tdx_cumulative=0.0,
                    ref_cumulative=float(ref_f.get("cumulative_factor", 0)),
                    ratio_diff_pct=100.0,
                    passed=False,
                    conflict_reason="Shift/Missing (REF单边事件)"
                )
                report.details.append(detail)
                
            # 重新根据细节排查日期排序
            report.details.sort(key=lambda d: d.ex_date)

            # 确定总体结果
            if report.conflict_count > 0:
                report.result = FactorValidationResult.CONFLICT
            elif report.tdx_only_count > 0 or report.ref_only_count > 0:
                report.result = FactorValidationResult.PARTIAL
            else:
                report.result = FactorValidationResult.ALL_PASS

        except Exception as e:
            report.result = FactorValidationResult.ERROR
            report.error_msg = str(e)
            tdx_validator_logger.error(
                f"[TdxFactorValidator] 验证出错 {instrument_id}: {e}"
            )

        return report

    def validate_batch(
        self,
        tdx_factors_map: dict[str, list[dict]],
        ref_factors_map: dict[str, list[dict]],
    ) -> dict[str, ValidationReport]:
        """批量验证

        Args:
            tdx_factors_map: {instrument_id: [factors]}
            ref_factors_map: {instrument_id: [factors]}

        Returns:
            {instrument_id: ValidationReport}
        """
        all_ids = set(tdx_factors_map.keys()) | set(ref_factors_map.keys())
        results: dict[str, ValidationReport] = {}

        for instrument_id in sorted(all_ids):
            tdx_factors = tdx_factors_map.get(instrument_id, [])
            ref_factors = ref_factors_map.get(instrument_id, [])
            results[instrument_id] = self.validate(
                instrument_id, tdx_factors, ref_factors
            )

        return results

    def generate_summary(
        self, reports: dict[str, ValidationReport]
    ) -> dict[str, Any]:
        """生成验证摘要报告"""
        total = len(reports)
        by_result: dict[str, int] = {}
        conflict_instruments: list[str] = []

        for instrument_id, report in reports.items():
            result_key = report.result.value
            by_result[result_key] = by_result.get(result_key, 0) + 1
            if report.result == FactorValidationResult.CONFLICT:
                conflict_instruments.append(instrument_id)

        return {
            "total_instruments": total,
            "by_result": by_result,
            "conflict_instruments": conflict_instruments[:20],  # 最多 20 个
            "all_pass_rate": round(
                by_result.get("all_pass", 0) / total * 100, 1
            ) if total > 0 else 0,
        }
