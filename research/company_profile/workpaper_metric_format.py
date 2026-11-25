"""Versioned opt-in preservation of source-native unnormalized metrics.

This is a local format operation, not a source or semantic approval. Callers
must still apply the ordinary typed fact, source binding and review contracts.
"""
from __future__ import annotations

import re
from collections.abc import Mapping
from copy import deepcopy
from decimal import Decimal

METRIC_FORMAT_POLICY = "source_metric_unparsed_value_null.v1"
RAW_NULL_METRIC_FORMAT_POLICY = "source_metric_unparsed_value_null.v2.raw-null-accounting"
METRIC_FORMAT_POLICIES = (METRIC_FORMAT_POLICY, RAW_NULL_METRIC_FORMAT_POLICY)
METRIC_FORMAT_FLAG = "workpaper_metric_format_policy"
_DIRECTIONAL_PERCENT = re.compile(r"(?:同比)?(?:下降|增长)[0-9]+(?:\.[0-9]+)?%")
_DECIMAL_STRING = re.compile(r"-?[0-9]+(?:\.[0-9]+)?")
_LOCAL_REASON = "程序说明：本地格式不能安全标准化；保留原始方向表达，不推断数值或符号。"
_RAW_NULL_REASON = "程序说明：模型未提供标准化数值及说明；保留原文和null，未判断来源不确定的原因。"
_ACCOUNTING_REASON = "程序说明：原会计括号写法与模型明确负值提案一致；保留原文和null，未作数值标准化。"
_ACCOUNTING_NUMBER = re.compile(r"(?:\((\d+|\d{1,3}(?:,\d{3})+)(\.\d+)?\)|（(\d+|\d{1,3}(?:,\d{3})+)(\.\d+)?）)")


def _compatible_units(metric):
    from .workpaper import _money_unit_spelling

    unit, source = metric.get("unit"), metric.get("source_unit")
    if unit is None or source is None or unit == source:
        return True
    return isinstance(unit, str) and isinstance(source, str) and (
        (unit, source) in {("百分点", "%"), ("百分点", "％"), ("百分点", "个百分点")}
        or bool(_money_unit_spelling(unit) and _money_unit_spelling(unit) == _money_unit_spelling(source)))


def normalize_directional_metric_format(raw_fact, *, policy=None):
    """Copy one raw fact, preserving narrow source phrases with null values.

    No policy means no change; v1 keeps its directional-percent behavior. V2
    additionally supplies a program-only explanation for raw/null or preserves
    complete accounting parentheses beside an agreeing explicit negative value.
    All other fields survive typed/source checks; none are dropped or guessed.
    """
    if policy not in (None, *METRIC_FORMAT_POLICIES):
        raise ValueError(f"unknown workpaper metric format policy: {policy!r}")
    copied = deepcopy(raw_fact)
    if policy is None or not isinstance(copied, Mapping):
        return copied, ()
    metrics = copied.get("metrics")
    if not isinstance(metrics, (tuple, list)):
        return copied, ()
    notes = []
    for index, metric in enumerate(metrics):
        if not isinstance(metric, dict):
            continue
        raw, value = metric.get("raw_value"), metric.get("value")
        if policy == RAW_NULL_METRIC_FORMAT_POLICY and isinstance(raw, str) and raw.strip() and _compatible_units(metric):
            reason = metric.get("uncertainty_reason")
            local_reason = None
            if value is None and reason is None:
                # Do not repair wrong types, qualifiers, units or other fields;
                # the unchanged strict model and source checks still own them.
                local_reason = _RAW_NULL_REASON
            elif (isinstance(value, str) and _DECIMAL_STRING.fullmatch(value)
                  and Decimal(value) < 0 and _ACCOUNTING_NUMBER.fullmatch(raw)
                  and metric.get("qualifier", "exact") == "exact"
                  and (reason is None or isinstance(reason, str) and bool(reason))):
                amount = Decimal(raw[1:-1].replace(",", ""))
                if -amount == Decimal(value):
                    local_reason = _ACCOUNTING_REASON
            if local_reason:
                metric["value"] = None
                metric["uncertainty_reason"] = "；".join(filter(None, (reason, local_reason)))
                notes.append(f"事实{copied.get('fact_id', '未知编号')}.metrics.{index}：{local_reason}")
                continue
        if not isinstance(raw, str) or not _DIRECTIONAL_PERCENT.fullmatch(raw):
            continue
        if (not isinstance(value, str) or not _DECIMAL_STRING.fullmatch(value)
            or metric.get("unit") != "%" or metric.get("source_unit") not in (None, "%")
            or metric.get("qualifier", "exact") != "exact"
            or (metric.get("uncertainty_reason") is not None
                and (not isinstance(metric["uncertainty_reason"], str) or not metric["uncertainty_reason"]))):
            continue
        metric["value"] = None
        metric["uncertainty_reason"] = "；".join(filter(None, (metric.get("uncertainty_reason"), _LOCAL_REASON)))
        notes.append(f"事实{copied.get('fact_id', '未知编号')}.metrics.{index}：{_LOCAL_REASON}")
    return copied, tuple(notes)
