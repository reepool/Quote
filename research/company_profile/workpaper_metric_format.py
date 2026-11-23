"""Opt-in preservation of the observed directional percentage spellings.

This is a local format operation, not a source or semantic approval. Callers
must still apply the ordinary typed fact, source binding and review contracts.
"""
from __future__ import annotations

import re
from collections.abc import Mapping
from copy import deepcopy

METRIC_FORMAT_POLICY = "source_metric_unparsed_value_null.v1"
METRIC_FORMAT_FLAG = "workpaper_metric_format_policy"
_DIRECTIONAL_PERCENT = re.compile(r"(?:同比)?(?:下降|增长)[0-9]+(?:\.[0-9]+)?%")
_DECIMAL_STRING = re.compile(r"-?[0-9]+(?:\.[0-9]+)?")
_LOCAL_REASON = "程序说明：本地格式不能安全标准化；保留原始方向表达，不推断数值或符号。"


def normalize_directional_metric_format(raw_fact, *, policy=None):
    """Copy one raw fact, preserving narrow source phrases with null values.

    No policy means no change. Unknown policies fail closed. Only a string
    decimal proposal beside an exact, same-unit directional percentage can be
    replaced by null. All other fields survive for the caller's typed checks;
    invalid types, units or unknown fields are never removed or repaired.
    """
    if policy not in (None, METRIC_FORMAT_POLICY):
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
