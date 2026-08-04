"""Disabled-by-default rollout policy for integrated backtest stages."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional


DEFAULT_ROLLOUT_PATH = Path(__file__).resolve().parents[2] / "config" / "backtest_data_rollout.json"


@dataclass(frozen=True)
class BacktestStagePolicy:
    enabled: bool = False
    timeout_seconds: int = 60
    retry_count: int = 0
    continue_on_error: bool = True
    freshness_hours: int = 24
    max_rows: int = 5000


class BacktestRolloutPolicy:
    def __init__(self, raw: Mapping[str, Any]):
        self.raw = dict(raw)

    @classmethod
    def load(cls, path: Optional[str | Path] = None) -> "BacktestRolloutPolicy":
        target = Path(path) if path else DEFAULT_ROLLOUT_PATH
        with target.open("r", encoding="utf-8") as handle:
            raw = json.load(handle)
        if raw.get("schema_version") != "backtest-data-rollout.v1":
            raise ValueError("unsupported backtest rollout schema_version")
        return cls(raw)

    def stage(self, name: str) -> BacktestStagePolicy:
        stages = self.raw.get("stages") if isinstance(self.raw.get("stages"), Mapping) else {}
        value = stages.get(name) if isinstance(stages.get(name), Mapping) else {}
        return BacktestStagePolicy(
            enabled=bool(value.get("enabled", False)),
            timeout_seconds=max(1, int(value.get("timeout_seconds", 60))),
            retry_count=max(0, int(value.get("retry_count", 0))),
            continue_on_error=bool(value.get("continue_on_error", True)),
            freshness_hours=max(1, int(value.get("freshness_hours", 24))),
            max_rows=max(1, int(value.get("max_rows", 5000))),
        )
