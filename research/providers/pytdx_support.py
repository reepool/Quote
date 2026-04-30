"""
Shared pytdx research provider helpers.
"""

from __future__ import annotations

from typing import Any, Optional, Sequence, Tuple

from pytdx.config.hosts import hq_hosts
from pytdx.hq import TdxHq_API


EXCHANGE_TO_MARKET = {
    "SZSE": 0,
    "SSE": 1,
    "BSE": 2,
}

MARKET_TO_EXCHANGE = {
    0: "SZSE",
    1: "SSE",
    2: "BSE",
}

SUFFIX_TO_MARKET = {
    ".SZ": 0,
    ".SH": 1,
    ".BJ": 2,
}


class PytdxProviderMixin:
    """Shared direct-connection behavior for pytdx research providers."""

    def __init__(
        self,
        *,
        connection_pool: Any = None,
        hosts: Optional[Sequence[Tuple[str, str, int]]] = None,
        connection_timeout: float = 10.0,
        max_connect_attempts: int = 5,
    ):
        self._connection_pool = connection_pool
        self._hosts = list(hosts) if hosts is not None else list(hq_hosts)
        self._connection_timeout = connection_timeout
        self._max_connect_attempts = max_connect_attempts

    def _get_api(self) -> Tuple[Any, bool]:
        if self._connection_pool is not None:
            return self._connection_pool.get_connection(), False

        api = TdxHq_API()
        for _, ip, port in self._hosts[:self._max_connect_attempts]:
            try:
                connected = api.connect(ip, port, time_out=self._connection_timeout)
            except Exception:
                connected = False
            if connected:
                return api, True

        raise ConnectionError("pytdx research provider could not connect to any configured host")

    @staticmethod
    def parse_instrument_id(instrument_id: str) -> Optional[Tuple[int, str]]:
        parts = instrument_id.rsplit(".", 1)
        if len(parts) != 2:
            return None
        code, suffix = parts[0], f".{parts[1].upper()}"
        market = SUFFIX_TO_MARKET.get(suffix)
        if market is None:
            return None
        return market, code

    @staticmethod
    def normalize_numeric_date(value: Any) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        if not text or text in {"0", "0000-00-00"}:
            return None
        if len(text) == 8 and text.isdigit():
            return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"
        return text

    @staticmethod
    def to_float(value: Any) -> Optional[float]:
        if value in {None, "", "--"}:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
