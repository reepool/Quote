"""Bounded parser for Sina factor assignment payloads without code execution."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Optional


MAX_SINA_FACTOR_BODY_BYTES = 512_000
MAX_SINA_FACTOR_ROWS = 20_000
_ASSIGNMENT = re.compile(
    r"\A\s*(?:var\s+)?[A-Za-z_$][A-Za-z0-9_$\.\[\]\"']*\s*=\s*(.*?)\s*;?\s*\Z",
    re.DOTALL,
)


@dataclass(frozen=True)
class SinaFactorParseError(ValueError):
    code: str
    message: str
    http_status: Optional[int] = None
    response_hash: Optional[str] = None

    def __str__(self) -> str:
        return self.message


def parse_sina_qfq_factor_response(
    body: str | bytes,
    *,
    http_status: int,
    base_date: str = "1900-01-01",
    require_base_date: bool = True,
    max_body_bytes: int = MAX_SINA_FACTOR_BODY_BYTES,
) -> list[dict[str, str]]:
    raw = body if isinstance(body, bytes) else str(body or "").encode("utf-8")
    response_hash = hashlib.sha256(raw).hexdigest()
    if http_status < 200 or http_status >= 300:
        raise _error("http_status_invalid", "Sina factor HTTP status is not successful", http_status, response_hash)
    if not raw:
        raise _error("empty_body", "Sina factor response is empty", http_status, response_hash)
    if len(raw) > max(1, int(max_body_bytes)):
        raise _error("body_oversized", "Sina factor response exceeds size bound", http_status, response_hash)
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        try:
            text = raw.decode("gb18030")
        except UnicodeDecodeError as exc:
            raise _error("body_encoding_invalid", "Sina factor response encoding is invalid", http_status, response_hash) from exc
    if text.lstrip().lower().startswith(("<html", "<!doctype")):
        raise _error("html_response", "Sina factor endpoint returned HTML", http_status, response_hash)
    match = _ASSIGNMENT.fullmatch(text)
    if match is None:
        raise _error("assignment_shape_invalid", "Sina factor response has invalid assignment shape", http_status, response_hash)
    literal = match.group(1).strip()
    try:
        payload = json.loads(literal)
    except json.JSONDecodeError:
        try:
            payload = _StrictLiteralParser(literal).parse()
        except ValueError as exc:
            raise _error("payload_syntax_invalid", "Sina factor payload is malformed", http_status, response_hash) from exc
    if not isinstance(payload, dict):
        raise _error("payload_type_invalid", "Sina factor payload must be an object", http_status, response_hash)
    rows = payload.get("data")
    if not isinstance(rows, list):
        raise _error("data_rows_missing", "Sina factor payload has no data rows", http_status, response_hash)
    if len(rows) > MAX_SINA_FACTOR_ROWS:
        raise _error("row_count_exceeded", "Sina factor row count exceeds bound", http_status, response_hash)
    declared = payload.get("total")
    if declared is not None:
        try:
            if int(declared) != len(rows):
                raise ValueError
        except (TypeError, ValueError):
            raise _error("declared_total_mismatch", "Sina factor response is truncated or inconsistent", http_status, response_hash)
    normalized: list[dict[str, str]] = []
    for index, row in enumerate(rows):
        if isinstance(row, dict):
            date_value = row.get("date")
            factor_value = row.get("qfq_factor")
        elif isinstance(row, list) and len(row) == 2:
            date_value, factor_value = row
        else:
            raise _error("row_shape_invalid", f"Sina factor row {index} has invalid shape", http_status, response_hash)
        date_text = str(date_value or "").strip()
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_text) is None:
            raise _error("row_date_invalid", f"Sina factor row {index} has invalid date", http_status, response_hash)
        try:
            factor = Decimal(str(factor_value))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise _error("row_factor_invalid", f"Sina factor row {index} has invalid factor", http_status, response_hash) from exc
        if not factor.is_finite() or factor <= 0:
            raise _error("row_factor_nonpositive", f"Sina factor row {index} has non-positive factor", http_status, response_hash)
        normalized.append({"date": date_text, "qfq_factor": str(factor)})
    if not normalized:
        raise _error("data_rows_empty", "Sina factor payload contains no rows", http_status, response_hash)
    if require_base_date and not any(row["date"] == base_date for row in normalized):
        raise _error("base_row_missing", "Sina factor payload is missing the configured base row", http_status, response_hash)
    return normalized


class _StrictLiteralParser:
    """Parse the JSON-compatible JS/Python literal subset used by Sina."""

    def __init__(self, text: str) -> None:
        self.text = text
        self.index = 0

    def parse(self) -> Any:
        value = self._value()
        self._space()
        if self.index != len(self.text):
            raise ValueError("trailing literal content")
        return value

    def _value(self) -> Any:
        self._space()
        if self.index >= len(self.text):
            raise ValueError("unexpected end")
        char = self.text[self.index]
        if char == "{":
            return self._object()
        if char == "[":
            return self._array()
        if char in {'"', "'"}:
            return self._string()
        for word, value in (("true", True), ("false", False), ("null", None), ("None", None)):
            if self.text.startswith(word, self.index):
                self.index += len(word)
                return value
        match = re.match(r"-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?", self.text[self.index :])
        if match:
            self.index += len(match.group(0))
            return Decimal(match.group(0))
        raise ValueError("unsupported literal token")

    def _object(self) -> dict[str, Any]:
        self.index += 1
        output: dict[str, Any] = {}
        self._space()
        if self._take("}"):
            return output
        while True:
            self._space()
            key = self._string()
            self._space()
            if not self._take(":"):
                raise ValueError("missing object colon")
            output[key] = self._value()
            self._space()
            if self._take("}"):
                return output
            if not self._take(","):
                raise ValueError("missing object comma")

    def _array(self) -> list[Any]:
        self.index += 1
        output: list[Any] = []
        self._space()
        if self._take("]"):
            return output
        while True:
            output.append(self._value())
            self._space()
            if self._take("]"):
                return output
            if not self._take(","):
                raise ValueError("missing array comma")

    def _string(self) -> str:
        quote = self.text[self.index]
        if quote not in {'"', "'"}:
            raise ValueError("object key must be quoted")
        self.index += 1
        output: list[str] = []
        while self.index < len(self.text):
            char = self.text[self.index]
            self.index += 1
            if char == quote:
                return "".join(output)
            if char == "\\":
                if self.index >= len(self.text):
                    raise ValueError("truncated string escape")
                escaped = self.text[self.index]
                self.index += 1
                mapping = {"n": "\n", "r": "\r", "t": "\t", "\\": "\\", "'": "'", '"': '"', "/": "/"}
                if escaped not in mapping:
                    raise ValueError("unsupported string escape")
                output.append(mapping[escaped])
            else:
                output.append(char)
        raise ValueError("unterminated string")

    def _space(self) -> None:
        while self.index < len(self.text) and self.text[self.index].isspace():
            self.index += 1

    def _take(self, token: str) -> bool:
        if self.text.startswith(token, self.index):
            self.index += len(token)
            return True
        return False


def _error(code: str, message: str, status: int, response_hash: str) -> SinaFactorParseError:
    return SinaFactorParseError(code, message, status, response_hash)
