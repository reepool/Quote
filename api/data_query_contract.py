"""Validation/error adaptation for the explicitly repaired data-read endpoints."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from fastapi import HTTPException as QueryHTTPException
from fastapi.dependencies.utils import get_flat_dependant
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.routing import APIRoute
from starlette.exceptions import HTTPException
from starlette.responses import JSONResponse

SCOPED_ENDPOINTS = frozenset(
    {
        "get_daily_quotes",
        "get_latest_quotes",
        "get_quotes_coverage",
        "get_research_financial_statements",
        "get_research_financial_statements_history",
        "get_research_technical_summary",
        "get_research_relative_valuation",
        "get_research_risk_free_rate",
        "get_financial_pit_cross_section",
    }
)


def invalid_query(
    field: str, message: str, hint: str = "Check the documented query parameters"
) -> QueryHTTPException:
    """Keep explicit new-parameter errors machine readable when unit-called too."""
    return QueryHTTPException(
        422,
        detail={
            "error_code": "invalid_query_parameter",
            "message": message,
            "field": field,
            "hint": hint,
        },
    )


class DataQueryRoute(APIRoute):
    """Preserve untouched routes; adapt the named repair endpoints only."""

    def get_route_handler(self) -> Callable:
        original = super().get_route_handler()
        if self.endpoint.__name__ not in SCOPED_ENDPOINTS:
            return original
        accepted = {
            field.alias for field in get_flat_dependant(self.dependant).query_params
        }

        async def handle(request):
            unknown = sorted(set(request.query_params) - accepted)
            if unknown:
                return JSONResponse(
                    status_code=422,
                    content={
                        "error_code": "unknown_query_parameter",
                        "message": "Unknown query parameters: " + ", ".join(unknown),
                        "field": unknown[0] if len(unknown) == 1 else None,
                        "hint": "Remove the unknown fields or use the documented parameter names",
                        "detail": {"unknown_fields": unknown},
                        "errors": [
                            {"field": name, "type": "unknown_query_parameter"}
                            for name in unknown
                        ],
                    },
                )
            try:
                return await original(request)
            except RequestValidationError as exc:
                errors = jsonable_encoder(
                    exc.errors(), custom_encoder={ValueError: str}
                )
                location = errors[0].get("loc", []) if errors else []
                return JSONResponse(
                    status_code=422,
                    content={
                        "error_code": "invalid_query_parameter",
                        "message": "Request validation failed",
                        "field": str(location[-1]) if location else None,
                        "hint": "Check the documented field types and bounds",
                        "detail": errors,
                        "errors": errors,
                    },
                )
            except HTTPException as exc:
                detail = exc.detail
                if isinstance(detail, dict) and detail.get("error_code"):
                    payload: dict[str, Any] = {**detail, "detail": detail}
                else:
                    missing_date = (
                        self.endpoint.__name__ == "get_quotes_coverage"
                        and str(detail).startswith("Provide either")
                    )
                    code = (
                        "missing_date_range"
                        if missing_date
                        else (
                            "invalid_query_parameter"
                            if exc.status_code in {400, 422}
                            else "not_found"
                            if exc.status_code == 404
                            else "data_unavailable"
                            if exc.status_code in {409, 503}
                            else "request_failed"
                        )
                    )
                    payload = {
                        "error_code": code,
                        "message": str(detail),
                        "field": "date"
                        if missing_date
                        else "adjust"
                        if self.endpoint.__name__ == "get_research_technical_summary"
                        and "adjust" in str(detail)
                        else None,
                        "hint": "Provide date or start_date/end_date"
                        if missing_date
                        else "Check the request and data availability",
                        "detail": detail,
                    }
                return JSONResponse(
                    status_code=exc.status_code,
                    content=jsonable_encoder(payload),
                    headers=exc.headers,
                )

        return handle
