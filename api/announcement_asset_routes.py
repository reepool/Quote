"""FastAPI resources for shared annual-report assets."""

from __future__ import annotations

import asyncio
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from data_manager import data_manager
from research.announcement_assets.access import (
    AssetContentGoneError,
    AssetContentIntegrityError,
    AssetContentMountError,
)
from research.announcement_assets.models import EnsureRequest
from research.announcement_assets.repository import IdempotencyConflictError

from .announcement_asset_models import (
    AcquisitionStatusValue,
    AnnualReportAssetListResponse,
    AnnualReportConsumerRequestResponse,
    AnnualReportEffectiveResponse,
    AnnualReportEnsureRequestModel,
    AnnualReportEnsureResponse,
    AnnualReportErrorEnvelope,
    AnnualReportReadinessResponse,
    AnnualReportRecordStateValue,
    AnnualReportRequestResponse,
    AssetAvailabilityValue,
    BusinessAnnualReportProcessRequest,
    IntegrityStatusValue,
)

router = APIRouter()

_ERROR_RESPONSES = {
    status: {"model": AnnualReportErrorEnvelope}
    for status in (401, 403, 404, 409, 410, 422, 429, 503)
}
_ENSURE_RESPONSES = {
    **_ERROR_RESPONSES,
    202: {"model": AnnualReportEnsureResponse},
}
_CONSUMER_RESPONSES = {
    **_ERROR_RESPONSES,
    202: {"model": AnnualReportConsumerRequestResponse},
}

_PERMISSION_KEYS = {
    "annual_report_assets:acquire": "acquire",
    "annual_report_assets:read_content": "read_content",
    "annual_report_assets:operator": "operator",
    "business_profile:process": "business_profile_process",
    "broker_risk_control:process": "broker_risk_control_process",
}


async def _stream_validated_handle(handle, *, chunk_size: int = 1024 * 1024):
    try:
        while True:
            chunk = await asyncio.to_thread(handle.read, chunk_size)
            if not chunk:
                break
            yield chunk
    finally:
        # Closing releases the durable read lease. Keep teardown synchronous so
        # ASGI response completion cannot strand a second default-executor job
        # after the final read.
        handle.close()


def _principal(request: Request, required_scope: str | None = None) -> str:
    principal = str(getattr(request.state, "annual_report_principal", "") or "").strip()
    if not principal:
        raise HTTPException(
            status_code=503,
            detail={
                "schema_version": "annual_report_error.v1",
                "error_code": "authorization_boundary_unavailable",
                "message": "trusted identity boundary is not configured",
                "retryable": False,
            },
        )
    if required_scope:
        permissions = getattr(request.state, "annual_report_permissions", frozenset())
        configured_names = getattr(
            request.state, "annual_report_permission_names", {}
        )
        effective_scope = configured_names.get(
            _PERMISSION_KEYS.get(required_scope, ""), required_scope
        )
        if effective_scope not in permissions:
            raise HTTPException(
                status_code=403,
                detail={
                    "schema_version": "annual_report_error.v1",
                    "error_code": "permission_denied",
                    "message": "required annual-report asset permission is missing",
                    "retryable": False,
                },
            )
    return principal


def _has_scope(request: Request, scope: str) -> bool:
    configured_names = getattr(request.state, "annual_report_permission_names", {})
    effective_scope = configured_names.get(_PERMISSION_KEYS.get(scope, ""), scope)
    permissions = getattr(request.state, "annual_report_permissions", frozenset())
    return effective_scope in permissions


def _require_consumer_query_scope(request: Request) -> bool:
    is_operator = _has_scope(request, "annual_report_assets:operator")
    if not is_operator and not any(
        _has_scope(request, scope)
        for scope in ("business_profile:process", "broker_risk_control:process")
    ):
        _principal(request, "business_profile:process")
    return is_operator


def _request_error(status_code: int, code: str, message: str, **details: Any) -> HTTPException:
    return HTTPException(
        status_code=status_code,
        detail={
            "schema_version": "annual_report_error.v1",
            "error_code": code,
            "message": message,
            "retryable": status_code in {429, 503},
            "details": details,
        },
    )


@router.get(
    "/research/company/{instrument_id}/annual-reports",
    response_model=AnnualReportAssetListResponse,
    responses=_ERROR_RESPONSES,
    tags=["Annual Report Assets"],
)
async def list_shared_annual_reports(
    instrument_id: str,
    fiscal_year: int | None = Query(None, ge=1990, le=2200),
    source: str | None = Query(None),
    source_announcement_id: str | None = Query(None),
    filing_id: str | None = Query(None),
    integrity: IntegrityStatusValue | None = Query(None),
    acquisition_status: AcquisitionStatusValue | None = Query(None),
    effective_state: AnnualReportRecordStateValue | None = Query(None),
    asset_availability: AssetAvailabilityValue | None = Query(None),
    availability: AssetAvailabilityValue | None = Query(None, deprecated=True),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    try:
        if (
            source_announcement_id
            and filing_id
            and source_announcement_id != filing_id
        ):
            raise ValueError(
                "filing_id conflicts with canonical source_announcement_id"
            )
        if asset_availability and availability and asset_availability != availability:
            raise ValueError("availability compatibility alias conflicts")
        return await data_manager.list_shared_annual_report_assets(
            instrument_id=instrument_id,
            fiscal_year=fiscal_year,
            source=source,
            source_announcement_id=source_announcement_id or filing_id,
            integrity=integrity,
            acquisition_status=acquisition_status,
            effective_state=effective_state,
            asset_availability=asset_availability or availability,
            limit=limit,
            offset=offset,
        )
    except ValueError as exc:
        raise _request_error(422, "invalid_selector", str(exc)) from exc


@router.get(
    "/research/company/{instrument_id}/annual-reports/effective",
    response_model=AnnualReportEffectiveResponse,
    responses=_ERROR_RESPONSES,
    tags=["Annual Report Assets"],
)
async def get_shared_effective_annual_report(
    instrument_id: str,
    fiscal_year: int | None = Query(None, ge=1990, le=2200),
    knowledge_cutoff: str | None = Query(None),
):
    try:
        result = await data_manager.get_shared_annual_report_asset(
            instrument_id,
            fiscal_year=fiscal_year,
            knowledge_cutoff=knowledge_cutoff,
        )
    except ValueError as exc:
        raise _request_error(422, "invalid_selector", str(exc)) from exc
    if result is None:
        return {
            "asset_availability": "missing",
            "availability": "missing",
            "asset": None,
        }
    availability = result["asset_availability"]
    return {
        "asset_availability": availability,
        "availability": availability,
        "asset": result,
    }


@router.post(
    "/research/company/{instrument_id}/annual-reports/ensure",
    response_model=AnnualReportEnsureResponse,
    responses=_ENSURE_RESPONSES,
    tags=["Annual Report Assets"],
)
async def ensure_shared_annual_report(
    request: Request,
    instrument_id: str,
    body: AnnualReportEnsureRequestModel,
):
    principal = _principal(
        request,
        "annual_report_assets:acquire" if body.allow_network else None,
    )
    if body.source and body.source_announcement_id:
        # Exact filing selectors must belong to the URL path instrument.
        # Exact-filing validation is a zero-write local read on a fresh catalog.
        access = data_manager._get_announcement_asset_access(initialize_schema=False)
        candidates = (
            access.repository.list_candidate_rows(
                source=body.source,
                source_announcement_id=body.source_announcement_id,
            )
            if access.repository.schema_initialized()
            else []
        )
        known = {str(row.get("instrument_id") or "").upper() for row in candidates}
        if known and instrument_id.upper() not in known:
            raise _request_error(422, "filing_instrument_mismatch", "exact filing is not bound to instrument")
    try:
        result = await data_manager.ensure_shared_annual_report(
            EnsureRequest(
                instrument_id=instrument_id,
                fiscal_year=body.fiscal_year,
                source=body.source,
                source_announcement_id=body.source_announcement_id,
                attachment_id=body.attachment_id,
                expected_content_hash=body.expected_content_hash,
                observation_version=body.observation_version,
                allow_network=body.allow_network,
                integrity_level=body.integrity_level,
                wait_seconds=body.wait_seconds,
                consumer=body.consumer,
                principal=principal,
                idempotency_key=request.headers.get("Idempotency-Key"),
                knowledge_cutoff=body.knowledge_cutoff,
            )
        )
    except PermissionError as exc:
        raise _request_error(403, "permission_denied", str(exc)) from exc
    except IdempotencyConflictError as exc:
        raise _request_error(409, "idempotency_conflict", str(exc)) from exc
    except (KeyError, ValueError) as exc:
        raise _request_error(422, "invalid_selector", str(exc)) from exc
    except RuntimeError as exc:
        raise _request_error(503, "asset_operation_blocked", str(exc)) from exc
    if result.get("disposition") in {"operation_created", "operation_reused"}:
        return _json_with_headers(result, 202)
    return result


def _json_with_headers(payload: dict[str, Any], status_code: int):
    from fastapi.responses import JSONResponse

    headers = {"Retry-After": "5"}
    request_id = payload.get("asset_request_id")
    if request_id:
        headers["Location"] = f"/api/v1/research/annual-report-asset-requests/{request_id}"
    content = AnnualReportEnsureResponse.model_validate(payload).model_dump(mode="json")
    return JSONResponse(status_code=status_code, content=content, headers=headers)


async def _run_business_annual_report_command(
    *,
    request: Request,
    instrument_id: str,
    body: BusinessAnnualReportProcessRequest,
    consumer: str,
):
    from fastapi.responses import JSONResponse

    from research.announcement_assets import IdempotencyConflictError

    principal = _principal(request, _consumer_permission(consumer))
    if body.allow_network:
        _principal(request, "annual_report_assets:acquire")
    ensure_request = EnsureRequest(
        instrument_id=instrument_id,
        fiscal_year=body.fiscal_year,
        source=body.source,
        source_announcement_id=body.source_announcement_id,
        attachment_id=body.attachment_id,
        expected_content_hash=body.expected_content_hash,
        observation_version=body.observation_version,
        allow_network=body.allow_network,
        integrity_level=body.integrity_level,
        wait_seconds=body.wait_seconds,
        principal=principal,
        idempotency_key=request.headers.get("Idempotency-Key"),
        knowledge_cutoff=body.knowledge_cutoff,
    )
    method = (
        data_manager.process_shared_business_profile_annual_report
        if consumer == "business_profile"
        else data_manager.process_shared_broker_risk_control_annual_report
    )
    try:
        result = await method(
            ensure_request,
            profile_name=body.processing_profile,
            expected_processing_fingerprint=(
                body.expected_processing_fingerprint
            ),
        )
    except IdempotencyConflictError as exc:
        raise _request_error(409, "idempotency_conflict", str(exc)) from exc
    except ValueError as exc:
        code = str(exc)
        if code in {
            "processing_fingerprint_mismatch",
            "candidate_ambiguous",
            "effective_state_conflict",
        }:
            raise _request_error(409, code, code) from exc
        if code == "unknown_consumer_processing_profile":
            raise _request_error(422, code, code) from exc
        raise _request_error(422, "invalid_selector", str(exc)) from exc
    except RuntimeError as exc:
        raise _request_error(503, "asset_operation_blocked", str(exc)) from exc
    status_code = int(result.pop("_http_status", 202))
    consumer_request_id = str(result["consumer_request_id"])
    headers = {
        "Location": (
            "/api/v1/research/annual-report-consumer-requests/"
            + consumer_request_id
        )
    }
    if status_code == 202:
        headers["Retry-After"] = "5"
    content = AnnualReportConsumerRequestResponse.model_validate(result).model_dump(
        mode="json"
    )
    return JSONResponse(status_code=status_code, content=content, headers=headers)


@router.post(
    "/research/company/{instrument_id}/business-profile/annual-report-process",
    response_model=AnnualReportConsumerRequestResponse,
    responses=_CONSUMER_RESPONSES,
    tags=["Annual Report Assets"],
)
async def process_business_profile_annual_report(
    request: Request,
    instrument_id: str,
    body: BusinessAnnualReportProcessRequest,
):
    return await _run_business_annual_report_command(
        request=request,
        instrument_id=instrument_id,
        body=body,
        consumer="business_profile",
    )


@router.post(
    "/research/company/{instrument_id}/broker-risk-control/annual-report-process",
    response_model=AnnualReportConsumerRequestResponse,
    responses=_CONSUMER_RESPONSES,
    tags=["Annual Report Assets"],
)
async def process_broker_risk_control_annual_report(
    request: Request,
    instrument_id: str,
    body: BusinessAnnualReportProcessRequest,
):
    return await _run_business_annual_report_command(
        request=request,
        instrument_id=instrument_id,
        body=body,
        consumer="broker_risk_control",
    )


@router.get(
    "/research/annual-report-asset-requests/{asset_request_id}",
    response_model=AnnualReportRequestResponse,
    responses=_ERROR_RESPONSES,
    tags=["Annual Report Assets"],
)
async def get_shared_annual_report_asset_request(
    request: Request,
    asset_request_id: str,
):
    principal = _principal(request)
    result = await data_manager.get_shared_annual_report_asset_request(
        asset_request_id,
        principal=principal,
    )
    if result is None:
        raise _request_error(404, "not_found", "asset request was not found")
    return result


@router.delete(
    "/research/annual-report-asset-requests/{asset_request_id}",
    response_model=AnnualReportRequestResponse,
    responses=_ERROR_RESPONSES,
    tags=["Annual Report Assets"],
)
async def cancel_shared_annual_report_asset_request(
    request: Request,
    asset_request_id: str,
):
    principal = _principal(request, "annual_report_assets:acquire")
    try:
        return await data_manager.cancel_shared_annual_report_asset_request(
            asset_request_id,
            principal=principal,
        )
    except KeyError as exc:
        raise _request_error(404, "not_found", "asset request was not found") from exc


def _consumer_permission(consumer: str) -> str:
    permissions = {
        "business_profile": "business_profile:process",
        "broker_risk_control": "broker_risk_control:process",
    }
    try:
        return permissions[consumer]
    except KeyError as exc:
        raise _request_error(
            409,
            "consumer_contract_unavailable",
            "consumer request has an unsupported owner",
        ) from exc


@router.get(
    "/research/annual-report-consumer-requests/{consumer_request_id}",
    response_model=AnnualReportConsumerRequestResponse,
    responses=_ERROR_RESPONSES,
    tags=["Annual Report Assets"],
)
async def get_shared_annual_report_consumer_request(
    request: Request,
    consumer_request_id: str,
):
    principal = _principal(request)
    operator = _require_consumer_query_scope(request)
    identity = await data_manager.get_shared_annual_report_consumer_request_identity(
        consumer_request_id,
        principal=None if operator else principal,
    )
    if identity is None:
        raise _request_error(404, "not_found", "consumer request was not found")
    if not operator and not _has_scope(
        request, _consumer_permission(identity["consumer"])
    ):
        raise _request_error(404, "not_found", "consumer request was not found")
    result = await data_manager.get_shared_annual_report_consumer_request(
        consumer_request_id,
        principal=principal,
        operator=operator,
    )
    if result is None:
        raise _request_error(404, "not_found", "consumer request was not found")
    return result


@router.delete(
    "/research/annual-report-consumer-requests/{consumer_request_id}",
    response_model=AnnualReportConsumerRequestResponse,
    responses=_CONSUMER_RESPONSES,
    tags=["Annual Report Assets"],
)
async def cancel_shared_annual_report_consumer_request(
    request: Request,
    consumer_request_id: str,
):
    from research.announcement_assets import ConsumerRequestNotCancellableError

    principal = _principal(request)
    operator = _require_consumer_query_scope(request)
    identity = await data_manager.get_shared_annual_report_consumer_request_identity(
        consumer_request_id,
        principal=None if operator else principal,
    )
    if identity is None:
        raise _request_error(404, "not_found", "consumer request was not found")
    if not operator and not _has_scope(
        request, _consumer_permission(identity["consumer"])
    ):
        raise _request_error(404, "not_found", "consumer request was not found")
    try:
        result, disposition = (
            await data_manager.cancel_shared_annual_report_consumer_request(
                consumer_request_id,
                principal=principal,
                operator=operator,
            )
        )
    except KeyError as exc:
        raise _request_error(404, "not_found", "consumer request was not found") from exc
    except ConsumerRequestNotCancellableError as exc:
        raise _request_error(
            409,
            "request_not_cancellable",
            "consumer request cannot be cancelled in its current state",
        ) from exc
    if disposition == "stop_requested":
        from fastapi.responses import JSONResponse

        content = AnnualReportConsumerRequestResponse.model_validate(result).model_dump(
            mode="json"
        )
        return JSONResponse(
            status_code=202,
            content=content,
            headers={"Retry-After": "5"},
        )
    return result


@router.get(
    "/research/annual-report-assets/readiness",
    response_model=AnnualReportReadinessResponse,
    responses=_ERROR_RESPONSES,
    tags=["Annual Report Assets"],
)
async def get_shared_annual_report_readiness(
    request: Request,
    operator: bool = Query(False),
):
    if operator:
        _principal(request, "annual_report_assets:operator")
    return await data_manager.get_shared_annual_report_readiness(operator=operator)


@router.get(
    "/research/annual-report-assets/{asset_id}/content",
    responses=_ERROR_RESPONSES,
    tags=["Annual Report Assets"],
)
async def stream_shared_annual_report_content(
    request: Request,
    asset_id: str,
):
    _principal(request, "annual_report_assets:read_content")
    try:
        content = await data_manager.get_shared_annual_report_content(asset_id)
    except AssetContentGoneError as exc:
        raise _request_error(
            410,
            "asset_content_gone",
            "annual-report asset is no longer publicly streamable",
            lifecycle_state=exc.lifecycle_state,
        ) from exc
    except AssetContentIntegrityError as exc:
        raise _request_error(
            409,
            "asset_integrity_failed",
            "current annual-report content failed integrity validation",
            integrity_status=exc.integrity_status,
        ) from exc
    except AssetContentMountError as exc:
        raise _request_error(
            503,
            "archive_mount_unavailable",
            "annual-report archive mount is unavailable or changed",
        ) from exc
    except KeyError as exc:
        raise _request_error(404, "not_found", str(exc)) from exc
    except FileNotFoundError as exc:
        raise _request_error(404, "not_found", str(exc)) from exc
    except RuntimeError as exc:
        raise _request_error(409, "asset_integrity_failed", str(exc)) from exc
    handle = content.get("file_handle")
    if handle is None:
        raise _request_error(
            503,
            "controlled_stream_unavailable",
            "annual-report controlled content handle is unavailable",
        )
    return StreamingResponse(
        _stream_validated_handle(handle),
        media_type=content["media_type"],
        headers={
            "Content-Length": str(content["content_length"]),
            "Content-Disposition": f'attachment; filename="{content["filename"]}"',
        },
    )
