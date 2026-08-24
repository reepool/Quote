from __future__ import annotations

import io
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from api.announcement_asset_models import AnnualReportEnsureRequestModel
from api.announcement_asset_routes import _stream_validated_handle
from api.app import app
from research.announcement_assets import IdempotencyConflictError
from research.announcement_assets.access import (
    AssetContentGoneError,
    AssetContentIntegrityError,
    AssetContentMountError,
)


def _trusted_config():
    return SimpleNamespace(
        modules={
            "official_announcement_assets": {
                "permissions": {
                    "trusted_identity_enabled": True,
                    "principals": [
                        {
                            "principal": "alice",
                            "token_env": "TEST_ANNUAL_REPORT_ASSET_TOKEN",
                            "scopes": [
                                "annual_report_assets:acquire",
                                "annual_report_assets:read_content",
                                "annual_report_assets:operator",
                            ],
                        }
                    ],
                }
            }
        }
    )


@pytest.mark.asyncio
async def test_annual_report_metadata_get_is_zero_network_and_public(monkeypatch):
    response_payload = {
        "items": [],
        "returned": 0,
        "limit": 100,
        "offset": 0,
    }
    method = AsyncMock(return_value=response_payload)
    monkeypatch.setattr(
        "api.announcement_asset_routes.data_manager.list_shared_annual_report_assets",
        method,
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/api/v1/research/company/600000.SH/annual-reports"
        )
    assert response.status_code == 200
    assert response.json() == response_payload
    method.assert_awaited_once()


@pytest.mark.asyncio
async def test_exact_filing_ensure_is_safe_before_catalog_schema_exists(monkeypatch):
    repository = SimpleNamespace(
        schema_initialized=lambda: False,
        list_candidate_rows=lambda **kwargs: pytest.fail(
            "fresh catalog must not query missing tables"
        ),
    )
    access = SimpleNamespace(repository=repository)
    calls: list[bool] = []

    def get_access(*, initialize_schema=False):
        calls.append(initialize_schema)
        return access

    monkeypatch.setattr(
        "api.announcement_asset_routes.data_manager._get_announcement_asset_access",
        get_access,
    )
    monkeypatch.setattr(
        "api.announcement_asset_routes.data_manager.ensure_shared_annual_report",
            AsyncMock(
                return_value={
                    "disposition": "local_miss",
                    "asset_availability": "missing",
                    "availability": "missing",
                    "asset": None,
                    "asset_request_id": None,
                }
            ),
        )
    monkeypatch.setenv("TEST_ANNUAL_REPORT_ASSET_TOKEN", "secret-token")
    monkeypatch.setattr("api.middleware.config_manager.get_research_config", _trusted_config)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v1/research/company/600000.SH/annual-reports/ensure",
            headers={"Authorization": "Bearer secret-token"},
            json={
                "source": "cninfo",
                "source_announcement_id": "fresh-filing",
                "allow_network": False,
            },
        )

    assert response.status_code == 200
    assert calls == [False]


@pytest.mark.asyncio
async def test_annual_report_list_projects_metadata_only_record_and_filters(monkeypatch):
    item = {
        "asset_record_id": "asset-record-metadata",
        "asset_id": None,
        "instrument_id": "600000.SH",
        "fiscal_year": 2025,
        "report_period": "2025-12-31",
        "source": "cninfo",
        "source_announcement_id": "filing-metadata",
        "filing_id": "filing-metadata",
        "attachment_id": "attachment-metadata",
        "observation_version": None,
        "version_available_at": "2026-03-20T01:00:00+00:00",
        "published_at": "2026-03-20T01:00:00+00:00",
        "variant": "original",
        "is_correction": False,
        "content_hash": None,
        "content_length": None,
        "content_url": None,
        "integrity": "unchecked",
        "asset_availability": "metadata_only",
        "availability": "metadata_only",
        "acquisition_status": "metadata_only",
        "effective_state": "historical",
        "effective_decision_state": None,
        "exact_content_state": "local_content_unavailable",
        "last_checked_at": "2026-03-20T01:00:00+00:00",
        "canonical_source_filing": {
            "source": "cninfo",
            "source_announcement_id": "filing-metadata",
            "attachment_id": "attachment-metadata",
        },
    }
    method = AsyncMock(
        return_value={"items": [item], "returned": 1, "limit": 25, "offset": 5}
    )
    monkeypatch.setattr(
        "api.announcement_asset_routes.data_manager.list_shared_annual_report_assets",
        method,
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/api/v1/research/company/600000.SH/annual-reports",
            params={
                "fiscal_year": 2025,
                "source": "cninfo",
                "filing_id": "filing-metadata",
                "integrity": "unchecked",
                "acquisition_status": "metadata_only",
                "effective_state": "historical",
                "asset_availability": "metadata_only",
                "limit": 25,
                "offset": 5,
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["items"][0]["asset_id"] is None
    assert payload["items"][0]["asset_availability"] == "metadata_only"
    assert payload["items"][0]["content_url"] is None
    assert payload["items"][0]["version_available_at"] == "2026-03-20T01:00:00+00:00"
    method.assert_awaited_once_with(
        instrument_id="600000.SH",
        fiscal_year=2025,
        source="cninfo",
        source_announcement_id="filing-metadata",
        integrity="unchecked",
        acquisition_status="metadata_only",
        effective_state="historical",
        asset_availability="metadata_only",
        limit=25,
        offset=5,
    )


@pytest.mark.asyncio
async def test_generic_ensure_maps_idempotency_conflict_to_409(monkeypatch):
    monkeypatch.setenv("TEST_ANNUAL_REPORT_ASSET_TOKEN", "secret-token")
    monkeypatch.setattr("api.middleware.config_manager.get_research_config", _trusted_config)
    method = AsyncMock(side_effect=IdempotencyConflictError("fingerprint differs"))
    monkeypatch.setattr(
        "api.announcement_asset_routes.data_manager.ensure_shared_annual_report",
        method,
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v1/research/company/600000.SH/annual-reports/ensure",
            headers={
                "Authorization": "Bearer secret-token",
                "Idempotency-Key": "same-key",
            },
            json={"fiscal_year": 2025, "allow_network": True},
        )

    assert response.status_code == 409
    assert response.json()["error_code"] == "idempotency_conflict"


@pytest.mark.asyncio
async def test_effective_get_has_one_stable_wrapper_for_hit_and_miss(monkeypatch):
    method = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "api.announcement_asset_routes.data_manager.get_shared_annual_report_asset",
        method,
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/api/v1/research/company/600000.SH/annual-reports/effective"
        )

    assert response.status_code == 200
    assert response.json() == {
        "asset_availability": "missing",
        "availability": "missing",
        "asset": None,
    }


@pytest.mark.asyncio
async def test_ensure_rejects_unknown_path_and_url_fields_before_work(monkeypatch):
    monkeypatch.setenv("TEST_ANNUAL_REPORT_ASSET_TOKEN", "test-secret")
    monkeypatch.setattr(
        "api.middleware.config_manager.get_research_config",
        _trusted_config,
    )
    method = AsyncMock()
    monkeypatch.setattr(
        "api.announcement_asset_routes.data_manager.ensure_shared_annual_report",
        method,
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v1/research/company/600000.SH/annual-reports/ensure",
            headers={"Authorization": "Bearer test-secret"},
            json={
                "fiscal_year": 2025,
                "allow_network": True,
                "path": "/tmp/report.pdf",
                "url": "https://example.invalid/report.pdf",
            },
        )

    assert response.status_code == 422
    assert response.json()["error_code"] == "invalid_request"
    method.assert_not_awaited()


@pytest.mark.asyncio
async def test_ensure_rejects_response_only_version_available_at_field(monkeypatch):
    monkeypatch.setenv("TEST_ANNUAL_REPORT_ASSET_TOKEN", "test-secret")
    monkeypatch.setattr(
        "api.middleware.config_manager.get_research_config",
        _trusted_config,
    )
    method = AsyncMock()
    monkeypatch.setattr(
        "api.announcement_asset_routes.data_manager.ensure_shared_annual_report",
        method,
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v1/research/company/600000.SH/annual-reports/ensure",
            headers={"Authorization": "Bearer test-secret"},
            json={
                "source": "cninfo",
                "source_announcement_id": "filing-2025",
                "version_available_at": "2026-04-30T10:00:00+00:00",
                "allow_network": False,
            },
        )

    assert response.status_code == 422
    assert response.json()["error_code"] == "invalid_request"
    method.assert_not_awaited()


@pytest.mark.asyncio
async def test_authorization_unavailable_precedes_invalid_body_validation(monkeypatch):
    monkeypatch.setattr(
        "api.middleware.config_manager.get_research_config",
        lambda: SimpleNamespace(
            modules={
                "official_announcement_assets": {
                    "permissions": {"trusted_identity_enabled": False}
                }
            }
        ),
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v1/research/company/600000.SH/annual-reports/ensure",
            json={},
        )
    assert response.status_code == 503
    assert response.json()["error_code"] == "authorization_boundary_unavailable"


@pytest.mark.asyncio
async def test_authorization_unavailable_precedes_all_protected_resource_work(
    monkeypatch,
):
    monkeypatch.setattr(
        "api.middleware.config_manager.get_research_config",
        lambda: SimpleNamespace(
            modules={
                "official_announcement_assets": {
                    "permissions": {"trusted_identity_enabled": False}
                }
            }
        ),
    )
    asset_cancel = AsyncMock()
    content = AsyncMock()
    monkeypatch.setattr(
        "api.announcement_asset_routes.data_manager.cancel_shared_annual_report_asset_request",
        asset_cancel,
    )
    monkeypatch.setattr(
        "api.announcement_asset_routes.data_manager.get_shared_annual_report_content",
        content,
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        responses = [
            await client.delete(
                "/api/v1/research/annual-report-asset-requests/private"
            ),
            await client.get(
                "/api/v1/research/annual-report-assets/private/content"
            ),
        ]

    assert {response.status_code for response in responses} == {503}
    assert {
        response.json()["error_code"] for response in responses
    } == {"authorization_boundary_unavailable"}
    asset_cancel.assert_not_awaited()
    content.assert_not_awaited()


@pytest.mark.asyncio
async def test_authenticated_ensure_returns_caller_handle_without_internal_operation_id(
    monkeypatch,
):
    monkeypatch.setenv("TEST_ANNUAL_REPORT_ASSET_TOKEN", "test-secret")
    monkeypatch.setattr(
        "api.middleware.config_manager.get_research_config",
        _trusted_config,
    )
    method = AsyncMock(
        return_value={
            "disposition": "operation_created",
            "asset_availability": "missing",
            "availability": "missing",
            "asset": None,
            "asset_request_id": "assetreq-public",
            "request": {
                "asset_request_id": "assetreq-public",
                "asset_request_status": "active",
                "status": "active",
                "consumer": "business-profile",
                "created_at": "2026-08-10T00:00:00+00:00",
                "updated_at": "2026-08-10T00:00:00+00:00",
                "operation_status": "queued",
                "operation_stage": "discovering",
                "progress": {},
                "diagnostics": {},
            },
            "reason_code": None,
        }
    )
    monkeypatch.setattr(
        "api.announcement_asset_routes.data_manager.ensure_shared_annual_report",
        method,
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v1/research/company/600000.SH/annual-reports/ensure",
            headers={
                "Authorization": "Bearer test-secret",
                "X-Annual-Report-Principal": "alice",
                "Idempotency-Key": "alice-request-1",
            },
            json={
                "fiscal_year": 2025,
                "allow_network": True,
            },
        )
    assert response.status_code == 202
    payload = response.json()
    assert payload["asset_request_id"] == "assetreq-public"
    assert "operation_id" not in response.text
    assert response.headers["location"].endswith("assetreq-public")
    assert response.headers["retry-after"] == "5"


@pytest.mark.asyncio
async def test_bearer_token_binds_principal_without_caller_identity_header(monkeypatch):
    monkeypatch.setenv("TEST_ANNUAL_REPORT_ASSET_TOKEN", "test-secret")
    monkeypatch.setattr(
        "api.middleware.config_manager.get_research_config",
        _trusted_config,
    )
    method = AsyncMock(
        return_value={
            "asset_request_id": "assetreq-public",
            "asset_request_status": "active",
            "status": "active",
            "consumer": None,
            "created_at": "2026-08-10T00:00:00+00:00",
            "updated_at": "2026-08-10T00:00:00+00:00",
            "operation_status": "queued",
            "operation_stage": "discovering",
            "progress": {},
            "diagnostics": {},
        }
    )
    monkeypatch.setattr(
        "api.announcement_asset_routes.data_manager.get_shared_annual_report_asset_request",
        method,
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/api/v1/research/annual-report-asset-requests/assetreq-public",
            headers={"Authorization": "Bearer test-secret"},
        )

    assert response.status_code == 200
    method.assert_awaited_once_with("assetreq-public", principal="alice")


@pytest.mark.asyncio
async def test_caller_cannot_spoof_token_bound_principal(monkeypatch):
    monkeypatch.setenv("TEST_ANNUAL_REPORT_ASSET_TOKEN", "test-secret")
    monkeypatch.setattr(
        "api.middleware.config_manager.get_research_config",
        _trusted_config,
    )
    method = AsyncMock()
    monkeypatch.setattr(
        "api.announcement_asset_routes.data_manager.get_shared_annual_report_asset_request",
        method,
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/api/v1/research/annual-report-asset-requests/assetreq-public",
            headers={
                "Authorization": "Bearer test-secret",
                "X-Annual-Report-Principal": "bob",
            },
        )

    assert response.status_code == 403
    assert response.json()["error_code"] == "principal_mismatch"
    method.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_bound_credential_fails_before_request_lookup(monkeypatch):
    monkeypatch.delenv("TEST_ANNUAL_REPORT_ASSET_TOKEN", raising=False)
    monkeypatch.setattr(
        "api.middleware.config_manager.get_research_config",
        _trusted_config,
    )
    method = AsyncMock()
    monkeypatch.setattr(
        "api.announcement_asset_routes.data_manager.get_shared_annual_report_asset_request",
        method,
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/api/v1/research/annual-report-asset-requests/private-id",
            headers={"Authorization": "Bearer unknown"},
        )

    assert response.status_code == 503
    assert response.json()["error_code"] == "authorization_boundary_unavailable"
    method.assert_not_awaited()


@pytest.mark.asyncio
async def test_invalid_bearer_token_returns_401_before_request_lookup(monkeypatch):
    monkeypatch.setenv("TEST_ANNUAL_REPORT_ASSET_TOKEN", "test-secret")
    monkeypatch.setattr(
        "api.middleware.config_manager.get_research_config",
        _trusted_config,
    )
    method = AsyncMock()
    monkeypatch.setattr(
        "api.announcement_asset_routes.data_manager.get_shared_annual_report_asset_request",
        method,
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/api/v1/research/annual-report-asset-requests/private-id",
            headers={"Authorization": "Bearer wrong-secret"},
        )

    assert response.status_code == 401
    method.assert_not_awaited()


@pytest.mark.asyncio
async def test_operator_readiness_auth_precedes_query_validation(monkeypatch):
    monkeypatch.setattr(
        "api.middleware.config_manager.get_research_config",
        lambda: SimpleNamespace(
            modules={
                "official_announcement_assets": {
                    "permissions": {"trusted_identity_enabled": False}
                }
            }
        ),
    )
    method = AsyncMock()
    monkeypatch.setattr(
        "api.announcement_asset_routes.data_manager.get_shared_annual_report_readiness",
        method,
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/api/v1/research/annual-report-assets/readiness?operator=invalid"
        )

    assert response.status_code == 503
    assert response.json()["error_code"] == "authorization_boundary_unavailable"
    method.assert_not_awaited()


@pytest.mark.asyncio
async def test_protected_route_cors_preflight_does_not_require_bearer_token():
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.options(
            "/api/v1/research/annual-report-asset-requests/private-id",
            headers={
                "Origin": "http://localhost:3000",
                "Access-Control-Request-Method": "DELETE",
                "Access-Control-Request-Headers": "authorization",
            },
        )

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == (
        "http://localhost:3000"
    )


@pytest.mark.asyncio
async def test_unknown_content_asset_returns_not_found(monkeypatch):
    monkeypatch.setenv("TEST_ANNUAL_REPORT_ASSET_TOKEN", "test-secret")
    monkeypatch.setattr(
        "api.middleware.config_manager.get_research_config",
        _trusted_config,
    )
    monkeypatch.setattr(
        "api.announcement_asset_routes.data_manager.get_shared_annual_report_content",
        AsyncMock(side_effect=KeyError("annual-report asset was not found")),
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/api/v1/research/annual-report-assets/unknown/content",
            headers={
                "Authorization": "Bearer test-secret",
                "X-Annual-Report-Principal": "alice",
            },
        )

    assert response.status_code == 404
    assert response.json()["error_code"] == "not_found"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "status_code", "error_code"),
    (
        (
            AssetContentGoneError("asset-old", "superseded"),
            410,
            "asset_content_gone",
        ),
        (
            AssetContentIntegrityError("hash_mismatch"),
            409,
            "asset_integrity_failed",
        ),
        (
            AssetContentMountError("mount changed"),
            503,
            "archive_mount_unavailable",
        ),
    ),
)
async def test_content_errors_have_stable_http_precedence(
    monkeypatch, error, status_code, error_code
):
    monkeypatch.setenv("TEST_ANNUAL_REPORT_ASSET_TOKEN", "test-secret")
    monkeypatch.setattr(
        "api.middleware.config_manager.get_research_config",
        _trusted_config,
    )
    monkeypatch.setattr(
        "api.announcement_asset_routes.data_manager.get_shared_annual_report_content",
        AsyncMock(side_effect=error),
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/api/v1/research/annual-report-assets/asset-old/content",
            headers={"Authorization": "Bearer test-secret"},
        )

    assert response.status_code == status_code
    assert response.json()["error_code"] == error_code


@pytest.mark.asyncio
async def test_content_stream_sets_safe_headers_and_closes_lease_handle(monkeypatch):
    monkeypatch.setenv("TEST_ANNUAL_REPORT_ASSET_TOKEN", "test-secret")
    monkeypatch.setattr(
        "api.middleware.config_manager.get_research_config",
        _trusted_config,
    )
    payload = b"%PDF-1.4\nstreamed\n%%EOF\n"
    handle = io.BytesIO(payload)
    monkeypatch.setattr(
        "api.announcement_asset_routes.data_manager.get_shared_annual_report_content",
        AsyncMock(
            return_value={
                "file_handle": handle,
                "content_length": len(payload),
                "media_type": "application/pdf",
                "filename": "600000.SH-2025-annual-report.pdf",
            }
        ),
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/api/v1/research/annual-report-assets/asset-current/content",
            headers={"Authorization": "Bearer test-secret"},
        )

    assert response.status_code == 200
    assert response.content == payload
    assert response.headers["content-type"].startswith("application/pdf")
    assert response.headers["content-length"] == str(len(payload))
    assert response.headers["content-disposition"] == (
        'attachment; filename="600000.SH-2025-annual-report.pdf"'
    )
    assert handle.closed is True


@pytest.mark.asyncio
async def test_content_stream_closes_handle_when_consumer_stops_early():
    handle = io.BytesIO(b"%PDF-1.4\nmore-data\n%%EOF\n")
    stream = _stream_validated_handle(handle, chunk_size=4)

    assert await anext(stream) == b"%PDF"
    await stream.aclose()

    assert handle.closed is True


def test_openapi_registers_shared_asset_resources():
    schema = app.openapi()
    paths = schema["paths"]
    assert "/api/v1/research/company/{instrument_id}/annual-reports" in paths
    assert "/api/v1/research/company/{instrument_id}/annual-reports/ensure" in paths
    assert "/api/v1/research/annual-report-asset-requests/{asset_request_id}" in paths
    assert not any("annual-report-process" in path for path in paths)
    assert not any("annual-report-consumer-requests" in path for path in paths)
    assert "/api/v1/research/annual-report-assets/{asset_id}/content" in paths
    asset_schema = schema["components"]["schemas"]["AnnualReportAssetResponse"]
    properties = asset_schema["properties"]
    assert "version_available_at" in properties
    assert "content_url" in properties
    assert properties["document_family"]["const"] == "annual_report"
    assert properties["variant"]["enum"] == ["original", "correction"]
    assert properties["is_full_report"]["const"] is True
    assert properties["classification_vocabulary_version"]["const"] == (
        "official_document_classification.v1"
    )
    assert properties["asset_availability"]["enum"] == [
        "local_valid",
        "metadata_only",
        "missing",
        "ambiguous",
        "corrupt",
        "superseded",
        "blocked",
    ]
    assert properties["exact_content_state"]["enum"] == [
        "local_valid",
        "retained_internal_only",
        "local_content_unavailable",
    ]
    assert asset_schema["additionalProperties"] is False
    ensure_request_schema = schema["components"]["schemas"][
        "AnnualReportEnsureRequestModel"
    ]
    assert ensure_request_schema["additionalProperties"] is False
    asset_request_schema = schema["components"]["schemas"][
        "AnnualReportRequestResponse"
    ]
    assert asset_request_schema["properties"]["asset_request_status"]["enum"] == [
        "active",
        "cancelled",
        "expired",
    ]
    ensure_responses = paths[
        "/api/v1/research/company/{instrument_id}/annual-reports/ensure"
    ]["post"]["responses"]
    assert set(ensure_responses) >= {
        "200",
        "202",
        "401",
        "403",
        "404",
        "409",
        "410",
        "422",
        "429",
        "503",
    }
    company_profile_schema = schema["components"]["schemas"][
        "ResearchCompanyBusinessProfileResponse"
    ]
    assert "source_assets" in company_profile_schema["properties"]
    assert "consumer_processing_status" in company_profile_schema["properties"]
    assert "measurement_contract" in company_profile_schema["properties"]
    company_specific_ref = company_profile_schema["properties"][
        "company_specific_profile"
    ]["$ref"]
    company_specific_schema = schema["components"]["schemas"][
        company_specific_ref.rsplit("/", 1)[-1]
    ]
    assert set(company_specific_schema["properties"]) >= {
        "operating_facts",
        "activities",
        "value_chain_roles",
        "supply_chain_relationships",
        "commodity_exposure_facts",
        "commodity_exposures",
    }
    assert company_specific_schema["additionalProperties"] is True
    measurement_ref = company_profile_schema["properties"]["measurement_contract"][
        "$ref"
    ]
    measurement_schema = schema["components"]["schemas"][
        measurement_ref.rsplit("/", 1)[-1]
    ]
    assert measurement_schema["properties"]["linkage_status"]["enum"] == [
        "not_applicable",
        "linked",
        "partially_linked",
        "unlinked",
    ]






def _asset_request_payload(status: str = "active") -> dict:
    return {
        "asset_request_id": "assetreq-public",
        "asset_request_status": status,
        "status": status,
        "consumer": "business_profile",
        "created_at": "2026-08-10T00:00:00+00:00",
        "updated_at": "2026-08-10T00:01:00+00:00",
        "cancelled_at": (
            "2026-08-10T00:01:00+00:00" if status == "cancelled" else None
        ),
        "operation_status": "running",
        "operation_stage": "downloading",
        "progress": {"current_stage": "downloading"},
        "diagnostics": {},
        "expires_at": "2026-08-17T00:00:00+00:00",
        "expired_at": None,
        "tombstone_until": None,
        "retention_policy_version": "asset_request_retention.v1",
    }






@pytest.mark.asyncio
async def test_content_scope_denial_precedes_asset_lookup(monkeypatch):
    monkeypatch.setenv("TEST_ANNUAL_REPORT_ASSET_TOKEN", "test-secret")

    def domain_only_config():
        config = _trusted_config()
        config.modules["official_announcement_assets"]["permissions"]["principals"][
            0
        ]["scopes"] = ["business_profile:process"]
        return config

    monkeypatch.setattr(
        "api.middleware.config_manager.get_research_config",
        domain_only_config,
    )
    content = AsyncMock()
    monkeypatch.setattr(
        "api.announcement_asset_routes.data_manager.get_shared_annual_report_content",
        content,
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/api/v1/research/annual-report-assets/private/content",
            headers={"Authorization": "Bearer test-secret"},
        )

    assert response.status_code == 403
    assert response.json()["error_code"] == "permission_denied"
    content.assert_not_awaited()








@pytest.mark.asyncio
async def test_asset_request_delete_is_repeatable_detach_only(monkeypatch):
    monkeypatch.setenv("TEST_ANNUAL_REPORT_ASSET_TOKEN", "test-secret")
    monkeypatch.setattr(
        "api.middleware.config_manager.get_research_config",
        _trusted_config,
    )
    cancel = AsyncMock(return_value=_asset_request_payload("cancelled"))
    monkeypatch.setattr(
        "api.announcement_asset_routes.data_manager.cancel_shared_annual_report_asset_request",
        cancel,
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        first = await client.delete(
            "/api/v1/research/annual-report-asset-requests/assetreq-public",
            headers={"Authorization": "Bearer test-secret"},
        )
        repeated = await client.delete(
            "/api/v1/research/annual-report-asset-requests/assetreq-public",
            headers={"Authorization": "Bearer test-secret"},
        )

    assert first.status_code == repeated.status_code == 200
    assert first.json() == repeated.json()
    assert first.json()["asset_request_status"] == "cancelled"
    assert first.json()["operation_status"] == "running"
    assert cancel.await_count == 2
    assert all(
        call.kwargs == {"principal": "alice"}
        for call in cancel.await_args_list
    )


@pytest.mark.asyncio
async def test_asset_request_delete_cross_owner_is_non_disclosing(monkeypatch):
    monkeypatch.setenv("TEST_ANNUAL_REPORT_ASSET_TOKEN", "test-secret")
    monkeypatch.setattr(
        "api.middleware.config_manager.get_research_config",
        _trusted_config,
    )
    cancel = AsyncMock(side_effect=KeyError("private-request"))
    monkeypatch.setattr(
        "api.announcement_asset_routes.data_manager.cancel_shared_annual_report_asset_request",
        cancel,
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.delete(
            "/api/v1/research/annual-report-asset-requests/private-request",
            headers={"Authorization": "Bearer test-secret"},
        )

    assert response.status_code == 404
    assert response.json()["error_code"] == "not_found"
    assert "private-request" not in response.text
    cancel.assert_awaited_once_with("private-request", principal="alice")




























def test_filing_id_alias_normalizes_to_canonical_source_identity():
    request = AnnualReportEnsureRequestModel(
        source="cninfo",
        filing_id="legacy-filing-id",
    )

    assert request.source_announcement_id == "legacy-filing-id"
    assert request.filing_id == "legacy-filing-id"
    with pytest.raises(ValueError, match="conflicts"):
        AnnualReportEnsureRequestModel(
            source="cninfo",
            source_announcement_id="canonical-id",
            filing_id="different-id",
        )
