from __future__ import annotations

import io

from research.business_profile_source_assets import (
    BUSINESS_PROFILE_SOURCE_ASSET_SCHEMA_VERSION,
    load_business_profile_source_assets,
    project_bound_business_profile_source_asset,
)


class _Handle(io.BytesIO):
    pass


def _asset(asset_id: str, *, instrument_id: str = "600000.SH") -> dict:
    return {
        "asset_id": asset_id,
        "instrument_id": instrument_id,
        "fiscal_year": 2025,
        "report_period": "2025-12-31",
        "source": "cninfo",
        "source_announcement_id": "annual-2025-correction",
        "attachment_id": "attachment-2025-correction",
        "observation_version": "version-2025-correction",
        "content_hash": "a" * 64,
        "published_at": "2026-04-20T08:00:00+08:00",
        "availability": "local_valid",
        "is_correction": True,
    }


def test_profile_source_loader_projects_only_verified_shared_effective_asset(tmp_path):
    asset = _asset("asset-correction-2025")
    handles: list[_Handle] = []

    class _Access:
        def list_effective_assets(self, **kwargs):
            assert kwargs["instrument_id"] == "600000.SH"
            assert kwargs["document_family"] == "annual_report"
            assert kwargs["knowledge_cutoff"] == "2026-05-01"
            assert kwargs["availability"] == "local_valid"
            return {"items": [asset], "returned": 1}

        def exact_observation_handle(self, request, *, authorized):
            assert authorized is True
            assert request.observation_version == asset["observation_version"]
            handle = _Handle(b"%PDF-1.4\nshared\n%%EOF")
            handles.append(handle)
            return {
                "path": tmp_path / "shared.pdf",
                "content_length": 24,
                "file_handle": handle,
            }

    rows = load_business_profile_source_assets(
        _Access(),
        "600000.SH",
        knowledge_cutoff="2026-05-01",
    )

    assert len(rows) == 1
    assert rows[0]["schema_version"] == BUSINESS_PROFILE_SOURCE_ASSET_SCHEMA_VERSION
    assert rows[0]["source_file_id"] == "shared-asset:asset-correction-2025"
    assert rows[0]["report_type"] == "annual_report_correction"
    assert rows[0]["content_hash"] == "a" * 64
    assert handles[0].closed is True


def test_profile_source_loader_never_uses_an_asset_after_knowledge_cutoff():
    asset = _asset("asset-correction-2025")

    class _Access:
        def list_effective_assets(self, **_kwargs):
            return {"items": [], "returned": 0}

        def exact_observation_handle(self, *_args, **_kwargs):
            raise AssertionError("future asset content must not be opened")

    assert (
        load_business_profile_source_assets(
            _Access(),
            "600000.SH",
            knowledge_cutoff="2026-04-01",
        )
        == ()
    )


def test_bound_profile_source_uses_exact_observation_after_effective_change(tmp_path):
    asset = _asset("asset-original-2025")
    requests = []

    class _Access:
        def exact_observation_handle(self, request, *, authorized):
            assert authorized is True
            requests.append(request)
            return {
                "path": tmp_path / "retained-original.pdf",
                "content_length": 321,
                "file_handle": _Handle(b"%PDF-1.4\noriginal\n%%EOF"),
            }

        def get_effective_asset(self, *_args, **_kwargs):
            raise AssertionError(
                "bound work must not reselect the effective correction"
            )

    row = project_bound_business_profile_source_asset(
        _Access(), asset, knowledge_cutoff="2026-04-01"
    )

    assert row["source_asset_id"] == "asset-original-2025"
    assert row["metadata"]["selector_kind"] == "bound_exact_observation"
    assert requests[0].observation_version == asset["observation_version"]
    assert requests[0].expected_content_hash == asset["content_hash"]
