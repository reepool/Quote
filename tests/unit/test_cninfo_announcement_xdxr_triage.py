from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from data_sources.cninfo_announcement_xdxr_triage import (
    ANNOUNCEMENT_XDXR_TRIAGE_SCHEMA_VERSION,
    AnnouncementXdxrTriageConfig,
    CninfoAnnouncementDocumentLoader,
    CninfoAnnouncementXdxrClassifier,
    CninfoAnnouncementXdxrDailyGovernanceService,
    CninfoAnnouncementXdxrTriageService,
    apply_announcement_xdxr_decision,
    build_announcement_xdxr_cases,
    build_source_reactivation_signals,
)


def _announcement(key, title, announcement_date, *, attachment_url=None):
    return {
        "announcement_key": key,
        "title": title,
        "announcement_date": announcement_date,
        "attachment_url": attachment_url or f"https://example.test/{key}.pdf",
    }


def _decision(case, *, likelihood=0.9, confidence=0.9, primary=None):
    keys = [item["announcement_key"] for item in case["announcements"]]
    primary = primary or keys[-1]
    return {
        "schema_version": ANNOUNCEMENT_XDXR_TRIAGE_SCHEMA_VERSION,
        "case_id": case["case_id"],
        "disposition": "probable_xdxr" if likelihood >= 0.5 else "non_xdxr",
        "xdxr_likelihood": likelihood,
        "judgment_confidence": confidence,
        "event_stage": "implementation",
        "action_family": case["action_family"],
        "primary_announcement_key": primary,
        "supporting_announcement_keys": [key for key in keys if key != primary],
        "rationale": "fixture decision",
    }


def test_config_validates_modes_and_thresholds():
    assert AnnouncementXdxrTriageConfig(mode="ACTIVE").mode == "active"
    with pytest.raises(ValueError, match="disabled, shadow, or active"):
        AnnouncementXdxrTriageConfig(mode="automatic")
    with pytest.raises(ValueError, match="0 <= low < high <= 1"):
        AnnouncementXdxrTriageConfig(low_likelihood=0.8, high_likelihood=0.8)


def test_related_announcements_share_stable_case_and_keep_supersession_lineage():
    initial = build_announcement_xdxr_cases({
        "600000.SH": [_announcement(
            "plan", "重整计划资本公积转增股本方案公告", "2026-01-01"
        )]
    })
    initial_id = initial[0]["case_id"]
    updated = build_announcement_xdxr_cases(
        {
            "600000.SH": [_announcement(
                "implementation",
                "重整计划资本公积转增股本实施公告",
                "2026-02-01",
            )]
        },
        existing_cases=initial,
    )

    assert len(updated) == 1
    assert updated[0]["case_id"] == initial_id
    assert [item["announcement_key"] for item in updated[0]["announcements"]] == [
        "plan",
        "implementation",
    ]

    result = apply_announcement_xdxr_decision(
        updated[0],
        _decision(updated[0], primary="implementation"),
        config=AnnouncementXdxrTriageConfig(mode="active"),
    )
    assert result["primary_announcement_key"] == "implementation"
    assert result["supporting_announcement_keys"] == ["plan"]
    assert result["superseded_primary_announcement_keys"] == ["plan"]


@pytest.mark.parametrize(
    ("likelihood", "confidence", "expected"),
    [
        (0.85, 0.8, "active_probable_xdxr"),
        (0.10, 0.8, "inactive_watch"),
        (0.50, 0.8, "active_uncertain"),
        (0.05, 0.4, "active_uncertain"),
    ],
)
def test_active_routing_uses_likelihood_and_confidence(
    likelihood, confidence, expected
):
    case = build_announcement_xdxr_cases({
        "600000.SH": [_announcement(
            "implementation", "权益分派实施公告", "2026-01-01"
        )]
    })[0]
    result = apply_announcement_xdxr_decision(
        case,
        _decision(case, likelihood=likelihood, confidence=confidence),
        config=AnnouncementXdxrTriageConfig(mode="active"),
    )
    assert result["routing_status"] == expected


@pytest.mark.asyncio
async def test_shadow_records_decision_without_changing_active_queue():
    case = build_announcement_xdxr_cases({
        "600000.SH": [_announcement("payment", "交易对价支付完成公告", "2026-01-01")]
    })[0]
    classifier = SimpleNamespace(classify=AsyncMock(return_value=_decision(
        case, likelihood=0.01, confidence=0.99
    )))
    service = CninfoAnnouncementXdxrTriageService(
        config=AnnouncementXdxrTriageConfig(mode="shadow"),
        classifier=classifier,
        document_loader=AsyncMock(return_value={"text": "交易对价已支付完毕"}),
    )

    result = await service.triage({
        "600000.SH": case["announcements"],
    })

    assert result["execution_status"] == "success"
    assert result["cases"][0]["semantic_disposition"] == "non_xdxr"
    assert result["cases"][0]["routing_status"] == "active_pending"
    assert result["deferred_instrument_ids"] == ["600000.SH"]


@pytest.mark.asyncio
async def test_active_mode_reuses_unchanged_shadow_decision_after_mode_switch():
    case = build_announcement_xdxr_cases({
        "600000.SH": [_announcement(
            "payment", "交易对价支付完成公告", "2026-01-01"
        )]
    })[0]
    shadow_classifier = SimpleNamespace(classify=AsyncMock(return_value=_decision(
        case, likelihood=0.01, confidence=0.99
    )))
    shadow = await CninfoAnnouncementXdxrTriageService(
        config=AnnouncementXdxrTriageConfig(mode="shadow"),
        classifier=shadow_classifier,
        document_loader=AsyncMock(return_value={"text": "交易对价已支付完毕"}),
    ).triage({"600000.SH": case["announcements"]})

    active_classifier = SimpleNamespace(classify=AsyncMock())
    active = await CninfoAnnouncementXdxrTriageService(
        config=AnnouncementXdxrTriageConfig(mode="active"),
        classifier=active_classifier,
        document_loader=AsyncMock(),
    ).triage({}, existing_cases=shadow["cases"])

    assert active["cases"][0]["routing_status"] == "inactive_watch"
    assert active["processed_case_count"] == 0
    active_classifier.classify.assert_not_awaited()


@pytest.mark.asyncio
async def test_document_failure_keeps_case_active_and_reports_partial():
    service = CninfoAnnouncementXdxrTriageService(
        config=AnnouncementXdxrTriageConfig(mode="active"),
        classifier=SimpleNamespace(classify=AsyncMock()),
        document_loader=AsyncMock(side_effect=RuntimeError("download failed")),
    )

    result = await service.triage({
        "600000.SH": [_announcement("notice", "重整实施公告", "2026-01-01")]
    })

    assert result["execution_status"] == "partial"
    assert result["error_count"] == 1
    assert result["cases"][0]["routing_status"] == "active_pending"


@pytest.mark.asyncio
async def test_inactive_case_reactivates_only_for_new_source_or_announcement_evidence():
    initial = build_announcement_xdxr_cases({
        "600000.SH": [_announcement("plan", "重整方案公告", "2026-01-01")]
    })[0]
    inactive = apply_announcement_xdxr_decision(
        initial,
        _decision(initial, likelihood=0.01, confidence=0.99),
        config=AnnouncementXdxrTriageConfig(mode="active"),
    )
    classifier = SimpleNamespace(classify=AsyncMock(return_value=_decision(
        inactive, likelihood=0.01, confidence=0.99
    )))
    service = CninfoAnnouncementXdxrTriageService(
        config=AnnouncementXdxrTriageConfig(mode="active"),
        classifier=classifier,
        document_loader=AsyncMock(return_value={"text": "公告正文"}),
    )

    first = await service.triage(
        {},
        existing_cases=[inactive],
        source_signals_by_instrument={"600000.SH": ["tdx_event_observed"]},
    )
    assert first["reactivated_case_count"] == 1
    assert classifier.classify.await_count == 1

    second = await service.triage(
        {},
        existing_cases=first["cases"],
        source_signals_by_instrument={"600000.SH": ["tdx_event_observed"]},
    )
    assert second["reactivated_case_count"] == 0
    assert classifier.classify.await_count == 1

    third = await service.triage(
        {
            "600000.SH": [_announcement(
                "implementation", "重整实施公告", "2026-02-01"
            )]
        },
        existing_cases=second["cases"],
    )
    assert third["reactivated_case_count"] == 1
    assert classifier.classify.await_count == 2


def test_source_reactivation_signals_cover_all_authoritative_paths():
    result = build_source_reactivation_signals(
        cninfo_result={
            "checkpoint_id": "daily-run-1",
            "inserted_instrument_ids": ["600000.SH"],
            "changed_instrument_ids": ["000001.SZ"],
            "persisted_event_keys_by_instrument": {
                "600000.SH": ["cninfo-1"],
                "000001.SZ": ["cninfo-2"],
            },
        },
        tdx_result={
            "event_instrument_ids": ["600000.SH"],
            "event_dates_by_instrument": {"600000.SH": ["2026-08-21"]},
        },
        reconciliation={
            "conflicts": [{"instrument_id": "000001.SZ"}],
            "tdx_only": [{"instrument_id": "000002.SZ"}],
        },
    )

    assert set(result) == {"000001.SZ", "000002.SZ", "600000.SH"}
    assert any(
        item.startswith("cninfo_event_inserted:")
        for item in result["600000.SH"]
    )
    assert any(
        item.startswith("tdx_event_observed:")
        for item in result["600000.SH"]
    )
    assert any(
        item.startswith("reconciliation_tdx_only:")
        for item in result["000002.SZ"]
    )

    later = build_source_reactivation_signals(
        cninfo_result={
            "checkpoint_id": "daily-run-2",
            "changed_instrument_ids": ["000001.SZ"],
            "persisted_event_keys_by_instrument": {
                "000001.SZ": ["cninfo-2"],
            },
        },
        tdx_result={},
        reconciliation={},
    )
    assert later["000001.SZ"] != [
        item for item in result["000001.SZ"]
        if item.startswith("cninfo_event_changed:")
    ]


@pytest.mark.asyncio
async def test_shadow_keeps_title_excluded_case_out_of_deterministic_queue():
    case = build_announcement_xdxr_cases({
        "002289.SZ": [_announcement(
            "payment",
            "关于重大资产购买剩余交易对价支付完成的公告",
            "2026-08-21",
        )]
    })[0]
    classifier = SimpleNamespace(classify=AsyncMock())
    service = CninfoAnnouncementXdxrDailyGovernanceService(
        CninfoAnnouncementXdxrTriageService(
            config=AnnouncementXdxrTriageConfig(mode="shadow"),
            classifier=classifier,
            document_loader=AsyncMock(),
        )
    )

    result = await service.govern(
        {
            "status": "success",
            "execution_status": "skipped",
            "unmatched_special_announcements_by_instrument": {},
            "deferred_special_announcements_by_instrument": {},
        },
        announcement_scan={"announcement_xdxr_cases": [case]},
        cninfo_result={},
        tdx_result={},
        rebuild_result={},
    )

    assert result["announcement_xdxr_cases"][0]["routing_status"] == (
        "deterministic_excluded"
    )
    assert result["deferred_instrument_ids"] == []
    classifier.classify.assert_not_awaited()


@pytest.mark.asyncio
async def test_shadow_removes_newly_excluded_announcement_from_existing_case():
    case = build_announcement_xdxr_cases({
        "600807.SH": [
            _announcement(
                "review",
                "中德证券关于济南高新股权分置改革限售股上市流通的核查意见",
                "2026-08-25",
            ),
            _announcement(
                "listing",
                "济高发展股改限售股上市流通公告",
                "2026-08-25",
            ),
        ]
    })[0]
    classifier = SimpleNamespace(
        classify=AsyncMock(side_effect=lambda current_case, documents, **_: _decision(
            current_case,
            likelihood=0.1,
            confidence=0.9,
        ))
    )
    service = CninfoAnnouncementXdxrDailyGovernanceService(
        CninfoAnnouncementXdxrTriageService(
            config=AnnouncementXdxrTriageConfig(mode="shadow"),
            classifier=classifier,
            document_loader=AsyncMock(return_value={"text": "公告正文"}),
        )
    )

    result = await service.govern(
        {
            "status": "success",
            "execution_status": "skipped",
            "unmatched_special_announcements_by_instrument": {
                "600807.SH": case["announcements"]
            },
            "deferred_special_announcements_by_instrument": {},
        },
        announcement_scan={"announcement_xdxr_cases": [case]},
        cninfo_result={},
        tdx_result={},
        rebuild_result={},
    )

    triage_case = result["announcement_xdxr_cases"][0]
    assert [item["announcement_key"] for item in triage_case["announcements"]] == [
        "listing"
    ]
    assert classifier.classify.await_count == 1


def test_source_signal_exposes_best_case_evidence_to_structured_governance():
    case = build_announcement_xdxr_cases({
        "600000.SH": [
            _announcement("plan", "重整方案公告", "2026-01-01"),
            _announcement("implementation", "重整实施公告", "2026-02-01"),
        ]
    })[0]
    case["primary_announcement_key"] = "implementation"

    result = CninfoAnnouncementXdxrDailyGovernanceService.prepare_structured_scan(
        {"announcement_xdxr_cases": [case]},
        source_signals_by_instrument={
            "600000.SH": ["cninfo_event_inserted:event-hash"]
        },
        max_announcements_per_case=2,
    )

    announcements = result[
        "deferred_special_announcements_by_instrument"
    ]["600000.SH"]
    assert [item["announcement_key"] for item in announcements] == [
        "implementation",
        "plan",
    ]


@pytest.mark.asyncio
async def test_classifier_validates_case_evidence_identities():
    case = build_announcement_xdxr_cases({
        "600000.SH": [_announcement("notice", "重整实施公告", "2026-01-01")]
    })[0]
    response = SimpleNamespace(
        data={**_decision(case), "primary_announcement_key": "invented"},
        model="fixture-model",
        request_id="request-id",
        request_hash="request-hash",
        response_hash="response-hash",
    )
    client = SimpleNamespace(complete=AsyncMock(return_value=response))

    with pytest.raises(ValueError, match="primary identity is not in evidence"):
        await CninfoAnnouncementXdxrClassifier(client).classify(
            case,
            [{**case["announcements"][0], "text": "公告正文"}],
        )


@pytest.mark.asyncio
async def test_document_loader_reuses_full_persisted_official_text():
    db_ops = SimpleNamespace(
        get_corporate_action_document_bundle=AsyncMock(return_value={
            "items": [{
                "content_hash": "document-hash",
                "source_url": "https://example.test/notice.pdf",
                "pages": [
                    {"text": "第一页实施条款"},
                    {"text": "第二页完成情况"},
                ],
            }]
        }),
        save_corporate_action_document_bundle=AsyncMock(),
    )
    loader = CninfoAnnouncementDocumentLoader(
        db_ops=db_ops,
        research_config=SimpleNamespace(),
    )

    result = await loader.load({
        "announcement_key": "notice",
        "title": "重整实施公告",
    })

    assert result["text"] == "第一页实施条款\n第二页完成情况"
    assert result["reused"] is True
    db_ops.save_corporate_action_document_bundle.assert_not_awaited()
