from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from data_sources.cninfo_announcement_title_llm import (
    CninfoAnnouncementTitleClassifier,
    TITLE_CLASSIFICATION_SCHEMA_VERSION,
)


def _response(events):
    return SimpleNamespace(
        data={
            "schema_version": TITLE_CLASSIFICATION_SCHEMA_VERSION,
            "events": events,
        },
        model="fake-model",
        request_hash="request-hash",
        response_hash="response-hash",
        request_id="request-id",
        latency_ms=12,
        attempt_count=1,
    )


def _event(announcements):
    return {
        "instrument_id": "000409.SZ",
        "source_event_key": "event-1",
        "source_profile": "cninfo_dividend",
        "action_type": "bonus",
        "record_date": "2015-07-17",
        "share_arrival_date": "2015-07-20",
        "description": "10送2.2275股",
        "candidate_effective_dates": ["2015-07-20"],
        "announcements": announcements,
    }


@pytest.mark.asyncio
async def test_title_classifier_accepts_compensation_share_implementation():
    client = SimpleNamespace(complete=AsyncMock(return_value=_response([{
        "source_event_key": "event-1",
        "event_applicability": "uncertain",
        "applicability_reason": "May be a compensation-share event",
        "classifications": [{
            "announcement_id": "announcement-1",
            "relevance": "relevant",
            "announcement_role": "compensation_share_distribution",
            "confidence": 0.97,
            "reason": "The title explicitly describes share compensation implementation",
        }],
    }])))
    classifier = CninfoAnnouncementTitleClassifier(client)

    result = await classifier.classify([_event([{
        "announcement_id": "announcement-1",
        "published_at": "2015-07-22",
        "title": "重大资产重组业绩承诺补偿股份赠与实施完成公告",
    }])])

    decision = result.decisions_by_event["event-1"]["announcement-1"]
    assert decision["relevance"] == "relevant"
    assert decision["announcement_role"] == "compensation_share_distribution"
    request = client.complete.await_args.args[0]
    assert request.content_is_untrusted is True
    assert request.response_schema["additionalProperties"] is False


@pytest.mark.asyncio
async def test_title_classifier_rejects_missing_or_invented_ids():
    client = SimpleNamespace(complete=AsyncMock(return_value=_response([{
        "source_event_key": "event-1",
        "event_applicability": "effectful",
        "applicability_reason": "Cash dividend",
        "classifications": [{
            "announcement_id": "invented",
            "relevance": "relevant",
            "announcement_role": "implementation",
            "confidence": 1.0,
            "reason": "invented",
        }],
    }])))
    classifier = CninfoAnnouncementTitleClassifier(client)

    with pytest.raises(ValueError, match="unexpected title classification id"):
        await classifier.classify([_event([{
            "announcement_id": "announcement-1",
            "title": "权益分派实施公告",
        }])])


@pytest.mark.asyncio
async def test_title_classifier_batches_without_dropping_titles():
    client = SimpleNamespace(complete=AsyncMock(side_effect=[
        _response([{
            "source_event_key": "event-1",
            "event_applicability": "effectful",
            "applicability_reason": "Distribution",
            "classifications": [{
                "announcement_id": "announcement-1",
                "relevance": "possibly_relevant",
                "announcement_role": "other",
                "confidence": 0.5,
                "reason": "Ambiguous",
            }],
        }]),
        _response([{
            "source_event_key": "event-1",
            "event_applicability": "effectful",
            "applicability_reason": "Distribution",
            "classifications": [{
                "announcement_id": "announcement-2",
                "relevance": "unrelated",
                "announcement_role": "periodic_report",
                "confidence": 0.9,
                "reason": "Periodic report",
            }],
        }]),
    ]))
    classifier = CninfoAnnouncementTitleClassifier(
        client, max_titles_per_request=1
    )

    result = await classifier.classify([_event([
        {"announcement_id": "announcement-1", "title": "公告一"},
        {"announcement_id": "announcement-2", "title": "公告二"},
    ])])

    assert result.request_count == 2
    assert set(result.decisions_by_event["event-1"]) == {
        "announcement-1", "announcement-2"
    }


@pytest.mark.asyncio
async def test_title_classifier_isolates_one_invalid_event_in_shared_request():
    second_event = {
        **_event([{
            "announcement_id": "announcement-2",
            "title": "权益分派实施公告",
        }]),
        "instrument_id": "000410.SZ",
        "source_event_key": "event-2",
    }
    client = SimpleNamespace(complete=AsyncMock(return_value=_response([
        {
            "source_event_key": "event-1",
            "event_applicability": "effectful",
            "applicability_reason": "Distribution",
            "classifications": [{
                "announcement_id": "invented",
                "relevance": "relevant",
                "announcement_role": "implementation",
                "confidence": 1.0,
                "reason": "Invalid identity",
            }],
        },
        {
            "source_event_key": "event-2",
            "event_applicability": "effectful",
            "applicability_reason": "Distribution",
            "classifications": [{
                "announcement_id": "announcement-2",
                "relevance": "relevant",
                "announcement_role": "implementation",
                "confidence": 0.99,
                "reason": "Implementation notice",
            }],
        },
    ])))
    classifier = CninfoAnnouncementTitleClassifier(client)

    result = await classifier.classify(
        [
            _event([{
                "announcement_id": "announcement-1",
                "title": "公告一",
            }]),
            second_event,
        ],
        tolerate_event_errors=True,
    )

    assert "event-1" in result.errors_by_event
    assert result.decisions_by_event["event-1"] == {}
    assert result.decisions_by_event["event-2"]["announcement-2"][
        "relevance"
    ] == "relevant"
    assert client.complete.await_count == 1
