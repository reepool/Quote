import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from data_sources.cninfo_announcement_title_llm import (
    CninfoAnnouncementTitleClassifier,
    TITLE_CLASSIFICATION_SCHEMA_VERSION,
)


def _response(events, *, request_id="request-id"):
    return SimpleNamespace(
        data={
            "schema_version": TITLE_CLASSIFICATION_SCHEMA_VERSION,
            "events": events,
        },
        model="fake-model",
        request_hash="request-hash",
        response_hash="response-hash",
        request_id=request_id,
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
async def test_title_classifier_runs_bounded_requests_concurrently_and_merges_in_order():
    class ConcurrentClient:
        def __init__(self):
            self.active = 0
            self.peak = 0
            self.release = asyncio.Event()
            self.completion_order = []

        async def complete(self, request):
            payload = json.loads(request.messages[1].content)
            event = payload["events"][0]
            announcement = event["announcements"][0]
            announcement_id = announcement["announcement_id"]
            number = int(announcement_id.rsplit("-", 1)[1])
            self.active += 1
            self.peak = max(self.peak, self.active)
            if self.peak == 50:
                self.release.set()
            try:
                await asyncio.wait_for(self.release.wait(), timeout=1)
                await asyncio.sleep((51 - number) * 0.001)
                self.completion_order.append(number)
                return _response([{
                    "source_event_key": event["source_event_key"],
                    "event_applicability": "effectful",
                    "applicability_reason": "Distribution",
                    "classifications": [{
                        "announcement_id": announcement_id,
                        "relevance": "relevant",
                        "announcement_role": "implementation",
                        "confidence": 0.99,
                        "reason": "Implementation notice",
                    }],
                }], request_id=f"request-{number}")
            finally:
                self.active -= 1

    client = ConcurrentClient()
    classifier = CninfoAnnouncementTitleClassifier(
        client,
        max_titles_per_request=1,
        max_concurrency=50,
    )
    announcements = [
        {"announcement_id": f"announcement-{index}", "title": f"公告{index}"}
        for index in range(1, 51)
    ]

    result = await classifier.classify([_event(announcements)])

    assert client.peak == 50
    assert set(client.completion_order) == set(range(1, 51))
    assert client.completion_order != list(range(1, 51))
    assert result.max_concurrency == 50
    assert result.peak_concurrency == 50
    assert [
        item["chunk_index"] for item in result.lineage_by_event["event-1"]
    ] == list(range(1, 51))
    assert set(result.decisions_by_event["event-1"]) == {
        f"announcement-{index}" for index in range(1, 51)
    }


@pytest.mark.asyncio
async def test_title_classifier_isolates_failed_concurrent_chunk():
    class PartiallyFailingClient:
        async def complete(self, request):
            payload = json.loads(request.messages[1].content)
            event = payload["events"][0]
            announcement = event["announcements"][0]
            if event["source_event_key"] == "event-2":
                raise RuntimeError("provider unavailable")
            return _response([{
                "source_event_key": event["source_event_key"],
                "event_applicability": "effectful",
                "applicability_reason": "Distribution",
                "classifications": [{
                    "announcement_id": announcement["announcement_id"],
                    "relevance": "relevant",
                    "announcement_role": "implementation",
                    "confidence": 0.99,
                    "reason": "Implementation notice",
                }],
            }])

    events = []
    for index in range(1, 4):
        events.append({
            **_event([{
                "announcement_id": f"announcement-{index}",
                "title": f"公告{index}",
            }]),
            "instrument_id": f"00040{index}.SZ",
            "source_event_key": f"event-{index}",
        })
    classifier = CninfoAnnouncementTitleClassifier(
        PartiallyFailingClient(),
        max_titles_per_request=1,
        max_concurrency=3,
    )

    result = await classifier.classify(
        events, tolerate_event_errors=True
    )

    assert set(result.decisions_by_event["event-1"]) == {"announcement-1"}
    assert result.decisions_by_event["event-2"] == {}
    assert set(result.decisions_by_event["event-3"]) == {"announcement-3"}
    assert "provider unavailable" in result.errors_by_event["event-2"]


@pytest.mark.asyncio
async def test_title_classifier_strict_failure_waits_for_in_flight_requests():
    class StrictFailingClient:
        def __init__(self):
            self.slow_finished = False

        async def complete(self, request):
            payload = json.loads(request.messages[1].content)
            event = payload["events"][0]
            if event["source_event_key"] == "event-1":
                raise RuntimeError("first request failed")
            await asyncio.sleep(0.03)
            self.slow_finished = True
            announcement = event["announcements"][0]
            return _response([{
                "source_event_key": event["source_event_key"],
                "event_applicability": "effectful",
                "applicability_reason": "Distribution",
                "classifications": [{
                    "announcement_id": announcement["announcement_id"],
                    "relevance": "relevant",
                    "announcement_role": "implementation",
                    "confidence": 0.99,
                    "reason": "Implementation notice",
                }],
            }])

    client = StrictFailingClient()
    classifier = CninfoAnnouncementTitleClassifier(
        client, max_titles_per_request=1, max_concurrency=2
    )
    second_event = {
        **_event([{"announcement_id": "announcement-2", "title": "公告二"}]),
        "instrument_id": "000410.SZ",
        "source_event_key": "event-2",
    }

    with pytest.raises(RuntimeError, match="first request failed"):
        await classifier.classify([
            _event([{"announcement_id": "announcement-1", "title": "公告一"}]),
            second_event,
        ])

    assert client.slow_finished is True


def test_title_classifier_bounds_business_concurrency():
    client = SimpleNamespace(complete=AsyncMock())

    assert CninfoAnnouncementTitleClassifier(
        client, max_concurrency=99
    ).max_concurrency == 50
    assert CninfoAnnouncementTitleClassifier(
        client, max_concurrency=0
    ).max_concurrency == 1


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


@pytest.mark.asyncio
async def test_title_classifier_publishes_each_event_as_soon_as_it_completes():
    callback_order = []

    class OutOfOrderClient:
        async def complete(self, request):
            payload = json.loads(request.messages[1].content)
            event = payload["events"][0]
            if event["source_event_key"] == "event-1":
                await asyncio.sleep(0.03)
            announcement = event["announcements"][0]
            return _response([{
                "source_event_key": event["source_event_key"],
                "event_applicability": "effectful",
                "applicability_reason": "Distribution",
                "classifications": [{
                    "announcement_id": announcement["announcement_id"],
                    "relevance": "relevant",
                    "announcement_role": "implementation",
                    "confidence": 0.99,
                    "reason": "Implementation notice",
                }],
            }])

    second_event = {
        **_event([{"announcement_id": "announcement-2", "title": "公告二"}]),
        "instrument_id": "000410.SZ",
        "source_event_key": "event-2",
    }
    classifier = CninfoAnnouncementTitleClassifier(
        OutOfOrderClient(), max_titles_per_request=1, max_concurrency=2
    )

    await classifier.classify(
        [
            _event([{"announcement_id": "announcement-1", "title": "公告一"}]),
            second_event,
        ],
        on_event_complete=lambda outcome: callback_order.append(
            outcome.source_event_key
        ),
    )

    assert callback_order == ["event-2", "event-1"]
