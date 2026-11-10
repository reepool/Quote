"""The post-closure observation may send only one fresh 600103 anchor run."""

import pytest

from research.company_profile.bounded_entry import (
    assert_anchor_aligned_observation,
    assert_post_closure_observation,
)


def _binding() -> dict:
    return {
        "asset": {"instrument_id": "600103.SH"},
        "reading_strategy": "direct_core_v1",
        "chapter_select": True,
        "source_reference": "anchor_v1",
        "task_route_labels": ["deepseek:deepseek-flash"],
        "task_route_first_only": True,
        "max_tokens": 120_000,
        "max_physical_requests": 16,
        "max_recoveries": 2,
    }


def test_post_closure_observation_accepts_only_the_fresh_anchor_entry():
    assert assert_post_closure_observation(_binding(), 900) is True
    assert assert_post_closure_observation(_binding(), 901) is False
    missing_anchor = _binding()
    missing_anchor["source_reference"] = "quote_v3"
    with pytest.raises(ValueError, match="reviewed deepseek anchor entry"):
        assert_post_closure_observation(missing_anchor, 900)
    saved = _binding()
    saved["accepted_source_query"] = "old-query.json"
    with pytest.raises(ValueError, match="refuses saved semantics"):
        assert_post_closure_observation(saved, 900)
    other = _binding()
    other["asset"] = {"instrument_id": "600000.SH"}
    with pytest.raises(ValueError, match="only 600103"):
        assert_post_closure_observation(other, 900)


def test_anchor_aligned_observation_is_a_separate_fresh_entry():
    assert assert_anchor_aligned_observation(_binding(), 900) is True
    saved = _binding()
    saved["accepted_draft_query"] = "old-draft.json"
    with pytest.raises(ValueError, match="refuses saved semantics"):
        assert_anchor_aligned_observation(saved, 900)
    bank = _binding()
    bank["task_route_labels"] = ["deepseek:deepseek-flash", "scorpio:grok-4.7"]
    with pytest.raises(ValueError, match="reviewed deepseek anchor entry"):
        assert_anchor_aligned_observation(bank, 900)
