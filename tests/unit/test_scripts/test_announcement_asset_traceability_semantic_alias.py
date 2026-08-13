from __future__ import annotations

from dataclasses import replace
import hashlib

from scripts.dev_validation.migrate_announcement_asset_traceability_v2 import (
    RequirementLeaf,
    _migrate_requirement_nodes,
)


def test_requirement_semantic_clarification_preserves_id_with_source_alias():
    original = RequirementLeaf(
        section="18.4",
        heading_path=("18", "18.4"),
        block_kind="paragraph",
        block_index=1,
        start_line=100,
        end_line=100,
        normalized_text="External UI is required.",
        text_sha256=hashlib.sha256(b"External UI is required.").hexdigest(),
        source_key="b" * 64,
    )
    previous_nodes, _ = _migrate_requirement_nodes([original], None)
    clarified = replace(
        original,
        normalized_text="This project is API-only.",
        text_sha256=hashlib.sha256(b"This project is API-only.").hexdigest(),
        source_key="d" * 64,
    )

    nodes, report = _migrate_requirement_nodes(
        [clarified], {"requirement_leaves": previous_nodes}
    )

    assert nodes[0]["requirement_leaf_id"] == "AAM-V1-REQ-0001"
    assert nodes[0]["source_aliases"] == [
        {
            "source_key": "b" * 64,
            "locator": previous_nodes[0]["source_locator"],
            "text_sha256": hashlib.sha256(
                b"External UI is required."
            ).hexdigest(),
            "reason": "semantic_clarification_same_locator",
        }
    ]
    assert report["requirement_locator_matches"] == 1
    assert report["new_requirement_leaf_ids"] == 0
