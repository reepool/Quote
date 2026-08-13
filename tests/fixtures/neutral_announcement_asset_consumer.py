"""Neutral consumer used to enforce the shared asset architecture boundary."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True)
class SharedAssetReference:
    asset_id: str
    document_family: str
    variant: str
    content_hash: str


class NeutralAnnouncementAssetConsumer:
    """Consume a shared asset without knowing its provider or storage layout."""

    consumer_id = "neutral-architecture-contract"
    parser_version = "neutral-test-parser.v1"

    def process(
        self,
        reference: SharedAssetReference,
        *,
        read_content: Callable[[str], bytes],
    ) -> dict[str, str | int]:
        content = read_content(reference.content_hash)
        return {
            "asset_id": reference.asset_id,
            "document_family": reference.document_family,
            "variant": reference.variant,
            "content_hash": reference.content_hash,
            "content_length": len(content),
        }
