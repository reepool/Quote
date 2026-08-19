#!/usr/bin/env python3
"""Run one bounded business-profile semantic production stage."""

from __future__ import annotations

import argparse
import asyncio
import copy
import json
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from research.business_profile_semantic_pipeline import (
    PIPELINE_MODES,
    BusinessProfileSemanticPipeline,
    SemanticProductionCheckpointStore,
    SemanticProductionScope,
    parse_semantic_production_config,
)
from research.business_profile_governance import BusinessProfileRepository
from research.business_profile_semantic_runtime import (
    BusinessProfileSemanticRuntime,
    build_business_profile_counterparty_resolver,
    compute_business_profile_semantic_source_revision,
)
from research.business_profile_source_assets import load_business_profile_source_assets
from research.announcement_assets import (
    AnnouncementAssetAccess,
    AnnouncementAssetConfig,
    AnnouncementAssetRepository,
    AnnouncementAssetService,
)
from research.storage import ResearchStorageManager
from utils.config_manager import UnifiedConfigManager
from utils.llm import LlmClient, shutdown_shared_llm_resources


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=PIPELINE_MODES)
    parser.add_argument("--instrument", action="append", required=True)
    parser.add_argument("--field-family", action="append", required=True)
    parser.add_argument("--knowledge-cutoff", required=True)
    parser.add_argument("--identities", required=True, type=Path)
    parser.add_argument("--promotion-manifests", type=Path)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--research-db", type=Path, default=Path("data/research.db"))
    parser.add_argument("--artifact-root", type=Path)
    parser.add_argument("--checkpoint", required=True, type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = parse_semantic_production_config(_read_json(args.config))
    promotion_manifests = (
        _promotion_manifests(_read_json(args.promotion_manifests))
        if args.promotion_manifests
        else {}
    )
    manifest_hashes = {
        family: manifest.manifest_hash
        for family, manifest in promotion_manifests.items()
    }
    scope = SemanticProductionScope(
        instruments=tuple(args.instrument),
        field_families=tuple(args.field_family),
        knowledge_cutoff=args.knowledge_cutoff,
        identities=_read_json(args.identities),
        promotion_manifest_hashes=manifest_hashes,
    )
    unified_config = UnifiedConfigManager("config")
    research_config = unified_config.get_research_config()
    storage = _build_storage(args.research_db, research_config)
    if args.mode != "report" and config.enabled:
        storage.initialize()
    repository = BusinessProfileRepository(storage)
    asset_config = AnnouncementAssetConfig.from_research_config(
        research_config,
        project_root=ROOT_DIR,
    )
    asset_repository = AnnouncementAssetRepository(args.research_db)
    asset_access = AnnouncementAssetAccess(
        repository=asset_repository,
        config=asset_config,
        service=AnnouncementAssetService(
            repository=asset_repository,
            config=asset_config,
        ),
    )
    source_asset_loader = lambda instrument_id: load_business_profile_source_assets(
        asset_access,
        instrument_id,
        knowledge_cutoff=scope.knowledge_cutoff,
    )
    llm_client = (
        LlmClient(unified_config.get_llm_config())
        if args.mode != "report"
        and config.enabled
        and not config.kill_switches["network_calls"]
        else None
    )
    runtime = BusinessProfileSemanticRuntime(
        repository=repository,
        artifact_root=(
            args.artifact_root
            or args.checkpoint.parent / "business_profile_semantic_artifacts"
        ),
        llm_client=llm_client,
        promotion_manifests=promotion_manifests,
        counterparty_resolver=(
            build_business_profile_counterparty_resolver(storage)
            if args.mode != "report" and "named_relationships" in scope.field_families
            else None
        ),
        planned_disclosure_acquirer=None,
        manifest_loader=source_asset_loader,
    )
    source_revision = (
        str(
            (
                json.loads(args.checkpoint.read_text(encoding="utf-8"))
                .get("scope", {})
                .get("source_revision", "")
            )
        )
        if args.mode == "report" and args.checkpoint.is_file()
        else (
            compute_business_profile_semantic_source_revision(
                repository,
                instruments=scope.instruments,
                field_families=scope.field_families,
                knowledge_cutoff=scope.knowledge_cutoff,
                manifest_loader=source_asset_loader,
                max_documents=(
                    1
                    if config.kill_switches["scope_widening"]
                    else config.budgets.max_documents
                ),
                max_specialist_documents=(
                    0
                    if config.kill_switches["scope_widening"]
                    else min(1, config.budgets.max_documents - 1)
                ),
            )
            if args.mode != "report"
            else ""
        )
    )
    scope = SemanticProductionScope(
        instruments=scope.instruments,
        field_families=scope.field_families,
        knowledge_cutoff=scope.knowledge_cutoff,
        identities=scope.identities,
        promotion_manifest_hashes=scope.promotion_manifest_hashes,
        source_revision=source_revision,
    )
    pipeline = BusinessProfileSemanticPipeline(
        config=config,
        checkpoint_store=SemanticProductionCheckpointStore(args.checkpoint),
        handlers=runtime.handlers(),
    )
    try:
        result = pipeline.run(args.mode, scope=scope)
    finally:
        runtime.close()
        # The runtime closes its client transport; the process-level pool and
        # provider registries are drained explicitly at application shutdown.
        asyncio.run(shutdown_shared_llm_resources())
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result.get("status") not in {"stopped"} else 2


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON root must be an object: {path}")
    return payload


def _build_storage(path: Path, research_config: Any) -> ResearchStorageManager:
    config = copy.deepcopy(research_config)
    config.storage.db_path = str(path)
    config.storage.attach_quotes_db = False
    return ResearchStorageManager(config)


def _promotion_manifests(payload: Mapping[str, Any]) -> dict[str, Any]:
    from research.business_profile_promotion import FieldFamilyPromotionManifest

    rows = payload.get("field_families", payload)
    if not isinstance(rows, Mapping):
        raise ValueError("promotion manifests must be an object keyed by field family")
    return {
        str(family): FieldFamilyPromotionManifest(**dict(value))
        for family, value in rows.items()
        if isinstance(value, Mapping)
    }


if __name__ == "__main__":
    sys.exit(main())
