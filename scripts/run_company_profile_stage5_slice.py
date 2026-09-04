#!/usr/bin/env python3
"""Run the isolated four-report company-profile stage-five slice."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from research.company_profile.contracts import (
    ContractErrorCode,
    RepairRequest,
    SemanticProviderError,
    SemanticTaskRequest,
    VerifyRequest,
)
from research.company_profile.stage5 import (
    PreparedRequestScope,
    load_stage5_evidence_plan,
    load_stage5_sample_manifest,
)
from research.company_profile.stage5_bundle import Stage5RunBundleStore
from research.company_profile.stage5_provider import CommonGatewaySemanticProvider
from research.company_profile.stage5_service import (
    ManufacturingMaterialsProfileSliceService,
)


@dataclass
class _ProviderCallBudget:
    maximum: int
    used: int = 0

    def consume(self) -> None:
        if self.used >= self.maximum:
            raise SemanticProviderError(
                ContractErrorCode.PROVIDER_UNAVAILABLE,
                "stage-five provider-call budget exhausted",
            )
        self.used += 1


class _BudgetedProvider:
    def __init__(self, provider: CommonGatewaySemanticProvider, budget: _ProviderCallBudget):
        self._provider = provider
        self._budget = budget

    @property
    def traces(self):
        return self._provider.traces

    def extract(self, request: SemanticTaskRequest):
        self._budget.consume()
        return self._provider.extract(request)

    def repair(self, request: RepairRequest):
        self._budget.consume()
        return self._provider.repair(request)

    def verify(self, request: VerifyRequest):
        self._budget.consume()
        return self._provider.verify(request)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        required=True,
        choices=("preparation-only", "semantic-run"),
    )
    parser.add_argument("--sample-manifest", required=True, type=Path)
    parser.add_argument("--evidence-plan", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--run-id", required=True)
    parser.add_argument(
        "--sample-id",
        action="append",
        dest="sample_ids",
        help="limit execution to one or more approved sample IDs; repeat for multiple reports",
    )
    parser.add_argument("--provider-route", required=True)
    parser.add_argument("--max-output-tokens", required=True, type=int)
    parser.add_argument("--timeout-seconds", required=True, type=float)
    parser.add_argument("--max-provider-calls", required=True, type=int)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    _validate_budget(args)
    manifest = load_stage5_sample_manifest(
        args.sample_manifest,
        repository_root=ROOT_DIR,
    )
    evidence_plan = load_stage5_evidence_plan(args.evidence_plan)
    store = Stage5RunBundleStore(args.output_root, repository_root=ROOT_DIR)
    service = ManufacturingMaterialsProfileSliceService()

    if args.mode == "preparation-only":
        execution = service.run_preparation_only(
            run_id=args.run_id,
            manifest=manifest,
            evidence_plan=evidence_plan,
            evidence_plan_path=args.evidence_plan,
            store=store,
            sample_ids=args.sample_ids,
        )
        _print_result(execution.model_dump(mode="json"), provider_calls=0)
        return 0

    from utils.config_manager import config_manager
    from utils.llm import (
        LlmClient,
        load_project_environment,
        shutdown_shared_llm_resources,
    )

    load_project_environment(ROOT_DIR, override=False)
    llm_config = config_manager.get_llm_config()
    if not llm_config.is_logical_profile_enabled(args.provider_route):
        raise ValueError(
            f"logical LLM profile is disabled or unavailable: {args.provider_route}"
        )
    runner = asyncio.Runner()
    client = LlmClient(llm_config)
    budget = _ProviderCallBudget(maximum=args.max_provider_calls)
    try:
        execution = service.run_semantic_slice(
            run_id=args.run_id,
            manifest=manifest,
            evidence_plan=evidence_plan,
            evidence_plan_path=args.evidence_plan,
            store=store,
            provider_factory=lambda scope: _provider_for_scope(
                scope,
                client=client,
                runner=runner,
                route=args.provider_route,
                max_output_tokens=args.max_output_tokens,
                timeout_seconds=args.timeout_seconds,
                budget=budget,
            ),
            sample_ids=args.sample_ids,
        )
    finally:
        runner.run(client.close())
        runner.run(shutdown_shared_llm_resources())
        runner.close()
    _print_result(execution.model_dump(mode="json"), provider_calls=budget.used)
    return 0


def _provider_for_scope(
    scope: PreparedRequestScope,
    *,
    client: Any,
    runner: asyncio.Runner,
    route: str,
    max_output_tokens: int,
    timeout_seconds: float,
    budget: _ProviderCallBudget,
) -> _BudgetedProvider:
    return _BudgetedProvider(
        CommonGatewaySemanticProvider(
            client=client,
            profile=route,
            prepared_scope=scope,
            max_output_tokens=max_output_tokens,
            timeout_seconds=timeout_seconds,
            runner=runner,
        ),
        budget,
    )


def _validate_budget(args: Any) -> None:
    if args.max_output_tokens < 1:
        raise ValueError("max-output-tokens must be positive")
    if args.timeout_seconds <= 0:
        raise ValueError("timeout-seconds must be positive")
    if args.max_provider_calls < 1:
        raise ValueError("max-provider-calls must be positive")


def _print_result(payload: dict[str, Any], *, provider_calls: int) -> None:
    result = dict(payload)
    result["provider_calls"] = provider_calls
    result["production_authorization"] = "not_authorized"
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    raise SystemExit(main())
