#!/usr/bin/env python3
"""Run an isolated company-profile stage-five research manifest."""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent
STAGE5_DEFAULT_PROVIDER_ROUTE = "semantic_extraction"
STAGE5_DEFAULT_EXTRACT_MAX_OUTPUT_TOKENS = 20_000
STAGE5_DEFAULT_VERIFY_MAX_OUTPUT_TOKENS = 18_000
STAGE5_DEFAULT_TIMEOUT_SECONDS = 300.0
STAGE5_DEFAULT_MAX_PROVIDER_CALLS = 129
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
    STAGE5_VALIDATION_MANIFEST_KINDS,
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


@dataclass(frozen=True)
class _OutputTokenBudget:
    extract: int
    verify: int
    tier: str


def _scope_output_token_budget(
    scope: PreparedRequestScope,
    *,
    extract_base: int = STAGE5_DEFAULT_EXTRACT_MAX_OUTPUT_TOKENS,
    verify_base: int = STAGE5_DEFAULT_VERIFY_MAX_OUTPUT_TOKENS,
) -> _OutputTokenBudget:
    """Choose a bounded output budget from request size, not semantic guesses.

    The base budget remains unchanged for ordinary scopes. Larger requests get
    one of two finite increases, capped below provider-specific runaway values.
    This is deliberately provider-free so it can be tested before any network I/O.
    """

    text_chars = sum(len(item.text) for item in scope.page_contexts)
    evidence_count = len(scope.evidence_bundle)
    field_count = len(scope.field_ids)
    page_count = len(scope.page_contexts)
    complexity = (
        math.ceil(text_chars / 4000)
        + field_count * 2
        + evidence_count
        + page_count
    )
    if complexity <= 18:
        return _OutputTokenBudget(extract=extract_base, verify=verify_base, tier="base")
    if complexity <= 32:
        return _OutputTokenBudget(
            extract=max(extract_base, 28_000),
            verify=max(verify_base, 22_000),
            tier="large",
        )
    return _OutputTokenBudget(
        extract=max(extract_base, 32_000),
        verify=max(verify_base, 24_000),
        tier="very_large",
    )


class _BudgetedProvider:
    def __init__(self, provider: CommonGatewaySemanticProvider):
        self._provider = provider

    @property
    def traces(self):
        return self._provider.traces

    def extract(self, request: SemanticTaskRequest):
        return self._provider.extract(request)

    def repair(self, request: RepairRequest):
        return self._provider.repair(request)

    def verify(self, request: VerifyRequest):
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
    parser.add_argument(
        "--scope-id",
        action="append",
        dest="scope_ids",
        help=(
            "limit execution to one or more request scopes within the explicitly selected "
            "approved samples; repeat for multiple held scopes"
        ),
    )
    parser.add_argument(
        "--provider-route",
        default=STAGE5_DEFAULT_PROVIDER_ROUTE,
        help="concrete LLM profile; defaults to the measured Stage 5 primary",
    )
    parser.add_argument(
        "--max-output-tokens",
        type=int,
        help="legacy global override applied to extract, repair, and verify",
    )
    parser.add_argument(
        "--extract-max-output-tokens",
        type=int,
    )
    parser.add_argument(
        "--verify-max-output-tokens",
        type=int,
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=STAGE5_DEFAULT_TIMEOUT_SECONDS,
    )
    parser.add_argument(
        "--max-provider-calls",
        type=int,
        default=STAGE5_DEFAULT_MAX_PROVIDER_CALLS,
    )
    parser.add_argument(
        "--no-dynamic-output-tokens",
        dest="dynamic_output_tokens",
        action="store_false",
        help="keep the explicit extract/verify token budgets for every scope",
    )
    parser.set_defaults(dynamic_output_tokens=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    extract_max_output_tokens, verify_max_output_tokens = _validate_budget(args)
    manifest = load_stage5_sample_manifest(
        args.sample_manifest,
        repository_root=ROOT_DIR,
    )
    evidence_plan = load_stage5_evidence_plan(args.evidence_plan)
    if (
        manifest.manifest_kind in STAGE5_VALIDATION_MANIFEST_KINDS
        and args.mode == "semantic-run"
        and args.scope_ids
    ):
        raise ValueError(
            "out-of-sample validation requires one complete report run; "
            "scope selection is preparation-only"
        )
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
            scope_ids=args.scope_ids,
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
    _validate_provider_route(llm_config, args.provider_route)
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
                max_output_tokens=extract_max_output_tokens,
                verify_max_output_tokens=verify_max_output_tokens,
                dynamic_output_tokens=_dynamic_output_tokens_enabled(args),
                timeout_seconds=args.timeout_seconds,
                budget=budget,
            ),
            sample_ids=args.sample_ids,
            scope_ids=args.scope_ids,
        )
    finally:
        runner.run(client.close())
        runner.run(shutdown_shared_llm_resources())
        runner.close()
    _print_result(execution.model_dump(mode="json"), provider_calls=budget.used)
    return 0


def _validate_provider_route(llm_config: Any, route: str) -> None:
    """Fail before provider I/O when the logical pool is not fully eligible."""
    logical_route = str(route or "").strip()
    if not llm_config.is_logical_profile_enabled(logical_route):
        raise ValueError(
            f"logical LLM profile is disabled or unavailable: {logical_route}"
        )
    pool = llm_config.pool_for_profile(logical_route)
    if pool is None:
        return
    members = tuple(pool.members)
    if len(members) < 4:
        raise ValueError(
            f"company-profile logical route requires four eligible pool members; got {len(members)}"
        )
    description = llm_config.describe_logical_profile(logical_route)
    if "json_object" not in description.supported_structured_output_modes:
        raise ValueError(
            f"company-profile logical route lacks common json_object output mode: {logical_route}"
        )


def _provider_for_scope(
    scope: PreparedRequestScope,
    *,
    client: Any,
    runner: asyncio.Runner,
    route: str,
    max_output_tokens: int,
    verify_max_output_tokens: int,
    dynamic_output_tokens: bool,
    timeout_seconds: float,
    budget: _ProviderCallBudget,
) -> _BudgetedProvider:
    if dynamic_output_tokens:
        selected = _scope_output_token_budget(
            scope,
            extract_base=max_output_tokens,
            verify_base=verify_max_output_tokens,
        )
        max_output_tokens = selected.extract
        verify_max_output_tokens = selected.verify
    return _BudgetedProvider(
        CommonGatewaySemanticProvider(
            client=client,
            profile=route,
            prepared_scope=scope,
            max_output_tokens=max_output_tokens,
            verify_max_output_tokens=verify_max_output_tokens,
            timeout_seconds=timeout_seconds,
            runner=runner,
            physical_call_admission=budget.consume,
        )
    )


def _validate_budget(args: Any) -> tuple[int, int]:
    if args.max_output_tokens is not None and args.max_output_tokens < 1:
        raise ValueError("max-output-tokens must be positive")
    if (
        args.extract_max_output_tokens is not None
        and args.extract_max_output_tokens < 1
    ):
        raise ValueError("extract-max-output-tokens must be positive")
    if (
        args.verify_max_output_tokens is not None
        and args.verify_max_output_tokens < 1
    ):
        raise ValueError("verify-max-output-tokens must be positive")
    if args.timeout_seconds <= 0:
        raise ValueError("timeout-seconds must be positive")
    if args.max_provider_calls < 1:
        raise ValueError("max-provider-calls must be positive")
    if args.max_output_tokens is not None:
        return args.max_output_tokens, args.max_output_tokens
    return (
        args.extract_max_output_tokens
        or STAGE5_DEFAULT_EXTRACT_MAX_OUTPUT_TOKENS,
        args.verify_max_output_tokens
        or STAGE5_DEFAULT_VERIFY_MAX_OUTPUT_TOKENS,
    )


def _dynamic_output_tokens_enabled(args: Any) -> bool:
    return bool(
        args.dynamic_output_tokens
        and args.max_output_tokens is None
        and args.extract_max_output_tokens is None
        and args.verify_max_output_tokens is None
    )


def _print_result(payload: dict[str, Any], *, provider_calls: int) -> None:
    result = dict(payload)
    result["provider_calls"] = provider_calls
    result["production_authorization"] = "not_authorized"
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    raise SystemExit(main())
