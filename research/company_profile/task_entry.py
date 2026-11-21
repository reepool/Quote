"""Explicit job-plan dependency wiring; operations remains the business owner."""
from __future__ import annotations

from dataclasses import replace
from datetime import date
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import time
from typing import Any, Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .models import PRODUCTION_AUTHORIZATION
from .operations import execute_published_task
from .publication import load_publication_control, publication_allows_new_writes
from .stage5_provider import CommonGatewaySemanticProvider
from .workpaper_enrichment import WorkpaperEnrichmentPlan
from .workpaper_llm import workpaper_processing_identity
from utils.config_manager import UnifiedConfigManager
from utils.llm import LlmClient, load_project_environment
from utils.llm.budget import DEVELOPMENT_UNPRICED_BILLING, LlmRunBudget, LogicalRecoveryPolicy
from utils.llm.models import project_root


class _PlanModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False, hide_input_in_errors=True)


class _Profile(_PlanModel):
    name: str = Field(min_length=1)
    model: str = Field(min_length=1)
    host: str = Field(min_length=1)
    attempt_seconds: float = Field(gt=0)
    reasoning_effort: str | None = None
    disable_thinking: bool | None = None
    max_output_tokens_field: Literal["max_tokens", "max_completion_tokens", "max_output_tokens"] | None = None


class _Budget(_PlanModel):
    seconds: float = Field(gt=0, le=12600)
    tokens: int = Field(gt=0, le=3_000_000)
    physical_requests: int = Field(gt=0, le=96)
    recoveries: int = Field(ge=1)
    logical_seconds: float = Field(gt=0)
    priced_cost_cny: Decimal = Field(gt=0, le=1000)
    billing_mode: Literal["development_unpriced"]


class CompanyProfileTaskPlan(_PlanModel):
    """Parameters of one explicitly authorized research invocation, not a grant."""
    schema_version: Literal["company_profile_task_plan.v1"]
    run_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,79}$")
    authorization_status: Literal["planned", "authorized"] = "planned"
    approval_reference: str = ""
    action: Literal["first_read", "review_batch", "enrich"]
    instrument_ids: tuple[str, ...] = Field(min_length=1, max_length=100)
    document_versions: dict[str, str]
    knowledge_cutoff: date
    output_root: Path
    checkpoint_root: Path
    extract: _Profile
    review: _Profile
    budget: _Budget
    review_plans: tuple[dict[str, Any], ...] = ()
    enrichment_plan: WorkpaperEnrichmentPlan | None = None

    @model_validator(mode="after")
    def bound_scope(self):
        import re

        if (len(set(self.instrument_ids)) != len(self.instrument_ids)
            or any(not re.fullmatch(r"\d{6}\.(?:SH|SZ|BJ)", code) for code in self.instrument_ids)
            or set(self.document_versions) != set(self.instrument_ids)
            or any(not value.strip() for value in self.document_versions.values())):
            raise ValueError("task plan requires distinct canonical stocks and their document versions")
        if self.authorization_status == "authorized" and not self.approval_reference.strip():
            raise ValueError("authorized task plan requires its approval reference")
        if (self.budget.logical_seconds > self.budget.seconds
            or self.budget.recoveries > self.budget.physical_requests
            or any(p.attempt_seconds > self.budget.logical_seconds for p in (self.extract, self.review))):
            raise ValueError("task plan time/recovery limits are inconsistent")
        if self.action != "review_batch" and self.review_plans:
            raise ValueError("only review_batch can load saved review plans")
        if self.action == "enrich":
            plan = self.enrichment_plan
            if (plan is None or self.instrument_ids != (plan.report.instrument_id,)
                or self.document_versions[plan.report.instrument_id] != plan.report.document_version):
                raise ValueError("enrich requires exactly its bound stock/report and enrichment plan")
        elif self.enrichment_plan is not None:
            raise ValueError("only enrich can load an enrichment plan")
        if self.action == "review_batch":
            if (len(self.review_plans) != len(self.instrument_ids)
                or {p.get("instrument_id") for p in self.review_plans} != set(self.instrument_ids)
                or any(not isinstance(p.get("report"), dict)
                       or p["report"].get("document_version") != self.document_versions[p["instrument_id"]]
                       for p in self.review_plans)):
                raise ValueError("review plan scope/report version differs from the task plan")
        return self


class _PlanProvider(CommonGatewaySemanticProvider):
    """Bind each original gateway request to this invocation's explicit source."""
    document_versions: dict[str, str]

    def workpaper_request(self, **kwargs):
        report = kwargs["report"]
        if self.document_versions.get(report.instrument_id) != report.document_version:
            raise ValueError("task plan does not authorize this stock/report version")
        self.execution_authorization["document_version"] = report.document_version
        return super().workpaper_request(**kwargs)


def _save_result(path: Path, value: dict) -> None:
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)


class _TaskReceiptTransport:
    """Keep this explicit research run's public wire bodies, never credentials."""

    def __init__(self, delegate: Any, directory: Path) -> None:
        self.delegate, self.directory, self.sequence = delegate, directory, 0

    async def send(self, url, headers, payload, timeout_seconds):
        self.sequence += 1
        self.directory.mkdir(parents=True, exist_ok=True)
        path = self.directory / f"{self.sequence:03d}.json"
        receipt = {"host": urlsplit(url).hostname, "request": payload,
                   "physical_timeout_seconds": timeout_seconds,
                   "response_representation": "parsed_provider_envelope_preserving_content_strings"}
        _save_result(path, receipt)
        started = time.monotonic()
        try:
            response = await self.delegate.send(url, headers, payload, timeout_seconds)
            receipt.update(status_code=response.status_code, response=response.data,
                           provider_request_id=response.provider_request_id)
            return response
        except BaseException as exc:
            receipt["error_type"] = type(exc).__name__
            raise
        finally:
            receipt["elapsed_seconds"] = time.monotonic() - started
            _save_result(path, receipt)

    async def close(self):
        await self.delegate.close()


async def execute_configured_task(
    *, action: str, storage: Any, execution_plan_file: str | Path,
    shared_asset_access: Any = None, output_directory: str | Path | None = None,
    reason: str = "operator_request", page_source: Any = None, llm_config: Any = None,
) -> dict[str, Any]:
    """Adapt an explicit job plan to the original application; never infer a run.

    Reusing a started run_id never constructs a fresh budget. An interrupted run
    remains inspectable; a new execution requires a new bounded authorized plan.
    """
    plan = CompanyProfileTaskPlan.model_validate_json(Path(execution_plan_file).read_text(encoding="utf-8"))
    normalized = action.strip().lower()
    root = project_root()
    output_root = root / plan.output_root
    checkpoint_root = root / plan.checkpoint_root
    arguments = dict(storage=storage, output_root=output_root, checkpoint_root=checkpoint_root,
        shared_asset_access=shared_asset_access, page_source=page_source,
        knowledge_cutoff=plan.knowledge_cutoff.isoformat(), instrument_ids=plan.instrument_ids)
    if normalized in {"preview", "status", "pause", "query", "export"}:
        return await execute_published_task(action=normalized, reason=reason,
            output_directory=output_directory, **arguments)
    if normalized != plan.action or plan.authorization_status != "authorized":
        raise ValueError("paid action requires a matching explicitly authorized task plan")
    parameters = plan.model_dump(mode="json")
    if plan.enrichment_plan is None:
        parameters.pop("enrichment_plan")  # Preserve fingerprints of existing v1 execution files.
    fingerprint = hashlib.sha256(json.dumps(parameters, sort_keys=True).encode()).hexdigest()
    run_root = checkpoint_root / "executions" / plan.run_id
    marker, result_path = run_root / "started.json", run_root / "result.json"
    if marker.exists():
        if json.loads(marker.read_text()) != {"plan_sha256": fingerprint}:
            raise ValueError("run_id already belongs to different task parameters")
        if result_path.exists():
            return json.loads(result_path.read_text())
        raise ValueError("task plan already started; inspect saved state instead of reopening its budget")
    if not publication_allows_new_writes(load_publication_control(checkpoint_root)):
        raise ValueError("research publication is stopped; task plan was not started")
    load_project_environment()
    config = llm_config if llm_config is not None else UnifiedConfigManager("config").get_llm_config()
    if not config.enabled:
        raise ValueError("LLM gateway is disabled")
    profiles = {}
    for spec in (plan.extract, plan.review):
        profile = config.profiles.get(spec.name)
        if (profile is None or not profile.enabled or profile.model != spec.model
            or urlsplit(profile.base_url).hostname != spec.host
            or (spec.reasoning_effort is not None and profile.reasoning_effort != spec.reasoning_effort)):
            raise ValueError("task profile identity differs from enabled shared configuration")
        if not os.environ.get(profile.api_key_env):
            raise ValueError("task profile credentials are not configured")
        overrides = {key: getattr(spec, key) for key in ("disable_thinking", "max_output_tokens_field")
                     if getattr(spec, key) is not None}
        bound = replace(profile, timeout_seconds=plan.budget.logical_seconds,
            attempt_timeout_seconds=spec.attempt_seconds, max_retries=1, max_schema_repair_attempts=1, **overrides)
        if spec.name in profiles and profiles[spec.name] != bound:
            raise ValueError("one profile cannot carry conflicting task-local settings")
        profiles[spec.name] = bound
    config = replace(config, profiles=profiles, routes={}, pools={})
    run_root.mkdir(parents=True, exist_ok=True)
    with marker.open("x", encoding="utf-8") as stream:
        json.dump({"plan_sha256": fingerprint}, stream)
    budget = LlmRunBudget(deadline=time.monotonic()+plan.budget.seconds, prices={},
        max_tokens=plan.budget.tokens, max_requests=plan.budget.physical_requests,
        max_cost_cny=plan.budget.priced_cost_cny, ledger_path=run_root/"budget.json",
        billing_mode=DEVELOPMENT_UNPRICED_BILLING, recovery_policy=LogicalRecoveryPolicy(plan.budget.recoveries, 1),
        run_authorization="company_profile_workpaper_development.2026-11-11")
    client = None
    try:
        client = LlmClient(config, physical_budget=budget)
        client.transport = _TaskReceiptTransport(client.transport, run_root / "receipts")
        provider = _PlanProvider(client=client, profile=plan.extract.name, max_output_tokens=12000,
            timeout_seconds=plan.budget.logical_seconds,
            execution_authorization={"status": "authorized", "approval_reference": plan.approval_reference})
        provider.document_versions = plan.document_versions
        identity = workpaper_processing_identity(real=True, review=True, review_profile=plan.review.name)
        identity.update(logical_request_timeout_seconds=plan.budget.logical_seconds,
            review_max_output_tokens=16384, repair_profile=plan.extract.name)
        result = await execute_published_task(action=normalized, provider=provider,
            processing_identity=identity, execution_budget=budget, token_budget=plan.budget.tokens,
            max_items=len(plan.instrument_ids), max_elapsed_seconds=budget.remaining_time(),
            review_plans=plan.review_plans or None,
            enrichment_plan=plan.enrichment_plan.model_dump(mode="json") if plan.enrichment_plan else None,
            **arguments)
        result["execution_plan"] = {"run_id": plan.run_id, "plan_sha256": fingerprint,
            "budget_path": str(run_root/"budget.json"), "production_authorization": PRODUCTION_AUTHORIZATION}
        _save_result(result_path, result)
        return result
    except Exception as exc:
        _save_result(result_path, {"action": normalized, "state": "failed", "error_type": type(exc).__name__,
            "production_authorization": PRODUCTION_AUTHORIZATION})
        raise
    finally:
        _save_result(run_root/"budget.json", budget.snapshot())
        if client is not None:
            await client.close()
