"""Isolated, supervised execution of the frozen B1 report through the existing owner.

The binding is a reviewed local artifact, never a production configuration. Offline
mode uses an empty inventory transport and cannot supply business answers. Real
mode requires a separate A authorization referencing the exact binding digest.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


def supervise(command: list[str], *, deadline: float, log_path: Path) -> int:
    """Bound synchronous parsing/DB work and kill owned workers on entry timeout."""
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("entry deadline exhausted before child startup")
    with log_path.open("x", encoding="utf-8") as log:
        process = subprocess.Popen(
            command, stdout=log, stderr=log, start_new_session=True
        )
        try:
            return process.wait(timeout=max(0, deadline - time.monotonic()))
        except BaseException:
            # Kill the entire owned process group, including any parsing helpers.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
            raise


def validate_binding(
    binding: dict, *, real: bool, authorization: dict, binding_hash: str
) -> None:
    """Fail before constructing a real client when review material is incomplete."""
    for name, expected in binding["file_hashes"].items():
        if digest(Path(name)) != expected:
            raise ValueError(f"frozen execution file changed: {name}")
    asset = binding["asset"]
    if digest(Path(asset["archive_path"])) != asset["content_hash"]:
        raise ValueError("frozen PDF hash mismatch")
    if not 0 < binding["max_elapsed_seconds"] <= 900:
        raise ValueError("entry deadline exceeds the card")
    if real:
        if (
            authorization.get("status") != "authorized"
            or authorization.get("binding_sha256") != binding_hash
            or authorization.get("document_version") != asset["content_hash"]
            or authorization.get("attempt") not in {"first", "successor"}
            or not authorization.get("approval_reference")
        ):
            raise ValueError(
                "A real execution authorization for this binding is required"
            )
        price = binding.get("price")
        if not price or not binding.get("resource_feasibility_passed"):
            raise ValueError(
                "verified price/context and resource feasibility are required"
            )


def _worker(
    binding: dict, *, run_root: Path, deadline: float, real: bool, authorization: dict
) -> dict:
    # Heavy imports and initialization run inside the parent's original deadline.
    import asyncio
    from decimal import Decimal
    from types import SimpleNamespace

    from research.announcements import AnnouncementAttachment, AnnouncementRecord
    from research.business_profile_pdf_artifacts import (
        BusinessProfilePdfArtifactStore,
        BusinessProfilePdfPageCache,
    )
    from research.business_profile_production_operations import (
        BusinessProfileAnnouncementFrontierRepository,
    )
    from research.company_profile.operations import (
        CompanyProfileTaskService,
        OfficialAnnualReportPageSource,
    )
    from research.company_profile.report_interpretation import (
        offline_processing_identity,
    )
    from research.company_profile.stage5_provider import CommonGatewaySemanticProvider
    from research.storage import ResearchStorageManager
    from utils.config_manager import ResearchConfig, ResearchStorageConfig
    from utils.llm import CallableTransport, LlmClient, load_project_environment
    from utils.llm.budget import LlmPrice, LlmRunBudget

    asset = binding["asset"]
    raw_config = json.loads(Path(binding["config_path"]).read_text())["llm"]
    profile_name = binding["profile"]
    profile = raw_config["profiles"][profile_name]
    if (
        profile["model"] != binding["model"]
        or profile.get("provider_resource") != binding["provider_resource"]
    ):
        raise ValueError("profile/model/resource binding mismatch")
    # Concrete profile only. Remove logical routes and forbid implicit fallback.
    raw_config["profiles"] = {profile_name: profile}
    raw_config["routes"] = {}
    raw_config["pools"] = {}
    raw_config["failover"] = {"enabled": False}
    config = ResearchConfig(
        storage=ResearchStorageConfig(
            db_path=str(run_root / "research.db"),
            financials_db_path=str(run_root / "research.db"),
            valuation_db_path=str(run_root / "research.db"),
            interests_db_path=str(run_root / "research.db"),
            quotes_db_path=str(run_root / "quotes.db"),
            attach_quotes_db=False,
        )
    )
    price = (
        binding.get("price")
        if real
        else {
            "input_cny_per_million": "1",
            "output_cny_per_million": "2",
            "basis": "OFFLINE SYNTHETIC PRICE; not evidence of actual billing",
            "context_tokens": 120_000,
        }
    )
    budget = LlmRunBudget(
        deadline=deadline,
        prices={
            (binding["provider_resource"], binding["model"]): LlmPrice(
                Decimal(price["input_cny_per_million"]),
                Decimal(price["output_cny_per_million"]),
                price["basis"],
                price["context_tokens"],
            )
        },
        ledger_path=run_root / "physical-budget.json",
    )
    transport_requests = []

    async def empty_inventory(_url, _headers, payload, _timeout):
        request = json.loads(
            next(m["content"] for m in payload["messages"] if m["role"] == "user")
        )
        transport_requests.append(
            {
                "purpose": request["purpose"],
                "pages": [p["page"] for p in request["source_pages"]],
                "payload_utf8_bytes": len(
                    json.dumps(
                        payload, ensure_ascii=False, separators=(",", ":")
                    ).encode()
                ),
            }
        )
        result = {"request_id": request["request_id"], "purpose": request["purpose"]}
        return {
            "status_code": 200,
            "data": {"choices": [{"message": {"content": json.dumps(result)}}]},
        }

    if real:
        load_project_environment()
    client = LlmClient(
        raw_config,
        physical_budget=budget,
        **(
            {}
            if real
            else {
                "transport": CallableTransport(empty_inventory),
                "environment": {profile["api_key_env"]: "offline-placeholder"},
            }
        ),
    )
    if (
        client.config.resource_for_profile(client.config.profiles[profile_name]).name
        != binding["provider_resource"]
    ):
        raise ValueError("resolved physical provider resource differs from binding")

    class ProbeProvider(CommonGatewaySemanticProvider):
        offline_only = True

    provider = (CommonGatewaySemanticProvider if real else ProbeProvider)(
        client=client,
        profile=profile_name,
        max_output_tokens=4000,
        timeout_seconds=float(binding["max_elapsed_seconds"]),
        execution_authorization=authorization,
    )

    class LocalArtifactStore(BusinessProfilePdfArtifactStore):
        def artifact_path(self, source_pdf_path, **kwargs):
            return super().artifact_path(
                run_root / "pdf-cache" / "source.pdf", **kwargs
            )

        def page_cache(self):
            return BusinessProfilePdfPageCache(run_root / "page-cache")

    bound_repository = SimpleNamespace(
        shared_asset_access=True,
        checkpoint_root=run_root / "checkpoints",
        get_bound_source_asset=lambda _item: asset,
    )
    native_source = OfficialAnnualReportPageSource(
        bound_repository, artifact_store=LocalArtifactStore()
    )
    source_receipt = {}

    def load_pages(item):
        if (item["instrument_id"], item["announcement_id"], item["report_period"]) != (
            asset["instrument_id"],
            asset["filing_id"],
            asset["report_period"],
        ):
            raise ValueError("queue item differs from frozen report")
        budget.remaining()
        source = native_source(item)
        if not source:
            raise ValueError("frozen report could not be parsed")
        pages = source["pages"]
        if len(pages) != binding["physical_pages"]:
            raise ValueError("physical page count differs from binding")
        source_receipt.update(
            {
                "report": source["report"].model_dump(mode="json"),
                "physical_pages": len(pages),
                "readable_pages": sum(p["readable"] for p in pages),
                "characters": sum(len(p["text"]) for p in pages),
                "utf8_bytes": sum(len(p["text"].encode()) for p in pages),
                "page_text_sha256": {
                    str(p["page"]): hashlib.sha256(p["text"].encode()).hexdigest()
                    for p in pages
                },
                "layout_pages": [p["page"] for p in pages if p.get("layout_text")],
            }
        )
        write_json(run_root / "source-receipt.json", source_receipt)
        budget.remaining()
        return source

    storage = ResearchStorageManager(config)
    identity = offline_processing_identity()
    if real:
        identity.update(execution_mode="provider", model=binding["model"])
    service = CompanyProfileTaskService(
        storage=storage,
        output_root=run_root / "output",
        checkpoint_root=run_root / "checkpoints",
        page_source=load_pages,
        provider=provider,
        processing_identity=identity,
        execution_budget=budget,
        token_budget=120_000,
        shared_asset_access=SimpleNamespace(),
    )
    announcement = binding["announcement"]
    BusinessProfileAnnouncementFrontierRepository(storage).upsert_record(
        instrument={
            "instrument_id": asset["instrument_id"],
            "symbol": asset["instrument_id"].split(".")[0],
            "exchange": announcement["exchange"],
        },
        record=AnnouncementRecord(
            source=asset["source"],
            source_announcement_id=asset["filing_id"],
            announcement_key=f"{asset['source']}:{asset['filing_id']}",
            title=announcement["title"],
            published_at=asset["published_at"],
            symbols=(asset["instrument_id"].split(".")[0],),
            attachments=(
                AnnouncementAttachment(
                    source_url=announcement["source_url"], file_extension="PDF"
                ),
            ),
        ),
    )

    async def execute():
        try:
            ran = await service.execute(
                "run",
                knowledge_cutoff=binding["knowledge_cutoff"],
                instrument_ids=(asset["instrument_id"],),
                max_items=1,
                max_elapsed_seconds=budget.remaining(),
            )
            query = await service.execute(
                "query", instrument_ids=(asset["instrument_id"],)
            )
            export = await service.execute(
                "export",
                instrument_ids=(asset["instrument_id"],),
                output_directory=str(run_root / "export"),
            )
            budget.remaining_time()
            write_json(run_root / "query.json", query)
            write_json(run_root / "export.json", export)
            return {
                "run": ran,
                "query_export_equal": query["profiles"] == export["profiles"],
                "delivered": query["delivered"],
            }
        finally:
            await client.close()

    try:
        result = asyncio.run(execute())
    finally:
        write_json(run_root / "budget-final.json", budget.snapshot())
        write_json(
            run_root / "offline-transport.json",
            {"real_calls": 0 if not real else None, "requests": transport_requests},
        )
    return {
        "classification": "real_attempt" if real else "offline_empty_inventory_probe",
        "source": source_receipt,
        **result,
    }


def main() -> int:
    started = time.monotonic()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binding", type=Path, required=True)
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument("--authorization", type=Path)
    parser.add_argument("--real", action="store_true")
    parser.add_argument("--worker-deadline", type=float, help=argparse.SUPPRESS)
    args = parser.parse_args()
    binding = json.loads(args.binding.read_text())
    authorization = (
        json.loads(args.authorization.read_text()) if args.authorization else {}
    )
    run_root = args.run_root.resolve()
    if not run_root.is_relative_to(Path("data/QuoteBak").resolve()):
        raise ValueError("B1 isolated run root must be under QuoteBak")
    validate_binding(
        binding,
        real=args.real,
        authorization=authorization,
        binding_hash=digest(args.binding),
    )
    if args.worker_deadline is not None:
        write_json(
            run_root / "entry-result.json",
            _worker(
                binding,
                run_root=run_root,
                deadline=args.worker_deadline,
                real=args.real,
                authorization=authorization,
            ),
        )
        return 0
    deadline = started + binding["max_elapsed_seconds"]
    run_root.mkdir(parents=True, exist_ok=False)
    if args.real:
        # A freezes one shared attempt directory for first + successor. Never resume
        # or overwrite an attempted identity, including a crash or a timeout.
        attempts = Path(binding["attempt_directory"])
        attempts.mkdir(parents=True, exist_ok=True)
        if (
            authorization["attempt"] == "successor"
            and not (attempts / "first.json").is_file()
        ):
            raise ValueError("successor requires the protected first attempt")
        with (attempts / f"{authorization['attempt']}.json").open("x") as marker:
            json.dump(
                {
                    "binding_sha256": digest(args.binding),
                    "authorization": authorization,
                    "run_root": str(run_root),
                },
                marker,
            )
    command = [
        sys.executable,
        "-m",
        __name__,
        "--binding",
        str(args.binding.resolve()),
        "--run-root",
        str(run_root),
        "--worker-deadline",
        str(deadline),
    ]
    # __main__ is not an importable worker module when invoked using -m.
    command[2] = "research.company_profile.bounded_entry"
    if args.real:
        command.append("--real")
        command.extend(["--authorization", str(args.authorization.resolve())])
    try:
        code = supervise(command, deadline=deadline, log_path=run_root / "entry.log")
        write_json(
            run_root / "supervisor.json",
            {"exit_code": code, "elapsed_seconds": time.monotonic() - started},
        )
        return code
    except (TimeoutError, subprocess.TimeoutExpired):
        write_json(
            run_root / "supervisor.json",
            {
                "exit_code": 124,
                "status": "entry_deadline_exhausted_workers_killed",
                "elapsed_seconds": time.monotonic() - started,
            },
        )
        return 124


if __name__ == "__main__":
    raise SystemExit(main())
