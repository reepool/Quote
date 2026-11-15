"""Evidence-led A-share delisting reconciliation inside the stock policy.

Providers own public protocols; DatabaseOperations owns every lifecycle write.
This module only joins their evidence to existing local securities.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Mapping, Sequence

from data_sources.a_share_code_lineage import load_lineage_catalog
from database.instrument_lifecycle import FUTURE, VERIFICATION, lifecycle_date
from utils.date_utils import get_shanghai_time


def _explicit_stock_scope(text: str) -> str | None:
    """Identify explicit A-share, other-security or combined stock wording."""
    text = re.sub(r"A[、/及和与]B股", "A股B股", text, flags=re.IGNORECASE)
    kinds = set(re.findall(r"[AB]股|债券|公司债|可转债|优先股|基金", text.upper()))
    if "A股" in kinds:
        return "a_share"
    return "other" if kinds else None


def announcement_terminal_evidence(
    record: Any, body: str, instrument_id: str
) -> dict | None:
    """Extract only explicit stock termination and its actual effective date.

    Titles discover candidates; neither a title nor publication time establishes
    completed delisting. Ambiguous or unsupported prose remains unconfirmed.
    """
    title = re.sub(r"\s+", "", str(record.title or ""))
    if not any(word in title for word in ("终止上市", "摘牌")) or any(
        word in title
        for word in ("可能", "风险", "申请", "受理", "拟", "现金选择权", "资产")
    ):
        return None
    compact = re.sub(r"\s+", "", body)
    symbol = instrument_id.split(".")[0]
    if not re.search(
        r"证券代码[：:]?.{0,180}(?<!\d)" + re.escape(symbol) + r"(?!\d)", compact
    ):
        return None
    # A shared issuer/code header does not establish which security is delisted.
    # Keep unqualified company-stock wording, but reject an explicitly different
    # class unless the terminal clauses also explicitly cover A shares.
    if _explicit_stock_scope(title) == "other":
        return None
    clauses = re.split(r"[。；;，,！？!?]", compact)
    terminal_clauses = [
        clause for clause in clauses if re.search(r"终止.{0,12}上市|摘牌", clause)
    ]
    scopes = {_explicit_stock_scope(clause) for clause in terminal_clauses}
    if "other" in scopes and "a_share" not in scopes:
        return None
    terminal_text = "。".join(
        clause for clause in clauses if _explicit_stock_scope(clause) != "other"
    )
    formal = re.search(
        r"(?:交易所|深交所|上交所|北交所|本所).{0,180}(?:决定|同意).{0,100}终止.{0,12}上市",
        terminal_text,
    ) or re.search(r"(?:交易所|深交所|上交所|北交所).{0,100}股票终止上市的决定", terminal_text)
    completed = bool(re.search(r"股票已(?:完成)?(?:终止上市|摘牌)", terminal_text))
    if not formal and not completed:
        return None
    date_pattern = r"(\d{4})[年/-](\d{1,2})[月/-](\d{1,2})日?"
    patterns = [
        r"(?:摘牌日期|摘牌日|终止上市日期)[为是：:]*" + date_pattern,
        r"(?:股票|A股|证券)(?:将于|将在|将自|自|于)"
        + date_pattern
        + r"(?:起)?(?:(?:在|从)(?:深圳证券交易所|上海证券交易所|北京证券交易所|深交所|上交所|北交所))?"
        + r"(?:被)?(?:摘牌|终止上市)",
    ]
    dates = set()
    for pattern in patterns:
        for match in re.finditer(pattern, terminal_text):
            try:
                dates.add(date(*(int(value) for value in match.groups())).isoformat())
            except ValueError:
                return None
    if len(dates) > 1 or (not dates and not completed):
        return None
    effective = next(iter(dates), None)
    return {
        "instrument_id": instrument_id,
        "symbol": symbol,
        "exchange": record.exchange,
        "type": "stock",
        "name": "",
        "listed_date": None,
        "delisted_date": effective,
        "completion_confirmed": bool(effective or completed),
        "source": f"cninfo_{str(record.exchange).lower()}_delisting",
        "announcement_id": record.source_announcement_id,
        "title": record.title,
        "source_url": record.attachments[0].source_url if record.attachments else None,
        "raw_snapshot_hash": hashlib.sha256(
            json.dumps(
                record.raw_payload,
                sort_keys=True,
                ensure_ascii=False,
                default=str,
            ).encode()
        ).hexdigest(),
        "hash_kind": "canonical_announcement_record",
        "body_text_hash": hashlib.sha256(body.encode()).hexdigest(),
        "parser_version": "official_stock_terminal_body.v2",
        "observed_at": get_shanghai_time().isoformat(),
        "evidence_excerpt": compact[:1200],
    }


def scan_terminal_announcements(
    service: Any,
    exchange: str,
    targets: Sequence[str],
    *,
    scan_days: int = 730,
    max_pages: int = 2,
) -> dict:
    """Bounded shared-announcement discovery and native PDF reading for known gaps."""
    from io import BytesIO
    from pypdf import PdfReader
    from research.announcements import AnnouncementQuery, AnnouncementScope
    from research.announcements.retrieval import AnnouncementAttachmentRetriever

    now = get_shanghai_time()
    snapshot = {"rows": [], "complete": True, "errors": []}
    if not targets:
        return snapshot
    records = {}
    for keyword in ("终止上市", "摘牌"):
        try:
            route = service.acquire(
                AnnouncementQuery(
                    purpose_key=(
                        "instrument_master_bse_delisting"
                        if exchange == "BSE"
                        else f"instrument_master_terminal_{exchange.lower()}"
                    ),
                    scope=AnnouncementScope(
                        exchange=exchange,
                        market=exchange,
                        keyword=keyword,
                        start_date=(now.date() - timedelta(days=scan_days)).isoformat(),
                        end_date=now.date().isoformat(),
                        page_size=30,
                        max_pages=max_pages,
                    ),
                )
            )
        except Exception as exc:
            snapshot["errors"].append(
                f"{exchange}: terminal announcement query {keyword} failed: {exc}"
            )
            continue
        scan = route.scan_result
        if scan is None:
            snapshot["errors"].append(
                f"{exchange}: terminal announcement source unavailable"
            )
            continue
        if scan.status not in {"success", "success_empty"} or not scan.is_complete:
            snapshot["errors"].append(
                f"{exchange}: terminal announcement query {keyword} incomplete "
                f"(status={scan.status}, stop_reason={scan.stop_reason})"
            )
        snapshot["errors"].extend(str(error) for error in scan.errors)
        for record in scan.records:
            if set(record.symbols) & {iid.split(".")[0] for iid in targets}:
                records[record.source_announcement_id] = record
    retriever = AnnouncementAttachmentRetriever.from_provider_configs(
        {
            source: {
                **dict(config),
                "max_attachment_bytes": min(
                    int(config.get("max_attachment_bytes") or 2_000_000), 2_000_000
                ),
                "retry_attempts": min(int(config.get("retry_attempts", 1)), 1),
            }
            for source, config in service.config.provider_configs.items()
        }
    )
    reads = 0
    risk_ids = set()
    for record in records.values():
        if record.exchange and record.exchange != exchange:
            continue
        matching = [iid for iid in targets if iid.split(".")[0] in record.symbols]
        if not matching:
            continue
        # Risk/application metadata does not justify spending a PDF request.
        if any(
            word in record.title
            for word in ("风险", "可能", "申请", "受理", "拟", "现金选择权")
        ):
            risk_ids.update(matching)
            continue
        body = str(record.raw_payload.get("body_text") or "")
        artifact = None
        if not body and record.attachments:
            if reads >= 5:
                snapshot["errors"].append("terminal attachment read bound reached")
                break
            reads += 1
            artifact = retriever.retrieve(
                record.source, record.attachments[0], require_pdf=True
            )
            if artifact.status != "success":
                snapshot["errors"].append(
                    f"terminal attachment unavailable: {record.source_announcement_id}"
                )
                continue
            try:
                document = PdfReader(BytesIO(artifact.content))
                body = "\n".join(
                    page.extract_text() or "" for page in document.pages[:10]
                )[:40000]
            except Exception as exc:
                snapshot["errors"].append(
                    f"terminal PDF parse failed: {type(exc).__name__}"
                )
                continue
        for iid in matching:
            evidence = announcement_terminal_evidence(record, body, iid)
            if evidence:
                evidence["exchange"] = exchange
                evidence["source"] = f"cninfo_{exchange.lower()}_delisting"
                if artifact is not None:
                    evidence.update(
                        raw_snapshot_hash=artifact.content_hash,
                        hash_kind="pdf_bytes",
                        source_url=artifact.final_url,
                    )
                snapshot["rows"].append(evidence)
    snapshot["complete"] = not snapshot["errors"]
    snapshot["risk_only_count"] = len(risk_ids)
    return snapshot


def in_a_share_scope(row: Mapping[str, Any], exchange: str) -> bool:
    symbol = str(row.get("symbol") or str(row.get("instrument_id", "")).split(".")[0])
    prefixes = {
        "SSE": ("60", "68"),
        "SZSE": ("00", "30"),
        "BSE": ("43", "83", "87", "88", "92"),
    }
    return (
        row.get("type") == "stock"
        and row.get("exchange") == exchange
        and len(symbol) == 6
        and symbol.isdigit()
        and symbol.startswith(prefixes[exchange])
    )


def _identity_issue(
    local: Mapping[str, Any], evidence: Mapping[str, Any], lineage: Any
) -> str | None:
    effective = lifecycle_date(evidence.get("delisted_date"))
    listed = lifecycle_date(evidence.get("listed_date"))
    local_listed = lifecycle_date(local.get("listed_date"))
    if lineage:
        regimes = (
            lineage.get("issuer_regimes", [])
            if isinstance(lineage, Mapping)
            else lineage.issuer_regimes
        )
        current = [
            regime
            for regime in regimes
            if (regime.get("role") if isinstance(regime, Mapping) else regime.role)
            == "current"
        ]
        if not current:
            return "issuer_regime_unresolved"
        start = (
            lifecycle_date(current[-1]["start_date"])
            if isinstance(current[-1], Mapping)
            else current[-1].start_date
        )
        if (effective and effective < start) or (listed and listed < start):
            return "predecessor_terminal_evidence"
        if listed is None and effective is None:
            return "issuer_regime_unresolved"
    elif listed and local_listed and listed != local_listed:
        return "listing_interval_conflict"
    if effective and local_listed and effective < local_listed:
        return "terminal_precedes_current_listing"
    return None


def _plan_key(evidence: Mapping[str, Any], local: Mapping[str, Any]) -> str:
    listed = lifecycle_date(local.get("listed_date"))
    stable_source = evidence.get("announcement_id") or [
        evidence.get("source"),
        evidence.get("listed_date"),
        evidence.get("delisted_date"),
    ]
    return hashlib.sha256(
        json.dumps(
            [
                local["instrument_id"],
                evidence.get("issuer_regime_start")
                or (listed.isoformat() if listed else None),
                evidence.get("delisted_date"),
                stable_source,
            ],
            sort_keys=True,
            default=str,
        ).encode()
    ).hexdigest()


class AShareDelistingReconciler:
    """Reconcile official positive evidence, plans and current-list discrepancies."""

    def __init__(
        self, db_ops: Any, source_factory: Any, announcement_loader: Any = None
    ):
        self.db_ops, self.source_factory = db_ops, source_factory
        self.announcement_loader = announcement_loader

    async def reconcile(
        self,
        exchange: str,
        before: Mapping[str, Any],
        current: Sequence[Mapping[str, Any]],
        diagnostics: Mapping[str, Any],
        *,
        audit_only: bool = False,
        snapshot: Mapping[str, Any] | None = None,
        freshness_hours: float = 48,
        enabled: bool = True,
    ) -> dict[str, Any]:
        now = get_shanghai_time()
        mode = "audit_only" if audit_only else "production"
        result: dict[str, Any] = {
            key: 0
            for key in (
                "checked_count",
                "confirmed_count",
                "deactivated_count",
                "date_enriched_count",
                "confirmed_unchanged_count",
                "unconfirmed_count",
                "future_effective_count",
                "conflict_count",
                "source_error_count",
                "write_failed_count",
            )
        }
        result.update(
            {
                "status": "success",
                "samples": [],
                "conflict_samples": [],
                "warnings": [],
                "errors": [],
                "audit_only": audit_only,
                "would_deactivate_count": 0,
            }
        )
        if not enabled:
            result.update(
                status="warning",
                coverage_complete=False,
                reason="delisting_check_disabled",
            )
            result["warnings"].append(
                f"{exchange}: terminal verification disabled by configuration"
            )
            await self._record_verification(exchange, mode, now, result)
            return result
        local_rows = {
            iid: dict(row)
            for iid, row in (before.get("rows_by_id") or {}).items()
            if in_a_share_scope(row, exchange)
        }
        result["checked_count"] = len(local_rows)
        if exchange == "BSE" and snapshot is None:
            snapshot = {"rows": [], "complete": True, "errors": []}
        if snapshot is None:
            try:
                snapshot = await self.source_factory.get_official_delisting_snapshot(
                    exchange
                )
                if not isinstance(snapshot, Mapping):
                    raise ValueError("invalid terminal snapshot")
            except Exception as exc:
                snapshot = {"rows": [], "complete": False, "errors": [str(exc)]}
        if self.announcement_loader is not None:
            covered = {row.get("instrument_id") for row in snapshot.get("rows") or []}
            current_ids = {row.get("instrument_id") for row in current}
            missing = [
                iid
                for iid, row in local_rows.items()
                if iid not in covered
                and (
                    (
                        row.get("is_active")
                        and iid not in current_ids
                        and diagnostics.get("official_current_list_complete")
                    )
                    or (
                        row.get("status") == "delisted" and not row.get("delisted_date")
                    )
                )
            ]
            if missing:
                try:
                    extra = await self.announcement_loader(exchange, missing)
                    snapshot = {
                        "rows": [*(snapshot.get("rows") or []), *extra["rows"]],
                        "complete": bool(snapshot.get("complete"))
                        and extra["complete"],
                        "errors": [*(snapshot.get("errors") or []), *extra["errors"]],
                        "risk_only_count": int(extra.get("risk_only_count", 0)),
                    }
                except Exception as exc:
                    snapshot = {
                        **snapshot,
                        "complete": False,
                        "errors": [
                            *(snapshot.get("errors") or []),
                            f"terminal announcement scan failed: {exc}",
                        ],
                    }
        result["coverage_complete"] = bool(snapshot.get("complete"))
        result["risk_only_count"] = int(snapshot.get("risk_only_count", 0))
        for error in snapshot.get("errors") or []:
            result["source_error_count"] += 1
            result["warnings"].append(str(error))
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        usable: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for item in snapshot.get("rows") or []:
            if item.get("instrument_id") in local_rows and in_a_share_scope(
                item, exchange
            ):
                observation = dict(item)
                grouped[item["instrument_id"]].append(observation)
                usable[item["instrument_id"]].append(observation)
        pending_plans: dict[str, list[dict[str, Any]]] = defaultdict(list)
        try:
            state = await self.db_ops.load_a_share_delisting_state([exchange])
            if not isinstance(state, Mapping):
                raise ValueError("invalid persisted terminal state")
            for plan in state["plans"]:
                iid = plan["instrument_id"]
                if iid not in local_rows:
                    continue
                verified = datetime.fromisoformat(plan["verified_at"])
                evidence = dict(plan["evidence"])
                grouped[iid].append(evidence)
                pending_plans[iid].append(plan)
                # Expired plans still participate in conflict checks and retain
                # their closure identity, but cannot establish a positive write.
                if (
                    evidence.get("parser_version") != "official_stock_terminal_body.v1"
                    and verified.tzinfo is not None
                    and now - verified <= timedelta(hours=freshness_hours)
                ):
                    usable[iid].append(evidence)
        except Exception as exc:
            # A new installation has no auxiliary table yet; this actual refresh
            # establishes its baseline. Other read failures remain visible.
            if "no such table" not in str(exc):
                result["warnings"].append(f"terminal plan read failed: {exc}")
        lineage_catalog = load_lineage_catalog()
        persisted_lineage = await self.db_ops.get_instrument_master_lineage_metadata(
            list(local_rows)
        )
        if isinstance(persisted_lineage, dict):
            lineage_catalog.update(persisted_lineage)
        future_rows = []
        conflict_rows = []
        current_by_id = {row.get("instrument_id"): row for row in current}
        handled_ids = set()
        for iid, observations in grouped.items():
            local = local_rows[iid]
            issues = {
                _identity_issue(local, item, lineage_catalog.get(iid))
                for item in observations
            }
            if issues == {"predecessor_terminal_evidence"}:
                continue
            observations = [
                item
                for item in observations
                if _identity_issue(local, item, lineage_catalog.get(iid))
                != "predecessor_terminal_evidence"
            ]
            if not observations:
                continue
            handled_ids.add(iid)
            dates = {lifecycle_date(item.get("delisted_date")) for item in observations}
            dates.discard(None)
            usable_observations = [
                item
                for item in usable[iid]
                if _identity_issue(local, item, lineage_catalog.get(iid))
                != "predecessor_terminal_evidence"
            ]
            usable_dates = {
                lifecycle_date(item.get("delisted_date"))
                for item in usable_observations
            } - {None}
            effective = next(iter(usable_dates)) if len(usable_dates) == 1 else None
            previous = lifecycle_date(local.get("delisted_date"))
            issue = next(
                (
                    issue
                    for issue in issues
                    if issue and issue != "predecessor_terminal_evidence"
                ),
                None,
            )
            if (
                issue
                or len(dates) > 1
                or (previous and effective and previous != effective)
            ):
                conflict_rows.append(
                    self._add_conflict(
                        result,
                        local,
                        observations,
                        kind=issue or "official effective-date conflict",
                        verified_at=now,
                    )
                )
                continue
            if not usable_observations:
                result["warnings"].append(
                    f"{iid}: persisted terminal evidence expired or requires revalidation"
                )
                continue
            evidence = next(
                (
                    item
                    for item in usable_observations
                    if lifecycle_date(item.get("delisted_date")) == effective
                ),
                usable_observations[0],
            )
            evidence = {
                **evidence,
                "delisted_date": effective.isoformat() if effective else None,
            }
            lineage = lineage_catalog.get(iid)
            if lineage:
                regimes = (
                    lineage.get("issuer_regimes", [])
                    if isinstance(lineage, Mapping)
                    else lineage.issuer_regimes
                )
                for regime in regimes:
                    if (
                        regime.get("role")
                        if isinstance(regime, Mapping)
                        else regime.role
                    ) == "current":
                        start = (
                            regime["start_date"]
                            if isinstance(regime, Mapping)
                            else regime.start_date
                        )
                        evidence["issuer_regime_start"] = lifecycle_date(
                            start
                        ).isoformat()
            if effective and effective > now.date():
                result["future_effective_count"] += 1
                future_rows.append(
                    {
                        "reason": FUTURE,
                        "scope": "a_share_stock",
                        "mode": mode,
                        "instrument_id": iid,
                        "plan_key": _plan_key(evidence, local),
                        "effective_date": effective.isoformat(),
                        "verified_at": now.isoformat(),
                        "issuer_listing_date": str(local.get("listed_date") or ""),
                        "evidence": evidence,
                    }
                )
                continue
            if not effective and not evidence.get("completion_confirmed"):
                result["unconfirmed_count"] += 1
                continue
            evidence["completion_confirmed"] = True
            result["confirmed_count"] += 1
            current_row = current_by_id.get(iid)
            if (
                current_row
                and current_row.get("status") != "delisted"
                and current_row.get("is_active", True)
            ):
                conflict_rows.append(
                    self._add_conflict(
                        result,
                        local,
                        observations,
                        kind="current list conflicts with effective terminal evidence",
                        verified_at=now,
                        current=current_row,
                    )
                )
            plan_keys = [
                plan["plan_key"]
                for plan in pending_plans[iid]
                if effective is not None
                and lifecycle_date(plan["effective_date"]) == effective
                and _identity_issue(local, plan["evidence"], lineage_catalog.get(iid))
                is None
            ]
            was_active = bool(local.get("is_active"))
            if audit_only:
                result["would_deactivate_count"] += int(was_active)
            else:
                written = await self.db_ops.mark_instrument_delisted(
                    iid,
                    delisted_date=effective,
                    source=evidence["source"],
                    evidence=evidence,
                    plan_keys=plan_keys,
                )
                if not written:
                    result["write_failed_count"] += 1
                    result["errors"].append(f"{iid}: terminal transaction failed")
                    continue
                result["deactivated_count"] += int(was_active)
                result["date_enriched_count"] += int(
                    previous is None and effective is not None
                )
                result["confirmed_unchanged_count"] += int(not was_active)
            if len(result["samples"]) < 20:
                result["samples"].append(
                    {
                        "instrument_id": iid,
                        "delisted_date": evidence["delisted_date"],
                        "announcement_id": evidence.get("announcement_id"),
                        "title": evidence.get("title"),
                    }
                )
        if diagnostics.get("official_current_list_complete"):
            current_ids = {item.get("instrument_id") for item in current}
            unexplained = {
                iid
                for iid, row in local_rows.items()
                if row.get("is_active")
                and iid not in current_ids
                and iid not in handled_ids
            }
            result["unconfirmed_count"] += len(unexplained)
            if unexplained:
                result["warnings"].append(
                    f"official current absence without terminal evidence: {len(unexplained)}"
                )
        else:
            result["warnings"].append("official current-list coverage not verified")
        if not snapshot.get("complete") and not snapshot.get("errors"):
            result["warnings"].append("official terminal-list coverage incomplete")
        if future_rows:
            saved = await self.db_ops.save_instrument_master_discrepancies(
                future_rows,
                exchange=exchange,
                run_id=f"terminal_plans:{now.isoformat()}",
            )
            if saved != len(future_rows):
                result["write_failed_count"] += len(future_rows)
                result["errors"].append("future terminal plans were not durably saved")
        if conflict_rows:
            saved = await self.db_ops.save_instrument_master_discrepancies(
                conflict_rows,
                exchange=exchange,
                run_id=f"terminal_conflicts:{now.isoformat()}",
            )
            if saved != len(conflict_rows):
                result["write_failed_count"] += len(conflict_rows) - saved
                result["errors"].append(
                    "terminal conflict evidence was not durably saved"
                )
        result["status"] = (
            "error"
            if result["errors"]
            else "warning" if result["warnings"] else "success"
        )
        await self._record_verification(exchange, mode, now, result)
        return result

    @staticmethod
    def _add_conflict(result, local, observations, *, kind, verified_at, current=None):
        """Retain both sides for every conflict; bound only the report samples."""
        fields = (
            "instrument_id",
            "symbol",
            "exchange",
            "type",
            "listed_date",
            "delisted_date",
            "completion_confirmed",
            "source",
            "source_url",
            "announcement_id",
            "raw_snapshot_hash",
            "hash_kind",
            "body_text_hash",
            "parser_version",
            "observed_at",
            "issuer_regime_start",
            "evidence_excerpt",
            "status",
            "is_active",
            "trading_status",
        )
        conflict = {
            "reason": "a_share_terminal_conflict",
            "scope": "a_share_stock",
            "mode": "audit_only" if result["audit_only"] else "production",
            "instrument_id": local["instrument_id"],
            "verified_at": verified_at.isoformat(),
            "conflict_type": kind,
            "existing_lifecycle": {key: local[key] for key in fields if key in local},
            "terminal_evidence": [
                {key: row[key] for key in fields if key in row} for row in observations
            ],
        }
        if current is not None:
            conflict["current_evidence"] = {
                key: current[key] for key in fields if key in current
            }
        result["conflict_count"] += 1
        result["warnings"].append(f"{local['instrument_id']}: {kind}")
        if len(result["conflict_samples"]) < 20:
            result["conflict_samples"].append(conflict)
        return conflict

    async def _record_verification(self, exchange, mode, now, result):
        """Persist the same narrow protocol for completed or disabled checks."""
        summary = {
            key: value
            for key, value in result.items()
            if key not in {"warnings", "errors"}
        }
        saved = await self.db_ops.save_instrument_master_discrepancies(
            [
                {
                    "reason": VERIFICATION,
                    "scope": "a_share_stock",
                    "mode": mode,
                    "instrument_id": None,
                    "verified_at": now.isoformat(),
                    "status": result["status"],
                    "summary": summary,
                    "warnings": result["warnings"],
                    "errors": result["errors"],
                }
            ],
            exchange=exchange,
            run_id=f"terminal_verification:{now.isoformat()}",
        )
        if saved != 1:
            result["errors"].append("terminal verification was not durably saved")
            result["status"] = "error"


async def reusable_terminal_verifications(
    db_ops: Any, exchanges: Sequence[str], hours: float
) -> dict | None:
    """Invalidate fresh reuse at an effective day, restart or failed verification."""
    try:
        state = await db_ops.load_a_share_delisting_state(exchanges)
        now = get_shanghai_time()
        if not state["has_baseline"]:
            return None
        if any(
            lifecycle_date(plan["effective_date"]) <= now.date()
            for plan in state["plans"]
        ):
            return None
        for value in state["verifications"].values():
            verified = datetime.fromisoformat(value["verified_at"])
            if now - verified > timedelta(hours=hours) or value["status"] == "error":
                return None
        return state["verifications"]
    except Exception:
        # Returning None forces actual refresh; it is never a healthy empty queue.
        return None
