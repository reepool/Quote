#!/usr/bin/env python3
"""Fail the rollout when active legacy announcement acquisition residue remains."""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, Sequence


@dataclass(frozen=True)
class ResidueFinding:
    category: str
    path: str
    line: int
    match: str


LEGACY_RUNTIME_PATTERNS: tuple[re.Pattern[str], ...] = tuple(
    re.compile(pattern)
    for pattern in (
        r"\bCninfoAnnouncementRecord\b",
        r"\bCninfoAnnouncementScanConfig\b",
        r"\bCninfoAnnouncementScanResult\b",
        r"\bCninfoAnnouncementScanner\b",
        r"\bCninfoBusinessProfileDiscoveryAdapter\b",
        r"\bOfficialExchangeBusinessProfileDiscoveryAdapter\b",
        r"\bExchangeBusinessProfileSourceConfig\b",
        r"\bbackup_adapters\b",
        r"\bbusiness_profile_candidate_url\b",
        r"\b_absolute_cninfo_url\b",
        r"\bdiscover_backup_instrument\b",
        r"\bdiscover_primary_instrument\b",
        r"\bget_cninfo_announcement_scan_state\b",
        r"\bupsert_cninfo_announcement_scan_state\b",
        r"\bstore_cninfo_announcement_audit\b",
        r"\blist_cninfo_announcement_audit\b",
    )
)
LEGACY_TABLE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\bcninfo_announcement_scan_state\b"),
    re.compile(r"\bcninfo_announcement_audit\b"),
)
OLD_CONFIG_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r'"announcement_scan"\s*:'),
    re.compile(r'"official_exchange_backups"\s*:'),
)
DIRECT_PROVIDER_IMPORT = re.compile(
    r"from\s+research\.providers\.cninfo_announcements\s+import"
)
DUPLICATED_TRANSPORT_PATTERN = re.compile(
    r"static\.cninfo\.com\.cn|query\.sse\.com\.cn/security/stock/"
    r"queryCompanyBulletin\.do|www\.szse\.cn/api/disc/announcement/annList|"
    r"www\.bse\.cn/disclosureInfoController/companyAnnouncement\.do"
)

PYTHON_SCAN_ROOTS = (
    "research",
    "data_sources",
    "scripts",
    "scheduler",
    "api",
    "tests",
)
DOMAIN_TRANSPORT_PATHS = (
    "research/business_profile_discovery.py",
    "research/business_profile_exchange_discovery.py",
    "research/business_profile_archive.py",
    "research/business_profile_official_archive_sync.py",
    "research/broker_risk_control.py",
    "data_sources/cninfo_corporate_action_documents.py",
    "data_sources/cninfo_special_action_resolution.py",
    "data_sources/corporate_action_validation.py",
    "data_manager.py",
)
ALLOWED_TABLE_PATHS = {
    "research/migrations/announcement_legacy_cleanup.py",
    "tests/unit/test_research/test_storage.py",
}
ALLOWED_DIRECT_IMPORT_PATHS = {
    "research/providers/registry.py",
    "tests/unit/test_research/test_announcement_acquisition.py",
}
SELF_PATHS = {
    "scripts/dev_validation/check_announcement_legacy_residue.py",
    "tests/unit/test_research/test_announcement_cleanup.py",
}


def _iter_files(root: Path, paths: Iterable[Path]) -> Iterable[Path]:
    for path in paths:
        if path.is_file():
            yield path
            continue
        if not path.exists():
            continue
        for candidate in path.rglob("*"):
            if not candidate.is_file():
                continue
            relative_parts = candidate.relative_to(root).parts
            if any(
                part in {".git", ".pytest_cache", "__pycache__"}
                for part in relative_parts
            ):
                continue
            yield candidate


def _find_patterns(
    *,
    root: Path,
    files: Iterable[Path],
    patterns: Sequence[re.Pattern[str]],
    category: str,
    allowed_paths: set[str] | None = None,
) -> list[ResidueFinding]:
    allowed = allowed_paths or set()
    findings: list[ResidueFinding] = []
    for path in files:
        relative = path.relative_to(root).as_posix()
        if relative in SELF_PATHS or relative in allowed:
            continue
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except UnicodeDecodeError:
            continue
        for line_number, line in enumerate(lines, start=1):
            for pattern in patterns:
                match = pattern.search(line)
                if match:
                    findings.append(
                        ResidueFinding(
                            category=category,
                            path=relative,
                            line=line_number,
                            match=match.group(0),
                        )
                    )
    return findings


def scan_repository(root: Path) -> list[ResidueFinding]:
    """Return every active residue finding; an empty result is the release gate."""
    root = root.resolve()
    python_files = list(
        _iter_files(
            root,
            [root / item for item in PYTHON_SCAN_ROOTS]
            + [root / "data_manager.py"],
        )
    )
    config_files = list(
        _iter_files(
            root,
            [root / "config", root / "tests" / "fixtures"],
        )
    )
    config_files = [path for path in config_files if path.suffix == ".json"]
    documentation_files = list(_iter_files(root, [root / "docs"]))
    documentation_files = [
        path for path in documentation_files if path.suffix.lower() in {".md", ".rst"}
    ]
    domain_transport_files = [
        root / relative
        for relative in DOMAIN_TRANSPORT_PATHS
        if (root / relative).exists()
    ]

    findings = _find_patterns(
        root=root,
        files=python_files,
        patterns=LEGACY_RUNTIME_PATTERNS,
        category="legacy_runtime_symbol",
        allowed_paths={"tests/unit/test_research/test_storage.py"},
    )
    findings.extend(
        _find_patterns(
            root=root,
            files=[*python_files, *documentation_files],
            patterns=LEGACY_TABLE_PATTERNS,
            category="legacy_runtime_table",
            allowed_paths=ALLOWED_TABLE_PATHS,
        )
    )
    findings.extend(
        _find_patterns(
            root=root,
            files=config_files,
            patterns=OLD_CONFIG_PATTERNS,
            category="obsolete_configuration",
        )
    )
    findings.extend(
        _find_patterns(
            root=root,
            files=python_files,
            patterns=(DIRECT_PROVIDER_IMPORT,),
            category="direct_consumer_provider_import",
            allowed_paths=ALLOWED_DIRECT_IMPORT_PATHS,
        )
    )
    findings.extend(
        _find_patterns(
            root=root,
            files=domain_transport_files,
            patterns=(DUPLICATED_TRANSPORT_PATTERN,),
            category="duplicated_domain_transport",
        )
    )
    return sorted(
        findings,
        key=lambda item: (item.category, item.path, item.line, item.match),
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check that legacy announcement acquisition residue is absent."
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[2],
    )
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    findings = scan_repository(args.root)
    payload = {
        "status": "failed" if findings else "success",
        "root": str(args.root.resolve()),
        "finding_count": len(findings),
        "findings": [asdict(item) for item in findings],
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    elif findings:
        print(f"announcement legacy residue check failed: {len(findings)} finding(s)")
        for item in findings:
            print(f"{item.category}: {item.path}:{item.line}: {item.match}")
    else:
        print("announcement legacy residue check passed")
    return 1 if findings else 0


if __name__ == "__main__":
    raise SystemExit(main())
