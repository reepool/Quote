from pathlib import Path

from scripts.dev_validation.check_announcement_legacy_residue import scan_repository


def test_repository_has_no_active_legacy_announcement_residue():
    repository_root = Path(__file__).resolve().parents[3]
    assert scan_repository(repository_root) == []


def test_residue_gate_rejects_removed_business_profile_announcement_helpers(
    tmp_path,
):
    research_dir = tmp_path / "research"
    research_dir.mkdir()
    (research_dir / "stale_consumer.py").write_text(
        "coordinator.backup_adapters\n"
        "business_profile_candidate_url('report.pdf')\n"
        "service._absolute_cninfo_url('report.pdf')\n",
        encoding="utf-8",
    )

    findings = scan_repository(tmp_path)

    assert {item.match for item in findings} == {
        "backup_adapters",
        "business_profile_candidate_url",
        "_absolute_cninfo_url",
    }
