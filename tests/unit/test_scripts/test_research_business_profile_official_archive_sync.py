import json

import pytest

from scripts import research_business_profile_official_archive_sync as cli


class _ConfigManager:
    def __init__(self, path):
        assert path == "config"

    def get_research_config(self):
        return object()


class _Storage:
    instances = []

    def __init__(self, config):
        self.config = config
        self.initialized = False
        self.__class__.instances.append(self)

    def initialize(self):
        self.initialized = True


class _Service:
    calls = []

    def __init__(self, *, storage, research_config):
        self.storage = storage
        self.research_config = research_config

    def sync(self, **kwargs):
        self.__class__.calls.append(kwargs)
        return {
            "status": "success",
            "mode": "archive_write" if kwargs["archive_write"] else "metadata_only",
        }


def test_metadata_cli_is_read_only_and_forwards_bounded_scope(
    tmp_path,
    monkeypatch,
):
    _Storage.instances.clear()
    _Service.calls.clear()
    monkeypatch.setattr(cli, "UnifiedConfigManager", _ConfigManager)
    monkeypatch.setattr(cli, "ResearchStorageManager", _Storage)
    monkeypatch.setattr(
        cli,
        "BusinessProfileOfficialArchiveSyncService",
        _Service,
    )
    output = tmp_path / "official-archive-probe.json"

    result = cli.main(
        [
            "--target-research-db",
            str(tmp_path / "candidate.db"),
            "--instrument",
            "601088.SH,600028.SH",
            "--report-period",
            "2025-12-31",
            "--max-instruments",
            "2",
            "--max-pages",
            "3",
            "--as-of-date",
            "2026-07-18",
            "--output",
            str(output),
        ]
    )

    assert result == 0
    assert _Storage.instances[0].initialized is False
    call = _Service.calls[0]
    assert call["archive_write"] is False
    assert call["instrument_ids"] == ["601088.SH", "600028.SH"]
    assert call["max_instruments"] == 2
    assert call["max_pages"] == 3
    assert json.loads(output.read_text(encoding="utf-8"))["mode"] == ("metadata_only")


def test_archive_cli_initializes_storage_and_forwards_operator_switch(
    monkeypatch,
):
    _Storage.instances.clear()
    _Service.calls.clear()
    monkeypatch.setattr(cli, "UnifiedConfigManager", _ConfigManager)
    monkeypatch.setattr(cli, "ResearchStorageManager", _Storage)
    monkeypatch.setattr(
        cli,
        "BusinessProfileOfficialArchiveSyncService",
        _Service,
    )

    result = cli.main(
        [
            "--archive-write",
            "--operator-switch",
            "BUSINESS_PROFILE_OFFICIAL_ARCHIVE_WRITE",
            "--instrument",
            "601088.SH",
        ]
    )

    assert result == 0
    assert _Storage.instances[0].initialized is True
    call = _Service.calls[0]
    assert call["archive_write"] is True
    assert call["operator_switch"] == "BUSINESS_PROFILE_OFFICIAL_ARCHIVE_WRITE"


def test_invalid_archive_switch_fails_before_storage_initialization(monkeypatch):
    _Storage.instances.clear()
    _Service.calls.clear()
    monkeypatch.setattr(cli, "UnifiedConfigManager", _ConfigManager)
    monkeypatch.setattr(cli, "ResearchStorageManager", _Storage)
    monkeypatch.setattr(
        cli,
        "BusinessProfileOfficialArchiveSyncService",
        _Service,
    )

    with pytest.raises(
        PermissionError,
        match="BUSINESS_PROFILE_OFFICIAL_ARCHIVE_WRITE",
    ):
        cli.main(
            [
                "--archive-write",
                "--operator-switch",
                "wrong",
                "--instrument",
                "601088.SH",
            ]
        )

    assert _Storage.instances == []
    assert _Service.calls == []
