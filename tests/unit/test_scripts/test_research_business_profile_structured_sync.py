import json

from scripts import research_business_profile_structured_sync as cli


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

    async def sync(self, **kwargs):
        self.__class__.calls.append(kwargs)
        return {
            "status": "success",
            "attempted_instruments": 1,
            "dry_run": kwargs["dry_run"],
        }


def test_cli_parses_bounded_dry_run_scope_without_initializing_storage(
    tmp_path,
    monkeypatch,
):
    _Storage.instances.clear()
    _Service.calls.clear()
    monkeypatch.setattr(cli, "UnifiedConfigManager", _ConfigManager)
    monkeypatch.setattr(cli, "ResearchStorageManager", _Storage)
    monkeypatch.setattr(cli, "StructuredBusinessProfileSyncService", _Service)
    output = tmp_path / "report.json"

    result = cli.main(
        [
            "--source",
            "eastmoney_main_composition,ths_main_business_intro",
            "--industry-group",
            "coal",
            "--instrument",
            "601088.SH",
            "--max-instruments",
            "1",
            "--max-elapsed-seconds",
            "30",
            "--probe-disabled-config",
            "--cache-raw-snapshots",
            "--raw-cache-root",
            str(tmp_path / "raw"),
            "--output",
            str(output),
        ]
    )

    assert result == 0
    assert _Storage.instances[0].initialized is False
    call = _Service.calls[0]
    assert call["dry_run"] is True
    assert call["sources"] == [
        "eastmoney_main_composition",
        "ths_main_business_intro",
    ]
    assert call["industry_groups"] == ["coal"]
    assert call["instrument_ids"] == ["601088.SH"]
    assert call["raw_cache_root"] == tmp_path / "raw"
    assert json.loads(output.read_text(encoding="utf-8"))["status"] == "success"


def test_candidate_write_cli_initializes_storage_and_forwards_operator_switch(
    monkeypatch,
):
    _Storage.instances.clear()
    _Service.calls.clear()
    monkeypatch.setattr(cli, "UnifiedConfigManager", _ConfigManager)
    monkeypatch.setattr(cli, "ResearchStorageManager", _Storage)
    monkeypatch.setattr(cli, "StructuredBusinessProfileSyncService", _Service)

    result = cli.main(
        [
            "--candidate-write",
            "--operator-switch",
            "BUSINESS_PROFILE_CANDIDATE_WRITE",
            "--instrument",
            "601088.SH",
        ]
    )

    assert result == 0
    assert _Storage.instances[0].initialized is True
    call = _Service.calls[0]
    assert call["dry_run"] is False
    assert call["candidate_write"] is True
    assert call["operator_switch"] == "BUSINESS_PROFILE_CANDIDATE_WRITE"
