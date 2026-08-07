import json

from sqlalchemy import create_engine, text

from database.connection import DatabaseManager
from utils.llm import LlmConfig


def test_source_lineage_migration_is_repeatable_and_preserves_legacy_rows(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'legacy.db'}")
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE corporate_action_llm_analyses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                analysis_key VARCHAR(64) NOT NULL
            )
        """))
        connection.execute(
            text(
                "INSERT INTO corporate_action_llm_analyses (analysis_key) "
                "VALUES ('legacy-analysis')"
            )
        )

    manager = DatabaseManager.__new__(DatabaseManager)
    manager.sync_engine = engine
    manager._ensure_change_watermark_schema()
    manager._ensure_change_watermark_schema()

    with engine.connect() as connection:
        columns = {
            row[1]
            for row in connection.execute(
                text("PRAGMA table_info(corporate_action_llm_analyses)")
            )
        }
        row = connection.execute(text(
            "SELECT analysis_key, source_label, lineage_json, failover_count "
            "FROM corporate_action_llm_analyses"
        )).one()

    assert {
        "source_label",
        "selected_profile",
        "route_fingerprint",
        "lineage_json",
        "failover_count",
    }.issubset(columns)
    assert row.analysis_key == "legacy-analysis"
    assert row.source_label is None
    assert row.lineage_json is None
    assert row.failover_count == 0
    engine.dispose()


def test_mixed_lineage_rows_remain_readable_after_route_rollback():
    engine = create_engine("sqlite:///:memory:")
    lineage = {
        "llm_source": "pipio:grok-4.5",
        "attempts": [{"status": "success"}],
    }
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE corporate_action_llm_analyses (
                analysis_key VARCHAR(64) PRIMARY KEY,
                source_label VARCHAR(128),
                selected_profile VARCHAR(128),
                route_fingerprint VARCHAR(64),
                lineage_json TEXT,
                failover_count INTEGER NOT NULL DEFAULT 0
            )
        """))
        connection.execute(text("""
            INSERT INTO corporate_action_llm_analyses (analysis_key)
            VALUES ('legacy-analysis')
        """))
        connection.execute(
            text("""
                INSERT INTO corporate_action_llm_analyses (
                    analysis_key, source_label, selected_profile,
                    route_fingerprint, lineage_json, failover_count
                ) VALUES (
                    'routed-analysis', :source_label, :selected_profile,
                    :route_fingerprint, :lineage_json, 0
                )
            """),
            {
                "source_label": "pipio:grok-4.5",
                "selected_profile": "semantic_extraction__pipio_grok",
                "route_fingerprint": "a" * 64,
                "lineage_json": json.dumps(lineage, sort_keys=True),
            },
        )

    rollback_config = LlmConfig.from_mapping({
        "enabled": True,
        "profiles": {
            "semantic_extraction": {
                "enabled": True,
                "model": "single-source-model",
            }
        },
    })
    assert rollback_config.routes == {}

    with engine.connect() as connection:
        rows = connection.execute(text(
            "SELECT analysis_key, source_label, lineage_json "
            "FROM corporate_action_llm_analyses"
        )).mappings().all()
    by_key = {row["analysis_key"]: row for row in rows}
    assert by_key["legacy-analysis"]["source_label"] is None
    assert by_key["legacy-analysis"]["lineage_json"] is None
    assert by_key["routed-analysis"]["source_label"] == "pipio:grok-4.5"
    assert json.loads(by_key["routed-analysis"]["lineage_json"])["attempts"] == [
        {"status": "success"}
    ]
    engine.dispose()
