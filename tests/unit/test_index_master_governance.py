from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from data_manager import DataManager


def _build_config_manager() -> Mock:
    config = Mock()
    config.get_nested.side_effect = lambda key, default=None: {
        'telegram_config.enabled': False,
        'data_config': {
            'data_dir': 'data',
            'download_chunk_days': 7,
            'index_master_governance': {
                'enabled': True,
                'run_before_daily_update': True,
                'exchanges': ['SZSE'],
                'official_sources': ['cnindex'],
                'allow_series_inference': True,
                'master_admission': {
                    'canonical_key': 'instrument_id',
                    'duplicate_key_policy': 'skip_ambiguous',
                    'ambiguous_duplicate_action': 'skip',
                    'collapse_identical_duplicates': True,
                    'conflict_signature_fields': [
                        'name',
                        'market',
                        'industry',
                        'sector',
                        'metadata.cni_code',
                        'metadata.full_name',
                    ],
                },
                'write_stale_no_quote': False,
                'sample_limit': 10,
                'timeout_sec': 30,
            },
        },
    }.get(key, default)
    return config


class FakeIndexDbOps:
    def __init__(self):
        self.rows = {
            '480055.SZ': {
                'instrument_id': '480055.SZ',
                'symbol': '480055',
                'name': '规模因子R',
                'exchange': 'SZSE',
                'type': 'index',
                'status': 'active',
                'is_active': True,
                'trading_status': 1,
                'source': 'akshare',
                'updated_at': '2026-06-10T20:00:00',
            }
        }
        self.latest_quotes = {'480055.SZ': datetime(2026, 5, 13)}
        self.evidence_rows = []
        self.saved_metadata_count = 0
        self.saved_instrument_batches = []

    async def execute_read_query(self, query, params=None):
        if "status = 'stale_no_quote'" in query:
            return []
        exchange = (params or {}).get('exchange', 'SZSE')
        return [
            row.copy()
            for row in self.rows.values()
            if row.get('exchange') == exchange and row.get('type') == 'index'
        ]

    async def save_instruments_batch(self, rows):
        self.saved_instrument_batches.append(list(rows))
        for row in rows:
            instrument_id = row['instrument_id']
            existing = self.rows.get(instrument_id, {})
            existing.update({
                'instrument_id': instrument_id,
                'symbol': row.get('symbol'),
                'name': row.get('name'),
                'exchange': row.get('exchange'),
                'type': row.get('type'),
                'status': row.get('status', 'active'),
                'is_active': row.get('is_active', True),
                'trading_status': row.get('trading_status', 1),
                'source': row.get('source'),
                'updated_at': '2026-06-12T20:00:00',
            })
            self.rows[instrument_id] = existing
        return True

    async def save_instrument_master_metadata_batch(self, rows):
        self.saved_metadata_count += len(rows)
        return len(rows)

    async def save_index_lifecycle_evidence(self, rows):
        self.evidence_rows.extend(rows)
        return len(rows)

    async def get_latest_quote_date(self, instrument_id):
        return self.latest_quotes.get(instrument_id)

    async def mark_index_lifecycle_state(
        self,
        instrument_id,
        *,
        lifecycle_state,
        source,
        effective_date=None,
        last_quote_date=None,
    ):
        row = self.rows.get(instrument_id)
        if row is None:
            return False
        row['status'] = lifecycle_state
        row['source'] = source
        row['is_active'] = lifecycle_state == 'active_quote'
        row['trading_status'] = 1 if lifecycle_state == 'active_quote' else 0
        return True

    async def get_active_instruments(self, exchange, instrument_types=None, tradable_only=True):
        return [
            row.copy()
            for row in self.rows.values()
            if row['exchange'] == exchange
            and row['type'] in set(instrument_types or ['index'])
            and (not tradable_only or row['is_active'])
        ]


class FakeCNIndexSource:
    async def get_index_master_snapshot(self):
        return SimpleNamespace(
            rows=[
                {
                    'instrument_id': '980055.SZ',
                    'symbol': '980055',
                    'name': '规模因子',
                    'exchange': 'SZSE',
                    'type': 'index',
                    'currency': 'CNY',
                    'status': 'active',
                    'is_active': True,
                    'trading_status': 1,
                    'source': 'cnindex',
                    'source_url': 'https://example.test/list.xlsx',
                    'parser_version': 'test',
                    'metadata': {},
                },
                {
                    'instrument_id': '480055.SZ',
                    'symbol': '480055',
                    'name': '规模因子R',
                    'exchange': 'SZSE',
                    'type': 'index',
                    'currency': 'CNY',
                    'status': 'active',
                    'is_active': True,
                    'trading_status': 1,
                    'source': 'cnindex',
                    'source_url': 'https://example.test/list.xlsx',
                    'parser_version': 'test',
                    'metadata': {},
                },
            ],
            raw_snapshot_hash='hash',
        )

    async def get_lifecycle_evidence(self):
        return [
            {
                'instrument_id': '980055.SZ',
                'symbol': '980055',
                'exchange': 'SZSE',
                'lifecycle_state': 'calculation_terminated',
                'event_type': 'calculation_terminated',
                'effective_date': date(2026, 5, 14),
                'announcement_date': date(2026, 4, 14),
                'announcement_title': '关于终止计算发布国证 AlphaFocus 中华规模因子指数等指数的公告',
                'evidence_url': 'https://example.test/announcement.pdf',
                'matched_code': '980055',
                'confidence': 'direct',
                'source': 'cnindex_announcement',
                'parser_version': 'test',
                'raw_snapshot_hash': 'pdf-hash',
                'diagnostics': {},
            }
        ]


class FakeSourceFactory:
    def __init__(self):
        self.cnindex = FakeCNIndexSource()

    def get_source_instance(self, base_name, *, exchange=None, region=None):
        return self.cnindex if base_name == 'cnindex' else None


@pytest.mark.asyncio
async def test_index_governance_applies_direct_and_series_inferred_termination():
    with patch('data_manager.config_manager', _build_config_manager()):
        manager = DataManager()
    manager.db_ops = FakeIndexDbOps()
    manager.source_factory = FakeSourceFactory()

    result = await manager.sync_index_master(['SZSE'], target_date=date(2026, 6, 12))

    assert result['summary']['direct_terminated_count'] == 1
    assert result['summary']['inferred_terminated_count'] == 1
    assert result['summary']['lifecycle_skip_count'] == 2
    assert manager.db_ops.rows['980055.SZ']['status'] == 'calculation_terminated'
    assert manager.db_ops.rows['480055.SZ']['status'] == 'calculation_terminated'
    assert not await manager.db_ops.get_active_instruments(
        'SZSE',
        instrument_types=['index'],
        tradable_only=True,
    )
    assert any(row['confidence'] == 'series_inferred' for row in manager.db_ops.evidence_rows)


class DuplicateCNIndexSource(FakeCNIndexSource):
    async def get_index_master_snapshot(self):
        snapshot = await super().get_index_master_snapshot()
        snapshot.rows.append({
            **snapshot.rows[0],
            'name': '规模因子-重复行',
        })
        return snapshot


@pytest.mark.asyncio
async def test_index_governance_skips_ambiguous_duplicate_official_master_rows():
    with patch('data_manager.config_manager', _build_config_manager()):
        manager = DataManager()
    manager.db_ops = FakeIndexDbOps()
    manager.source_factory = FakeSourceFactory()
    manager.source_factory.cnindex = DuplicateCNIndexSource()

    result = await manager.sync_index_master(['SZSE'], target_date=date(2026, 6, 12))

    written_batch = manager.db_ops.saved_instrument_batches[0]
    assert '980055.SZ' not in [row['instrument_id'] for row in written_batch]
    assert result['summary']['master_rows_saved'] == 1
    assert result['summary']['ambiguous_master_duplicate_groups_skipped'] == 1
    assert any('ambiguous duplicate key groups by rule' in warning for warning in result['warnings'])
