import json
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
        self.metadata_by_id = {}
        self.saved_instrument_batches = []

    async def execute_read_query(self, query, params=None):
        if "type = 'stock'" in query:
            ids = set((params or {}).values())
            return [
                {'instrument_id': row['instrument_id']}
                for row in self.rows.values()
                if row.get('type') == 'stock' and row.get('instrument_id') in ids
            ]
        if "FROM daily_quotes" in query:
            ids = set((params or {}).values())
            return [
                {'instrument_id': instrument_id}
                for instrument_id in self.latest_quotes
                if instrument_id in ids
            ]
        if "status = 'stale_no_quote'" in query:
            return []
        if "instrument_master_metadata" in query:
            rows = []
            for instrument_id, row in self.rows.items():
                metadata = self.metadata_by_id.get(instrument_id)
                if not metadata:
                    continue
                metadata_payload = metadata.get('metadata') or {}
                if (
                    row.get('exchange') == 'SZSE'
                    and row.get('type') == 'index'
                    and row.get('is_active') is True
                    and row.get('trading_status') == 1
                    and row.get('source') in {'cnindex', 'cnindex_index_list'}
                    and row.get('instrument_id') == f"{row.get('symbol')}.SZ"
                    and not metadata_payload.get('szse_quote_code')
                    and metadata_payload.get('cni_code')
                ):
                    rows.append({
                        **row.copy(),
                        'metadata_json': json.dumps(metadata, ensure_ascii=False),
                    })
            return rows
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
        for row in rows:
            self.metadata_by_id[row['instrument_id']] = row
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
        if row.get('type') != 'index':
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
                    'instrument_id': 'CNI980055.SZ',
                    'symbol': '980055',
                    'name': '规模因子',
                    'exchange': 'SZSE',
                    'type': 'index',
                    'currency': 'CNY',
                    'status': 'metadata_only',
                    'is_active': False,
                    'trading_status': 0,
                    'source': 'cnindex',
                    'source_symbol': '980055',
                    'source_url': 'https://example.test/list.xlsx',
                    'parser_version': 'test',
                    'metadata': {'cni_code': 'CNI980055.SZ', 'szse_quote_code': ''},
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
                    'source_symbol': '480055',
                    'source_url': 'https://example.test/list.xlsx',
                    'parser_version': 'test',
                    'metadata': {'szse_quote_code': '480055'},
                },
            ],
            raw_snapshot_hash='hash',
        )

    async def get_lifecycle_evidence(self):
        return [
            {
                'instrument_id': 'CNI980055.SZ',
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
        self.csindex = None

    def get_source_instance(self, base_name, *, exchange=None, region=None):
        if base_name == 'cnindex':
            return self.cnindex
        if base_name == 'csindex':
            return self.csindex
        return None


@pytest.mark.asyncio
async def test_index_governance_applies_direct_and_series_inferred_termination():
    with patch('data_manager.config_manager', _build_config_manager()):
        manager = DataManager()
    manager.data_config['index_master_governance']['exchanges'] = ['SSE', 'SZSE']
    manager.data_config['index_master_governance']['official_sources'] = ['cnindex', 'csindex']
    manager.db_ops = FakeIndexDbOps()
    manager.source_factory = FakeSourceFactory()

    result = await manager.sync_index_master(['SZSE'], target_date=date(2026, 6, 12))

    assert result['summary']['direct_terminated_count'] == 1
    assert result['summary']['inferred_terminated_count'] == 1
    assert result['summary']['terminal_boundary_missing_count'] == 1
    assert result['summary']['lifecycle_skip_count'] == 2
    assert manager.db_ops.rows['CNI980055.SZ']['status'] == 'calculation_terminated'
    assert manager.db_ops.rows['480055.SZ']['status'] == 'calculation_terminated'
    assert not await manager.db_ops.get_active_instruments(
        'SZSE',
        instrument_types=['index'],
        tradable_only=True,
    )
    assert any(row['confidence'] == 'series_inferred' for row in manager.db_ops.evidence_rows)


@pytest.mark.asyncio
async def test_index_governance_infers_missing_direct_terminal_quote_boundary():
    with patch('data_manager.config_manager', _build_config_manager()):
        manager = DataManager()
    manager.data_config['index_master_governance']['exchanges'] = ['SSE', 'SZSE']
    manager.data_config['index_master_governance']['official_sources'] = ['cnindex', 'csindex']
    manager.db_ops = FakeIndexDbOps()
    manager.db_ops.latest_quotes['CNI980055.SZ'] = datetime(2026, 5, 13)
    manager.source_factory = FakeSourceFactory()

    result = await manager.sync_index_master(['SZSE'], target_date=date(2026, 6, 12))

    assert result['summary']['terminal_boundary_inferred_count'] == 1
    assert result['summary']['terminal_boundary_missing_count'] == 0
    direct_evidence = [
        row for row in manager.db_ops.evidence_rows
        if row.get('instrument_id') == 'CNI980055.SZ'
    ][0]
    assert direct_evidence['last_quote_date'] == date(2026, 5, 13)
    assert direct_evidence['confidence'] == 'direct_lifecycle_local_quote_boundary'
    assert direct_evidence['diagnostics']['terminal_boundary_inference'] == (
        'local_latest_quote_on_or_before_effective_date'
    )


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
    assert 'CNI980055.SZ' in [row['instrument_id'] for row in written_batch]
    assert result['summary']['master_rows_saved'] == 2
    assert result['summary']['handled_ambiguous_master_duplicate_groups'] == 1
    assert result['summary']['ambiguous_master_duplicate_groups_skipped'] == 0
    assert not any('ambiguous duplicate' in warning for warning in result['warnings'])


class LegacyMetadataOnlyCNIndexSource(FakeCNIndexSource):
    async def get_index_master_snapshot(self):
        return SimpleNamespace(
            rows=[
                {
                    'instrument_id': 'CNB00001.CNI',
                    'symbol': '000001',
                    'name': '国证利率',
                    'exchange': 'SZSE',
                    'type': 'index',
                    'currency': 'CNY',
                    'status': 'metadata_only',
                    'is_active': False,
                    'trading_status': 0,
                    'source': 'cnindex',
                    'source_symbol': '000001',
                    'source_url': 'https://example.test/list.xlsx',
                    'parser_version': 'test',
                    'metadata': {
                        'cni_code': 'CNB00001.CNI',
                        'szse_quote_code': '',
                    },
                }
            ],
            raw_snapshot_hash='hash',
        )

    async def get_lifecycle_evidence(self):
        return []


@pytest.mark.asyncio
async def test_index_governance_deactivates_legacy_cnindex_metadata_only_quote_key():
    with patch('data_manager.config_manager', _build_config_manager()):
        manager = DataManager()
    manager.db_ops = FakeIndexDbOps()
    manager.db_ops.rows['000001.SZ'] = {
        'instrument_id': '000001.SZ',
        'symbol': '000001',
        'name': '国证利率',
        'exchange': 'SZSE',
        'type': 'index',
        'status': 'active',
        'is_active': True,
        'trading_status': 1,
        'source': 'cnindex',
        'updated_at': '2026-06-10T20:00:00',
    }
    manager.source_factory = FakeSourceFactory()
    manager.source_factory.cnindex = LegacyMetadataOnlyCNIndexSource()

    result = await manager.sync_index_master(['SZSE'], target_date=date(2026, 6, 12))

    assert result['summary']['metadata_only_legacy_deactivated_count'] == 1
    assert result['summary']['lifecycle_skip_count'] == 1
    assert manager.db_ops.rows['000001.SZ']['status'] == 'metadata_only'
    assert manager.db_ops.rows['000001.SZ']['is_active'] is False
    assert manager.db_ops.rows['CNB00001.CNI']['status'] == 'metadata_only'
    assert any(
        row['event_type'] == 'cnindex_metadata_only_identity'
        and row['instrument_id'] == '000001.SZ'
        for row in manager.db_ops.evidence_rows
    )


@pytest.mark.asyncio
async def test_index_governance_deactivates_persisted_cnindex_metadata_only_quote_key():
    with patch('data_manager.config_manager', _build_config_manager()):
        manager = DataManager()
    manager.db_ops = FakeIndexDbOps()
    manager.db_ops.rows['005125.SZ'] = {
        'instrument_id': '005125.SZ',
        'symbol': '005125',
        'name': '中小可选',
        'exchange': 'SZSE',
        'type': 'index',
        'status': 'active',
        'is_active': True,
        'trading_status': 1,
        'source': 'cnindex',
        'updated_at': '2026-06-10T20:00:00',
    }
    manager.db_ops.metadata_by_id['005125.SZ'] = {
        'instrument_id': '005125.SZ',
        'exchange': 'SZSE',
        'parser_version': 'official-index-source-v1',
        'raw_snapshot_hash': 'hash',
        'metadata': {
            'full_name': '中小创新可选消费行业指数',
            'publisher': '深圳证券交易所',
            'szse_quote_code': '',
            'cni_code': 'CN5125.CNI',
            'index_family': '深证系列',
            'index_category': '行业指数',
            'coverage_scope': '深市A股',
        },
    }
    manager.source_factory = FakeSourceFactory()

    result = await manager.sync_index_master(['SZSE'], target_date=date(2026, 6, 12))

    assert result['summary']['metadata_only_legacy_deactivated_count'] == 1
    assert manager.db_ops.rows['005125.SZ']['status'] == 'metadata_only'
    assert manager.db_ops.rows['005125.SZ']['is_active'] is False
    assert any(
        row['event_type'] == 'cnindex_metadata_only_identity'
        and row['instrument_id'] == '005125.SZ'
        and row['diagnostics']['cni_code'] == 'CN5125.CNI'
        for row in manager.db_ops.evidence_rows
    )


@pytest.mark.asyncio
async def test_index_governance_deactivates_cnindex_non_six_digit_quote_key():
    with patch('data_manager.config_manager', _build_config_manager()):
        manager = DataManager()
    manager.db_ops = FakeIndexDbOps()
    manager.db_ops.rows['39926401.SZ'] = {
        'instrument_id': '39926401.SZ',
        'symbol': '39926401',
        'name': '创业软件R',
        'exchange': 'SZSE',
        'type': 'index',
        'status': 'active',
        'is_active': True,
        'trading_status': 1,
        'source': 'cnindex',
        'updated_at': '2026-06-18T20:00:00',
    }
    manager.source_factory = FakeSourceFactory()

    result = await manager.sync_index_master(['SZSE'], target_date=date(2026, 6, 18))

    assert result['summary']['invalid_quote_code_deactivated_count'] == 1
    assert result['summary']['lifecycle_skip_count'] == 3
    assert manager.db_ops.rows['39926401.SZ']['status'] == 'metadata_only'
    assert manager.db_ops.rows['39926401.SZ']['is_active'] is False
    assert any(
        row['event_type'] == 'cnindex_invalid_quote_code_identity'
        and row['instrument_id'] == '39926401.SZ'
        and row['diagnostics']['reason'] == 'szse_quote_code_must_be_six_digits'
        for row in manager.db_ops.evidence_rows
    )


class FakeCSIndexSource:
    async def get_index_master_snapshot(self):
        return SimpleNamespace(
            rows=[
                {
                    'instrument_id': '000300.SH',
                    'symbol': '000300',
                    'name': '沪深300',
                    'exchange': 'SSE',
                    'type': 'index',
                    'currency': 'CNY',
                    'status': 'active',
                    'is_active': True,
                    'trading_status': 1,
                    'source': 'csindex',
                    'source_symbol': '000300',
                    'source_url': 'https://example.test/csindex',
                    'parser_version': 'test',
                    'metadata': {'publisher': 'CSIndex'},
                },
                {
                    'instrument_id': '930001.SH',
                    'symbol': '930001',
                    'name': '无行情参考指数',
                    'exchange': 'SSE',
                    'type': 'index',
                    'currency': 'CNY',
                    'status': 'active',
                    'is_active': True,
                    'trading_status': 1,
                    'source': 'csindex',
                    'source_symbol': '930001',
                    'source_url': 'https://example.test/csindex',
                    'parser_version': 'test',
                    'metadata': {'publisher': 'CSIndex'},
                },
            ],
            raw_snapshot_hash='cs-hash',
        )


class EmptyCNIndexSource(FakeCNIndexSource):
    async def get_index_master_snapshot(self):
        return SimpleNamespace(rows=[], raw_snapshot_hash='empty')

    async def get_lifecycle_evidence(self):
        return []


@pytest.mark.asyncio
async def test_csindex_reference_only_rows_do_not_expand_active_universe():
    with patch('data_manager.config_manager', _build_config_manager()):
        manager = DataManager()
    manager.data_config['index_master_governance']['exchanges'] = ['SSE', 'SZSE']
    manager.data_config['index_master_governance']['official_sources'] = ['cnindex', 'csindex']
    manager.db_ops = FakeIndexDbOps()
    manager.db_ops.rows = {
    }
    manager.db_ops.latest_quotes = {'000300.SH': datetime(2026, 6, 17)}
    manager.source_factory = FakeSourceFactory()
    manager.source_factory.cnindex = EmptyCNIndexSource()
    manager.source_factory.csindex = FakeCSIndexSource()

    result = await manager.sync_index_master(['SSE'], target_date=date(2026, 6, 18))

    saved_ids = [
        row['instrument_id']
        for batch in manager.db_ops.saved_instrument_batches
        for row in batch
    ]
    assert '000300.SH' in saved_ids
    assert '930001.SH' not in saved_ids
    assert result['summary']['csindex_active_admitted_count'] == 1
    assert result['summary']['csindex_reference_only_count'] == 1
    assert not any('diagnostic-only' in warning for warning in result['warnings'])


class StockCollisionCNIndexSource(FakeCNIndexSource):
    async def get_index_master_snapshot(self):
        return SimpleNamespace(
            rows=[
                {
                    'instrument_id': '000001.SZ',
                    'symbol': '000001',
                    'name': '错误指数身份',
                    'exchange': 'SZSE',
                    'type': 'index',
                    'currency': 'CNY',
                    'status': 'active',
                    'is_active': True,
                    'trading_status': 1,
                    'source': 'cnindex',
                    'source_symbol': '000001',
                    'source_url': 'https://example.test/list.xlsx',
                    'parser_version': 'test',
                    'metadata': {
                        'szse_quote_code': '000001',
                        'cni_code': 'CNB00001.CNI',
                    },
                }
            ],
            raw_snapshot_hash='hash',
        )

    async def get_lifecycle_evidence(self):
        return []


class DuplicateQuoteVariantCNIndexSource(FakeCNIndexSource):
    async def get_index_master_snapshot(self):
        snapshot = await super().get_index_master_snapshot()
        snapshot.rows.extend([
            {
                'instrument_id': '399264.SZ',
                'symbol': '399264',
                'name': '创业软件',
                'exchange': 'SZSE',
                'type': 'index',
                'currency': 'CNY',
                'status': 'active',
                'is_active': True,
                'trading_status': 1,
                'source': 'cnindex',
                'source_symbol': '399264',
                'source_url': 'https://example.test/list.xlsx',
                'parser_version': 'test',
                'metadata': {
                    'szse_quote_code': '399264',
                    'cni_code': '399264.SZ',
                    'full_name': '创业板软件指数',
                    'price_return_type': '价格指数',
                },
            },
            {
                'instrument_id': '399264.SZ',
                'symbol': '399264',
                'name': '创业软件R',
                'exchange': 'SZSE',
                'type': 'index',
                'currency': 'CNY',
                'status': 'active',
                'is_active': True,
                'trading_status': 1,
                'source': 'cnindex',
                'source_symbol': '39926401',
                'source_url': 'https://example.test/list.xlsx',
                'parser_version': 'test',
                'metadata': {
                    'szse_quote_code': '399264',
                    'cni_code': '399264.SZ',
                    'full_name': '创业板软件全收益指数',
                    'price_return_type': '收益指数',
                },
            },
            {
                'instrument_id': '988201.SZ',
                'symbol': '988201',
                'name': '湾创100R',
                'exchange': 'SZSE',
                'type': 'index',
                'currency': 'CNY',
                'status': 'active',
                'is_active': True,
                'trading_status': 1,
                'source': 'cnindex',
                'source_symbol': '480001',
                'source_url': 'https://example.test/list.xlsx',
                'parser_version': 'test',
                'metadata': {
                    'szse_quote_code': '988201',
                    'cni_code': '480001.CNI',
                    'full_name': '粤港澳大湾区创新100全收益指数',
                    'price_return_type': '收益指数',
                },
            },
            {
                'instrument_id': '988201.SZ',
                'symbol': '988201',
                'name': '湾创100R(港币)',
                'exchange': 'SZSE',
                'type': 'index',
                'currency': 'CNY',
                'status': 'active',
                'is_active': True,
                'trading_status': 1,
                'source': 'cnindex',
                'source_symbol': '480002',
                'source_url': 'https://example.test/list.xlsx',
                'parser_version': 'test',
                'metadata': {
                    'szse_quote_code': '988201',
                    'cni_code': '480001.CNI',
                    'full_name': '粤港澳大湾区创新100全收益指数(港币)',
                    'price_return_type': '收益指数',
                },
            },
        ])
        return snapshot

    async def get_lifecycle_evidence(self):
        return []


@pytest.mark.asyncio
async def test_index_governance_does_not_overwrite_stock_instrument_id():
    with patch('data_manager.config_manager', _build_config_manager()):
        manager = DataManager()
    manager.db_ops = FakeIndexDbOps()
    manager.db_ops.rows['000001.SZ'] = {
        'instrument_id': '000001.SZ',
        'symbol': '000001',
        'name': '平安银行',
        'exchange': 'SZSE',
        'type': 'stock',
        'status': 'active',
        'is_active': True,
        'trading_status': 1,
        'source': 'szse_official',
        'updated_at': '2026-06-10T20:00:00',
    }
    manager.source_factory = FakeSourceFactory()
    manager.source_factory.cnindex = StockCollisionCNIndexSource()

    result = await manager.sync_index_master(['SZSE'], target_date=date(2026, 6, 18))

    assert manager.db_ops.rows['000001.SZ']['type'] == 'stock'
    assert manager.db_ops.rows['000001.SZ']['name'] == '平安银行'
    assert result['summary']['stock_collision_index_rows_skipped'] == 1
    saved_ids = [
        row['instrument_id']
        for batch in manager.db_ops.saved_instrument_batches
        for row in batch
    ]
    assert '000001.SZ' not in saved_ids


@pytest.mark.asyncio
async def test_index_governance_handles_duplicate_quote_variants_without_warning():
    with patch('data_manager.config_manager', _build_config_manager()):
        manager = DataManager()
    manager.db_ops = FakeIndexDbOps()
    manager.source_factory = FakeSourceFactory()
    manager.source_factory.cnindex = DuplicateQuoteVariantCNIndexSource()

    result = await manager.sync_index_master(['SZSE'], target_date=date(2026, 6, 18))

    written_rows = [
        row
        for batch in manager.db_ops.saved_instrument_batches
        for row in batch
    ]
    by_id = {row['instrument_id']: row for row in written_rows}
    assert by_id['399264.SZ']['name'] == '创业软件'
    assert by_id['399264.SZ']['source_symbol'] == '399264'
    assert by_id['988201.SZ']['name'] == '湾创100R'
    assert by_id['988201.SZ']['source_symbol'] == '480001'
    assert result['summary']['handled_ambiguous_master_duplicate_groups'] == 2
    assert result['summary']['ambiguous_master_duplicate_groups_skipped'] == 0
    assert not any('unhandled ambiguous' in warning for warning in result['warnings'])
