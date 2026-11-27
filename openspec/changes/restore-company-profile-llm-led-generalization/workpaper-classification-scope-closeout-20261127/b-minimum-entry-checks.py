"""Only the two repaired dependencies requested by 6.FU; no semantic run."""
import asyncio
from copy import deepcopy
import importlib.util
from pathlib import Path
import sys
from types import SimpleNamespace

ROOT = Path('/home/python/Quote')
sys.path.insert(0, str(ROOT))
HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('op', HERE / 'review_operator.py')
op = importlib.util.module_from_spec(spec)
spec.loader.exec_module(op)


def check(temp):
    from research.company_profile.workpaper_llm import run_workpaper, workpaper_processing_identity
    from research.company_profile.workpaper_sections import build_section_packet, index_sections
    from research.company_profile.stage5_provider import CommonGatewaySemanticProvider
    from tests.unit.test_research.test_company_profile_workpaper import PAGES, _draft, _report, _selection
    from tests.unit.test_utils.test_llm_gateway import _config, _response
    from tests.unit.test_utils.test_llm_physical_budget import _budget, _client
    from utils.llm.testing import ScriptedTransport
    cfg, access = op.dependencies()
    manifest = op.read(HERE.parent / 'workpaper-current-version-acceptance-20261125/b-sample-manifest.json')
    aliases = []
    for stock in ('601788.SH', '002736.SZ'):
        old = next(c for c in manifest['recommended'] if c['instrument_id'] == stock)
        asset = op.exact_asset(access, stock)
        assert asset['asset_id'] != old['asset_id'] and op.digest(old['pdf_path']) == old['content_hash']
        storage = op.storage_for(temp / 'metadata-only' / stock, cfg, asset)
        with storage.get_connection() as conn:
            assert conn.execute('SELECT COUNT(*) FROM business_profile_announcement_frontier WHERE instrument_id=?', (stock,)).fetchone()[0] == 1
        rejected = []
        for key in ('content_hash', 'source_announcement_id', 'published_at', 'report_period'):
            changed = {**asset, key: 'wrong'}
            try:
                op.exact_asset(SimpleNamespace(get_effective_asset=lambda *a, value=changed, **kw: value), stock)
            except AssertionError:
                rejected.append(key)
        assert len(rejected) == 4
        aliases.append(dict(instrument_id=stock, old_asset_id=old['asset_id'], resolved_asset_id=asset['asset_id'],
            document_version=asset['content_hash'], normal_owner='register_business_profile_shared_annual_report_asset',
            state='registered_metadata_only', rejected_identity_changes=rejected, semantic_requests=0))
    packet = build_section_packet(_report(), PAGES, index_sections(PAGES), _selection(index_sections(PAGES)))
    raw = _draft(packet).model_dump(mode='json')
    metric = raw['elements']['scale_and_mix']['facts'][0]['metrics'][0]
    metric.update(value=None, uncertainty_reason=None)
    before = deepcopy(raw)
    transport = ScriptedTransport([_response(_selection(index_sections(PAGES)).model_dump(mode='json')), _response(raw)])
    client = _client(transport, _budget(), _config(max_schema_repair_attempts=0))
    provider = CommonGatewaySemanticProvider(client=client, profile='test', max_output_tokens=2500, timeout_seconds=10)
    provider.offline_only = True
    try:
        result = run_workpaper(report=_report(), pages=PAGES, processing_identity=workpaper_processing_identity(real=False),
                              provider=provider, work_id='6fu-raw-null-normal-entry')
        fact = result.workpaper.elements.scale_and_mix.facts[0]
        assert fact.metrics[0].value is None and fact.metrics[0].uncertainty_reason
        assert fact.metrics[0].raw_value == metric['raw_value']
        assert raw == before and provider.workpaper_receipts[-1]['response_payload'] == before
        assert len(transport.calls) == 2 and result.workpaper.quality_review is None
    finally:
        asyncio.run(client.close())
    op.save(HERE / 'b-minimum-entry-proof.json', dict(state='passed', aliases=aliases,
        raw_null=dict(normal_entry='run_workpaper -> actual gateway -> normalize -> source bind',
            scripted_requests=2, fact_retained=True, value=None, unchanged_raw_response=True, semantic_approval=False),
        real_clients=0, real_requests=0, shared_config_changes=0))
    print('Two real metadata aliases registered locally, wrong identities rejected; normal raw/null gateway retained source fact.')


if __name__ == '__main__':
    check(Path(sys.argv[1]).resolve())
