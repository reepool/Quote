"""Real-DB full panel build + measurements (M2/M3 acceptance evidence)."""
import sys, time, json
sys.path.insert(0, '/home/python/Quote')
from research.financial_pit_panel import FinancialPitPanelStore

store = FinancialPitPanelStore('/home/python/Quote/data/financials.db', '/home/python/Quote/data/quotes.db', '/home/python/Quote/data/valuation.db')
store.initialize()
t0 = time.time()
stats = store.build_full('v1')
build_seconds = round(time.time() - t0, 1)
print('BUILD:', json.dumps({**stats, 'seconds': build_seconds}, ensure_ascii=False), flush=True)

impact = store.measure_revision_impact()
print('REVISION_IMPACT:', json.dumps(impact, ensure_ascii=False), flush=True)

t1 = time.time()
cs = store.query_cross_section('20260801', limit=5)
cs_seconds = round(time.time() - t1, 3)
print('CROSS_SECTION as_of=20260801:', json.dumps({
    'total': cs['total'], 'seconds': cs_seconds,
    'stats': cs['stats'], 'universe_note': cs['universe_note'],
    'sample': cs['items'][0] if cs['items'] else None,
}, ensure_ascii=False, default=str)[:1500], flush=True)

t2 = time.time()
cov = store.coverage()
print('COVERAGE_SECONDS:', round(time.time() - t2, 2), flush=True)
for p in cov['periods']:
    print('  ', p['report_period'], 'actual', p['actual_count'], 'expected', p.get('expected_count'), 'gap', p.get('gap_count'), 'complete', p['is_complete'], 'quality', p['available_at_quality'], flush=True)
print('LATEST_COMPLETE:', cov['latest_complete_period'], flush=True)

t3 = time.time()
panel = store.query_panel(report_period='2026-06-30')
print('PANEL_PAGE_SECONDS:', round(time.time() - t3, 2), 'total:', panel['total'], flush=True)

chg = store.changes(limit=3)
print('CHANGES_SAMPLE:', json.dumps(chg, ensure_ascii=False)[:400], flush=True)
