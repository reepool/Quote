"""Task 2.1: four-anchor share scan across ALL periods; output suspect candidates.

Anchors per (instrument, period):
  ths      : share_capital_amount canonical, fact_name='equity'           (share count)
  pic      : share_capital_amount canonical, fact_name LIKE '实收资本%'    (CNY amount)
  val      : valuation-implied (market_cap/close) in (period_end, +45d]   (share count)
  eps      : net_income / reported basic_eps                              (weighted count)

Suspect rule (design D3): ths disagrees (>50%) with >=2 other AVAILABLE anchors.
"""
import sys, sqlite3, json, time
sys.path.insert(0, '/home/python/Quote')
from datetime import date, timedelta
from collections import Counter

DB = '/home/python/Quote/data/financials.db'
VDB = '/home/python/Quote/data/valuation.db'

fconn = sqlite3.connect(f'file:{DB}?mode=ro', uri=True, timeout=30)
fconn.row_factory = sqlite3.Row
vconn = sqlite3.connect(f'file:{VDB}?mode=ro', uri=True, timeout=30)
vconn.row_factory = sqlite3.Row

t0 = time.time()
instruments = [r[0] for r in fconn.execute(
    "SELECT DISTINCT instrument_id FROM financial_facts ORDER BY instrument_id")]
print(f'instruments: {len(instruments)}', flush=True)

suspects = []
stats = Counter()
eps_skipped = 0
for k, iid in enumerate(instruments):
    if k % 800 == 0:
        print(f'  {k}/{len(instruments)} elapsed={time.time()-t0:.0f}s suspects={len(suspects)}', flush=True)
    # one indexed query per instrument pulls all three fact families
    rows = fconn.execute("""
        SELECT report_period, fact_name, fact_value, canonical_fact_name
        FROM financial_numeric_facts_hot
        WHERE instrument_id=?
          AND (COALESCE(canonical_fact_name,fact_name)='share_capital_amount'
               OR COALESCE(canonical_fact_name,fact_name)='profit_sheet.basic_eps')
          AND fact_value IS NOT NULL
        ORDER BY report_period, updated_at DESC, rowid DESC
    """, (iid,)).fetchall()
    if not rows: continue
    by_period = {}
    eps_by_period = {}
    for r in rows:
        pe = str(r['report_period'])[:10]
        cname = r['canonical_fact_name'] or r['fact_name']
        if cname == 'profit_sheet.basic_eps':
            if pe not in eps_by_period and r['fact_value'] != 0:
                eps_by_period[pe] = r['fact_value']
            continue
        fname = r['fact_name']
        if fname == 'equity':
            if r['fact_value'] > 0:
                by_period.setdefault(pe, {})['ths'] = r['fact_value']
        elif fname.startswith('实收资本'):
            if r['fact_value'] != 0 and 'pic' not in by_period.get(pe, {}):
                by_period.setdefault(pe, {})['pic'] = r['fact_value']
    ni_rows = fconn.execute("""
        SELECT report_period, net_income FROM financial_facts
        WHERE instrument_id=? AND net_income IS NOT NULL""", (iid,)).fetchall()
    ni_map = {str(r['report_period'])[:10]: r['net_income'] for r in ni_rows}
    vser = [(str(x['as_of_date'])[:10], x['market_cap']/x['close_price'])
            for x in vconn.execute("""SELECT as_of_date, market_cap, close_price FROM valuation_history
                WHERE instrument_id=? AND market_cap>0 AND close_price>0 ORDER BY as_of_date""", (iid,))]
    for pe, anchors in sorted(by_period.items()):
        ths = anchors.get('ths')
        if ths is None: continue
        available = {}
        if 'pic' in anchors and anchors['pic']:
            available['pic'] = anchors['pic']
        we = (date.fromisoformat(pe) + timedelta(days=45)).isoformat()
        inw = [v for d, v in vser if pe < d <= we]
        if inw: available['val'] = inw[0]
        ni = ni_map.get(pe)
        eps = eps_by_period.get(pe)
        if ni is not None and eps and abs(ni) > 0 and abs(eps) > 1e-9:
            available['eps'] = ni / eps
        else:
            eps_skipped += 1
        if not available: continue
        dissent = [name for name, v in available.items() if abs(ths/v - 1.0) > 0.5]
        if len(dissent) >= 2:
            stats['suspect'] += 1
            suspects.append({
                'instrument_id': iid, 'report_period': pe,
                'ths': ths, 'dissent': dissent,
                **{f'{n}_v': round(v, 2) for n, v in available.items()},
            })
        else:
            stats['ok'] += 1

print(f'\nSCAN DONE in {time.time()-t0:.0f}s: ok={stats["ok"]} suspect={stats["suspect"]} eps_skipped_periods={eps_skipped}')
print('suspect by exchange:', Counter(s['instrument_id'].split(".")[-1] for s in suspects))
by_orig = Counter()
for s in suspects[:0]: pass
print('\nsuspect samples (first 15):')
for s in suspects[:15]:
    print(' ', json.dumps(s, ensure_ascii=False))
with open('/home/python/Quote/data/QuoteBak/pit-panel-dev/suspect_candidates.json', 'w') as f:
    json.dump(suspects, f, ensure_ascii=False, indent=1)
print('candidates written to suspect_candidates.json')
