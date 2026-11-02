"""Round-3.5 gap ②: acceptance #12 pre-run — panel vs AKShare yjbb overlap check."""
import sys, time
sys.path.insert(0, '/home/python/Quote')
import akshare as ak
import sqlite3

PANEL_COLS = {
    '每股收益': 'eps', '每股净资产': 'bps', '每股经营现金流量': 'ocfps',
    '净资产收益率': 'roe', '销售毛利率': 'gross_margin',
}
conn = sqlite3.connect('file:/home/python/Quote/data/financials.db?mode=ro', uri=True, timeout=30)
conn.row_factory = sqlite3.Row

for period in ('20231231', '20240331', '20240630'):
    t0 = time.time()
    try:
        df = ak.stock_yjbb_em(date=period)
    except Exception as exc:
        print(f'{period}: akshare fetch FAILED: {exc}')
        continue
    df = df[['股票代码'] + list(PANEL_COLS)].dropna(subset=['股票代码'])
    rows = conn.execute(
        f"SELECT instrument_id, eps, bps, ocfps, roe, gross_margin "
        f"FROM financial_pit_panel WHERE report_period='{period[:4]}-{period[4:6]}-{period[6:]}'"
    ).fetchall()
    panel = {
        r['instrument_id'].split('.')[0]: dict(r)
        for r in rows
    }
    report = {}
    for ak_col, pc in PANEL_COLS.items():
        matched = compared = big_diff = 0
        samples = []
        for _, akrow in df.iterrows():
            code = str(akrow['股票代码']).zfill(6)
            p = panel.get(code)
            ak_val, p_val = akrow[ak_col], p.get(pc) if p else None
            try:
                ak_val = float(ak_val); p_val = float(p_val)
            except (TypeError, ValueError):
                continue
            matched += 1
            if abs(ak_val) < 1e-9 and abs(p_val) < 1e-9:
                compared += 1; continue
            denom = max(abs(ak_val), abs(p_val), 1e-9)
            rel = abs(ak_val - p_val) / denom
            compared += 1
            if rel > 0.02:
                big_diff += 1
                if len(samples) < 3:
                    samples.append((code, round(ak_val,4), round(p_val,4)))
        rate = big_diff / compared if compared else 0
        report[pc] = (compared, big_diff, round(rate*100,2), samples)
    print(f'== {period} (akshare {len(df)} rows, panel {len(panel)}, {time.time()-t0:.0f}s) ==')
    for pc, (n, bad, rate, samples) in report.items():
        flag = 'PASS' if rate < 1.0 else 'FAIL'
        print(f'  {pc}: n={n} >2%diff={bad} ({rate}%) {flag} samples={samples}')
conn.close()
