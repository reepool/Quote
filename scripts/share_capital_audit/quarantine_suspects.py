"""Task 2.3: quarantine mis-mapped share-capital rows (reversible, audited).

canonical_fact_name: share_capital_amount -> share_capital_suspect
row_version += 1; data_change_log entry with old_hash == new_hash (metadata
rename, NOT a substantive value change - must not push panel availability).
"""
import sys, sqlite3, json, time
sys.path.insert(0, '/home/python/Quote')
from utils.date_utils import get_shanghai_time

DB = '/home/python/Quote/data/financials.db'
import os
_input = os.environ.get('SUSPECT_INPUT', os.path.join(os.path.dirname(__file__), 'suspect_candidates.json'))
suspects = json.load(open(_input))
print('suspects:', len(suspects))

conn = sqlite3.connect(DB, timeout=60)
conn.row_factory = sqlite3.Row
conn.execute("PRAGMA busy_timeout=60000")
now = get_shanghai_time().isoformat()
hot_n = hist_n = log_n = 0
seq = conn.execute("SELECT MAX(sequence_id) FROM data_change_log").fetchone()[0] or 0

for s in suspects:
    iid, pe = s['instrument_id'], s['report_period']
    for table in ('financial_numeric_facts_hot', 'financial_numeric_facts_history'):
        rows = conn.execute(f"""
            SELECT rowid AS rid, row_hash, row_version FROM {table}
            WHERE instrument_id=? AND report_period=?
              AND canonical_fact_name='share_capital_amount' AND fact_name='equity'
        """, (iid, pe)).fetchall()
        for r in rows:
            conn.execute(f"""
                UPDATE {table}
                SET canonical_fact_name='share_capital_suspect',
                    row_version=row_version+1, updated_at=?
                WHERE rowid=?
            """, (now, r['rid']))
            if table.endswith('hot'):
                hot_n += 1
            else:
                hist_n += 1
    seq += 1
    conn.execute("""
        INSERT INTO data_change_log (
            domain, dataset, change_type, business_key_json, instrument_id,
            series_id, observation_date, period, old_hash, new_hash,
            row_version, source, source_mode, source_profile,
            ingestion_run_id, batch_id, changed_at
        ) VALUES (
            'financial', 'financial_numeric_facts', 'canonical_quarantine',
            ?, ?, NULL, NULL, ?, ?, ?, 1, 'pit_panel_audit', 'direct',
            'cross-validate-share-capital-denominators', NULL, ?, ?
        )
    """, (
        json.dumps({'instrument_id': iid, 'report_period': pe,
                    'reason': 'share_capital_mis_mapping_multi_anchor',
                    'dissent': s['dissent']}, ensure_ascii=False),
        iid, pe, s.get('ths'), s.get('ths'), 'share_quarantine_20261031',
        now,
    ))
    log_n += 1

conn.commit()
print(f'quarantined: hot={hot_n} history={hist_n} change_log={log_n}')
# verification: no equity-named share_capital rows remain among suspect keys
remaining = 0
for s in suspects:
    n = conn.execute("""
        SELECT COUNT(*) FROM financial_numeric_facts_hot
        WHERE instrument_id=? AND report_period=?
          AND canonical_fact_name='share_capital_amount' AND fact_name='equity'
    """, (s['instrument_id'], s['report_period'])).fetchone()[0]
    remaining += n
print('remaining un-quarantined among suspects:', remaining)
conn.close()
