import sqlite3
from pathlib import Path

db = Path('metrifiy.db')
if not db.exists():
    print('DB not found', db)
    raise SystemExit(1)

conn = sqlite3.connect(str(db))
cur = conn.cursor()

cutoffs = ['2025-11-30','2025-12-01','2025-12-02']
for cutoff in cutoffs:
    cur.execute("SELECT COALESCE(SUM(valor),0) FROM finance_transactions WHERE data_lancamento < ?", (cutoff,))
    total = cur.fetchone()[0]
    cur.execute("SELECT COALESCE(SUM(valor),0) FROM finance_transactions WHERE tipo='MP_NET' AND data_lancamento < ?", (cutoff,))
    mp_net = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*), COALESCE(SUM(valor),0) FROM finance_transactions WHERE tipo='MP_NET' AND data_lancamento < ?", (cutoff,))
    mp_count, mp_sum = cur.fetchone()
    print(f"Before {cutoff}: total={total}, MP_NET_count={mp_count}, MP_NET_sum={mp_sum}")

conn.close()
