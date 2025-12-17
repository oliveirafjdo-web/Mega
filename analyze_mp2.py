import sqlite3
from pathlib import Path

db = Path('metrifiy.db')
if not db.exists():
    print('DB not found', db)
    raise SystemExit(1)

conn = sqlite3.connect(str(db))
cur = conn.cursor()

print('Duplicates (external_id_mp) with count>1 and their sum and count:')
cur.execute("SELECT external_id_mp, COUNT(*), COALESCE(SUM(valor),0) FROM finance_transactions WHERE tipo='MP_NET' GROUP BY external_id_mp HAVING COUNT(*)>1 ORDER BY COUNT(*) DESC LIMIT 50")
for r in cur.fetchall():
    print(r)

print('\nTotals per day around 2025-11-28..2025-12-02:')
cur.execute("SELECT substr(data_lancamento,1,10) as day, COUNT(*), COALESCE(SUM(valor),0) FROM finance_transactions WHERE tipo='MP_NET' AND data_lancamento BETWEEN '2025-11-28' AND '2025-12-02' GROUP BY day ORDER BY day")
for r in cur.fetchall():
    print(r)

print('\nTop 20 largest MP_NET values:')
cur.execute("SELECT id,data_lancamento,valor,external_id_mp FROM finance_transactions WHERE tipo='MP_NET' ORDER BY valor DESC LIMIT 20")
for r in cur.fetchall():
    print(r)

conn.close()
