import sqlite3
from pathlib import Path

db = Path('metrifiy.db')
if not db.exists():
    print('DB not found', db)
    raise SystemExit(1)

conn = sqlite3.connect(str(db))
cur = conn.cursor()

cur.execute("SELECT COUNT(*), COALESCE(SUM(valor),0) FROM finance_transactions WHERE tipo='MP_NET' AND (external_id_mp IS NULL OR external_id_mp='')")
print('MP_NET with NULL external_id:', cur.fetchone())
cur.execute("SELECT COUNT(*), COALESCE(SUM(valor),0) FROM finance_transactions WHERE tipo='MP_NET' AND external_id_mp IS NOT NULL AND external_id_mp<>''")
print('MP_NET with external_id:', cur.fetchone())

print('\nSample MP_NET with NULL external_id:')
cur.execute("SELECT id, data_lancamento, valor, external_id_mp, descricao FROM finance_transactions WHERE tipo='MP_NET' AND (external_id_mp IS NULL OR external_id_mp='') ORDER BY data_lancamento DESC LIMIT 20")
for r in cur.fetchall():
    print(r)

print('\nSample MP_NET with external_id:')
cur.execute("SELECT id, data_lancamento, valor, external_id_mp, descricao FROM finance_transactions WHERE tipo='MP_NET' AND external_id_mp IS NOT NULL AND external_id_mp<>'' ORDER BY data_lancamento DESC LIMIT 20")
for r in cur.fetchall():
    print(r)

conn.close()
