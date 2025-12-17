import sqlite3
from pathlib import Path
import shutil
from datetime import datetime
import argparse

p = argparse.ArgumentParser()
p.add_argument('--valor', required=True, type=float)
p.add_argument('--date', required=True)
args = p.parse_args()

DB = Path('metrifiy.db')
if not DB.exists():
    print('DB not found:', DB)
    raise SystemExit(1)

# Backup
ts = datetime.now().strftime('%Y%m%d%H%M%S')
backup = DB.with_name(f"metrifiy.db.bak.apply.{ts}")
shutil.copy2(DB, backup)
print('Backup created:', backup)

conn = sqlite3.connect(str(DB))
cur = conn.cursor()
cutoff = args.date
cur.execute("SELECT COALESCE(SUM(valor),0) FROM finance_transactions WHERE data_lancamento < ?", (cutoff,))
before = cur.fetchone()[0]
print(f'Before insertion: total before {cutoff} = {before:.2f}')

# Insert adjustment
sql = "INSERT INTO finance_transactions (data_lancamento, tipo, valor, descricao, external_id_mp) VALUES (?, ?, ?, ?, ?)"
desc = f"Ajuste saldo anterior para {cutoff}"
cur.execute(sql, (cutoff, 'ADJUST', args.valor, desc, None))
conn.commit()
print('Inserted adjustment:', args.valor)

cur.execute("SELECT COALESCE(SUM(valor),0) FROM finance_transactions WHERE data_lancamento < ?", (cutoff,))
after = cur.fetchone()[0]
print(f'After insertion: total before {cutoff} = {after:.2f}')

conn.close()
