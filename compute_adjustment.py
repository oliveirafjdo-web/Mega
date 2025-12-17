import sqlite3
from pathlib import Path
import shutil
from datetime import datetime

DB = Path('metrifiy.db')
if not DB.exists():
    print('DB not found:', DB)
    raise SystemExit(1)

# Backup
ts = datetime.now().strftime('%Y%m%d%H%M%S')
backup = DB.with_name(f"metrifiy.db.bak.{ts}")
shutil.copy2(DB, backup)
print('Backup created:', backup)

# Compute current total before cutoff
cutoff = '2025-11-30'
desired = 7129.29
conn = sqlite3.connect(str(DB))
cur = conn.cursor()
cur.execute("SELECT COALESCE(SUM(valor),0) FROM finance_transactions WHERE data_lancamento < ?", (cutoff,))
current_total = cur.fetchone()[0]
cur.execute("SELECT COUNT(*), COALESCE(SUM(valor),0) FROM finance_transactions WHERE tipo='MP_NET' AND data_lancamento < ?", (cutoff,))
mp_count, mp_sum = cur.fetchone()
conn.close()

adjustment = desired - current_total
print(f"Cutoff: {cutoff}")
print(f"Current total before cutoff: {current_total:.2f}")
print(f"MP_NET before cutoff: count={mp_count}, sum={mp_sum:.2f}")
print(f"Desired total: {desired:.2f}")
print(f"Adjustment needed (desired - current): {adjustment:.2f}")

if abs(adjustment) < 0.005:
    print('No meaningful adjustment needed.')
else:
    # Prepare SQL for insertion (user must confirm before applying)
    sql = ("INSERT INTO finance_transactions (data_lancamento, tipo, valor, descricao) "
           "VALUES ('2025-11-30', 'ADJUST', %f, 'Ajuste saldo anterior para 2025-11-30')" % (adjustment,))
    print('\nProposed SQL (run only after confirmation):')
    print(sql)
    print('\nIf you confirm, run:')
    print("python apply_adjustment.py --valor %f --date 2025-11-30" % (adjustment,))
