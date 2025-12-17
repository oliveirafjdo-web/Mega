import sqlite3
import sys
from pathlib import Path

db_path = Path('metrifiy.db')
if not db_path.exists():
    print('Banco não encontrado em', db_path)
    sys.exit(1)

cutoffs = ['2025-12-01', '2025-11-30']
if len(sys.argv) > 1:
    cutoffs = [sys.argv[1]]

conn = sqlite3.connect(str(db_path))
cur = conn.cursor()
for cutoff in cutoffs:
    q = "SELECT COALESCE(SUM(valor),0) FROM finance_transactions WHERE data_lancamento < ?"
    cur.execute(q, (cutoff,))
    s = cur.fetchone()[0]
    print(f"Sum of transactions with data_lancamento < {cutoff}: {s}")

# Also show counts by tipo before cutoff
for cutoff in cutoffs:
    print('\nBreakdown before', cutoff)
    cur.execute("SELECT tipo, COALESCE(SUM(valor),0), COUNT(*) FROM finance_transactions WHERE data_lancamento < ? GROUP BY tipo", (cutoff,))
    for row in cur.fetchall():
        print(row)

conn.close()
