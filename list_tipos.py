import sqlite3
c=sqlite3.connect('metrifiy.db').cursor()
for row in c.execute("SELECT tipo, COUNT(*), COALESCE(SUM(valor),0) FROM finance_transactions GROUP BY tipo"):
    print(row)
