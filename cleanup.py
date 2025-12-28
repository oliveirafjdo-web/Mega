import sqlite3

db_path = "metrifiy.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Busca valores problemáticos
cursor.execute("SELECT id, descricao, valor FROM finance_transactions WHERE valor < -1000000000 OR valor > 10000000000")
problematicos = cursor.fetchall()

if problematicos:
    print(f"⚠️ Encontrados {len(problematicos)} valores problemáticos:")
    for id_trans, desc, valor in problematicos[:10]:  # Mostra os primeiros 10
        print(f"  ID {id_trans}: {desc} = {valor}")
    
    cursor.execute("DELETE FROM finance_transactions WHERE valor < -1000000000 OR valor > 10000000000")
    conn.commit()
    print(f"✅ {cursor.rowcount} registros deletados")
else:
    print("✅ OK - Nenhum valor problemático")

conn.close()
