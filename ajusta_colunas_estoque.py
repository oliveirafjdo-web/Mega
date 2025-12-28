import sqlite3

# Caminho do banco de dados
DB_PATH = 'metrifiy.db'

# Colunas que queremos garantir
CAMPOS = [
    ('estoque_full', 'INTEGER DEFAULT 0'),
    ('estoque_flex', 'INTEGER DEFAULT 0'),
]

def garantir_colunas():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    # Verificar colunas existentes
    cur.execute("PRAGMA table_info(produtos)")
    existentes = [row[1] for row in cur.fetchall()]
    for nome, tipo in CAMPOS:
        if nome not in existentes:
            print(f"Adicionando coluna {nome}...")
            cur.execute(f"ALTER TABLE produtos ADD COLUMN {nome} {tipo}")
            con.commit()
        else:
            print(f"Coluna {nome} já existe.")
    con.close()
    print("Verificação concluída.")

if __name__ == "__main__":
    garantir_colunas()
