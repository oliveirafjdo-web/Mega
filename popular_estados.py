import os
import random
from sqlalchemy import create_engine, text

raw_db_url = os.environ.get("DATABASE_URL")
if raw_db_url and raw_db_url.startswith("postgres://"):
    raw_db_url = raw_db_url.replace("postgres://", "postgresql+psycopg2://", 1)

DATABASE_URL = raw_db_url or "sqlite:///metrifiy.db"
engine = create_engine(DATABASE_URL)

# Estados brasileiros com peso (distribuição mais realista de vendas no Brasil)
estados_pesos = [
    ("SP", 30),  # São Paulo - maior concentração
    ("RJ", 15),  # Rio de Janeiro
    ("MG", 12),  # Minas Gerais
    ("RS", 8),   # Rio Grande do Sul
    ("PR", 8),   # Paraná
    ("SC", 7),   # Santa Catarina
    ("BA", 5),   # Bahia
    ("PE", 4),   # Pernambuco
    ("CE", 3),   # Ceará
    ("GO", 2),   # Goiás
    ("DF", 2),   # Distrito Federal
    ("PA", 1),   # Pará
    ("ES", 1),   # Espírito Santo
    ("MT", 1),   # Mato Grosso
    ("AM", 1),   # Amazonas
]

# Criar lista ponderada de estados
estados_list = []
for estado, peso in estados_pesos:
    estados_list.extend([estado] * peso)

print("Atualizando estados nas vendas...")

with engine.begin() as conn:
    # Buscar todos os IDs de vendas
    result = conn.execute(text("SELECT id FROM vendas")).fetchall()
    total = len(result)
    
    print(f"Total de vendas a atualizar: {total}")
    
    # Atualizar cada venda com um estado aleatório
    for i, row in enumerate(result):
        venda_id = row[0]
        estado = random.choice(estados_list)
        
        conn.execute(
            text("UPDATE vendas SET estado = :estado WHERE id = :id"),
            {"estado": estado, "id": venda_id}
        )
        
        if (i + 1) % 500 == 0:
            print(f"  Processadas {i + 1}/{total} vendas...")
    
    print(f"\n✅ {total} vendas atualizadas com estados!")

# Verificar resultado
with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT estado, COUNT(*) as qtd, SUM(receita_total) as receita
        FROM vendas
        WHERE estado IS NOT NULL AND estado != ''
        GROUP BY estado
        ORDER BY SUM(receita_total) DESC
        LIMIT 15
    """)).fetchall()
    
    print("\nTop 15 estados por receita:")
    for row in result:
        print(f"  {row[0]}: {row[1]} vendas, R$ {row[2]:.2f}")
