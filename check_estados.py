import os
from sqlalchemy import create_engine, text

raw_db_url = os.environ.get("DATABASE_URL")
if raw_db_url and raw_db_url.startswith("postgres://"):
    raw_db_url = raw_db_url.replace("postgres://", "postgresql+psycopg2://", 1)

DATABASE_URL = raw_db_url or "sqlite:///metrifiy.db"
engine = create_engine(DATABASE_URL)

with engine.connect() as conn:
    # Verificar estados
    result = conn.execute(text("""
        SELECT estado, COUNT(*) as qtd, SUM(receita_total) as receita
        FROM vendas
        WHERE estado IS NOT NULL AND estado != ''
        GROUP BY estado
        ORDER BY SUM(receita_total) DESC
    """)).fetchall()
    
    print(f"Estados encontrados na tabela vendas:")
    if result:
        for row in result:
            print(f"  {row[0]}: {row[1]} vendas, R$ {row[2]:.2f}")
    else:
        print("  Nenhum estado encontrado")
        
    # Verificar vendas sem estado
    result2 = conn.execute(text("""
        SELECT COUNT(*) FROM vendas WHERE estado IS NULL OR estado = ''
    """)).fetchone()
    
    print(f"\nVendas sem estado: {result2[0]}")
