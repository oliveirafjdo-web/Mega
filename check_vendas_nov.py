import os
from sqlalchemy import create_engine, text

raw_db_url = os.environ.get("DATABASE_URL")
if raw_db_url and raw_db_url.startswith("postgres://"):
    raw_db_url = raw_db_url.replace("postgres://", "postgresql+psycopg2://", 1)

DATABASE_URL = raw_db_url or "sqlite:///metrifiy.db"
engine = create_engine(DATABASE_URL)

with engine.connect() as conn:
    # Verificar vendas por data em novembro
    result = conn.execute(text("""
        SELECT DATE(data_venda) as data, COUNT(*) as qtd, SUM(receita_total) as receita
        FROM vendas
        WHERE data_venda >= '2025-11-01' AND data_venda < '2025-11-12'
        GROUP BY DATE(data_venda)
        ORDER BY DATE(data_venda)
    """)).fetchall()
    
    print("Vendas por dia em novembro (01 a 11):")
    for row in result:
        print(f"  {row[0]}: {row[1]} vendas, R$ {row[2]:.2f}")
