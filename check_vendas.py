import os
from sqlalchemy import create_engine, text

raw_db_url = os.environ.get("DATABASE_URL")
if raw_db_url and raw_db_url.startswith("postgres://"):
    raw_db_url = raw_db_url.replace("postgres://", "postgresql+psycopg2://", 1)

DATABASE_URL = raw_db_url or "sqlite:///metrifiy.db"
engine = create_engine(DATABASE_URL)

with engine.connect() as conn:
    # Verificar vendas antes de 11 de novembro
    result = conn.execute(text(
        "SELECT COUNT(*) as total, "
        "MIN(data_venda) as min_data, "
        "MAX(data_venda) as max_data "
        "FROM vendas WHERE data_venda < '2024-11-11'"
    )).fetchone()
    
    print(f"Vendas antes de 11/nov/2024:")
    print(f"  Total: {result[0]}")
    print(f"  Data mínima: {result[1]}")
    print(f"  Data máxima: {result[2]}")
    
    # Verificar todas as vendas
    result2 = conn.execute(text(
        "SELECT COUNT(*) as total, "
        "MIN(data_venda) as min_data, "
        "MAX(data_venda) as max_data "
        "FROM vendas"
    )).fetchone()
    
    print(f"\nTodas as vendas:")
    print(f"  Total: {result2[0]}")
    print(f"  Data mínima: {result2[1]}")
    print(f"  Data máxima: {result2[2]}")
