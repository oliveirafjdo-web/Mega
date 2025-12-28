import os
from sqlalchemy import create_engine, inspect

raw_db_url = os.environ.get("DATABASE_URL")
if raw_db_url and raw_db_url.startswith("postgres://"):
    raw_db_url = raw_db_url.replace("postgres://", "postgresql+psycopg2://", 1)

DATABASE_URL = raw_db_url or "sqlite:///metrifiy.db"
engine = create_engine(DATABASE_URL)

inspector = inspect(engine)

print("Colunas da tabela 'vendas':")
for column in inspector.get_columns('vendas'):
    print(f"  - {column['name']} ({column['type']})")
