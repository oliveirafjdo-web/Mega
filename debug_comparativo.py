import os
from datetime import date, timedelta, datetime
from sqlalchemy import create_engine, select, MetaData, Table

raw_db_url = os.environ.get("DATABASE_URL")
if raw_db_url and raw_db_url.startswith("postgres://"):
    raw_db_url = raw_db_url.replace("postgres://", "postgresql+psycopg2://", 1)

DATABASE_URL = raw_db_url or "sqlite:///metrifiy.db"
engine = create_engine(DATABASE_URL)

metadata = MetaData()
metadata.reflect(bind=engine)
vendas = metadata.tables["vendas"]

# Simular período padrão (últimos 30 dias)
hoje = date.today()
trinta_dias_atras = hoje - timedelta(days=29)
data_inicio_dt = trinta_dias_atras
data_fim_dt = hoje

print(f"Período atual: {data_inicio_dt} a {data_fim_dt}")

# Calcular período anterior
periodo_atual_dias = (data_fim_dt - data_inicio_dt).days + 1
periodo_anterior_fim = data_inicio_dt - timedelta(days=1)
periodo_anterior_inicio = periodo_anterior_fim - timedelta(days=periodo_atual_dias - 1)

print(f"Período anterior: {periodo_anterior_inicio} a {periodo_anterior_fim}")
print(f"Dias: {periodo_atual_dias}")

# Buscar vendas
with engine.connect() as conn:
    rows_cmp = conn.execute(
        select(
            vendas.c.data_venda,
            vendas.c.receita_total
        ).where(
            vendas.c.data_venda >= periodo_anterior_inicio.isoformat(),
            vendas.c.data_venda <= data_fim_dt.isoformat() + "T23:59:59"
        )
    ).mappings().all()
    
    print(f"\nTotal de registros retornados: {len(rows_cmp)}")
    
    # Processar
    faturamento_periodo_atual = {}
    faturamento_periodo_anterior = {}
    
    for v in rows_cmp:
        data_raw = v["data_venda"]
        if not data_raw:
            continue
        try:
            dt = datetime.fromisoformat(str(data_raw)).date()
        except Exception:
            continue
        
        receita = float(v["receita_total"] or 0)
        if receita == 0:
            continue
        
        # Período atual
        if data_inicio_dt <= dt <= data_fim_dt:
            dia_offset = (dt - data_inicio_dt).days + 1
            faturamento_periodo_atual[dia_offset] = faturamento_periodo_atual.get(dia_offset, 0) + receita
        
        # Período anterior
        elif periodo_anterior_inicio <= dt <= periodo_anterior_fim:
            dia_offset = (dt - periodo_anterior_inicio).days + 1
            faturamento_periodo_anterior[dia_offset] = faturamento_periodo_anterior.get(dia_offset, 0) + receita
    
    print(f"\nDias com faturamento no período atual: {len(faturamento_periodo_atual)}")
    print(f"Dias com faturamento no período anterior: {len(faturamento_periodo_anterior)}")
    
    print("\nPrimeiros 10 dias do período atual:")
    for i in range(1, min(11, periodo_atual_dias + 1)):
        valor = faturamento_periodo_atual.get(i, 0)
        data_real = data_inicio_dt + timedelta(days=i-1)
        print(f"  Dia {i:02d} ({data_real}): R$ {valor:.2f}")
    
    print("\nPrimeiros 10 dias do período anterior:")
    for i in range(1, min(11, periodo_atual_dias + 1)):
        valor = faturamento_periodo_anterior.get(i, 0)
        data_real = periodo_anterior_inicio + timedelta(days=i-1)
        print(f"  Dia {i:02d} ({data_real}): R$ {valor:.2f}")
