from sqlalchemy import create_engine, text

# Conectar ao banco
engine = create_engine('sqlite:///metrifiy.db')

with engine.begin() as conn:
    # Zerar todos os estoques
    result = conn.execute(text("UPDATE produtos SET estoque_atual = 0"))
    linhas_afetadas = result.rowcount
    
    print(f"✅ Estoque zerado para {linhas_afetadas} produtos!")
    
    # Verificar
    resultado = conn.execute(text("SELECT COUNT(*) as total FROM produtos WHERE estoque_atual > 0"))
    count = resultado.fetchone()[0]
    print(f"✅ Produtos com estoque > 0: {count}")
