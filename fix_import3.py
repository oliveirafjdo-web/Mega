with open('app.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Encontrar o bloco que processa linha por linha e substituir por agrupamento
old_block = """    produtos_atualizados = 0
    produtos_nao_encontrados = []
    ajustes_registrados = []
    erros = []
    
    with engine.begin() as conn:
        for idx, row in df.iterrows():
            try:
                sku = str(row.get(col_sku) or "").strip()
                if not sku:
                    continue
            
                quantidade_ml = row.get(col_quantidade)
                try:
                    quantidade_ml = int(float(quantidade_ml)) if pd.notna(quantidade_ml) else 0
                except:
                    quantidade_ml = 0"""

new_block = """    produtos_atualizados = 0
    produtos_nao_encontrados = []
    ajustes_registrados = []
    erros = []
    
    # Agrupar por SKU e somar quantidades (produtos com múltiplos anúncios)
    print(f"DEBUG: Processando {len(df)} linhas da planilha")
    df_grouped = df.groupby(col_sku, as_index=False).agg({
        col_quantidade: 'sum',
        col_produto: 'first'  # Pegar o primeiro nome
    })
    print(f"DEBUG: Após agrupamento: {len(df_grouped)} SKUs únicos")
    
    with engine.begin() as conn:
        for idx, row in df_grouped.iterrows():
            try:
                sku = str(row.get(col_sku) or "").strip()
                if not sku:
                    continue
            
                quantidade_ml = row.get(col_quantidade)
                try:
                    quantidade_ml = int(float(quantidade_ml)) if pd.notna(quantidade_ml) else 0
                except:
                    quantidade_ml = 0"""

content = content.replace(old_block, new_block)

# Atualizar a referência de produto para usar col_produto
content = content.replace(
    "'nome': row.get(col_produto, row.get(\"Produto\", \"\"))",
    "'nome': row.get(col_produto, \"\")"
)

with open('app.py', 'w', encoding='utf-8') as f:
    f.write(content)

print('✅ Atualizado para agrupar SKUs duplicados e somar quantidades!')
print('Agora produtos que aparecem em múltiplas linhas terão suas quantidades somadas.')
