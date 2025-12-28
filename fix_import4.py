with open('app.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Substituir o bloco de agrupamento para converter números ANTES
old_block = """    # Agrupar por SKU e somar quantidades (produtos com múltiplos anúncios)
    print(f"DEBUG: Processando {len(df)} linhas da planilha")
    df_grouped = df.groupby(col_sku, as_index=False).agg({
        col_quantidade: 'sum',
        col_produto: 'first'  # Pegar o primeiro nome
    })
    print(f"DEBUG: Após agrupamento: {len(df_grouped)} SKUs únicos")"""

new_block = """    # Converter quantidade para numérico ANTES de agrupar
    print(f"DEBUG: Processando {len(df)} linhas da planilha")
    print(f"DEBUG: Coluna quantidade identificada: {col_quantidade}")
    print(f"DEBUG: Coluna SKU identificada: {col_sku}")
    
    # Limpar e converter coluna de quantidade para numérico
    df[col_quantidade] = pd.to_numeric(df[col_quantidade], errors='coerce').fillna(0).astype(int)
    
    # Agrupar por SKU e somar quantidades (produtos com múltiplos anúncios)
    df_grouped = df.groupby(col_sku, as_index=False).agg({
        col_quantidade: 'sum',
        col_produto: 'first' if col_produto else col_sku  # Pegar o primeiro nome ou usar SKU
    })
    print(f"DEBUG: Após agrupamento: {len(df_grouped)} SKUs únicos")
    
    # Mostrar amostra dos primeiros 3 produtos agrupados
    for i in range(min(3, len(df_grouped))):
        row_sample = df_grouped.iloc[i]
        print(f"DEBUG: SKU={row_sample[col_sku]}, Qtd={row_sample[col_quantidade]}")"""

content = content.replace(old_block, new_block)

with open('app.py', 'w', encoding='utf-8') as f:
    f.write(content)

print('✅ Corrigido: conversão para número ANTES do agrupamento!')
