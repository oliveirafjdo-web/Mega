with open('app.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Atualizar a busca de colunas
old_block = """    # Identificar coluna de quantidade (pode ser "Unidades aptas", "Estoque", etc)
    col_quantidade = None
    for col in df.columns:
        col_lower = col.lower()
        if 'unidade' in col_lower or 'apta' in col_lower or 'estoque' in col_lower or 'quantidade' in col_lower:
            col_quantidade = col
            break
    
    if not col_quantidade:
        raise ValueError("Planilha deve ter coluna de quantidade (Unidades aptas, Estoque, etc)")
    
    if "SKU" not in df.columns:
        raise ValueError("Planilha deve ter coluna 'SKU'")"""

new_block = """    # Identificar colunas (busca case-insensitive e por índice)
    col_quantidade = None
    col_sku = None
    col_produto = None
    
    for col in df.columns:
        col_lower = str(col).lower().strip()
        if 'unidade' in col_lower and 'apta' in col_lower:
            col_quantidade = col
        elif col_lower == 'sku' or ('seller' in col_lower and 'sku' in col_lower):
            col_sku = col
        elif col_lower == 'produto' or 'title' in col_lower or 'título' in col_lower:
            col_produto = col
    
    # Se não encontrou, tentar por índice (ML Full: D=SKU, F=Produto, O=Unidades aptas)
    if not col_sku and len(df.columns) > 3:
        col_sku = df.columns[3]  # Coluna D (índice 3)
    if not col_produto and len(df.columns) > 5:
        col_produto = df.columns[5]  # Coluna F (índice 5)
    if not col_quantidade and len(df.columns) > 14:
        col_quantidade = df.columns[14]  # Coluna O (índice 14)
    
    if not col_quantidade:
        raise ValueError(f"Não foi possível identificar coluna de quantidade. Colunas: {list(df.columns)}")
    
    if not col_sku:
        raise ValueError(f"Não foi possível identificar coluna SKU. Colunas: {list(df.columns)}")"""

content = content.replace(old_block, new_block)

# Atualizar as referências de SKU e Produto
content = content.replace('sku = str(row.get("SKU") or "").strip()', 'sku = str(row.get(col_sku) or "").strip()')
content = content.replace('"nome": row.get("Produto", "")', '"nome": row.get(col_produto, row.get("Produto", ""))')

with open('app.py', 'w', encoding='utf-8') as f:
    f.write(content)

print('✅ Detecção de colunas atualizada!')
print('Agora suporta:')
print('  - Coluna D (índice 3): SKU')
print('  - Coluna F (índice 5): Produto')
print('  - Coluna O (índice 14): Unidades aptas')
