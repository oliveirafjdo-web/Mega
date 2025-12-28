with open('app.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Encontrar e atualizar a seção de detecção de colunas
output = []
i = 0
while i < len(lines):
    line = lines[i]
    
    # Encontrar o início da detecção de colunas
    if '# Identificar colunas (busca case-insensitive e por índice)' in line:
        # Adicionar linha de debug primeiro
        output.append(line)
        output.append('    \n')
        output.append('    # Debug: mostrar colunas encontradas\n')
        output.append('    print(f"DEBUG: Total de colunas: {len(df.columns)}")\n')
        output.append('    print(f"DEBUG: Colunas: {list(df.columns)}")\n')
        output.append('    \n')
        i += 1
        continue
    
    # Substituir a linha de busca por unidades aptas
    if "if 'unidade' in col_lower and 'apta' in col_lower:" in line:
        output.append('        # Buscar coluna de quantidade (múltiplas variações)\n')
        output.append('        if ("unidade" in col_lower and "apta" in col_lower) or \\\n')
        output.append('           ("disponível" in col_lower or "disponivel" in col_lower) or \\\n')
        output.append('           (col_lower == "estoque") or \\\n')
        output.append('           ("qty" in col_lower or "quantity" in col_lower):\n')
        i += 1
        continue
    
    output.append(line)
    i += 1

with open('app.py', 'w', encoding='utf-8') as f:
    f.writelines(output)

print('✅ Atualizado com debug e busca mais robusta!')
