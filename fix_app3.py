import re

with open('app.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Encontrar linhas para remover
to_remove = set()

i = 0
while i < len(lines):
    line = lines[i]
    stripped = line.strip()
    
    # Se é uma definição de função
    if re.match(r'def\s+\w+\s*\([^)]*\):', stripped):
        # Verificar se a próxima linha é indentada (tem corpo)
        has_body = False
        if i + 1 < len(lines):
            next_line = lines[i + 1]
            if (next_line.startswith('    ') or next_line.startswith('\t')) and next_line.strip():
                has_body = True
        
        # Se não tem corpo, remover função e decoradores acima
        if not has_body:
            to_remove.add(i)
            # Remover decoradores
            j = i - 1
            while j >= 0 and (lines[j].strip().startswith('@') or not lines[j].strip()):
                if lines[j].strip().startswith('@'):
                    to_remove.add(j)
                j -= 1
    
    i += 1

# Criar novo arquivo
new_lines = [lines[i] for i in range(len(lines)) if i not in to_remove]

with open('app.py', 'w', encoding='utf-8') as f:
    f.writelines(new_lines)

print(f"Removidas {len(to_remove)} linhas!")
