import re

with open('app.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Encontrar índices das funções incompletas (sem corpo)
to_remove = set()
for i in range(len(lines)):
    line = lines[i].strip()
    # Se é uma definição de função
    if re.match(r'def\s+\w+\s*\([^)]*\):', line):
        # Verificar se a próxima linha não é indentada (função vazia)
        if i + 1 < len(lines):
            next_line = lines[i + 1]
            # Se a próxima linha não está indentada, é uma função vazia
            if next_line.strip() and not (next_line.startswith('    ') or next_line.startswith('\t')):
                to_remove.add(i)
                # Remover decoradores acima também
                j = i - 1
                while j >= 0 and (lines[j].strip().startswith('@') or not lines[j].strip()):
                    if lines[j].strip().startswith('@'):
                        to_remove.add(j)
                    j -= 1

# Escrever linhas que não serão removidas
new_lines = [lines[i] for i in range(len(lines)) if i not in to_remove]

with open('app.py', 'w', encoding='utf-8') as f:
    f.writelines(new_lines)

print(f"Removidas {len(to_remove)} linhas com funções incompletas!")
