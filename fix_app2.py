import re

with open('app.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Encontrar a seção de imports e inicialização (depois de "import os")
imports_match = re.search(r'(import os\nfrom datetime.*?from sqlalchemy\.engine.*?import Engine\nimport pandas as pd)', content, re.DOTALL)

if imports_match:
    imports_section = imports_match.group(1)
    
    # Remover a seção de imports do lugar onde ela está
    content_without_imports = content[:imports_match.start()] + content[imports_match.end():]
    
    # Encontrar onde está Flask(app) - procurar por padrão similar
    flask_init = re.search(r'(app = Flask\(__name__\).*?)(?=@app\.route|@login_required|# Rota|# ---)', content, re.DOTALL)
    
    if flask_init:
        # Encontrar o início de rotas
        routes_start = re.search(r'(@app\.route\(|# Rota de|@login_required)', content_without_imports)
        
        if routes_start:
            # Construir novo conteúdo com ordem correta
            new_content = imports_section + '\n\n'
            new_content += 'app = Flask(__name__)\n'
            new_content += 'app.secret_key = "sua_chave_secreta"\n'
            new_content += 'metadata = MetaData()\n'
            new_content += 'engine = create_engine("sqlite:///database.db")\n'
            new_content += 'metadata.create_all(engine)\n\n'
            new_content += content_without_imports[routes_start.start():]
            
            # Remover funções vazias e decoradores órfãos
            lines = new_content.split('\n')
            to_remove = set()
            for i in range(len(lines)):
                line = lines[i].strip()
                if re.match(r'def\s+\w+\s*\([^)]*\):', line):
                    if i + 1 < len(lines):
                        next_line = lines[i + 1]
                        if next_line.strip() and not (next_line.startswith('    ') or next_line.startswith('\t')):
                            to_remove.add(i)
                            j = i - 1
                            while j >= 0 and lines[j].strip().startswith('@'):
                                to_remove.add(j)
                                j -= 1
            
            new_lines = [lines[i] for i in range(len(lines)) if i not in to_remove]
            new_content = '\n'.join(new_lines)
            
            with open('app.py', 'w', encoding='utf-8') as f:
                f.write(new_content)
            
            print("Arquivo reorganizado e funções vazias removidas!")
        else:
            print("Não foi possível encontrar o início de rotas")
    else:
        print("Não foi possível encontrar inicialização Flask")
else:
    print("Não foi possível encontrar seção de imports")
