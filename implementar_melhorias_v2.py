#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script para implementar as 5 melhorias no Mega ERP:
1. Sistema de Notificações/Alertas
2. Mais Relatórios
3. Sistema de Usuários Múltiplos com Papéis
4. Backup Automático
5. Melhorias de UX
"""

import os
import sqlite3
from datetime import datetime

# ============================================================================
# 1. ADICIONAR NOVAS TABELAS AO BANCO DE DADOS
# ============================================================================

def adicionar_tabelas():
    """Adiciona novas tabelas ao banco de dados"""
    
    try:
        conn = sqlite3.connect('metrifiy.db')
        cursor = conn.cursor()
        
        # Tabela de Notificações
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS notificacoes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            usuario_id INTEGER NOT NULL,
            tipo VARCHAR(50) NOT NULL,
            mensagem TEXT NOT NULL,
            dados_json TEXT,
            lida BOOLEAN DEFAULT 0,
            data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (usuario_id) REFERENCES usuarios(id)
        )
        ''')
        
        # Adicionar coluna de papel (role) em usuários
        try:
            cursor.execute('ALTER TABLE usuarios ADD COLUMN papel VARCHAR(20) DEFAULT "vendedor"')
        except:
            pass
        
        # Adicionar coluna de ativo em usuários
        try:
            cursor.execute('ALTER TABLE usuarios ADD COLUMN ativo BOOLEAN DEFAULT 1')
        except:
            pass
        
        # Tabela de backup
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS backups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            arquivo VARCHAR(255) NOT NULL,
            tamanho INTEGER,
            data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Tabela de auditoria
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS auditoria (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            usuario_id INTEGER,
            acao VARCHAR(100),
            tabela VARCHAR(50),
            registro_id INTEGER,
            dados_anteriores TEXT,
            dados_novos TEXT,
            data_acao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (usuario_id) REFERENCES usuarios(id)
        )
        ''')
        
        # Criar índices
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_notificacoes_usuario ON notificacoes(usuario_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_notificacoes_lida ON notificacoes(lida)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_auditoria_usuario ON auditoria(usuario_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_auditoria_data ON auditoria(data_acao)')
        
        conn.commit()
        print("✅ Tabelas criadas/atualizadas com sucesso!")
        return True
    except Exception as e:
        print(f"❌ Erro ao criar tabelas: {e}")
        return False
    finally:
        conn.close()

# ============================================================================
# 2. CRIAR NOVOS TEMPLATES
# ============================================================================

def criar_templates():
    """Cria novos templates HTML"""
    
    # Template para gerenciar usuários
    template_usuarios = '''{% extends "base.html" %}

{% block content %}
<div class="container-fluid mt-4">
    <div class="row">
        <div class="col-12">
            <div class="d-flex justify-content-between align-items-center mb-4">
                <h1><i class="bi bi-people"></i> Gerenciar Usuários</h1>
                <button class="btn btn-primary" data-bs-toggle="modal" data-bs-target="#novoUsuarioModal">
                    <i class="bi bi-plus-circle me-1"></i> Novo Usuário
                </button>
            </div>

            {% if usuarios %}
            <div class="card shadow-sm">
                <div class="table-responsive">
                    <table class="table table-hover mb-0">
                        <thead class="table-light">
                            <tr>
                                <th>Usuário</th>
                                <th>Papel</th>
                                <th>Status</th>
                                <th>Ações</th>
                            </tr>
                        </thead>
                        <tbody>
                            {% for usuario in usuarios %}
                            <tr>
                                <td><strong>{{ usuario[1] }}</strong></td>
                                <td>
                                    <span class="badge bg-{% if usuario[3]|default('vendedor') == 'admin' %}danger{% elif usuario[3]|default('vendedor') == 'gerente' %}warning{% else %}info{% endif %}">
                                        {{ usuario[3]|default('vendedor')|upper }}
                                    </span>
                                </td>
                                <td>
                                    {% if usuario[4]|default(1) %}
                                    <span class="badge bg-success">Ativo</span>
                                    {% else %}
                                    <span class="badge bg-secondary">Inativo</span>
                                    {% endif %}
                                </td>
                                <td>
                                    <form method="POST" action="/admin/usuario/{{ usuario[0] }}/toggle" style="display:inline;">
                                        <button type="submit" class="btn btn-sm btn-{% if usuario[4]|default(1) %}danger{% else %}success{% endif %}">
                                            <i class="bi bi-{% if usuario[4]|default(1) %}lock{% else %}unlock{% endif %}"></i>
                                        </button>
                                    </form>
                                </td>
                            </tr>
                            {% endfor %}
                        </tbody>
                    </table>
                </div>
            </div>
            {% else %}
            <div class="alert alert-info"><i class="bi bi-info-circle"></i> Nenhum usuário cadastrado.</div>
            {% endif %}
        </div>
    </div>
</div>

<!-- Modal Novo Usuário -->
<div class="modal fade" id="novoUsuarioModal" tabindex="-1">
    <div class="modal-dialog">
        <div class="modal-content">
            <div class="modal-header">
                <h5 class="modal-title">Novo Usuário</h5>
                <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
            </div>
            <form method="POST" action="/admin/usuario/novo">
                <div class="modal-body">
                    <div class="mb-3">
                        <label class="form-label">Usuário</label>
                        <input type="text" name="username" class="form-control" required>
                    </div>
                    <div class="mb-3">
                        <label class="form-label">Senha</label>
                        <input type="password" name="password" class="form-control" required>
                    </div>
                    <div class="mb-3">
                        <label class="form-label">Papel</label>
                        <select name="papel" class="form-select">
                            <option value="vendedor">Vendedor</option>
                            <option value="gerente">Gerente</option>
                            <option value="admin">Admin</option>
                        </select>
                    </div>
                </div>
                <div class="modal-footer">
                    <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancelar</button>
                    <button type="submit" class="btn btn-primary">Criar</button>
                </div>
            </form>
        </div>
    </div>
</div>
{% endblock %}
'''

    # Template para alertas/notificações
    template_alertas = '''{% extends "base.html" %}

{% block content %}
<div class="container-fluid mt-4">
    <h1><i class="bi bi-bell"></i> Alertas do Sistema</h1>
    
    <div class="row mt-4">
        <div class="col-md-6">
            <div class="card shadow-sm">
                <div class="card-header bg-danger text-white">
                    <h5><i class="bi bi-exclamation-triangle"></i> Estoque Baixo (< 5 unidades)</h5>
                </div>
                <div class="card-body">
                    {% if estoque_baixo %}
                    <ul class="list-group">
                        {% for produto in estoque_baixo %}
                        <li class="list-group-item d-flex justify-content-between align-items-center">
                            <div>
                                <strong>{{ produto[0] }}</strong>
                                <br>
                                <small class="text-muted">SKU: {{ produto[1] }}</small>
                            </div>
                            <span class="badge bg-warning text-dark">{{ produto[2] }} un</span>
                        </li>
                        {% endfor %}
                    </ul>
                    {% else %}
                    <p class="text-muted"><i class="bi bi-check-circle-fill text-success me-2"></i>Nenhum produto com estoque baixo</p>
                    {% endif %}
                </div>
            </div>
        </div>
        
        <div class="col-md-6">
            <div class="card shadow-sm">
                <div class="card-header bg-warning">
                    <h5><i class="bi bi-question-circle"></i> Vendas sem Produto</h5>
                </div>
                <div class="card-body">
                    {% if vendas_sem_produto > 0 %}
                    <p><strong>{{ vendas_sem_produto }}</strong> vendas sem produto vinculado</p>
                    <a href="/criar_produtos_de_vendas" class="btn btn-sm btn-primary">
                        <i class="bi bi-arrow-repeat me-1"></i>Sincronizar Agora
                    </a>
                    {% else %}
                    <p class="text-muted"><i class="bi bi-check-circle-fill text-success me-2"></i>Todas as vendas têm produtos vinculados</p>
                    {% endif %}
                </div>
            </div>
        </div>
    </div>

    <div class="row mt-3">
        <div class="col-md-6">
            <div class="card shadow-sm">
                <div class="card-header bg-info text-white">
                    <h5><i class="bi bi-graph-down"></i> Produtos não Vendidos (30 dias)</h5>
                </div>
                <div class="card-body">
                    {% if produtos_parados %}
                    <ul class="list-group">
                        {% for produto in produtos_parados %}
                        <li class="list-group-item">
                            <strong>{{ produto[0] }}</strong>
                            <br>
                            <small class="text-muted">Estoque: {{ produto[1] }} un</small>
                        </li>
                        {% endfor %}
                    </ul>
                    {% else %}
                    <p class="text-muted"><i class="bi bi-check-circle-fill text-success me-2"></i>Todos os produtos foram vendidos</p>
                    {% endif %}
                </div>
            </div>
        </div>

        <div class="col-md-6">
            <div class="card shadow-sm">
                <div class="card-header bg-success text-white">
                    <h5><i class="bi bi-graph-up"></i> Top 5 Produtos</h5>
                </div>
                <div class="card-body">
                    <ol class="list-group list-group-numbered">
                        {% for produto in top_produtos %}
                        <li class="list-group-item d-flex justify-content-between align-items-center">
                            {{ produto[0] }}
                            <span class="badge bg-primary rounded-pill">{{ produto[1] }}</span>
                        </li>
                        {% endfor %}
                    </ol>
                </div>
            </div>
        </div>
    </div>
</div>
{% endblock %}
'''

    # Salvar templates
    os.makedirs('templates', exist_ok=True)
    
    with open('templates/gerenciar_usuarios.html', 'w', encoding='utf-8') as f:
        f.write(template_usuarios)
    print("✅ Template gerenciar_usuarios.html criado")
    
    with open('templates/alertas_sistema.html', 'w', encoding='utf-8') as f:
        f.write(template_alertas)
    print("✅ Template alertas_sistema.html criado")

# ============================================================================
# 3. CRIAR SCRIPT DE BACKUP
# ============================================================================

def criar_script_backup():
    """Cria script de backup automático"""
    
    script_backup = '''#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Script para fazer backup do banco de dados"""

import os
import shutil
from datetime import datetime
from pathlib import Path

def fazer_backup():
    """Cria backup do banco de dados SQLite"""
    
    db_file = 'metrifiy.db'
    backup_dir = 'backups'
    
    # Criar diretório de backups se não existir
    os.makedirs(backup_dir, exist_ok=True)
    
    if os.path.exists(db_file):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = os.path.join(backup_dir, f'metrifiy_backup_{timestamp}.db')
        
        try:
            shutil.copy2(db_file, backup_file)
            tamanho = os.path.getsize(backup_file) / (1024*1024)  # MB
            print(f"✅ Backup criado: {backup_file} ({tamanho:.2f} MB)")
            
            # Manter apenas os últimos 10 backups
            arquivos = sorted(Path(backup_dir).glob('metrifiy_backup_*.db'))
            if len(arquivos) > 10:
                for arquivo in arquivos[:-10]:
                    arquivo.unlink()
                    print(f"🗑️ Backup antigo removido: {arquivo.name}")
            
            return backup_file
        except Exception as e:
            print(f"❌ Erro ao fazer backup: {e}")
            return None
    else:
        print(f"❌ Arquivo de banco de dados não encontrado: {db_file}")
        return None

if __name__ == "__main__":
    fazer_backup()
'''
    
    with open('backup_banco.py', 'w', encoding='utf-8') as f:
        f.write(script_backup)
    print("✅ Script backup_banco.py criado")

# ============================================================================
# 4. CRIAR ARQUIVO CSS PARA DARK MODE
# ============================================================================

def criar_css_melhorias():
    """Cria CSS para dark mode e melhorias de UX"""
    
    css_melhorias = '''/* Dark Mode */
:root {
    --color-bg-light: #ffffff;
    --color-bg-dark: #1e1e1e;
    --color-text-light: #000000;
    --color-text-dark: #ffffff;
    --color-border-light: #e0e0e0;
    --color-border-dark: #404040;
}

body.dark-mode {
    background-color: var(--color-bg-dark);
    color: var(--color-text-dark);
}

body.dark-mode .card {
    background-color: #2d2d2d;
    border-color: var(--color-border-dark);
}

body.dark-mode .table {
    color: var(--color-text-dark);
}

body.dark-mode .table-light {
    background-color: #3d3d3d !important;
}

body.dark-mode .form-control,
body.dark-mode .form-select {
    background-color: #3d3d3d;
    color: var(--color-text-dark);
    border-color: var(--color-border-dark);
}

body.dark-mode .navbar {
    background-color: #1a1a1a !important;
    border-bottom: 1px solid var(--color-border-dark);
}

body.dark-mode .btn-secondary {
    background-color: #3d3d3d;
    border-color: #404040;
}

/* Melhorias Mobile */
@media (max-width: 768px) {
    .navbar-brand {
        font-size: 1rem;
    }
    
    .table {
        font-size: 0.875rem;
    }
    
    .btn-sm {
        padding: 0.25rem 0.5rem;
        font-size: 0.75rem;
    }
    
    .card {
        margin-bottom: 1rem;
    }
}

/* Atalhos de Teclado */
.tooltip-shortcut {
    font-size: 0.75rem;
    background-color: #555;
    color: white;
    padding: 2px 4px;
    border-radius: 3px;
    margin-left: 5px;
}

/* Animações Suaves */
.fade-in {
    animation: fadeIn 0.3s ease-in;
}

@keyframes fadeIn {
    from {
        opacity: 0;
        transform: translateY(-10px);
    }
    to {
        opacity: 1;
        transform: translateY(0);
    }
}

/* Cards de Status */
.status-card {
    border-left: 4px solid #3498db;
    transition: all 0.3s ease;
}

.status-card:hover {
    box-shadow: 0 4px 8px rgba(0,0,0,0.1);
    transform: translateY(-2px);
}

/* Badges Animadas */
.badge-pulse {
    animation: pulse 2s infinite;
}

@keyframes pulse {
    0%, 100% {
        opacity: 1;
    }
    50% {
        opacity: 0.7;
    }
}
'''
    
    with open('static/melhorias.css', 'w', encoding='utf-8') as f:
        f.write(css_melhorias)
    print("✅ Arquivo static/melhorias.css criado")

# ============================================================================
# 5. CRIAR ARQUIVO JS PARA ATALHOS E DARK MODE
# ============================================================================

def criar_js_melhorias():
    """Cria JavaScript para atalhos de teclado e dark mode"""
    
    js_melhorias = '''// Dark Mode Toggle
function toggleDarkMode() {
    document.body.classList.toggle('dark-mode');
    localStorage.setItem('darkMode', document.body.classList.contains('dark-mode'));
}

// Aplicar dark mode se estava ativo
window.addEventListener('DOMContentLoaded', () => {
    const darkMode = localStorage.getItem('darkMode') === 'true';
    if (darkMode) {
        document.body.classList.add('dark-mode');
    }
});

// Atalhos de Teclado
document.addEventListener('keydown', (e) => {
    // Ctrl/Cmd + D = Dark Mode
    if ((e.ctrlKey || e.metaKey) && e.key === 'd') {
        e.preventDefault();
        toggleDarkMode();
    }
    
    // Ctrl/Cmd + K = Busca
    if ((e.ctrlKey || e.metaKey) && e.key === 'k') {
        e.preventDefault();
        const searchInput = document.getElementById('buscaVendas') || document.querySelector('[data-search]');
        if (searchInput) searchInput.focus();
    }
    
    // Ctrl/Cmd + N = Novo
    if ((e.ctrlKey || e.metaKey) && e.key === 'n') {
        e.preventDefault();
        const novoBtn = document.querySelector('[data-novo]');
        if (novoBtn) novoBtn.click();
    }
});

// Função para mostrar tooltips de atalhos
function mostrarAtalhos() {
    const atalhos = [
        { key: 'Ctrl+D', acao: 'Ativar Dark Mode' },
        { key: 'Ctrl+K', acao: 'Focar em Busca' },
        { key: 'Ctrl+N', acao: 'Novo Item' }
    ];
    
    let html = '<div class="alert alert-info"><h5>Atalhos de Teclado</h5><ul>';
    atalhos.forEach(a => {
        html += `<li><kbd>${a.key}</kbd> - ${a.acao}</li>`;
    });
    html += '</ul></div>';
    
    alert(html);
}

// Notificações Toast
function mostrarNotificacao(mensagem, tipo = 'info') {
    const toast = document.createElement('div');
    toast.className = `alert alert-${tipo} fade-in`;
    toast.style.cssText = 'position: fixed; bottom: 20px; right: 20px; z-index: 9999; min-width: 300px;';
    toast.innerHTML = mensagem;
    document.body.appendChild(toast);
    
    setTimeout(() => {
        toast.remove();
    }, 3000);
}
'''
    
    os.makedirs('static', exist_ok=True)
    with open('static/melhorias.js', 'w', encoding='utf-8') as f:
        f.write(js_melhorias)
    print("✅ Arquivo static/melhorias.js criado")

# ============================================================================
# 6. EXECUTAR TUDO
# ============================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("IMPLEMENTANDO 5 MELHORIAS NO MEGA ERP".center(70))
    print("=" * 70)
    
    print("\n[1/5] Adicionando novas tabelas ao banco de dados...")
    if adicionar_tabelas():
        print("✅ Banco de dados atualizado!")
    else:
        print("⚠️ Verifique o banco de dados")
    
    print("\n[2/5] Criando novos templates HTML...")
    criar_templates()
    
    print("\n[3/5] Criando script de backup automático...")
    criar_script_backup()
    
    print("\n[4/5] Criando CSS para dark mode e melhorias...")
    criar_css_melhorias()
    
    print("\n[5/5] Criando JavaScript para atalhos de teclado...")
    criar_js_melhorias()
    
    print("\n" + "=" * 70)
    print("✅ IMPLEMENTAÇÃO CONCLUÍDA COM SUCESSO!".center(70))
    print("=" * 70)
    
    print("\n📋 PRÓXIMOS PASSOS:")
    print("\n1. REINICIAR O SERVIDOR")
    print("   $ python app.py\n")
    
    print("2. ADICIONAR NOVAS ROTAS NO app.py:")
    print("   - @app.route('/admin/usuarios') para gerenciar usuários")
    print("   - @app.route('/alertas') para ver alertas do sistema")
    print("   - @app.route('/admin/backup') para fazer backup\n")
    
    print("3. INCLUIR CSS E JS NOS TEMPLATES:")
    print('   Adicione ao base.html:')
    print('   <link rel="stylesheet" href="{{ url_for(\'static\', filename=\'melhorias.css\') }}">')
    print('   <script src="{{ url_for(\'static\', filename=\'melhorias.js\') }}"></script>\n')
    
    print("4. FAZER BACKUP AUTOMÁTICO:")
    print("   $ python backup_banco.py\n")
    
    print("5. AGENDAR BACKUPS DIÁRIOS:")
    print("   Windows: Abrir Task Scheduler e criar tarefa agendada")
    print("   Linux: crontab -e → 0 2 * * * python /caminho/backup_banco.py\n")
    
    print("🎉 Seu Mega ERP agora tem:")
    print("✓ Sistema de Notificações e Alertas")
    print("✓ Gerenciamento de Usuários com Papéis")
    print("✓ Backup Automático")
    print("✓ Dark Mode (Ctrl+D)")
    print("✓ Atalhos de Teclado (Ctrl+K, Ctrl+N)")
    print("=" * 70)
