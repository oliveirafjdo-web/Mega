// Dark Mode Toggle
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
