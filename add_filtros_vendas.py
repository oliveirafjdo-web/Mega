with open('templates/vendas.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Adicionar botões rápidos após o form mas antes dos chips
old_block = '''            <form class="row g-3 align-items-end" method="GET">
                <div class="col-md-3">
                    <label class="form-label">Data início</label>
                    <input type="date" name="data_inicio" class="form-control" value="{{ data_inicio }}">
                </div>
                <div class="col-md-3">
                    <label class="form-label">Data fim</label>
                    <input type="date" name="data_fim" class="form-control" value="{{ data_fim }}">
                </div>'''

new_block = '''            <form class="row g-3 align-items-end" method="GET" id="formPeriodo">
                <!-- Botões rápidos de período -->
                <div class="col-12">
                    <div class="d-flex flex-wrap gap-2">
                        <button type="button" class="btn btn-sm btn-outline-primary" onclick="setQuickPeriod(0)">
                            <i class="bi bi-calendar-day me-1"></i> Hoje
                        </button>
                        <button type="button" class="btn btn-sm btn-outline-primary" onclick="setQuickPeriod(7)">
                            <i class="bi bi-calendar-week me-1"></i> 7 dias
                        </button>
                        <button type="button" class="btn btn-sm btn-outline-primary" onclick="setQuickPeriod(30)">
                            <i class="bi bi-calendar-month me-1"></i> 30 dias
                        </button>
                        <button type="button" class="btn btn-sm btn-outline-primary" onclick="setCurrentMonth()">
                            <i class="bi bi-calendar-check me-1"></i> Este mês
                        </button>
                        <button type="button" class="btn btn-sm btn-outline-primary" onclick="setLastMonth()">
                            <i class="bi bi-calendar me-1"></i> Mês anterior
                        </button>
                    </div>
                </div>
                <div class="col-md-3">
                    <label class="form-label">Data início</label>
                    <input type="date" name="data_inicio" id="data_inicio" class="form-control" value="{{ data_inicio }}">
                </div>
                <div class="col-md-3">
                    <label class="form-label">Data fim</label>
                    <input type="date" name="data_fim" id="data_fim" class="form-control" value="{{ data_fim }}">
                </div>'''

content = content.replace(old_block, new_block)

# Adicionar funções JavaScript antes do fechamento do script
old_script_end = '''document.addEventListener("DOMContentLoaded", () => {
    // Marcar defaults
    const origemDefault = document.querySelector('[data-origem=""]');
    if (origemDefault) origemDefault.classList.add("active");
    const statusDefault = document.querySelector('[data-status=""]');
    if (statusDefault) statusDefault.classList.add("active");
});
</script>'''

new_script_end = '''document.addEventListener("DOMContentLoaded", () => {
    // Marcar defaults
    const origemDefault = document.querySelector('[data-origem=""]');
    if (origemDefault) origemDefault.classList.add("active");
    const statusDefault = document.querySelector('[data-status=""]');
    if (statusDefault) statusDefault.classList.add("active");
});

// Funções de período rápido
function setQuickPeriod(days) {
    const hoje = new Date();
    const dataFim = hoje.toISOString().split('T')[0];
    
    let dataInicio;
    if (days === 0) {
        dataInicio = dataFim;
    } else {
        const inicio = new Date();
        inicio.setDate(inicio.getDate() - days);
        dataInicio = inicio.toISOString().split('T')[0];
    }
    
    document.getElementById('data_inicio').value = dataInicio;
    document.getElementById('data_fim').value = dataFim;
    document.getElementById('formPeriodo').submit();
}

function setCurrentMonth() {
    const hoje = new Date();
    const ano = hoje.getFullYear();
    const mes = String(hoje.getMonth() + 1).padStart(2, '0');
    
    const dataInicio = `${ano}-${mes}-01`;
    const dataFim = hoje.toISOString().split('T')[0];
    
    document.getElementById('data_inicio').value = dataInicio;
    document.getElementById('data_fim').value = dataFim;
    document.getElementById('formPeriodo').submit();
}

function setLastMonth() {
    const hoje = new Date();
    const primeiroDiaEstemês = new Date(hoje.getFullYear(), hoje.getMonth(), 1);
    const ultimoDiaMesAnterior = new Date(primeiroDiaEstemês.getTime() - 1);
    const primeiroDiaMesAnterior = new Date(ultimoDiaMesAnterior.getFullYear(), ultimoDiaMesAnterior.getMonth(), 1);
    
    const dataInicio = primeiroDiaMesAnterior.toISOString().split('T')[0];
    const dataFim = ultimoDiaMesAnterior.toISOString().split('T')[0];
    
    document.getElementById('data_inicio').value = dataInicio;
    document.getElementById('data_fim').value = dataFim;
    document.getElementById('formPeriodo').submit();
}
</script>'''

content = content.replace(old_script_end, new_script_end)

with open('templates/vendas.html', 'w', encoding='utf-8') as f:
    f.write(content)

print('✅ Filtros rápidos adicionados à página de vendas!')
