from flask import render_template, request, redirect, url_for
from datetime import datetime, date
import calendar
from sqlalchemy import select, func
from app import app, engine
from app import vendas, produtos, configuracoes


@app.route("/vendas")
def lista_vendas():

    data_inicio = request.args.get("data_inicio") or ""
    data_fim = request.args.get("data_fim") or ""

    with engine.connect() as conn:

        # ======================================================
        # 1. CONSULTA PRINCIPAL DE VENDAS
        # ======================================================
        query_vendas = (
            select(
                vendas.c.id,
                vendas.c.data_venda,
                vendas.c.quantidade,
                vendas.c.preco_venda_unitario,
                vendas.c.receita_total,
                vendas.c.custo_total,
                vendas.c.margem_contribuicao,
                vendas.c.origem,
                vendas.c.numero_venda_ml,
                vendas.c.lote_importacao,
                produtos.c.nome,
            )
            .select_from(vendas.join(produtos))
        )

        if data_inicio:
            query_vendas = query_vendas.where(vendas.c.data_venda >= data_inicio)

        if data_fim:
            query_vendas = query_vendas.where(
                vendas.c.data_venda <= data_fim + "T23:59:59"
            )

        query_vendas = query_vendas.order_by(vendas.c.data_venda.asc())
        vendas_rows = conn.execute(query_vendas).mappings().all()

        # ======================================================
        # 2. CONSULTA DE LOTES (para tabela de importações)
        # ======================================================
        query_lotes = (
            select(
                vendas.c.lote_importacao.label("lote_importacao"),
                func.count().label("qtd_vendas"),
                func.coalesce(func.sum(vendas.c.receita_total), 0).label("receita_lote"),
            )
            .where(vendas.c.lote_importacao.isnot(None))
        )

        if data_inicio:
            query_lotes = query_lotes.where(vendas.c.data_venda >= data_inicio)

        if data_fim:
            query_lotes = query_lotes.where(
                vendas.c.data_venda <= data_fim + "T23:59:59"
            )

        query_lotes = query_lotes.group_by(vendas.c.lote_importacao)
        lotes = conn.execute(query_lotes).mappings().all()

    # ======================================================
    # 3. GERAÇÃO DOS GRÁFICOS (por dia)
    # ======================================================

    faturamento_dia = {}
    quantidade_dia = {}
    lucro_dia = {}

    for v in vendas_rows:
        if not v["data_venda"]:
            continue

        try:
            dt = datetime.fromisoformat(v["data_venda"]).date()
        except Exception:
            continue

        receita = float(v["receita_total"] or 0)
        
        # PULAR vendas canceladas (receita <= 0) nos gráficos e totais
        if receita <= 0:
            continue
            
        custo = float(v["custo_total"] or 0)
        margem = float(v["margem_contribuicao"] or 0)
        qtd = float(v["quantidade"] or 0)

        # LUCRO = usando a margem de contribuição já calculada no import
        lucro = float(margem)

        # FATURAMENTO diário
        faturamento_dia[dt] = faturamento_dia.get(dt, 0) + receita

        # QUANTIDADE diária
        quantidade_dia[dt] = quantidade_dia.get(dt, 0) + qtd

        # LUCRO diário
        lucro_dia[dt] = lucro_dia.get(dt, 0) + lucro

    datas_ordenadas = sorted(faturamento_dia.keys())

    grafico_labels = [d.isoformat() for d in datas_ordenadas]
    grafico_faturamento = [faturamento_dia[d] for d in datas_ordenadas]
    grafico_quantidade = [quantidade_dia.get(d, 0) for d in datas_ordenadas]
    grafico_lucro = [lucro_dia.get(d, 0) for d in datas_ordenadas]

    # ======================================================
    # 4. COMPARATIVO MÊS ATUAL VS MÊS ANTERIOR
    # ======================================================
    hoje = date.today()
    inicio_mes_atual = hoje.replace(day=1)

    if inicio_mes_atual.month == 1:
        inicio_mes_anterior = inicio_mes_atual.replace(
            year=inicio_mes_atual.year - 1, month=12
        )
    else:
        inicio_mes_anterior = inicio_mes_atual.replace(
            month=inicio_mes_atual.month - 1
        )

    # Recarrega dados sem filtro para comparar mês atual x anterior (exclui canceladas)
    with engine.connect() as conn_cmp:
        vendas_cmp_rows = conn_cmp.execute(
            select(vendas.c.data_venda, vendas.c.receita_total)
            .where(vendas.c.data_venda >= inicio_mes_anterior.isoformat())
            .where(vendas.c.data_venda <= hoje.isoformat() + "T23:59:59")
        ).mappings().all()

    # Limites de dias por mês
    dias_mes_atual = hoje.day  # até hoje
    dias_mes_anterior = calendar.monthrange(inicio_mes_anterior.year, inicio_mes_anterior.month)[1]
    max_dias = max(dias_mes_atual, dias_mes_anterior)

    # Inicializa listas com zeros para manter alinhamento
    faturamento_mes_atual = {d: 0 for d in range(1, max_dias + 1)}
    faturamento_mes_anterior = {d: 0 for d in range(1, max_dias + 1)}

    for v in vendas_cmp_rows:
        if not v["data_venda"]:
            continue

        dt = datetime.fromisoformat(v["data_venda"]).date()
        receita = float(v["receita_total"] or 0)
        
        # PULAR vendas canceladas
        if receita <= 0:
            continue

        dia = dt.day

        # Mês atual
        if dt.month == inicio_mes_atual.month and dt.year == inicio_mes_atual.year and dia <= dias_mes_atual:
            faturamento_mes_atual[dia] += receita

        # Mês anterior
        if dt.month == inicio_mes_anterior.month and dt.year == inicio_mes_anterior.year and dia <= dias_mes_anterior:
            faturamento_mes_anterior[dia] += receita

    # Labels dia a dia (01, 02, ...)
    grafico_cmp_labels = [f"{d:02d}" for d in range(1, max_dias + 1)]

    grafico_cmp_atual = [faturamento_mes_atual.get(d, 0) if d <= dias_mes_atual else 0 for d in range(1, max_dias + 1)]
    grafico_cmp_anterior = [faturamento_mes_anterior.get(d, 0) if d <= dias_mes_anterior else 0 for d in range(1, max_dias + 1)]

    # ======================================================
    # 5. TOTAIS GERAIS (para os cards de topo) - EXCLUINDO CANCELADAS
    # ======================================================
    vendas_validas = [v for v in vendas_rows if v["receita_total"] > 0]
    
    total_receita = sum(v["receita_total"] for v in vendas_validas)
    total_custo = sum(v["custo_total"] for v in vendas_validas)
    total_margem = sum(v["margem_contribuicao"] for v in vendas_validas)
    
    # Busca configurações para calcular lucro líquido
    with engine.connect() as conn2:
        cfg = conn2.execute(
            select(configuracoes).where(configuracoes.c.id == 1)
        ).mappings().first()
    
    imposto_percent = float(cfg["imposto_percent"]) if cfg else 0.0
    despesas_percent = float(cfg["despesas_percent"]) if cfg else 0.0
    
    # Comissão ML estimada
    comissao_total = max(0.0, (total_receita - total_custo) - total_margem)
    imposto_total = total_receita * (imposto_percent / 100.0)
    despesas_total = total_receita * (despesas_percent / 100.0)
    
    lucro_liquido = total_receita - total_custo - comissao_total - imposto_total - despesas_total
    
    totais = {
        "qtd": sum(v["quantidade"] for v in vendas_validas),
        "receita": total_receita,
        "custo": total_custo,
        "lucro_liquido": lucro_liquido,
    }

    # ======================================================
    # 6. RENDERIZA TEMPLATE FINAL
    # ======================================================
    return render_template(
        "vendas.html",
        vendas=vendas_rows,
        lotes=lotes,
        data_inicio=data_inicio,
        data_fim=data_fim,
        totais=totais,
        grafico_labels=grafico_labels,
        grafico_faturamento=grafico_faturamento,
        grafico_quantidade=grafico_quantidade,
        grafico_lucro=grafico_lucro,
        grafico_cmp_labels=grafico_cmp_labels,
        grafico_cmp_atual=grafico_cmp_atual,
        grafico_cmp_anterior=grafico_cmp_anterior,
    )


@app.route("/excluir_lote/<lote>", methods=["POST"])
def excluir_lote(lote):
    """
    Exclui todas as vendas de um determinado lote_importacao
    e volta para a tela de vendas.
    """
    with engine.begin() as conn:
        conn.execute(
            vendas.delete().where(vendas.c.lote_importacao == lote)
        )

    return redirect(url_for("lista_vendas"))
