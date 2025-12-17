from flask import render_template, request, redirect, url_for
from datetime import datetime, date
from sqlalchemy import select, func
from app import app, engine
from app import vendas, produtos


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

    faturamento_mes_atual = {}
    faturamento_mes_anterior = {}

    for v in vendas_rows:
        if not v["data_venda"]:
            continue

        dt = datetime.fromisoformat(v["data_venda"]).date()
        receita = float(v["receita_total"] or 0)

        if dt >= inicio_mes_atual:
            faturamento_mes_atual[dt] = faturamento_mes_atual.get(dt, 0) + receita

        if inicio_mes_anterior <= dt < inicio_mes_atual:
            faturamento_mes_anterior[dt] = faturamento_mes_anterior.get(dt, 0) + receita

    datas_atual = sorted(faturamento_mes_atual.keys())

    grafico_cmp_labels = [d.isoformat() for d in datas_atual]
    grafico_cmp_atual = [faturamento_mes_atual[d] for d in datas_atual]

    # alinhar dias do mês anterior pelos dias do mês atual
    grafico_cmp_anterior = [
        faturamento_mes_anterior.get(
            inicio_mes_anterior.replace(day=d.day), 0
        )
        for d in datas_atual
    ]

    # ======================================================
    # 5. TOTAIS GERAIS (para os cards de topo)
    # ======================================================
    totais = {
        "qtd": sum(v["quantidade"] for v in vendas_rows),
        "receita": sum(v["receita_total"] for v in vendas_rows),
        "custo": sum(v["custo_total"] for v in vendas_rows),
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
