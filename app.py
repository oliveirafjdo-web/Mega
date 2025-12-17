import os
from datetime import datetime, date, timedelta
from io import BytesIO
import requests
import base64
import re
import time

from flask import Flask, render_template, request, redirect, url_for, flash, send_file, jsonify
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from flask_bcrypt import Bcrypt
from werkzeug.utils import secure_filename

from sqlalchemy import (
    create_engine, MetaData, Table, Column, Integer, String, Float,
    ForeignKey, func, select, insert, update, delete, inspect, text
)
from sqlalchemy.engine import Engine
import pandas as pd

try:
    from pypdf import PdfReader, PdfWriter
    print("DEBUG: pypdf importado com sucesso!")
except ImportError:
    try:
        from PyPDF2 import PdfReader, PdfWriter
        print("DEBUG: PyPDF2 importado com sucesso!")
    except ImportError:
        PdfReader = None
        PdfWriter = None
        print("DEBUG: NENHUMA biblioteca PDF encontrada!")

# --------------------------------------------------------------------
# Configuração de banco: Postgres em produção, SQLite em desenvolvimento
# --------------------------------------------------------------------
# Detecta Postgres (Render) ou cai para SQLite local
raw_db_url = os.environ.get("DATABASE_URL")

if raw_db_url:
    # Render costuma entregar "postgres://", mas o SQLAlchemy quer "postgresql+psycopg2://"
    if raw_db_url.startswith("postgres://"):
        raw_db_url = raw_db_url.replace("postgres://", "postgresql+psycopg2://", 1)
    DATABASE_URL = raw_db_url
else:
    DATABASE_URL = "sqlite:///metrifiy.db"
UPLOAD_FOLDER = os.environ.get("UPLOAD_FOLDER", "uploads")

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
app.secret_key = os.environ.get("SECRET_KEY", "metrifypremium-secret")

os.makedirs(UPLOAD_FOLDER, exist_ok=True)

engine: Engine = create_engine(DATABASE_URL, future=True)
metadata = MetaData()

# Inicializa Flask-Login e Bcrypt
bcrypt = Bcrypt(app)
login_manager = LoginManager(app)
login_manager.login_view = "login_view"

# Tabela de usuários
usuarios = Table(
    "usuarios",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("username", String(80), unique=True, nullable=False),
    Column("password_hash", String(128), nullable=False),
)

# Definição das outras tabelas (produtos, vendas, etc.)
# ...existing code...

# Criar todas as tabelas no banco de dados
metadata.create_all(engine)

# Importar dados automaticamente se banco estiver vazio (apenas em produção)
if raw_db_url:  # Só em produção (PostgreSQL)
    try:
        from auto_import import auto_import_data_if_empty
        auto_import_data_if_empty(engine)
    except Exception as e:
        print(f"⚠️ Aviso: Não foi possível importar dados automaticamente: {e}")

# Classe User para Flask-Login
class User(UserMixin):
    def __init__(self, id, username, password_hash):
        self.id = id
        self.username = username
        self.password_hash = password_hash

    @staticmethod
    def get_by_username(username):
        with engine.connect() as conn:
            row = conn.execute(select(usuarios).where(usuarios.c.username == username)).mappings().first()
            if row:
                return User(row["id"], row["username"], row["password_hash"])
        return None

    @staticmethod
    def get(user_id):
        with engine.connect() as conn:
            row = conn.execute(select(usuarios).where(usuarios.c.id == user_id)).mappings().first()
            if row:
                return User(row["id"], row["username"], row["password_hash"])
        return None

@login_manager.user_loader
def load_user(user_id):
    return User.get(user_id)
def criar_usuario_inicial():
    with engine.begin() as conn:
        row = conn.execute(select(usuarios).where(usuarios.c.username == "julio")).first()
        if not row:
            senha_hash = bcrypt.generate_password_hash("12345").decode("utf-8")
            conn.execute(usuarios.insert().values(username="julio", password_hash=senha_hash))

criar_usuario_inicial()
# Rota de login
@app.route("/login", methods=["GET", "POST"])
def login_view():
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]
        user = User.get_by_username(username)
        if user and bcrypt.check_password_hash(user.password_hash, password):
            login_user(user)
            flash("Login realizado com sucesso!", "success")
            next_page = request.args.get("next")
            return redirect(next_page or url_for("dashboard"))
        else:
            flash("Usuário ou senha inválidos.", "danger")
    return render_template("login.html")

# Rota de logout
@app.route("/logout")
@login_required
def logout_view():
    logout_user()
    flash("Logout realizado!", "success")
    return redirect(url_for("login_view"))

# --------------------------------------------------------------------
# Definição das tabelas
# --------------------------------------------------------------------
# Rota para excluir lote de vendas
@app.route("/excluir_lote_venda/<path:lote>", methods=["POST"])
@login_required
def excluir_lote_venda(lote):
    print(f"[DEBUG] Excluindo lote de vendas: {lote}")
    with engine.begin() as conn:
        # Contar antes
        count_before = conn.execute(
            select(func.count()).select_from(vendas).where(vendas.c.lote_importacao == lote)
        ).scalar()
        print(f"[DEBUG] Vendas antes da exclusão: {count_before}")
        deleted = conn.execute(
            delete(vendas)
            .where(vendas.c.lote_importacao == lote)
        )
        # Contar depois
        count_after = conn.execute(
            select(func.count()).select_from(vendas).where(vendas.c.lote_importacao == lote)
        ).scalar()
        print(f"[DEBUG] Vendas após exclusão: {count_after}")
        print(f"[DEBUG] deleted.rowcount: {deleted.rowcount}")
    flash(f"Lote de vendas {lote} excluído ({deleted.rowcount} vendas).", "success")
    return redirect(url_for("lista_vendas"))
# --------------------------------------------------------------------
produtos = Table(
    "produtos",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("nome", String(255), nullable=False),
    Column("sku", String(100), unique=True),
    Column("custo_unitario", Float, nullable=False, server_default="0"),
    Column("preco_venda_sugerido", Float, nullable=False, server_default="0"),
    Column("estoque_inicial", Integer, nullable=False, server_default="0"),
    Column("estoque_atual", Integer, nullable=False, server_default="0"),
    Column("curva", String(1)),
)

vendas = Table(
    "vendas",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("produto_id", Integer, ForeignKey("produtos.id"), nullable=False),
    Column("data_venda", String(50)),
    Column("quantidade", Integer, nullable=False),
    Column("preco_venda_unitario", Float, nullable=False),
    Column("receita_total", Float, nullable=False),
    Column("comissao_ml", Float, nullable=False, server_default="0"),
    Column("custo_total", Float, nullable=False),
    Column("margem_contribuicao", Float, nullable=False),
    Column("origem", String(50)),
    Column("numero_venda_ml", String(100)),
    Column("lote_importacao", String(50)),
    Column("estado", String(2)),  # UF do estado
)

ajustes_estoque = Table(
    "ajustes_estoque",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("produto_id", Integer, ForeignKey("produtos.id"), nullable=False),
    Column("data_ajuste", String(50)),
    Column("tipo", String(20)),  # entrada, saida
    Column("quantidade", Integer),
    Column("custo_unitario", Float),
    Column("observacao", String(255)),
)

configuracoes = Table(
    "configuracoes",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("imposto_percent", Float, nullable=False, server_default="0"),
    Column("despesas_percent", Float, nullable=False, server_default="0"),
)

finance_transactions = Table(
    "finance_transactions",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("data_lancamento", String(50), nullable=False),
    Column("tipo", String(50), nullable=False),  # OPENING_BALANCE, MP_NET, REFUND, WITHDRAWAL, ADJUSTMENT
    Column("valor", Float, nullable=False),
    Column("origem", String(50), nullable=False, server_default="manual"),  # mercado_pago | manual
    Column("external_id_mp", String(120), unique=True),  # ID DA TRANSAÇÃO NO MERCADO PAGO
    Column("descricao", String(255)),
    Column("criado_em", String(50)),
    Column("lote_importacao", String(50)),  # lote de importação
)


def init_db():
    """Cria as tabelas se não existirem e garante 1 linha em configuracoes.
    Também aplica pequenos 'migrations' (ALTER TABLE) quando necessário.
    """
    metadata.create_all(engine)

    with engine.begin() as conn:
        # garante 1 linha em configuracoes
        row = conn.execute(select(configuracoes.c.id).limit(1)).first()
        if not row:
            conn.execute(insert(configuracoes).values(id=1, imposto_percent=0.0, despesas_percent=0.0))

        # ---- migrations leves (compatível com SQLite/Postgres) ----
        insp = inspect(engine)

        # vendas.comissao_ml
        try:
            cols = [c["name"] for c in insp.get_columns("vendas")]
            if "comissao_ml" not in cols:
                conn.execute(text('ALTER TABLE vendas ADD COLUMN comissao_ml FLOAT DEFAULT 0'))
        except Exception:
            pass

        # vendas.estado
        try:
            cols = [c["name"] for c in insp.get_columns("vendas")]
            if "estado" not in cols:
                conn.execute(text('ALTER TABLE vendas ADD COLUMN estado TEXT'))
        except Exception:
            pass

        # finance_transactions.lote_importacao
        try:
            cols = [c["name"] for c in insp.get_columns("finance_transactions")]
            if "lote_importacao" not in cols:
                conn.execute(text('ALTER TABLE finance_transactions ADD COLUMN lote_importacao TEXT'))
        except Exception:
            pass


# --------------------------------------------------------------------
# Utilidades para datas
# --------------------------------------------------------------------
MESES_PT = {
    "janeiro": 1, "fevereiro": 2, "março": 3, "marco": 3,
    "abril": 4, "maio": 5, "junho": 6, "julho": 7,
    "agosto": 8, "setembro": 9, "outubro": 10,
    "novembro": 11, "dezembro": 12,
}


def parse_data_venda(texto):
    if isinstance(texto, datetime):
        return texto
    if not isinstance(texto, str) or not texto.strip():
        return None
    try:
        partes = texto.split()
        dia = int(partes[0])
        mes_nome = partes[2].lower()
        ano = int(partes[4])
        hora_min = partes[5]
        hora, minuto = hora_min.split(":")
        return datetime(ano, MESES_PT[mes_nome], int(dia), int(hora), int(minuto))
    except Exception:
        # tenta ISO
        try:
            return datetime.fromisoformat(texto)
        except Exception:
            return None
        except Exception:
            return None


# --------------------------------------------------------------------
# Normalização de UF (estado) - mapeia nomes completos para siglas
# --------------------------------------------------------------------
STATE_TO_SIGLA = {
    'acre': 'AC', 'alagoas': 'AL', 'amapa': 'AP', 'amapá': 'AP', 'amazonas': 'AM',
    'bahia': 'BA', 'ceara': 'CE', 'ceará': 'CE', 'distrito federal': 'DF',
    'espirito santo': 'ES', 'espírito santo': 'ES', 'goias': 'GO', 'goiás': 'GO',
    'maranhao': 'MA', 'maranhão': 'MA', 'mato grosso': 'MT', 'mato grosso do sul': 'MS',
    'minas gerais': 'MG', 'para': 'PA', 'pará': 'PA', 'paraiba': 'PB', 'paraíba': 'PB',
    'parana': 'PR', 'paraná': 'PR', 'pernambuco': 'PE', 'piaui': 'PI', 'piauí': 'PI',
    'rio de janeiro': 'RJ', 'rio grande do norte': 'RN', 'rio grande do sul': 'RS',
    'rondonia': 'RO', 'rondônia': 'RO', 'roraima': 'RR', 'santa catarina': 'SC',
    'sao paulo': 'SP', 'são paulo': 'SP', 'sergipe': 'SE', 'tocantins': 'TO'
}

def normalize_uf(value):
    """Converte nomes completos de estados para siglas. Mantém siglas válidas.

    Retorna None para valores vazios/None.
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    # já é sigla?
    if len(s) == 2 and s.upper() in STATE_TO_SIGLA.values():
        return s.upper()
    key = s.lower()
    # tentativa direta
    if key in STATE_TO_SIGLA:
        return STATE_TO_SIGLA[key]
    # remover acentos simples
    replacements = {'á':'a','à':'a','ã':'a','â':'a','é':'e','ê':'e','í':'i','ó':'o','ô':'o','õ':'o','ú':'u','ç':'c','Á':'A','Ã':'A','Â':'A'}
    key2 = ''.join(replacements.get(ch, ch) for ch in key)
    if key2 in STATE_TO_SIGLA:
        return STATE_TO_SIGLA[key2]
    # tentar última palavra
    parts = key2.split()
    for i in range(len(parts)):
        candidate = ' '.join(parts[i:])
        if candidate in STATE_TO_SIGLA:
            return STATE_TO_SIGLA[candidate]
    # não reconhecido
    return s.upper()


def normalize_df_uf(df):
    """Procura coluna de UF no DataFrame e normaliza seus valores in-place.

    Retorna lista de tuplas (original, convertido) para valores não reconhecidos.
    """
    candidates = ["UF", "Estado", "Estado do comprador", "Estado do Cliente", "estado", "uf"]
    cols_lower = {c.lower(): c for c in df.columns}
    uf_col = None
    for cand in candidates:
        if cand.lower() in cols_lower:
            uf_col = cols_lower[cand.lower()]
            break
    if not uf_col:
        # tenta qualquer coluna que contenha 'estado' ou seja 'uf'
        for c in df.columns:
            if 'estado' in c.lower() or c.lower() == 'uf':
                uf_col = c
                break
    not_recognized = []
    if uf_col:
        for idx, val in df[uf_col].fillna('').items():
            conv = normalize_uf(val)
            df.at[idx, uf_col] = conv
            if conv is None or (not isinstance(conv, str)) or len(conv) != 2:
                not_recognized.append((val, conv))
    return uf_col, not_recognized


# --------------------------------------------------------------------
# Importação de vendas do Mercado Livre
# --------------------------------------------------------------------
def importar_vendas_ml(caminho_arquivo, engine: Engine):
    lote_id = datetime.now().isoformat(timespec="seconds")

    # OTIMIZAÇÃO: Ler apenas primeiras 1000 linhas para limitar memória
    # Renderização Free (512MB) não suporta arquivos muito grandes
    MAX_ROWS = 1000
    
    df = pd.read_excel(
        caminho_arquivo,
        sheet_name="Vendas BR",
        header=5,
        nrows=MAX_ROWS  # Limitar linhas lidas
    )
    
    print(f"📊 DEBUG - Linhas totais lidas: {len(df)}")
    print(f"📊 DEBUG - Colunas encontradas ({len(df.columns)}): {list(df.columns)[:10]}")  # Primeiras 10 colunas
    
    if "N.º de venda" not in df.columns:
        # Tentar variações do nome da coluna
        possible_cols = [col for col in df.columns if 'venda' in str(col).lower() or 'pedido' in str(col).lower()]
        print(f"⚠️ Coluna 'N.º de venda' não encontrada. Colunas similares: {possible_cols}")
        raise ValueError(f"Planilha não está no formato esperado. Colunas disponíveis: {list(df.columns)[:20]}")

    print(f"📊 DEBUG - Linhas antes do filtro: {len(df)}")
    df = df[df["N.º de venda"].notna()]
    print(f"📊 DEBUG - Linhas após filtrar N.º de venda: {len(df)}")
    
    # normaliza coluna UF se existir (sem salvar relatório para economizar I/O)
    uf_col, not_rec = normalize_df_uf(df)
    if uf_col and not_rec:
        print(f"⚠️ {len(not_rec)} UFs não reconhecidos (ignorados para economizar memória)")

    vendas_importadas = 0
    vendas_sem_sku = 0
    vendas_sem_produto = 0
    
    # OTIMIZAÇÃO: Processar em lotes menores de 50 linhas
    BATCH_SIZE = 50
    total_rows = len(df)
    
    for batch_start in range(0, total_rows, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, total_rows)
        df_batch = df.iloc[batch_start:batch_end]
        
        with engine.begin() as conn:
            for _, row in df_batch.iterrows():
                sku = str(row.get("SKU") or "").strip()
                titulo = str(row.get("Título do anúncio") or "").strip()

                produto_row = None

                if sku:
                    produto_row = conn.execute(
                        select(produtos.c.id, produtos.c.custo_unitario)
                        .where(produtos.c.sku == sku)
                    ).mappings().first()
                else:
                    # tenta pelo nome do produto = título do anúncio
                    if titulo:
                        produto_row = conn.execute(
                            select(produtos.c.id, produtos.c.custo_unitario)
                            .where(produtos.c.nome == titulo)
                        ).mappings().first()

            if not sku and not produto_row:
                vendas_sem_sku += 1
                    print(f"⚠️ Venda sem SKU/produto: {titulo[:50] if titulo else 'sem título'}")
                    continue

                if not produto_row:
                    vendas_sem_produto += 1
                    print(f"⚠️ Produto não cadastrado - SKU: {sku}")
            produto_id = produto_row["id"]
            custo_unitario = float(produto_row["custo_unitario"] or 0.0)

            data_venda_raw = row.get("Data da venda")
            data_venda = parse_data_venda(data_venda_raw)
            unidades = row.get("Unidades")
            try:
                unidades = int(unidades) if unidades == unidades else 0
            except Exception:
                unidades = 0

            # Coluna H: Receita por produtos (BRL) - valor bruto da venda
            receita_bruta = row.get("Receita por produtos (BRL)")
            try:
                receita_total = float(receita_bruta) if receita_bruta == receita_bruta else 0.0
            except Exception:
                receita_total = 0.0

            preco_medio_venda = receita_total / unidades if unidades > 0 else 0.0
            custo_total = custo_unitario * unidades

            # Comissão Mercado Livre a partir da coluna 'Tarifa de venda e impostos (BRL)'
            tarifa = row.get("Tarifa de venda e impostos (BRL)")
            try:
                comissao_ml = float(tarifa) if tarifa == tarifa else 0.0
            except Exception:
                comissao_ml = 0.0
            if comissao_ml < 0:
                comissao_ml = -comissao_ml

            margem_contribuicao = receita_total - custo_total - comissao_ml
            numero_venda_ml = str(row.get("N.º de venda"))
            estado = None
            # Coluna AL (Estado.1) tem prioridade
            for col in ["Estado.1", "UF", "Estado", "Estado do comprador", "Estado do Cliente"]:
                if col in df.columns and row.get(col):
                    estado_raw = row.get(col)
                    sigla = normalize_uf(estado_raw)
                    if sigla and isinstance(sigla, str) and len(sigla) == 2:
                        estado = sigla
                    else:
                        # fallback: tentar extrair apenas letras e pegar primeiras 2
                        import re
                        letters = re.sub(r'[^A-Za-z]', '', str(estado_raw))
                        if len(letters) >= 2:
                            estado = letters[:2].upper()
                        else:
                            estado = None
                    break

                conn.execute(
                    insert(vendas).values(
                        produto_id=produto_id,
                        data_venda=data_venda.isoformat() if data_venda else None,
                        quantidade=unidades,
                        preco_venda_unitario=preco_medio_venda,
                        receita_total=receita_total,
                        comissao_ml=comissao_ml,
                        custo_total=custo_total,
                        margem_contribuicao=margem_contribuicao,
                        origem="Mercado Livre",
                        numero_venda_ml=numero_venda_ml,
                        lote_importacao=lote_id,
                        estado=estado,
                    )
                )

                # --- Insere lançamento financeiro no caixa Mercado Pago (valor líquido) ---
                try:
                    external_id = str(numero_venda_ml) if numero_venda_ml else None
                    already = None
                    if external_id:
                        already = conn.execute(
                            select(finance_transactions.c.id)
                            .where(finance_transactions.c.external_id_mp == external_id)
                            .where(finance_transactions.c.tipo == 'MP_NET')
                        ).mappings().first()

                    if not already:
                        valor_liquido = float(receita_total or 0.0) - float(comissao_ml or 0.0)
                        conn.execute(
                            insert(finance_transactions).values(
                                data_lancamento=(data_venda.isoformat() if data_venda else None),
                                tipo="MP_NET",
                                valor=valor_liquido,
                                origem="mercado_pago",
                                external_id_mp=external_id,
                                descricao=f"Venda ML {external_id}",
                                criado_em=datetime.now().isoformat(timespec="seconds"),
                                lote_importacao=lote_id,
                            )
                        )
                except Exception as e:
                    print(f"Erro ao inserir transação financeira para venda {numero_venda_ml}: {e}")

                conn.execute(
                    update(produtos)
                    .where(produtos.c.id == produto_id)
                    .values(estoque_atual=produtos.c.estoque_atual - unidades)
                )

                vendas_importadas += 1
        
        # Liberar memória após cada lote
        import gc
        gc.collect()
        
        if (batch_end % 100) == 0:
            print(f"Processadas {batch_end}/{total_rows} vendas...")

    return {
        "lote_id": lote_id,
        "vendas_importadas": vendas_importadas,
        "vendas_sem_sku": vendas_sem_sku,
        "vendas_sem_produto": vendas_sem_produto,
    }


# --------------------------------------------------------------------
# Importação de produtos via Excel
# --------------------------------------------------------------------
def importar_produtos_excel(caminho_arquivo, engine: Engine):
    # OTIMIZAÇÃO: Limitar linhas para economizar memória
    MAX_ROWS = 500
    df = pd.read_excel(caminho_arquivo, header=0, nrows=MAX_ROWS)

    # normaliza coluna UF se houver (sem salvar relatório)
    try:
        uf_col, not_rec = normalize_df_uf(df)
        if uf_col and not_rec:
            print(f"⚠️ {len(not_rec)} UFs não reconhecidos (ignorados)")
    except Exception:
        pass

    if "SKU" not in df.columns:
        raise ValueError("Planilha deve ter uma coluna 'SKU'.")

    produtos_importados = 0
    produtos_atualizados = 0
    erros = []
    
    # OTIMIZAÇÃO: Processar em lotes pequenos
    BATCH_SIZE = 50
    total_rows = len(df)
    
    for batch_start in range(0, total_rows, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, total_rows)
        df_batch = df.iloc[batch_start:batch_end]
        
        with engine.begin() as conn:
            for _, row in df_batch.iterrows():
                sku = str(row.get("SKU") or "").strip()
                if not sku:
                    erros.append("Linha sem SKU")
                    continue

                nome = str(row.get("Nome") or "").strip() or sku  # default to SKU if no name
                estoque = row.get("Estoque")
                try:
                    estoque = int(estoque) if estoque == estoque else 0
                except Exception:
                    estoque = 0

                custo = row.get("Custo")
                try:
                    custo = float(custo) if custo == custo else 0.0
                except Exception:
                    custo = 0.0

                
                # check if product exists
                produto_row = conn.execute(
                    select(produtos.c.id, produtos.c.estoque_atual)
                    .where(produtos.c.sku == sku)
                ).mappings().first()

                if produto_row:
                    # update
                    conn.execute(
                        update(produtos)
                        .where(produtos.c.id == produto_row["id"])
                        .values(
                            nome=nome,
                            custo_unitario=custo,
                            estoque_atual=estoque,
                        )
                    )
                    produtos_atualizados += 1
                else:
                    # insert
                    conn.execute(
                        insert(produtos).values(
                            nome=nome,
                            sku=sku,
                            custo_unitario=custo,
                            preco_venda_sugerido=custo * 1.5,  # default markup
                            estoque_inicial=estoque,
                            estoque_atual=estoque,
                        )
                    )
                    produtos_importados += 1
        
        # Liberar memória após cada lote
        import gc
        gc.collect()

    return {
        "produtos_importados": produtos_importados,
        "produtos_atualizados": produtos_atualizados,
        "erros": erros,
    }


# --------------------------------------------------------------------
# Rotas principais
# --------------------------------------------------------------------
@app.route("/")
@login_required
def dashboard():
    # --- filtro de período ---
    data_inicio = request.args.get("data_inicio") or ""
    data_fim = request.args.get("data_fim") or ""

    # padrão: mês vigente
    if not data_inicio and not data_fim:
        hoje = date.today()
        inicio_mes = hoje.replace(day=1)
        data_inicio = inicio_mes.isoformat()
        data_fim = hoje.isoformat()

    # cria filtro SQL
    filtro_data = []
    if data_inicio:
        filtro_data.append(vendas.c.data_venda >= data_inicio)
    if data_fim:
        filtro_data.append(vendas.c.data_venda <= data_fim + "T23:59:59")

    with engine.connect() as conn:

        # totais de estoque (não dependem do período)
        total_produtos = conn.execute(
            select(func.count()).select_from(produtos)
        ).scalar_one()

        estoque_total = conn.execute(
            select(func.coalesce(func.sum(produtos.c.estoque_atual), 0))
        ).scalar_one()

        # --- totais filtrados por período ---
        receita_total = conn.execute(
            select(func.coalesce(func.sum(vendas.c.receita_total), 0))
            .where(*filtro_data)
        ).scalar_one()

        custo_total = conn.execute(
            select(func.coalesce(func.sum(vendas.c.custo_total), 0))
            .where(*filtro_data)
        ).scalar_one()

        margem_total = conn.execute(
            select(func.coalesce(func.sum(vendas.c.margem_contribuicao), 0))
            .where(*filtro_data)
        ).scalar_one()

        cfg = conn.execute(
            select(configuracoes).where(configuracoes.c.id == 1)
        ).mappings().first()

        imposto_percent = float(cfg["imposto_percent"]) if cfg else 0.0
        despesas_percent = float(cfg["despesas_percent"]) if cfg else 0.0

        comissao_total = max(0.0, (receita_total - custo_total) - margem_total)
        imposto_total = receita_total * (imposto_percent / 100.0)
        despesas_total = receita_total * (despesas_percent / 100.0)

        lucro_liquido_total = (
            receita_total
            - custo_total
            - comissao_total
            - imposto_total
            - despesas_total
        )

        receita_liquida_total = receita_total - comissao_total 

        margem_liquida_percent = (
            (lucro_liquido_total / receita_total) * 100.0
            if receita_total > 0 else 0.0
        )

        ticket_medio = conn.execute(
            select(func.coalesce(func.avg(vendas.c.preco_venda_unitario), 0))
            .where(*filtro_data)
        ).scalar_one()

        # produto mais vendido no período
        produto_mais_vendido = conn.execute(
            select(produtos.c.nome, func.sum(vendas.c.quantidade).label("qtd"))
            .select_from(vendas.join(produtos))
            .where(*filtro_data)
            .group_by(produtos.c.id)
            .order_by(func.sum(vendas.c.quantidade).desc())
            .limit(1)
        ).first()

        produto_maior_lucro = conn.execute(
            select(produtos.c.nome, func.sum(vendas.c.margem_contribuicao).label("lucro"))
            .select_from(vendas.join(produtos))
            .where(*filtro_data)
            .group_by(produtos.c.id)
            .order_by(func.sum(vendas.c.margem_contribuicao).desc())
            .limit(1)
        ).first()

        produto_pior_margem = conn.execute(
            select(produtos.c.nome, func.sum(vendas.c.margem_contribuicao).label("margem"))
            .select_from(vendas.join(produtos))
            .where(*filtro_data)
            .group_by(produtos.c.id)
            .order_by(func.sum(vendas.c.margem_contribuicao).asc())
            .limit(1)
        ).first()

    return render_template(
        "dashboard.html",
        receita_total=receita_total,
        receita_liquida_total=receita_liquida_total,
        lucro_liquido_total=lucro_liquido_total,
        margem_liquida_percent=margem_liquida_percent,
        custo_total=custo_total,
        comissao_total=comissao_total,
        imposto_total=imposto_total,
        despesas_total=despesas_total,
        ticket_medio=ticket_medio,
        total_produtos=total_produtos,
        estoque_total=estoque_total,
        produto_mais_vendido=produto_mais_vendido,
        produto_maior_lucro=produto_maior_lucro,
        produto_pior_margem=produto_pior_margem,
        cfg=cfg,
        data_inicio=data_inicio,
        data_fim=data_fim,
    )

# ---------------- PRODUTOS ----------------
@app.route("/produtos")
@login_required
def lista_produtos():
    with engine.connect() as conn:
        produtos_rows = conn.execute(select(produtos).order_by(produtos.c.nome)).mappings().all()
    return render_template("produtos.html", produtos=produtos_rows)


@app.route("/produtos/novo", methods=["GET", "POST"])
@login_required
def novo_produto():
    if request.method == "POST":
        nome = request.form["nome"]
        sku = request.form["sku"]
        custo_unitario = float(request.form.get("custo_unitario", 0) or 0)
        preco_venda_sugerido = float(request.form.get("preco_venda_sugerido", 0) or 0)
        estoque_inicial = int(request.form.get("estoque_inicial", 0) or 0)

        with engine.begin() as conn:
            conn.execute(
                insert(produtos).values(
                    nome=nome,
                    sku=sku,
                    custo_unitario=custo_unitario,
                    preco_venda_sugerido=preco_venda_sugerido,
                    estoque_inicial=estoque_inicial,
                    estoque_atual=estoque_inicial,
                )
            )
        flash("Produto cadastrado com sucesso!", "success")
        return redirect(url_for("lista_produtos"))

    return render_template("produto_form.html", produto=None)


@app.route("/produtos/<int:produto_id>/editar", methods=["GET", "POST"])
@login_required
def editar_produto(produto_id):
    if request.method == "POST":
        nome = request.form["nome"]
        sku = request.form["sku"]
        custo_unitario = float(request.form.get("custo_unitario", 0) or 0)
        preco_venda_sugerido = float(request.form.get("preco_venda_sugerido", 0) or 0)
        estoque_atual = int(request.form.get("estoque_atual", 0) or 0)

        with engine.begin() as conn:
            conn.execute(
                update(produtos)
                .where(produtos.c.id == produto_id)
                .values(
                    nome=nome,
                    sku=sku,
                    custo_unitario=custo_unitario,
                    preco_venda_sugerido=preco_venda_sugerido,
                    estoque_atual=estoque_atual,
                )
            )
        flash("Produto atualizado!", "success")
        return redirect(url_for("lista_produtos"))

    with engine.connect() as conn:
        produto_row = conn.execute(
            select(produtos).where(produtos.c.id == produto_id)
        ).mappings().first()

    if not produto_row:
        flash("Produto não encontrado.", "danger")
        return redirect(url_for("lista_produtos"))

    return render_template("produto_form.html", produto=produto_row)


@app.route("/produtos/<int:produto_id>/excluir", methods=["POST"])
@login_required
def excluir_produto(produto_id):
    with engine.begin() as conn:
        conn.execute(delete(produtos).where(produtos.c.id == produto_id))
    flash("Produto excluído.", "success")
    return redirect(url_for("lista_produtos"))


@app.route("/produtos/importar", methods=["GET", "POST"])
@login_required
def importar_produtos_view():
    if request.method == "POST":
        if "arquivo" not in request.files:
            flash("Nenhum arquivo enviado.", "danger")
            return redirect(request.url)
        file = request.files["arquivo"]
        if file.filename == "":
            flash("Selecione um arquivo.", "danger")
            return redirect(request.url)
        filename = secure_filename(file.filename)
        caminho = os.path.join(app.config["UPLOAD_FOLDER"], filename)
        file.save(caminho)

        try:
            resumo = importar_produtos_excel(caminho, engine)
            flash(
                f"Importação concluída. "
                f"{resumo['produtos_importados']} produtos importados, "
                f"{resumo['produtos_atualizados']} atualizados. "
                f"Erros: {len(resumo['erros'])}",
                "success",
            )
        except Exception as e:
            flash(f"Erro na importação: {e}", "danger")
        return redirect(url_for("lista_produtos"))

    return render_template("importar_produtos.html")


# ---------------- VENDAS ----------------
from flask import request, render_template
from sqlalchemy import select, func
from datetime import date, datetime, timedelta
from math import ceil

VENDAS_POR_PAGINA = 100

@app.route("/vendas")
@login_required
def lista_vendas():
    data_inicio = request.args.get("data_inicio") or ""
    data_fim = request.args.get("data_fim") or ""
    page = int(request.args.get("page", 1))

    # percentuais de imposto e despesas do período (mesma lógica do dashboard)
    imposto_percent = 0.0
    despesas_percent = 0.0

    # =======================
    # PERÍODO PADRÃO: ÚLTIMOS 30 DIAS
    # =======================
    hoje = date.today()
    trinta_dias_atras = hoje - timedelta(days=29)
    default_data_inicio = trinta_dias_atras.isoformat()
    default_data_fim = hoje.isoformat()

    if not data_inicio:
        data_inicio = default_data_inicio
    if not data_fim:
        data_fim = default_data_fim

    # defaults para o gráfico pizza (para não quebrar template)
    pizza_estados_labels = []
    pizza_estados_valores = []
    pizza_produtos_labels = []
    pizza_produtos_valores = []

    with engine.connect() as conn:
        # =======================
        # CONSULTA VENDAS (RESPEITA FILTRO DA TELA)
        # =======================
        query_vendas = select(
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
        ).select_from(vendas.join(produtos))

        query_vendas = query_vendas.where(
            vendas.c.data_venda >= data_inicio,
            vendas.c.data_venda <= data_fim + "T23:59:59"
        ).order_by(vendas.c.data_venda.asc())

        vendas_all = conn.execute(query_vendas).mappings().all()

        # Paginação
        total_vendas = len(vendas_all)
        total_pages = ceil(total_vendas / VENDAS_POR_PAGINA) if total_vendas else 1
        start = (page - 1) * VENDAS_POR_PAGINA
        end = start + VENDAS_POR_PAGINA
        vendas_rows = vendas_all[start:end]

        # =======================
        # CONSULTA LOTES (RESPEITA FILTRO)
        # =======================
        query_lotes = select(
            vendas.c.lote_importacao.label("lote_importacao"),
            func.count().label("qtd_vendas"),
            func.coalesce(func.sum(vendas.c.receita_total), 0).label("receita_lote"),
        ).where(
            vendas.c.lote_importacao.isnot(None),
            vendas.c.data_venda >= data_inicio,
            vendas.c.data_venda <= data_fim + "T23:59:59"
        ).group_by(vendas.c.lote_importacao)

        lotes = conn.execute(query_lotes).mappings().all()

        # Produtos (pra venda manual, etc.)
        produtos_rows = conn.execute(
            select(produtos.c.id, produtos.c.nome).order_by(produtos.c.nome)
        ).mappings().all()

        # Busca configuração de impostos/despesas para usar no cálculo do lucro líquido
        cfg = conn.execute(
            select(configuracoes).where(configuracoes.c.id == 1)
        ).mappings().first()
        imposto_percent = float(cfg["imposto_percent"]) if cfg else 0.0
        despesas_percent = float(cfg["despesas_percent"]) if cfg else 0.0

        # =======================
        # GRÁFICO PIZZA POR ESTADO (UF) - RESPEITA FILTRO
        # =======================
        # tenta achar uma coluna de UF/Estado na sua tabela vendas
        col_uf = None
        for candidate in ["uf", "estado", "estado_uf", "uf_cliente", "estado_cliente"]:
            if candidate in vendas.c:
                col_uf = vendas.c[candidate]
                break

        if col_uf is not None:
            query_estados = select(
                func.coalesce(col_uf, "N/I").label("uf"),
                func.coalesce(func.sum(vendas.c.receita_total), 0).label("total_receita"),
                func.count().label("qtd_vendas"),
            ).where(
                vendas.c.data_venda >= data_inicio,
                vendas.c.data_venda <= data_fim + "T23:59:59"
            ).group_by(func.coalesce(col_uf, "N/I")) \
             .order_by(func.coalesce(func.sum(vendas.c.receita_total), 0).desc())

            estados_rows = conn.execute(query_estados).mappings().all()

            # ✅ Pizza por Receita (padrão)
            # Formatar siglas dos estados: maiúsculas e limitado a 2 caracteres
            pizza_estados_labels = []
            pizza_estados_valores = []
            for r in estados_rows:
                uf = str(r["uf"] or "N/I").strip().upper()[:2]
                if uf:
                    pizza_estados_labels.append(uf)
                    pizza_estados_valores.append(float(r["total_receita"] or 0))

            # Se quiser por quantidade, use isto no lugar:
            # pizza_estados_valores = [int(r["qtd_vendas"] or 0) for r in estados_rows]

        # =======================
        # NOVO: PIZZA POR PRODUTO (SKU) - RESPEITA FILTRO
        # =======================
        query_produtos = select(
            produtos.c.sku.label("sku"),
            produtos.c.nome.label("nome"),
            func.coalesce(func.sum(vendas.c.receita_total), 0).label("total_receita"),
            func.count().label("qtd_vendas"),
        ).select_from(vendas.join(produtos)).where(
            vendas.c.data_venda >= data_inicio,
            vendas.c.data_venda <= data_fim + "T23:59:59"
        ).group_by(produtos.c.sku, produtos.c.nome) \
         .order_by(func.coalesce(func.sum(vendas.c.receita_total), 0).desc()) \
         .limit(10)  # top 10 produtos

        produtos_rows_grafico = conn.execute(query_produtos).mappings().all()

        pizza_produtos_labels = []
        pizza_produtos_valores = []
        for r in produtos_rows_grafico:
            sku = str(r["sku"] or "S/SKU").strip().upper()[:20]  # limita a 20 caracteres
            if sku:
                pizza_produtos_labels.append(sku)
                pizza_produtos_valores.append(float(r["total_receita"] or 0))

    # =======================
    # GRÁFICOS 30 DIAS (FATURAMENTO / QTD / LUCRO)
    # =======================
    faturamento_dia = {}
    quantidade_dia = {}
    lucro_dia = {}

    for v in vendas_all:
        if not v["data_venda"]:
            continue
        try:
            dt = datetime.fromisoformat(str(v["data_venda"])).date()
        except Exception:
            continue

        receita = float(v["receita_total"] or 0)
        custo = float(v["custo_total"] or 0)
        margem = float(v["margem_contribuicao"] or 0)
        qtd = float(v["quantidade"] or 0)

        # lucro líquido do dia (mesma lógica do dashboard)
        comissao_ml = max(0.0, (receita - custo) - margem)
        lucro = receita - custo - comissao_ml

        faturamento_dia[dt] = faturamento_dia.get(dt, 0) + receita
        quantidade_dia[dt] = quantidade_dia.get(dt, 0) + qtd
        lucro_dia[dt] = lucro_dia.get(dt, 0) + lucro

    # Últimos 30 dias ordenados
    dias = [hoje - timedelta(days=i) for i in range(29, -1, -1)]
    grafico_labels = [d.isoformat() for d in dias]
    grafico_faturamento = [faturamento_dia.get(d, 0) for d in dias]
    grafico_quantidade = [quantidade_dia.get(d, 0) for d in dias]
    grafico_lucro = [lucro_dia.get(d, 0) for d in dias]

    # =========================
    # COMPARATIVO MÊS ATUAL x MÊS ANTERIOR
    # (IGNORA O FILTRO DA TELA E BUSCA DIRETO NO BANCO)
    # =========================
    inicio_mes_atual = hoje.replace(day=1)

    if inicio_mes_atual.month == 1:
        ano_ant = inicio_mes_atual.year - 1
        mes_ant = 12
    else:
        ano_ant = inicio_mes_atual.year
        mes_ant = inicio_mes_atual.month - 1
    inicio_mes_anterior = date(ano_ant, mes_ant, 1)

    if inicio_mes_atual.month == 12:
        primeiro_prox_mes_atual = date(inicio_mes_atual.year + 1, 1, 1)
    else:
        primeiro_prox_mes_atual = date(inicio_mes_atual.year, inicio_mes_atual.month + 1, 1)
    fim_mes_atual = min(hoje, primeiro_prox_mes_atual - timedelta(days=1))

    if mes_ant == 12:
        primeiro_mes_pos_ant = date(ano_ant + 1, 1, 1)
    else:
        primeiro_mes_pos_ant = date(ano_ant, mes_ant + 1, 1)
    fim_mes_anterior = primeiro_mes_pos_ant - timedelta(days=1)

    with engine.connect() as conn_cmp:
        rows_cmp = conn_cmp.execute(
            select(
                vendas.c.data_venda,
                vendas.c.receita_total
            ).where(
                vendas.c.data_venda >= inicio_mes_anterior.isoformat(),
                vendas.c.data_venda <= fim_mes_atual.isoformat() + "T23:59:59"
            )
        ).mappings().all()

    faturamento_mes_atual = {}
    faturamento_mes_anterior = {}

    for v in rows_cmp:
        data_raw = v["data_venda"]
        if not data_raw:
            continue
        try:
            dt = datetime.fromisoformat(str(data_raw)).date()
        except Exception:
            # se você já tem parse_data_venda no projeto, mantém
            dt_parsed = parse_data_venda(data_raw)
            if not dt_parsed:
                continue
            dt = dt_parsed.date()

        receita = float(v["receita_total"] or 0)

        if inicio_mes_atual <= dt <= fim_mes_atual:
            faturamento_mes_atual[dt] = faturamento_mes_atual.get(dt, 0) + receita
        elif inicio_mes_anterior <= dt <= fim_mes_anterior:
            faturamento_mes_anterior[dt] = faturamento_mes_anterior.get(dt, 0) + receita

    dias_mes_atual = []
    d = inicio_mes_atual
    while d <= fim_mes_atual:
        dias_mes_atual.append(d)
        d += timedelta(days=1)

    grafico_cmp_labels = [d.isoformat() for d in dias_mes_atual]
    grafico_cmp_atual = [faturamento_mes_atual.get(d, 0) for d in dias_mes_atual]

    grafico_cmp_anterior = []
    for i, _d in enumerate(dias_mes_atual):
        dia_ant = inicio_mes_anterior + timedelta(days=i)
        grafico_cmp_anterior.append(faturamento_mes_anterior.get(dia_ant, 0))

    # =========================
    # TOTAIS (RESPEITAM O FILTRO DA TELA)
    # =========================
    total_qtd = sum(float(q.get("quantidade") or 0) for q in vendas_all)
    receita_total = sum(float(q.get("receita_total") or 0) for q in vendas_all)
    custo_total = sum(float(q.get("custo_total") or 0) for q in vendas_all)
    margem_total = sum(float(q.get("margem_contribuicao") or 0) for q in vendas_all)

    comissao_total = max(0.0, (receita_total - custo_total) - margem_total)
    imposto_total = receita_total * (imposto_percent / 100.0)
    despesas_total = receita_total * (despesas_percent / 100.0)
    lucro_liquido_total = receita_total - custo_total - comissao_total - imposto_total - despesas_total

    totais = {
        "qtd": total_qtd,
        "receita": receita_total,
        "custo": custo_total,
        "margem": margem_total,
        "comissao": comissao_total,
        "imposto": imposto_total,
        "despesas": despesas_total,
        "lucro_liquido": lucro_liquido_total,
    }

    return render_template(
        "vendas.html",
        vendas=vendas_rows,
        lotes=lotes,
        produtos=produtos_rows,
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
        pizza_estados_labels=pizza_estados_labels,
        pizza_estados_valores=pizza_estados_valores,
        pizza_produtos_labels=pizza_produtos_labels,
        pizza_produtos_valores=pizza_produtos_valores,
        total_pages=total_pages,
        current_page=page
    )


# ---------------- IMPORT / EXPORT ----------------
@app.route("/importar_ml", methods=["GET", "POST"])
@login_required
def importar_ml_view():
    if request.method == "POST":
        if "arquivo" not in request.files:
            flash("Nenhum arquivo enviado.", "danger")
            return redirect(request.url)
        file = request.files["arquivo"]
        if file.filename == "":
            flash("Selecione um arquivo.", "danger")
            return redirect(request.url)
        filename = secure_filename(file.filename)
        caminho = os.path.join(app.config["UPLOAD_FOLDER"], filename)
        file.save(caminho)

        try:
            resumo = importar_vendas_ml(caminho, engine)
            flash(
                f"Importação concluída. Lote {resumo['lote_id']} - "
                f"{resumo['vendas_importadas']} vendas importadas, "
                f"{resumo['vendas_sem_sku']} sem SKU/Título, "
                f"{resumo['vendas_sem_produto']} sem produto cadastrado.",
                "success",
            )
        except Exception as e:
            flash(f"Erro na importação: {e}", "danger")
        return redirect(url_for("importar_ml_view"))

    return render_template("importar_ml.html")


@app.route("/exportar_consolidado")
@login_required
def exportar_consolidado():
    """Exporta planilha de consolidação das vendas."""
    with engine.connect() as conn:
        rows = conn.execute(
            select(
                vendas.c.id.label("ID Venda"),
                vendas.c.data_venda.label("Data venda"),
                produtos.c.nome.label("Produto"),
                produtos.c.sku.label("SKU"),
                vendas.c.quantidade.label("Quantidade"),
                vendas.c.preco_venda_unitario.label("Preço unitário"),
                vendas.c.receita_total.label("Receita total"),
                vendas.c.custo_total.label("Custo total"),
                vendas.c.margem_contribuicao.label("Margem contribuição"),
                vendas.c.origem.label("Origem"),
                vendas.c.numero_venda_ml.label("Nº venda ML"),
                vendas.c.lote_importacao.label("Lote importação"),
            ).select_from(vendas.join(produtos))
        ).mappings().all()

    df = pd.DataFrame(rows)
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Consolidado")
    output.seek(0)

    return send_file(
        output,
        as_attachment=True,
        download_name=f"consolidado_vendas_{datetime.now().date()}.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.route("/exportar_template")
@login_required
def exportar_template():
    """Exporta o modelo de planilha para preenchimento manual (SKU, Título, Quantidade, Receita, Comissao, PrecoMedio)."""
    cols = ["SKU", "Título", "Quantidade", "Receita", "Comissao", "PrecoMedio"]
    df = pd.DataFrame(columns=cols)
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Template")
    output.seek(0)
    return send_file(
        output,
        as_attachment=True,
        download_name="template_consolidacao_vendas.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


# ---------------- ESTOQUE / AJUSTES ----------------
# ---------------- ESTOQUE / AJUSTES ----------------
@app.route("/estoque")
@login_required
def estoque_view():
    """Visão de estoque com médias reais dos últimos 30 dias
    + receita potencial (bruta - comissão ML)
    + lucro estimado (após custo, comissão, imposto e despesas).
    """

    JANELA_DIAS = 30     # últimos 30 dias sempre
    DIAS_MINIMOS = 15    # estoque mínimo desejado em dias

    hoje = datetime.now()
    limite_30dias = hoje - timedelta(days=JANELA_DIAS)

    with engine.connect() as conn:
        # Produtos
        produtos_rows = conn.execute(
            select(
                produtos.c.id,
                produtos.c.nome,
                produtos.c.sku,
                produtos.c.estoque_atual,
                produtos.c.custo_unitario,
            ).order_by(produtos.c.nome)
        ).mappings().all()

        # Vendas (para média dos últimos 30 dias)
        vendas_rows = conn.execute(
            select(
                vendas.c.produto_id,
                vendas.c.data_venda,
                vendas.c.quantidade,
            )
        ).mappings().all()

        # Configurações de imposto e despesas
        cfg = conn.execute(
            select(configuracoes).where(configuracoes.c.id == 1)
        ).mappings().first() or {}

        imposto_percent = float(cfg.get("imposto_percent") or 0)
        despesas_percent = float(cfg.get("despesas_percent") or 0)

        # Agregado histórico de vendas por produto (para estimar ticket, comissao, etc.)
        vendas_historico = conn.execute(
            select(
                vendas.c.produto_id,
                func.coalesce(func.sum(vendas.c.quantidade), 0).label("qtd"),
                func.coalesce(func.sum(vendas.c.receita_total), 0).label("receita"),
                func.coalesce(func.sum(vendas.c.custo_total), 0).label("custo"),
                func.coalesce(func.sum(vendas.c.margem_contribuicao), 0).label("margem_atual"),
            )
            .group_by(vendas.c.produto_id)
        ).mappings().all()

    # Indexa histórico por produto_id
    hist_por_produto = {h["produto_id"]: h for h in vendas_historico}

    # Soma das vendas por produto dentro da janela (últimos 30 dias)
    vendas_por_produto = {}

    for v in vendas_rows:
        pid = v["produto_id"]
        qtd = int(v["quantidade"] or 0)
        data_raw = v["data_venda"]

        if not data_raw:
            continue

        dt = parse_data_venda(data_raw)
        if not dt:
            try:
                dt = datetime.fromisoformat(str(data_raw))
            except Exception:
                continue

        # só considera vendas dentro dos últimos 30 dias
        if dt < limite_30dias or dt > hoje:
            continue

        vendas_por_produto[pid] = vendas_por_produto.get(pid, 0) + qtd

    # Construção da tabela / totais
    produtos_enriquecidos = []

    total_unidades_estoque = 0.0
    total_custo_estoque = 0.0

    # novos totais:
    receita_potencial_total = 0.0      # receita bruta - comissão ML (estoque)
    lucro_estimado_total = 0.0         # lucro líquido estimado (estoque)

    for p in produtos_rows:
        pid = p["id"]
        estoque_atual = float(p["estoque_atual"] or 0)
        qtd_30dias = float(vendas_por_produto.get(pid, 0))
        custo_unitario = float(p["custo_unitario"] or 0)
        custo_estoque = estoque_atual * custo_unitario

        # Média diária usando 30 dias
        media_diaria = qtd_30dias / 30.0
        media_mensal = media_diaria * 30.0

        # Cobertura
        dias_cobertura = estoque_atual / media_diaria if media_diaria > 0 else None
        precisa_repor = dias_cobertura is not None and dias_cobertura < DIAS_MINIMOS

        # -------- CÁLCULOS ESTIMADOS COM BASE NO HISTÓRICO --------
        h = hist_por_produto.get(pid)
        lucro_potencial = 0.0          # lucro líquido estimado por produto (estoque)
        retorno_percent = 0.0
        receita_potencial_prod = 0.0   # receita bruta - comissão ML (estoque)

        if h:
            qtd_vendida = float(h["qtd"] or 0)
            receita_total = float(h["receita"] or 0)
            custo_total = float(h["custo"] or 0)
            margem_atual = float(h["margem_atual"] or 0)

            # Comissão ML estimada (mesma lógica do relatório de lucro)
            comissao_ml_total = max(0.0, (receita_total - custo_total) - margem_atual)

            imposto_val_total = receita_total * (imposto_percent / 100.0)
            despesas_val_total = receita_total * (despesas_percent / 100.0)

            lucro_liquido_total_hist = (
                receita_total
                - custo_total
                - comissao_ml_total
                - imposto_val_total
                - despesas_val_total
            )

            if qtd_vendida > 0:
                receita_unit = receita_total / qtd_vendida
                custo_unit_hist = custo_total / qtd_vendida
                comissao_unit = comissao_ml_total / qtd_vendida
                imposto_unit = imposto_val_total / qtd_vendida
                despesas_unit = despesas_val_total / qtd_vendida

                # Receita potencial = receita bruta - comissão ML (por unidade * estoque)
                receita_potencial_prod = (receita_unit - comissao_unit) * estoque_atual

                # Lucro líquido estimado (igual ao lucro_potencial que já existia)
                lucro_liquido_unitario = (
                    receita_unit
                    - custo_unit_hist
                    - comissao_unit
                    - imposto_unit
                    - despesas_unit
                )
                lucro_potencial = lucro_liquido_unitario * estoque_atual
            else:
                receita_potencial_prod = 0.0
                lucro_potencial = 0.0

            if custo_estoque > 0:
                retorno_percent = (lucro_potencial / custo_estoque) * 100.0

        # acumula totais globais
        total_unidades_estoque += estoque_atual
        total_custo_estoque += custo_estoque
        receita_potencial_total += receita_potencial_prod
        lucro_estimado_total += lucro_potencial

        produtos_enriquecidos.append({
            "id": pid,
            "nome": p["nome"],
            "sku": p["sku"],
            "estoque_atual": estoque_atual,
            "custo_unitario": custo_unitario,
            "custo_estoque": custo_estoque,
            "media_diaria": media_diaria,
            "media_mensal": media_mensal,
            "dias_cobertura": dias_cobertura,
            "precisa_repor": precisa_repor,
            "lucro_potencial": lucro_potencial,
            "retorno_percent": retorno_percent,
        })

    # Percentual de lucro global (lucro estimado / custo do estoque)
    if total_custo_estoque > 0:
        percentual_lucro_total = (lucro_estimado_total / total_custo_estoque) * 100.0
    else:
        percentual_lucro_total = 0.0

    return render_template(
        "estoque.html",
        produtos=produtos_enriquecidos,
        janela_dias=JANELA_DIAS,
        dias_minimos=DIAS_MINIMOS,
        total_unidades_estoque=total_unidades_estoque,
        total_custo_estoque=total_custo_estoque,
        receita_potencial_total=receita_potencial_total,
        lucro_estimado_total=lucro_estimado_total,
        percentual_lucro_total=percentual_lucro_total,
        imposto_percent=imposto_percent,
        despesas_percent=despesas_percent,
    )
# GET – formulário de ajuste
@app.route("/estoque/ajuste", methods=["GET"])
def ajuste_estoque_form():
    with engine.connect() as conn:
        produtos_rows = conn.execute(
            select(
                produtos.c.id,
                produtos.c.nome,
                produtos.c.sku
            ).order_by(produtos.c.nome)
        ).mappings().all()

    if not produtos_rows:
        flash("Cadastre ao menos 1 produto antes de ajustar estoque.", "warning")
        return redirect(url_for("estoque_view"))

    return render_template("ajuste_estoque.html", produtos=produtos_rows)


# POST – grava ajuste com custo médio ponderado
@app.route("/estoque/ajuste", methods=["POST"])
def ajuste_estoque():
    produto_id = int(request.form["produto_id"])
    tipo = request.form["tipo"]  # entrada ou saida
    quantidade = int(request.form.get("quantidade", 0) or 0)
    custo_unitario = request.form.get("custo_unitario")
    observacao = request.form.get("observacao") or ""

    custo_unitario_val = (
        float(custo_unitario) if custo_unitario not in (None, "",) else None
    )

    fator = 1 if tipo == "entrada" else -1

    with engine.begin() as conn:
        prod = conn.execute(
            select(
                produtos.c.estoque_atual,
                produtos.c.custo_unitario
            ).where(produtos.c.id == produto_id)
        ).mappings().first()

        if not prod:
            flash("Produto não encontrado para ajuste de estoque.", "danger")
            return redirect(url_for("estoque_view"))

        estoque_atual = float(prod["estoque_atual"] or 0)
        custo_atual = float(prod["custo_unitario"] or 0)

        novo_custo_medio = custo_atual

        # só recalcula custo em ENTRADA com custo informado
        if tipo == "entrada" and quantidade > 0 and custo_unitario_val is not None:
            if estoque_atual <= 0:
                novo_custo_medio = custo_unitario_val
            else:
                novo_custo_medio = (
                    (estoque_atual * custo_atual) + (quantidade * custo_unitario_val)
                ) / (estoque_atual + quantidade)

        novo_estoque = estoque_atual + fator * quantidade

        conn.execute(
            update(produtos)
            .where(produtos.c.id == produto_id)
            .values(
                estoque_atual=novo_estoque,
                custo_unitario=novo_custo_medio,
            )
        )

        if tipo == "saida":
            custo_ajuste_registro = custo_atual
        else:
            custo_ajuste_registro = custo_unitario_val

        conn.execute(
            insert(ajustes_estoque).values(
                produto_id=produto_id,
                data_ajuste=datetime.now().isoformat(),
                tipo=tipo,
                quantidade=quantidade,
                custo_unitario=custo_ajuste_registro,
                observacao=observacao,
            )
        )

    flash("Ajuste de estoque registrado com custo médio atualizado!", "success")
    return redirect(url_for("estoque_view"))
@app.route("/ajuste_estoque")
def ajuste_estoque_view():
    return render_template("ajuste_estoque.html")

# ---------------- CONFIGURAÇÕES ----------------
@app.route("/configuracoes", methods=["GET", "POST"])
@login_required
def configuracoes_view():
    if request.method == "POST":
        imposto_percent = float(request.form.get("imposto_percent", 0) or 0)
        despesas_percent = float(request.form.get("despesas_percent", 0) or 0)
        with engine.begin() as conn:
            conn.execute(
                update(configuracoes)
                .where(configuracoes.c.id == 1)
                .values(imposto_percent=imposto_percent, despesas_percent=despesas_percent)
            )
        flash("Configurações salvas!", "success")
        return redirect(url_for("configuracoes_view"))

    with engine.connect() as conn:
        cfg = conn.execute(
            select(configuracoes).where(configuracoes.c.id == 1)
        ).mappings().first()

    return render_template("configuracoes.html", cfg=cfg)


# ---------------- RELATÓRIO LUCRO ----------------
@app.route("/relatorio_lucro")
@login_required
def relatorio_lucro():
    """Relatório de lucro detalhado por produto, com filtro de período.

    Por padrão: mês vigente (do dia 1 até hoje).
    Margem líquida = Receita - Comissão ML - Custo - Despesas - Imposto
    """

    # --- período: vem da URL ou cai para mês vigente ---
    data_inicio = request.args.get("data_inicio") or ""
    data_fim = request.args.get("data_fim") or ""

    if not data_inicio and not data_fim:
        hoje = date.today()
        inicio_mes = hoje.replace(day=1)
        data_inicio = inicio_mes.isoformat()
        data_fim = hoje.isoformat()

    with engine.connect() as conn:
        cfg = conn.execute(
            select(configuracoes)
            .where(configuracoes.c.id == 1)
        ).mappings().first() or {}

        imposto_percent = float(cfg.get("imposto_percent") or 0)
        despesas_percent = float(cfg.get("despesas_percent") or 0)

        # monta query com filtro de datas
        query = (
            select(
                produtos.c.nome.label("produto"),
                func.sum(vendas.c.quantidade).label("qtd"),
                func.sum(vendas.c.receita_total).label("receita"),
                func.sum(vendas.c.custo_total).label("custo"),
                func.sum(vendas.c.margem_contribuicao).label("margem_atual"),
            )
            .select_from(vendas.join(produtos))
        )

        if data_inicio:
            query = query.where(vendas.c.data_venda >= data_inicio)
        if data_fim:
            query = query.where(vendas.c.data_venda <= data_fim + "T23:59:59")

        query = query.group_by(produtos.c.id)
        rows = conn.execute(query).mappings().all()

    linhas = []
    totais = {
        "qtd": 0.0,
        "receita": 0.0,
        "custo": 0.0,
        "comissao": 0.0,
        "imposto": 0.0,
        "despesas": 0.0,
        "margem_liquida": 0.0,
    }

    for r in rows:
        receita = float(r["receita"] or 0)
        custo = float(r["custo"] or 0)
        margem_atual = float(r["margem_atual"] or 0)

        # Comissão estimada do ML
        comissao_ml = max(0.0, (receita - custo) - margem_atual)

        imposto_val = receita * (imposto_percent / 100.0)
        despesas_val = receita * (despesas_percent / 100.0)

        margem_liquida = receita - custo - comissao_ml - imposto_val - despesas_val

        linha = {
            "produto": r["produto"],
            "qtd": float(r["qtd"] or 0),
            "receita": receita,
            "custo": custo,
            "comissao": comissao_ml,
            "imposto": imposto_val,
            "despesas": despesas_val,
            "margem_liquida": margem_liquida,
        }
        linhas.append(linha)

        totais["qtd"] += linha["qtd"]
        totais["receita"] += receita
        totais["custo"] += custo
        totais["comissao"] += comissao_ml
        totais["imposto"] += imposto_val
        totais["despesas"] += despesas_val
        totais["margem_liquida"] += margem_liquida

    # Ordena do maior lucro líquido para o menor
    linhas.sort(key=lambda x: x["margem_liquida"], reverse=True)

    return render_template(
        "relatorio_lucro.html",
        linhas=linhas,
        totais=totais,
        imposto_percent=imposto_percent,
        despesas_percent=despesas_percent,
        data_inicio=data_inicio,
        data_fim=data_fim,
    )
@app.route("/relatorio_lucro/exportar")
@login_required
def relatorio_lucro_exportar():
    # mesmo critério de período do relatorio_lucro
    data_inicio = request.args.get("data_inicio") or ""
    data_fim = request.args.get("data_fim") or ""

    if not data_inicio and not data_fim:
        hoje = date.today()
        inicio_mes = hoje.replace(day=1)
        data_inicio = inicio_mes.isoformat()
        data_fim = hoje.isoformat()

    with engine.connect() as conn:
        cfg = conn.execute(
            select(configuracoes)
            .where(configuracoes.c.id == 1)
        ).mappings().first() or {}

        imposto_percent = float(cfg.get("imposto_percent") or 0)
        despesas_percent = float(cfg.get("despesas_percent") or 0)

        query = (
            select(
                produtos.c.nome.label("produto"),
                func.sum(vendas.c.quantidade).label("qtd"),
                func.sum(vendas.c.receita_total).label("receita"),
                func.sum(vendas.c.custo_total).label("custo"),
                func.sum(vendas.c.margem_contribuicao).label("margem_atual"),
            )
            .select_from(vendas.join(produtos))
        )

        if data_inicio:
            query = query.where(vendas.c.data_venda >= data_inicio)
        if data_fim:
            query = query.where(vendas.c.data_venda <= data_fim + "T23:59:59")

        query = query.group_by(produtos.c.id)
        rows = conn.execute(query).mappings().all()

    linhas_export = []

    for r in rows:
        receita = float(r["receita"] or 0)
        custo = float(r["custo"] or 0)
        margem_atual = float(r["margem_atual"] or 0)
        qtd = float(r["qtd"] or 0)

        comissao_ml = max(0.0, (receita - custo) - margem_atual)
        imposto_val = receita * (imposto_percent / 100.0)
        despesas_val = receita * (despesas_percent / 100.0)
        margem_liquida = receita - custo - comissao_ml - imposto_val - despesas_val

        linhas_export.append({
            "Produto": r["produto"],
            "Quantidade": qtd,
            "Receita (R$)": receita,
            "Custo (R$)": custo,
            "Comissão ML (R$)": comissao_ml,
            "Imposto (R$)": imposto_val,
            "Despesas (R$)": despesas_val,
            "Lucro líquido (R$)": margem_liquida,
        })

    df = pd.DataFrame(linhas_export)
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="RelatorioLucro")
    output.seek(0)

    return send_file(
        output,
        as_attachment=True,
        download_name=f"relatorio_lucro_{datetime.now().date()}.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
# --------------------------------------------------------------------
# Inicialização
# --------------------------------------------------------------------
init_db()



# --------------------------------------------------------------------
# Financeiro / Mercado Pago (caixa) + Conciliação ML x MP
# --------------------------------------------------------------------

def _parse_iso_or_none(value):
    if value is None or (isinstance(value, float) and value != value):
        return None
    if isinstance(value, (datetime, date)):
        # se vier como datetime/date do pandas
        if isinstance(value, date) and not isinstance(value, datetime):
            return datetime.combine(value, datetime.min.time())
        return value
    try:
        s = str(value)
        # tenta ISO completo com timezone
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def importar_settlement_mp(caminho_arquivo, engine: Engine):
    lote_id = datetime.now().isoformat(timespec="seconds")

    df = pd.read_excel(caminho_arquivo)
    df.columns = [str(c).strip() for c in df.columns]

    # Novos e antigos layouts suportados
    required_new = [
        "Data de pagamento",
        "Tipo de operação",
        "Número do movimento",
        "Operação relacionada",
        "Valor",
    ]
    required_old = ["ID DA TRANSAÇÃO NO MERCADO PAGO", "TIPO DE TRANSAÇÃO", "VALOR LÍQUIDO DA TRANSAÇÃO"]

    use_new = all(col in df.columns for col in required_new)
    use_old = all(col in df.columns for col in required_old)

    if not use_new and not use_old:
        raise ValueError("Relatório MP fora do padrão esperado: colunas necessárias não encontradas.")

    importadas = 0
    atualizadas = 0
    ignoradas = 0

    now_iso = datetime.now().isoformat(timespec="seconds")
    processed_ids = set()

    with engine.begin() as conn:
        for _, row in df.iterrows():
            if use_new:
                external_id = row.get("Número do movimento")
                tipo_trans = str(row.get("Tipo de operação") or "").strip()
                oper_rel = str(row.get("Operação relacionada") or "").strip()
                val = row.get("Valor")
                dt = _parse_iso_or_none(row.get("Data de pagamento")) or datetime.now()
            else:
                external_id = row.get("ID DA TRANSAÇÃO NO MERCADO PAGO")
                tipo_trans = str(row.get("TIPO DE TRANSAÇÃO") or "").strip()
                oper_rel = str(row.get("CANAL DE VENDA") or "").strip()
                val = row.get("VALOR LÍQUIDO DA TRANSAÇÃO")
                dt = _parse_iso_or_none(row.get("DATA DE LIBERAÇÃO DO DINHEIRO")) \
                     or _parse_iso_or_none(row.get("DATA DE APROVAÇÃO")) \
                     or _parse_iso_or_none(row.get("DATA DE ORIGEM")) \
                     or datetime.now()

            try:
                external_id = str(int(external_id)) if external_id == external_id else None
            except Exception:
                external_id = str(external_id).strip() if external_id == external_id else None

            if not external_id or external_id in processed_ids:
                ignoradas += 1
                continue
            processed_ids.add(external_id)

            try:
                valor = float(val) if val == val else 0.0
            except Exception:
                valor = 0.0

            tipo_fin = "MP_NET"
            lower_tipo = tipo_trans.lower()
            if "estorno" in lower_tipo or "chargeback" in lower_tipo or "devolu" in lower_tipo or "contesta" in lower_tipo:
                tipo_fin = "REFUND"
                valor = -abs(valor) if valor != 0 else 0.0
            elif "retirada" in lower_tipo or "saque" in lower_tipo or "payout" in lower_tipo:
                tipo_fin = "WITHDRAWAL"
                valor = -abs(valor) if valor != 0 else 0.0
            elif "tarifa" in lower_tipo:
                tipo_fin = "FEE_ML"
                valor = -abs(valor) if valor != 0 else 0.0
            elif "pagamento" in lower_tipo or "venda" in lower_tipo:
                tipo_fin = "MP_NET"
                valor = abs(valor)

            data_lancamento = dt.isoformat()
            descricao = f"{tipo_trans} - {oper_rel}".strip(" -")

            existing = conn.execute(
                select(finance_transactions.c.id).where(finance_transactions.c.external_id_mp == external_id)
            ).first()

            if existing:
                conn.execute(
                    update(finance_transactions)
                    .where(finance_transactions.c.external_id_mp == external_id)
                    .values(
                        data_lancamento=data_lancamento,
                        tipo=tipo_fin,
                        valor=valor,
                        origem="mercado_pago",
                        descricao=descricao,
                        lote_importacao=lote_id,
                    )
                )
                atualizadas += 1
            else:
                conn.execute(
                    insert(finance_transactions).values(
                        data_lancamento=data_lancamento,
                        tipo=tipo_fin,
                        valor=valor,
                        origem="mercado_pago",
                        external_id_mp=external_id,
                        descricao=descricao,
                        criado_em=now_iso,
                        lote_importacao=lote_id,
                    )
                )
                importadas += 1

    return {"lote_id": lote_id, "importadas": importadas, "atualizadas": atualizadas, "ignoradas": ignoradas}


@app.route("/importar_mp", methods=["GET", "POST"])
@login_required
def importar_mp_view():
    if request.method == "POST":
        if "arquivo" not in request.files:
            flash("Nenhum arquivo enviado.", "danger")
            return redirect(request.url)
        file = request.files["arquivo"]
        if file.filename == "":
            flash("Selecione um arquivo.", "danger")
            return redirect(request.url)

        filename = secure_filename(file.filename)
        caminho = os.path.join(app.config["UPLOAD_FOLDER"], filename)
        file.save(caminho)

        try:
            resumo = importar_settlement_mp(caminho, engine)
            flash(
                f"Importação MP concluída. Lote {resumo['lote_id']} - "
                f"{resumo['importadas']} novas, {resumo['atualizadas']} atualizadas, {resumo['ignoradas']} ignoradas.",
                "success",
            )
        except Exception as e:
            flash(f"Erro na importação MP: {e}", "danger")

        return redirect(url_for("importar_mp_view"))

    # lotes de importação MP
    with engine.connect() as conn:
        lotes_mp = conn.execute(
            select(
                finance_transactions.c.lote_importacao,
                func.count().label("qtd"),
                func.min(finance_transactions.c.data_lancamento).label("data_min"),
                func.max(finance_transactions.c.data_lancamento).label("data_max"),
            )
            .where(finance_transactions.c.origem == "mercado_pago")
            .where(finance_transactions.c.lote_importacao.isnot(None))
            .group_by(finance_transactions.c.lote_importacao)
            .order_by(func.min(finance_transactions.c.data_lancamento).desc())
        ).mappings().all()

    return render_template("importar_mp.html", lotes_mp=lotes_mp)


def _date_only(iso_str: str):
    try:
        return iso_str[:10]
    except Exception:
        return None


@app.route("/financeiro", methods=["GET", "POST"])
@login_required
def financeiro_view():
    # Ações (saldo inicial, devolução, retirada)
    if request.method == "POST":
        acao = request.form.get("acao")
        data = request.form.get("data") or date.today().isoformat()
        descricao = (request.form.get("descricao") or "").strip() or None

        try:
            valor = float((request.form.get("valor") or "0").replace(",", "."))
        except Exception:
            valor = 0.0

        tipo = None
        if acao == "saldo_inicial":
            tipo = "OPENING_BALANCE"
        elif acao == "devolucao":
            tipo = "REFUND"
            valor = -abs(valor)
        elif acao == "retirada":
            tipo = "WITHDRAWAL"
            valor = -abs(valor)
        elif acao == "ajuste":
            tipo = "ADJUSTMENT"

        # Special action: set editable saldo anterior for the chosen period
        if acao == "set_saldo_anterior":
            try:
                valor = float((request.form.get("saldo_anterior_val") or "0").replace(",", "."))
            except Exception:
                valor = 0.0
            data_inicio_form = request.form.get("data_inicio") or data
            # place the opening balance one day before the period so it's counted in < data_inicio
            insert_date = (date.fromisoformat(data_inicio_form) - timedelta(days=1)).isoformat()
            desc_open = f"Saldo anterior manual for {data_inicio_form}"
            with engine.begin() as conn:
                # remove previous manual opening created by this UI for the same period
                conn.execute(
                    delete(finance_transactions).where(
                        (finance_transactions.c.tipo == "OPENING_BALANCE")
                        & (finance_transactions.c.descricao == desc_open)
                    )
                )
                conn.execute(
                    insert(finance_transactions).values(
                        data_lancamento=f"{insert_date}T00:00:00",
                        tipo="OPENING_BALANCE",
                        valor=valor,
                        origem="manual",
                        descricao=desc_open,
                        criado_em=datetime.now().isoformat(timespec="seconds"),
                    )
                )
            flash("Saldo anterior atualizado.", "success")
        elif tipo:
            with engine.begin() as conn:
                conn.execute(
                    insert(finance_transactions).values(
                        data_lancamento=f"{data}T00:00:00",
                        tipo=tipo,
                        valor=valor,
                        origem="manual",
                        descricao=descricao,
                        criado_em=datetime.now().isoformat(timespec="seconds"),
                    )
                )
            flash("Lançamento registrado com sucesso.", "success")
        else:
            flash("Ação inválida.", "danger")

        return redirect(url_for("financeiro_view"))

    # Período
    data_inicio = request.args.get("data_inicio") or (date.today().replace(day=1)).isoformat()
    data_fim = request.args.get("data_fim") or date.today().isoformat()

    filtro = []
    if data_inicio:
        filtro.append(finance_transactions.c.data_lancamento >= data_inicio)
    if data_fim:
        filtro.append(finance_transactions.c.data_lancamento <= data_fim + "T23:59:59")

    with engine.connect() as conn:
        # saldo antes do período (para abrir o saldo do período)
        saldo_anterior = conn.execute(
            select(func.coalesce(func.sum(finance_transactions.c.valor), 0.0))
            .where(finance_transactions.c.data_lancamento < data_inicio)
        ).scalar() or 0.0

        entradas_mp = conn.execute(
            select(func.coalesce(func.sum(finance_transactions.c.valor), 0.0))
            .where(*(filtro + [finance_transactions.c.tipo == "MP_NET"]))
        ).scalar() or 0.0

        devolucoes = conn.execute(
            select(func.coalesce(func.sum(finance_transactions.c.valor), 0.0))
            .where(*(filtro + [finance_transactions.c.tipo == "REFUND"]))
        ).scalar() or 0.0

        retiradas = conn.execute(
            select(func.coalesce(func.sum(finance_transactions.c.valor), 0.0))
            .where(*(filtro + [finance_transactions.c.tipo == "WITHDRAWAL"]))
        ).scalar() or 0.0

        tarifas_ml = conn.execute(
            select(func.coalesce(func.sum(finance_transactions.c.valor), 0.0))
            .where(*(filtro + [finance_transactions.c.tipo == "FEE_ML"]))
        ).scalar() or 0.0

        ajustes = conn.execute(
            select(func.coalesce(func.sum(finance_transactions.c.valor), 0.0))
            .where(*(filtro + [finance_transactions.c.tipo == "ADJUSTMENT"]))
        ).scalar() or 0.0

        saldo_periodo = entradas_mp + devolucoes + retiradas + tarifas_ml + ajustes
        saldo_atual = saldo_anterior + saldo_periodo

        transacoes = conn.execute(
            select(
                finance_transactions.c.data_lancamento,
                finance_transactions.c.tipo,
                finance_transactions.c.valor,
                finance_transactions.c.origem,
                finance_transactions.c.external_id_mp,
                finance_transactions.c.descricao,
            )
            .where(*filtro)
            .order_by(finance_transactions.c.data_lancamento.desc())
            .limit(500)
        ).mappings().all()

        # lotes de importação MP
        lotes_mp = conn.execute(
            select(
                finance_transactions.c.lote_importacao,
                func.count().label("qtd"),
                func.min(finance_transactions.c.data_lancamento).label("data_min"),
                func.max(finance_transactions.c.data_lancamento).label("data_max"),
            )
            .where(finance_transactions.c.origem == "mercado_pago")
            .where(finance_transactions.c.lote_importacao.isnot(None))
            .group_by(finance_transactions.c.lote_importacao)
            .order_by(func.min(finance_transactions.c.data_lancamento).desc())
        ).mappings().all()

    return render_template(
        "financeiro.html",
        data_inicio=data_inicio,
        data_fim=data_fim,
        saldo_anterior=saldo_anterior,
        entradas_mp=entradas_mp,
        devolucoes=devolucoes,
        retiradas=retiradas,
        tarifas_ml=tarifas_ml,
        ajustes=ajustes,
        saldo_atual=saldo_atual,
        transacoes=transacoes,
        lotes_mp=lotes_mp,
    )


@app.route("/excluir_lote/<path:lote>", methods=["POST"])
def excluir_lote_financeiro(lote):
    print("Excluindo lote:", lote)
    with engine.begin() as conn:
        deleted = conn.execute(
            delete(finance_transactions)
            .where(finance_transactions.c.lote_importacao == lote)
            .where(finance_transactions.c.origem == "mercado_pago")
        )
    flash(f"Lote {lote} excluído ({deleted.rowcount} transações).", "success")
    # redirect based on referrer or default to financeiro
    if "importar_mp" in request.referrer:
        return redirect(url_for("importar_mp_view"))
    return redirect(url_for("financeiro_view"))


@app.route("/conciliacao", methods=["GET"])
@login_required
def conciliacao_view():
    data_inicio = request.args.get("data_inicio") or (date.today().replace(day=1)).isoformat()
    data_fim = request.args.get("data_fim") or date.today().isoformat()

    # filtros
    filtro_v = []
    if data_inicio:
        filtro_v.append(vendas.c.data_venda >= data_inicio)
    if data_fim:
        filtro_v.append(vendas.c.data_venda <= data_fim + "T23:59:59")

    filtro_f = []
    if data_inicio:
        filtro_f.append(finance_transactions.c.data_lancamento >= data_inicio)
    if data_fim:
        filtro_f.append(finance_transactions.c.data_lancamento <= data_fim + "T23:59:59")

    with engine.connect() as conn:
        # ML: receita líquida gerencial = bruta - comissão
        ml_liquida = conn.execute(
            select(func.coalesce(func.sum(vendas.c.receita_total - vendas.c.comissao_ml), 0.0))
            .where(*filtro_v)
        ).scalar() or 0.0

        # MP: receita líquida financeira = MP_NET + FEE_ML (FEE_ML já é negativo)
        mp_bruta = conn.execute(
            select(func.coalesce(func.sum(finance_transactions.c.valor), 0.0))
            .where(*(filtro_f + [finance_transactions.c.tipo == "MP_NET"]))
        ).scalar() or 0.0
        
        mp_tarifas = conn.execute(
            select(func.coalesce(func.sum(finance_transactions.c.valor), 0.0))
            .where(*(filtro_f + [finance_transactions.c.tipo == "FEE_ML"]))
        ).scalar() or 0.0
        
        mp_liquida = mp_bruta + mp_tarifas  # tarifas já são negativas

        diferenca_total = ml_liquida - mp_liquida

        # Série diária (ML por data_venda; MP por data_lancamento)
        v_rows = conn.execute(
            select(vendas.c.data_venda, vendas.c.receita_total, vendas.c.comissao_ml).where(*filtro_v)
        ).all()

        f_rows = conn.execute(
            select(finance_transactions.c.data_lancamento, finance_transactions.c.valor)
            .where(*(filtro_f + [finance_transactions.c.tipo == "MP_NET"]))
        ).all()

    # agrupa em Python (mantém simples e compatível)
    ml_por_dia = {}
    for dv, bruta, com in v_rows:
        if not dv:
            continue
        dia = str(dv)[:10]
        try:
            bruta = float(bruta or 0)
            com = float(com or 0)
        except Exception:
            bruta, com = 0.0, 0.0
        ml_por_dia[dia] = ml_por_dia.get(dia, 0.0) + (bruta - com)

    mp_por_dia = {}
    for dl, val in f_rows:
        if not dl:
            continue
        dia = str(dl)[:10]
        try:
            val = float(val or 0)
        except Exception:
            val = 0.0
        mp_por_dia[dia] = mp_por_dia.get(dia, 0.0) + val

    dias = sorted(set(list(ml_por_dia.keys()) + list(mp_por_dia.keys())))
    linhas = []
    for d in dias:
        ml = ml_por_dia.get(d, 0.0)
        mp = mp_por_dia.get(d, 0.0)
        linhas.append({"dia": d, "ml": ml, "mp": mp, "diff": ml - mp})

    return render_template(
        "conciliacao.html",
        data_inicio=data_inicio,
        data_fim=data_fim,
        ml_liquida=ml_liquida,
        mp_liquida=mp_liquida,
        diferenca_total=diferenca_total,
        linhas=linhas,
    )


# ============================================================
# ROTA: Imprimir Etiquetas ZPL (Mercado Livre)
# ============================================================
@app.route("/etiquetas_zpl", methods=["GET", "POST"])
@login_required
def etiquetas_zpl():
    """
    Página para inserir código ZPL do Mercado Livre e converter para PDF.
    Usa a API Labelary para converter ZPL em imagem/PDF.
    """
    if request.method == "POST":
        zpl_code = request.form.get("zpl_code", "").strip()
        largura_cm = request.form.get("largura_cm", "4").strip()
        altura_cm = request.form.get("altura_cm", "2.5").strip()
        quantidade = request.form.get("quantidade", "1").strip()
        
        if not zpl_code:
            flash("Por favor, insira o código ZPL da etiqueta.", "danger")
            return redirect(url_for("etiquetas_zpl"))
        
        try:
            # Converter cm para polegadas (1 inch = 2.54 cm)
            largura_inch = float(largura_cm) / 2.54
            altura_inch = float(altura_cm) / 2.54
            qtd = int(quantidade)
            
            # Formatar com 1 casa decimal
            size_str = f"{largura_inch:.1f}x{altura_inch:.1f}"
            
            # Detectar se há múltiplas etiquetas no código (múltiplos ^XA)
            # Separar cada etiqueta individual (cada bloco ^XA...^XZ)
            etiquetas = re.findall(r'\^XA.*?\^XZ', zpl_code, re.DOTALL)
            num_etiquetas_no_codigo = len(etiquetas)
            print(f"DEBUG: Detectadas {num_etiquetas_no_codigo} etiquetas no código ZPL")
            
            labelary_url = f"http://api.labelary.com/v1/printers/8dpmm/labels/{size_str}/0/"
            headers = {
                'Accept': 'application/pdf',
                'Content-Type': 'application/x-www-form-urlencoded'
            }
            
            if num_etiquetas_no_codigo > 1:
                # Caso de etiquetas sequenciais diferentes - processar cada uma
                print(f"DEBUG: Processando {num_etiquetas_no_codigo} etiquetas sequenciais")
                
                if not (PdfReader and PdfWriter):
                    flash("Biblioteca PDF não disponível. Instale pypdf.", "danger")
                    return redirect(url_for("etiquetas_zpl"))
                
                writer = PdfWriter()
                
                for idx, etiqueta_zpl in enumerate(etiquetas, 1):
                    print(f"DEBUG: Processando etiqueta {idx}/{num_etiquetas_no_codigo}")
                    
                    # Aguardar um pouco para evitar rate limit da API (429 Too Many Requests)
                    if idx > 1:
                        time.sleep(0.5)  # Aguardar 500ms entre requisições
                    
                    # Limpar e adicionar ^PQ1
                    etiqueta_limpa = re.sub(r'\^PQ\d+[^\^]*', '', etiqueta_zpl)
                    etiqueta_limpa = re.sub(r'\^XZ\s*$', '', etiqueta_limpa)
                    etiqueta_limpa = f"{etiqueta_limpa}\n^PQ1,0,1,Y^XZ"
                    
                    # Converter para PDF com retry em caso de erro 429
                    max_retries = 3
                    for tentativa in range(max_retries):
                        resp = requests.post(labelary_url, data=etiqueta_limpa.encode('utf-8'), headers=headers)
                        
                        if resp.status_code == 200:
                            # Adicionar ao PDF final
                            pdf_temp = BytesIO(resp.content)
                            reader_temp = PdfReader(pdf_temp)
                            writer.add_page(reader_temp.pages[0])
                            break
                        elif resp.status_code == 429 and tentativa < max_retries - 1:
                            # Rate limit - aguardar e tentar novamente
                            print(f"DEBUG: Rate limit na etiqueta {idx}, aguardando {(tentativa + 1) * 2}s...")
                            time.sleep((tentativa + 1) * 2)  # Aguardar 2s, 4s, 6s...
                        else:
                            print(f"DEBUG: Erro ao processar etiqueta {idx}: Status {resp.status_code}")
                            break
                
                # Gerar PDF final
                output_buffer = BytesIO()
                writer.write(output_buffer)
                output_buffer.seek(0)
                
                print(f"DEBUG: PDF final gerado com {len(writer.pages)} páginas")
                flash(f"PDF gerado com sucesso: {len(writer.pages)} etiquetas sequenciais!", "success")
                
                return send_file(
                    output_buffer,
                    mimetype='application/pdf',
                    as_attachment=True,
                    download_name=f'etiquetas_seq_{len(writer.pages)}x_{largura_cm}x{altura_cm}cm_{datetime.now().strftime("%Y%m%d_%H%M%S")}.pdf'
                )
            else:
                # Caso de etiqueta única que será replicada
                # Modificar o código ZPL para gerar apenas 1 etiqueta
                zpl_sem_fim = re.sub(r'\^XZ\s*$', '', zpl_code)
                zpl_sem_pq = re.sub(r'\^PQ\d+[^\^]*', '', zpl_sem_fim)
                zpl_modificado = f"{zpl_sem_pq}\n^PQ1,0,1,Y^XZ"
                
                response = requests.post(labelary_url, data=zpl_modificado.encode('utf-8'), headers=headers)
            
                if response.status_code == 200:
                    print(f"DEBUG: Quantidade solicitada = {qtd}")
                    print(f"DEBUG: PyPDF2 disponível = {PdfReader is not None and PdfWriter is not None}")
                    
                    # Se quantidade é 1, retornar direto
                    if qtd == 1:
                        pdf_buffer = BytesIO(response.content)
                        pdf_buffer.seek(0)
                        return send_file(
                            pdf_buffer,
                            mimetype='application/pdf',
                            as_attachment=True,
                            download_name=f'etiqueta_{largura_cm}x{altura_cm}cm_{datetime.now().strftime("%Y%m%d_%H%M%S")}.pdf'
                        )
                    
                    # Se quantidade > 1, replicar a página usando PyPDF2
                    if PdfReader and PdfWriter:
                        try:
                            print(f"DEBUG: Iniciando replicação de {qtd} etiquetas")
                            # Ler o PDF original (1 etiqueta)
                            pdf_original = BytesIO(response.content)
                            reader = PdfReader(pdf_original)
                            writer = PdfWriter()
                            
                            print(f"DEBUG: PDF original tem {len(reader.pages)} página(s)")
                            
                            # Adicionar a página qtd vezes
                            page = reader.pages[0]
                            for i in range(qtd):
                                writer.add_page(page)
                                if (i + 1) % 50 == 0:
                                    print(f"DEBUG: Adicionadas {i + 1}/{qtd} páginas")
                            
                            print(f"DEBUG: Total de páginas no PDF final: {len(writer.pages)}")
                            
                            # Criar buffer de saída
                            output_buffer = BytesIO()
                            writer.write(output_buffer)
                            output_buffer.seek(0)
                            
                            print(f"DEBUG: PDF gerado com sucesso! Tamanho: {len(output_buffer.getvalue())} bytes")
                            
                            flash(f"PDF gerado com sucesso: {qtd} etiquetas!", "success")
                            
                            return send_file(
                                output_buffer,
                                mimetype='application/pdf',
                                as_attachment=True,
                                download_name=f'etiquetas_{qtd}x_{largura_cm}x{altura_cm}cm_{datetime.now().strftime("%Y%m%d_%H%M%S")}.pdf'
                            )
                        except Exception as e:
                            print(f"DEBUG: ERRO ao criar PDF múltiplo: {str(e)}")
                            import traceback
                            traceback.print_exc()
                            flash(f"Erro ao criar PDF com múltiplas etiquetas: {str(e)}. Baixando apenas 1 etiqueta.", "warning")
                            pdf_buffer = BytesIO(response.content)
                            pdf_buffer.seek(0)
                            return send_file(
                                pdf_buffer,
                                mimetype='application/pdf',
                                as_attachment=True,
                                download_name=f'etiqueta_1x_{largura_cm}x{altura_cm}cm_{datetime.now().strftime("%Y%m%d_%H%M%S")}.pdf'
                            )
                    else:
                        print("DEBUG: PyPDF2 NÃO está disponível!")
                        flash(f"PyPDF2 não está instalado. Baixando apenas 1 etiqueta. Instale com: pip install PyPDF2", "warning")
                        pdf_buffer = BytesIO(response.content)
                        pdf_buffer.seek(0)
                        return send_file(
                            pdf_buffer,
                            mimetype='application/pdf',
                            as_attachment=True,
                            download_name=f'etiqueta_1x_{largura_cm}x{altura_cm}cm_{datetime.now().strftime("%Y%m%d_%H%M%S")}.pdf'
                        )
                else:
                    flash(f"Erro ao converter ZPL para PDF. Status: {response.status_code}", "danger")
                    return redirect(url_for("etiquetas_zpl"))
                
        except ValueError:
            flash("Por favor, insira valores numéricos válidos para largura, altura e quantidade.", "danger")
            return redirect(url_for("etiquetas_zpl"))
        except Exception as e:
            flash(f"Erro ao processar etiqueta: {str(e)}", "danger")
            return redirect(url_for("etiquetas_zpl"))
    
    return render_template("etiquetas_zpl.html")


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
