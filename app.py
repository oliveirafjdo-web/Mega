import os
from datetime import datetime, date, timedelta
from io import BytesIO
import requests
import threading

from flask import Flask, render_template, request, redirect, url_for, flash, send_file
from apscheduler.schedulers.background import BackgroundScheduler
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from flask_bcrypt import Bcrypt
from werkzeug.utils import secure_filename

from sqlalchemy import (
    create_engine, MetaData, Table, Column, Integer, String, Float,
    ForeignKey, func, select, insert, update, delete, inspect, text
)
from sqlalchemy.engine import Engine
import pandas as pd

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

# Configuração do engine com pool settings para Render (melhor tratamento de SSL/conexão)
if raw_db_url:
    engine: Engine = create_engine(
        DATABASE_URL,
        future=True,
        pool_pre_ping=True,          # Verifica conexão antes de usar
        pool_recycle=300,             # Recicla conexões a cada 5min (mais agressivo)
        pool_size=3,                  # Reduzir pool (Render Free tem limites)
        max_overflow=1,               # Até 1 conexão extra
        pool_timeout=30,              # Timeout ao esperar conexão do pool
        echo_pool='debug',            # Log de debug do pool (remover em produção)
        connect_args={
            'connect_timeout': 10,
            'keepalives': 1,
            'keepalives_idle': 30,
            'keepalives_interval': 10,
            'keepalives_count': 5,
            'sslmode': 'require',     # Força SSL mas não valida certificado
            'options': '-c statement_timeout=30000'
        }
    )
else:
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

# Migração: adicionar colunas ML (PostgreSQL)
if raw_db_url:
    try:
        with engine.begin() as conn:
            alteracoes = [
                "ALTER TABLE configuracoes ADD COLUMN IF NOT EXISTS ml_client_id VARCHAR(255)",
                "ALTER TABLE configuracoes ADD COLUMN IF NOT EXISTS ml_client_secret VARCHAR(255)",
                "ALTER TABLE configuracoes ADD COLUMN IF NOT EXISTS ml_access_token VARCHAR(500)",
                "ALTER TABLE configuracoes ADD COLUMN IF NOT EXISTS ml_refresh_token VARCHAR(500)",
                "ALTER TABLE configuracoes ADD COLUMN IF NOT EXISTS ml_token_expira VARCHAR(50)",
                "ALTER TABLE configuracoes ADD COLUMN IF NOT EXISTS ml_user_id VARCHAR(100)",
                "ALTER TABLE vendas ADD COLUMN IF NOT EXISTS ml_order_id VARCHAR(50)",
                "ALTER TABLE vendas ADD COLUMN IF NOT EXISTS ml_status VARCHAR(50)",
                "ALTER TABLE configuracoes ADD COLUMN IF NOT EXISTS ml_sync_auto VARCHAR(10) DEFAULT 'false'",
                "ALTER TABLE configuracoes ADD COLUMN IF NOT EXISTS ml_ultimo_sync VARCHAR(50)",
            ]
            for sql in alteracoes:
                try:
                    conn.execute(text(sql))
                except Exception:
                    pass
            # Criar índice único para ml_order_id (ignora se já existe)
            try:
                conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS idx_vendas_ml_order_id ON vendas(ml_order_id) WHERE ml_order_id IS NOT NULL"))
            except Exception:
                pass
        print("✅ Migração ML concluída")
    except Exception as e:
        print(f"⚠️ Migração: {e}")

# Helper para retry em operações de banco (SSL intermitente)
def db_retry(func, max_attempts=3):
    """Executa função com retry em caso de erro SSL/conexão"""
    import time
    from sqlalchemy.exc import OperationalError
    
    for attempt in range(max_attempts):
        try:
            return func()
        except OperationalError as e:
            if 'SSL' in str(e) or 'connection' in str(e).lower():
                if attempt < max_attempts - 1:
                    print(f"⚠️ Erro SSL (tentativa {attempt + 1}/{max_attempts}), retry em 1s...")
                    time.sleep(1)
                    continue
            raise
    return None

# Classe User para Flask-Login
class User(UserMixin):
    def __init__(self, id, username, password_hash):
        self.id = id
        self.username = username
        self.password_hash = password_hash

    @staticmethod
    def get_by_username(username):
        def _query():
            with engine.connect() as conn:
                row = conn.execute(select(usuarios).where(usuarios.c.username == username)).mappings().first()
                if row:
                    return User(row["id"], row["username"], row["password_hash"])
            return None
        return db_retry(_query)

    @staticmethod
    def get(user_id):
        def _query():
            with engine.connect() as conn:
                row = conn.execute(select(usuarios).where(usuarios.c.id == user_id)).mappings().first()
                if row:
                    return User(row["id"], row["username"], row["password_hash"])
            return None
        return db_retry(_query)

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
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

engine: Engine = create_engine(DATABASE_URL, future=True)
metadata = MetaData()

 # --------------------------------------------------------------------
 # Definição das tabelas
# --------------------------------------------------------------------
# Rota para excluir lote de vendas
@app.route("/excluir_lote_venda/<path:lote>", methods=["POST"])
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
    Column("ml_order_id", String(50)),  # ID único do pedido ML para evitar duplicação
    Column("ml_status", String(50)),  # Status da venda ML: paid, confirmed, ready_to_ship, shipped, delivered, cancelled
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
    Column("ml_client_id", String(255)),
    Column("ml_client_secret", String(255)),
    Column("ml_access_token", String(500)),
    Column("ml_refresh_token", String(500)),
    Column("ml_token_expira", String(50)),
    Column("ml_user_id", String(100)),
    Column("ml_sync_auto", String(10), server_default="false"),  # ativar sync automática
    Column("ml_ultimo_sync", String(50)),  # última sincronização automática
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

    df = pd.read_excel(
        caminho_arquivo,
        sheet_name="Vendas BR",
        header=5
    )
    if "N.º de venda" not in df.columns:
        raise ValueError("Planilha não está no formato esperado: coluna 'N.º de venda' não encontrada.")

    print("Colunas encontradas:", list(df.columns))

    df = df[df["N.º de venda"].notna()]
    
    # normaliza coluna UF se existir
    uf_col, not_rec = normalize_df_uf(df)
    if uf_col and not_rec:
        # salva relatório de valores não reconhecidos
        try:
            rpt_path = os.path.join(app.config["UPLOAD_FOLDER"], f"uf_not_recognized_settlement_{lote_id}.csv")
            pd.DataFrame([{'original': o, 'converted': c} for o, c in not_rec]).to_csv(rpt_path, index=False)
            print(f"Relatório UF não reconhecidos salvo em: {rpt_path}")
        except Exception:
            print("Falha ao salvar relatório de UF não reconhecidos.")

    vendas_importadas = 0
    vendas_sem_sku = 0
    vendas_sem_produto = 0

    with engine.begin() as conn:
        for _, row in df.iterrows():
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
                continue

            if not produto_row:
                vendas_sem_produto += 1
                continue

            produto_id = produto_row["id"]
            custo_unitario = float(produto_row["custo_unitario"] or 0.0)

            data_venda_raw = row.get("Data da venda")
            data_venda = parse_data_venda(data_venda_raw)
            unidades = row.get("Unidades")
            try:
                unidades = int(unidades) if unidades == unidades else 0
            except Exception:
                unidades = 0

            total_brl = row.get("Total (BRL)")
            try:
                receita_total = float(total_brl) if total_brl == total_brl else 0.0
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
            for col in ["UF", "Estado", "Estado do comprador", "Estado do Cliente"]:
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
    df = pd.read_excel(caminho_arquivo, header=0)  # assume header in first row

    # normaliza coluna UF se houver (algumas planilhas de produtos podem conter UF)
    try:
        uf_col, not_rec = normalize_df_uf(df)
        if uf_col and not_rec:
            rpt = str(caminho_arquivo).replace('.xlsx', '_uf_not_recognized.csv')
            pd.DataFrame([{'original': o, 'converted': c} for o, c in not_rec]).to_csv(rpt, index=False)
            print(f"Relatório UF não reconhecidos salvo em: {rpt}")
    except Exception:
        pass

    if "SKU" not in df.columns:
        raise ValueError("Planilha deve ter uma coluna 'SKU'.")

    produtos_importados = 0
    produtos_atualizados = 0
    erros = []

    with engine.begin() as conn:
        for _, row in df.iterrows():
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

    return {
        "produtos_importados": produtos_importados,
        "produtos_atualizados": produtos_atualizados,
        "erros": erros,
    }


# --------------------------------------------------------------------
# Rotas principais
# --------------------------------------------------------------------
@app.route("/")
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
def lista_produtos():
    with engine.connect() as conn:
        produtos_rows = conn.execute(select(produtos).order_by(produtos.c.nome)).mappings().all()
    return render_template("produtos.html", produtos=produtos_rows)


@app.route("/produtos/novo", methods=["GET", "POST"])
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
def excluir_produto(produto_id):
    with engine.begin() as conn:
        conn.execute(delete(produtos).where(produtos.c.id == produto_id))
    flash("Produto excluído.", "success")
    return redirect(url_for("lista_produtos"))


@app.route("/produtos/importar", methods=["GET", "POST"])
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
def lista_vendas():
    data_inicio = request.args.get("data_inicio") or ""
    data_fim = request.args.get("data_fim") or ""
    page = int(request.args.get("page", 1))

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
            pizza_estados_labels = [r["uf"] for r in estados_rows]
            pizza_estados_valores = [float(r["total_receita"] or 0) for r in estados_rows]

            # Se quiser por quantidade, use isto no lugar:
            # pizza_estados_valores = [int(r["qtd_vendas"] or 0) for r in estados_rows]

    # =======================
    # GRÁFICOS 30 DIAS (FATURAMENTO / QTD / LUCRO)
    # =======================
    faturamento_dia = {}
    quantidade_dia = {}
    lucro_dia = {}
    receita_liquida_dia = {}

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
        receita_liquida_dia[dt] = receita_liquida_dia.get(dt, 0) + (receita - comissao_ml)

    # Últimos 30 dias ordenados
    dias = [hoje - timedelta(days=i) for i in range(29, -1, -1)]
    grafico_labels = [d.isoformat() for d in dias]
    grafico_faturamento = [faturamento_dia.get(d, 0) for d in dias]
    grafico_quantidade = [quantidade_dia.get(d, 0) for d in dias]
    grafico_lucro = [lucro_dia.get(d, 0) for d in dias]
    grafico_receita_liquida = [receita_liquida_dia.get(d, 0) for d in dias]

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
    totais = {
        "qtd": sum(float(q.get("quantidade") or 0) for q in vendas_all),
        "receita": sum(float(q.get("receita_total") or 0) for q in vendas_all),
        "custo": sum(float(q.get("custo_total") or 0) for q in vendas_all),
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
        grafico_receita_liquida=grafico_receita_liquida,
        grafico_cmp_labels=grafico_cmp_labels,
        grafico_cmp_atual=grafico_cmp_atual,
        grafico_cmp_anterior=grafico_cmp_anterior,
        pizza_estados_labels=pizza_estados_labels,
        pizza_estados_valores=pizza_estados_valores,
        total_pages=total_pages,
        current_page=page
    )


# ---------------- IMPORT / EXPORT ----------------
@app.route("/importar_ml", methods=["GET", "POST"])
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
    # normaliza colunas
    df.columns = [str(c).strip() for c in df.columns]

    # normaliza coluna UF se existir
    try:
        uf_col, not_rec = normalize_df_uf(df)
        if uf_col and not_rec:
            rpt_path = os.path.join(app.config["UPLOAD_FOLDER"], f"uf_not_recognized_settlement_{lote_id}.csv")
            pd.DataFrame([{'original': o, 'converted': c} for o, c in not_rec]).to_csv(rpt_path, index=False)
            print(f"Relatório UF não reconhecidos salvo em: {rpt_path}")
    except Exception:
        pass

    required = ["ID DA TRANSAÇÃO NO MERCADO PAGO", "TIPO DE TRANSAÇÃO", "VALOR LÍQUIDO DA TRANSAÇÃO"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Relatório MP fora do padrão esperado: coluna '{col}' não encontrada.")

    importadas = 0
    atualizadas = 0
    ignoradas = 0

    now_iso = datetime.now().isoformat(timespec="seconds")
    processed_ids = set()

    with engine.begin() as conn:
        for _, row in df.iterrows():
            external_id = row.get("ID DA TRANSAÇÃO NO MERCADO PAGO")
            try:
                external_id = str(int(external_id)) if external_id == external_id else None
            except Exception:
                external_id = str(external_id).strip() if external_id == external_id else None

            if not external_id or external_id in processed_ids:
                ignoradas += 1
                continue

            processed_ids.add(external_id)

            tipo_trans = str(row.get("TIPO DE TRANSAÇÃO") or "").strip()

            # valor líquido do MP (entrada real)
            val = row.get("VALOR LÍQUIDO DA TRANSAÇÃO")
            try:
                valor = float(val) if val == val else 0.0
            except Exception:
                valor = 0.0

            # mapeia tipo financeiro
            tipo_fin = "MP_NET"
            if "estorno" in tipo_trans.lower() or "chargeback" in tipo_trans.lower() or "devolu" in tipo_trans.lower() or "contestação" in tipo_trans.lower():
                tipo_fin = "REFUND"
                valor = -abs(valor) if valor != 0 else 0.0
            elif "retirada" in tipo_trans.lower() or "saque" in tipo_trans.lower() or "payouts" in tipo_trans.lower():
                tipo_fin = "WITHDRAWAL"
                valor = -abs(valor) if valor != 0 else 0.0
            elif "pagamento" in tipo_trans.lower():
                tipo_fin = "MP_NET"
                valor = abs(valor)  # garantir positivo para vendas

            # data do caixa: preferir liberação
            dt = _parse_iso_or_none(row.get("DATA DE LIBERAÇÃO DO DINHEIRO"))                  or _parse_iso_or_none(row.get("DATA DE APROVAÇÃO"))                  or _parse_iso_or_none(row.get("DATA DE ORIGEM"))                  or datetime.now()

            data_lancamento = dt.isoformat()

            canal = str(row.get("CANAL DE VENDA") or "").strip()
            descricao = f"{tipo_trans} - {canal}".strip(" -")

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

        ajustes = conn.execute(
            select(func.coalesce(func.sum(finance_transactions.c.valor), 0.0))
            .where(*(filtro + [finance_transactions.c.tipo == "ADJUSTMENT"]))
        ).scalar() or 0.0

        saldo_periodo = entradas_mp + devolucoes + retiradas + ajustes
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

        # MP: receita líquida financeira = MP_NET
        mp_liquida = conn.execute(
            select(func.coalesce(func.sum(finance_transactions.c.valor), 0.0))
            .where(*(filtro_f + [finance_transactions.c.tipo == "MP_NET"]))
        ).scalar() or 0.0

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
    """Página para converter código ZPL do Mercado Livre para PDF."""
    if request.method == "POST":
        zpl_code = request.form.get("zpl_code", "").strip()
        largura_cm = request.form.get("largura_cm", "4").strip()
        altura_cm = request.form.get("altura_cm", "2.5").strip()
        quantidade = request.form.get("quantidade", "1").strip()
        
        if not zpl_code:
            flash("Por favor, insira o código ZPL da etiqueta.", "danger")
            return redirect(url_for("etiquetas_zpl"))
        
        try:
            import requests
            import re
            
            # Converter cm para polegadas
            largura_inch = float(largura_cm) / 2.54
            altura_inch = float(altura_cm) / 2.54
            qtd = int(quantidade)
            size_str = f"{largura_inch:.1f}x{altura_inch:.1f}"
            
            # Modificar ZPL para quantidade 1
            zpl_sem_fim = re.sub(r'\^XZ\s*$', '', zpl_code)
            zpl_sem_pq = re.sub(r'\^PQ\d+[^\^]*', '', zpl_sem_fim)
            zpl_modificado = f"{zpl_sem_pq}\n^PQ1,0,1,Y^XZ"
            
            # Converter via API Labelary
            labelary_url = f"http://api.labelary.com/v1/printers/8dpmm/labels/{size_str}/0/"
            headers = {'Accept': 'application/pdf', 'Content-Type': 'application/x-www-form-urlencoded'}
            response = requests.post(labelary_url, data=zpl_modificado.encode('utf-8'), headers=headers)
            
            if response.status_code == 200:
                if qtd == 1:
                    pdf_buffer = BytesIO(response.content)
                    pdf_buffer.seek(0)
                    return send_file(
                        pdf_buffer,
                        mimetype='application/pdf',
                        as_attachment=True,
                        download_name=f'etiqueta_{largura_cm}x{altura_cm}cm_{datetime.now().strftime("%Y%m%d_%H%M%S")}.pdf'
                    )
                else:
                    # Replicar etiqueta usando pypdf
                    try:
                        from pypdf import PdfReader, PdfWriter
                    except ImportError:
                        from PyPDF2 import PdfReader, PdfWriter
                    
                    pdf_original = BytesIO(response.content)
                    reader = PdfReader(pdf_original)
                    writer = PdfWriter()
                    
                    page = reader.pages[0]
                    for _ in range(qtd):
                        writer.add_page(page)
                    
                    output_buffer = BytesIO()
                    writer.write(output_buffer)
                    output_buffer.seek(0)
                    
                    flash(f"PDF gerado com sucesso: {qtd} etiquetas!", "success")
                    return send_file(
                        output_buffer,
                        mimetype='application/pdf',
                        as_attachment=True,
                        download_name=f'etiquetas_{qtd}x_{largura_cm}x{altura_cm}cm_{datetime.now().strftime("%Y%m%d_%H%M%S")}.pdf'
                    )
            else:
                flash(f"Erro ao converter ZPL para PDF. Status: {response.status_code}", "danger")
                return redirect(url_for("etiquetas_zpl"))
                
        except Exception as e:
            flash(f"Erro ao processar etiqueta: {str(e)}", "danger")
            return redirect(url_for("etiquetas_zpl"))
    
    return render_template("etiquetas_zpl.html")


# ============================================================
# ROTA: Limpar Banco de Dados (apenas admin)
# ============================================================
@app.route("/limpar_dados", methods=["GET", "POST"])
@login_required
def limpar_dados():
    """Limpa produtos, vendas e transações do banco (mantém usuários)"""
    if request.method == "POST":
        confirmacao = request.form.get("confirmacao", "").strip()
        
        if confirmacao != "LIMPAR":
            flash("Digite 'LIMPAR' para confirmar a operação.", "warning")
            return redirect(url_for("limpar_dados"))
        
        try:
            with engine.begin() as conn:
                # Deletar na ordem correta (foreign keys)
                r1 = conn.execute(text("DELETE FROM finance_transactions"))
                r2 = conn.execute(text("DELETE FROM vendas"))
                r3 = conn.execute(text("DELETE FROM produtos"))
                
                total = r1.rowcount + r2.rowcount + r3.rowcount
                
                flash(f"✅ Banco limpo! {total} registros deletados (transações: {r1.rowcount}, vendas: {r2.rowcount}, produtos: {r3.rowcount})", "success")
                return redirect(url_for("dashboard"))
                
        except Exception as e:
            flash(f"Erro ao limpar banco: {str(e)}", "danger")
            return redirect(url_for("limpar_dados"))
    
    # GET - mostrar página de confirmação
    return render_template("limpar_dados.html")


# ============================================================
# INTEGRAÇÃO MERCADO LIVRE - API
# ============================================================

def get_ml_config():
    """Retorna configurações do Mercado Livre"""
    try:
        with engine.connect() as conn:
            config = conn.execute(select(configuracoes)).mappings().first()
            if config:
                return {
                    'client_id': config.get('ml_client_id'),
                    'client_secret': config.get('ml_client_secret'),
                    'access_token': config.get('ml_access_token'),
                    'refresh_token': config.get('ml_refresh_token'),
                    'token_expira': config.get('ml_token_expira'),
                    'user_id': config.get('ml_user_id'),
                }
    except Exception as e:
        print(f"Erro ao buscar config ML: {e}")
    return None

def save_ml_tokens(access_token, refresh_token, expires_in, user_id):
    """Salva tokens do ML no banco"""
    expira_em = datetime.now() + timedelta(seconds=expires_in)
    with engine.begin() as conn:
        config = conn.execute(select(configuracoes)).mappings().first()
        if config:
            conn.execute(
                update(configuracoes)
                .where(configuracoes.c.id == config['id'])
                .values(
                    ml_access_token=access_token,
                    ml_refresh_token=refresh_token,
                    ml_token_expira=expira_em.isoformat(),
                    ml_user_id=str(user_id),
                )
            )

def refresh_ml_token():
    """Renova o token de acesso do ML se expirou"""
    config = get_ml_config()
    if not config or not config['refresh_token']:
        return None
    
    # Verificar se expirou
    if config['token_expira']:
        expira = datetime.fromisoformat(config['token_expira'])
        if datetime.now() < expira:
            return config['access_token']  # Ainda válido
    
    # Renovar token
    url = "https://api.mercadolibre.com/oauth/token"
    data = {
        'grant_type': 'refresh_token',
        'client_id': config['client_id'],
        'client_secret': config['client_secret'],
        'refresh_token': config['refresh_token'],
    }
    
    response = requests.post(url, data=data)
    if response.status_code == 200:
        result = response.json()
        save_ml_tokens(
            result['access_token'],
            result['refresh_token'],
            result['expires_in'],
            result['user_id']
        )
        return result['access_token']
    
    return None


@app.route("/ml_configurar", methods=["GET", "POST"])
@login_required
def ml_configurar():
    """Configurar credenciais do Mercado Livre"""
    if request.method == "POST":
        client_id = request.form.get("client_id", "").strip()
        client_secret = request.form.get("client_secret", "").strip()
        
        if not client_id or not client_secret:
            flash("Preencha Client ID e Client Secret", "warning")
            return redirect(url_for("ml_configurar"))
        
        try:
            with engine.begin() as conn:
                config = conn.execute(select(configuracoes)).mappings().first()
                if config:
                    conn.execute(
                        update(configuracoes)
                        .where(configuracoes.c.id == config['id'])
                        .values(ml_client_id=client_id, ml_client_secret=client_secret)
                    )
                else:
                    conn.execute(
                        insert(configuracoes).values(
                            ml_client_id=client_id,
                            ml_client_secret=client_secret
                        )
                    )
            
            flash("Credenciais salvas! Agora conecte sua conta.", "success")
            return redirect(url_for("ml_conectar"))
            
        except Exception as e:
            flash(f"Erro ao salvar: {str(e)}", "danger")
            return redirect(url_for("ml_configurar"))
    
    config = get_ml_config()
    return render_template("ml_configurar.html", config=config)


@app.route("/ml_conectar")
@login_required
def ml_conectar():
    """Redireciona para OAuth do Mercado Livre"""
    try:
        config = get_ml_config()
        if not config or not config.get('client_id'):
            flash("Configure as credenciais primeiro", "warning")
            return redirect(url_for("ml_configurar"))
        
        redirect_uri = url_for("ml_callback", _external=True)
        auth_url = (
            f"https://auth.mercadolivre.com.br/authorization?"
            f"response_type=code&client_id={config['client_id']}&redirect_uri={redirect_uri}"
        )
        return redirect(auth_url)
    except Exception as e:
        flash(f"Erro ao conectar: {str(e)}", "danger")
        return redirect(url_for("ml_configurar"))


@app.route("/ml_callback")
@login_required
def ml_callback():
    """Callback do OAuth do Mercado Livre"""
    code = request.args.get("code")
    if not code:
        flash("Autorização cancelada", "warning")
        return redirect(url_for("ml_configurar"))
    
    config = get_ml_config()
    redirect_uri = url_for("ml_callback", _external=True)
    
    # Trocar código por access token
    url = "https://api.mercadolibre.com/oauth/token"
    data = {
        'grant_type': 'authorization_code',
        'client_id': config['client_id'],
        'client_secret': config['client_secret'],
        'code': code,
        'redirect_uri': redirect_uri,
    }
    
    response = requests.post(url, data=data)
    if response.status_code == 200:
        result = response.json()
        save_ml_tokens(
            result['access_token'],
            result['refresh_token'],
            result['expires_in'],
            result['user_id']
        )
        flash("✅ Conectado com sucesso ao Mercado Livre!", "success")
        return redirect(url_for("ml_sincronizar_view"))
    else:
        flash(f"Erro na autenticação: {response.text}", "danger")
        return redirect(url_for("ml_configurar"))


@app.route("/ml_sincronizar_view")
@login_required
def ml_sincronizar_view():
    """Página para sincronizar vendas do ML"""
    config = get_ml_config()
    return render_template("ml_sincronizar.html", config=config, now=datetime.now(), timedelta=timedelta)


@app.route("/ml_sincronizar", methods=["POST"])
@login_required
def ml_sincronizar():
    """Sincroniza vendas do Mercado Livre via API"""
    data_inicio = request.form.get("data_inicio")
    data_fim = request.form.get("data_fim")
    
    if not data_inicio or not data_fim:
        flash("Selecione o período", "warning")
        return redirect(url_for("ml_sincronizar_view"))
    
    try:
        access_token = refresh_ml_token()
        if not access_token:
            flash("Token expirado. Reconecte sua conta.", "danger")
            return redirect(url_for("ml_conectar"))
        
        config = get_ml_config()
        user_id = config['user_id']
        
        # Buscar vendas do período (OTIMIZADO para Render Free - 512MB)
        url = f"https://api.mercadolibre.com/orders/search"
        params = {
            'seller': user_id,
            'order.date_created.from': f"{data_inicio}T00:00:00.000-00:00",
            'order.date_created.to': f"{data_fim}T23:59:59.999-00:00",
            'limit': 10,  # Reduzido para economizar memória
        }
        headers = {'Authorization': f'Bearer {access_token}'}
        
        vendas_importadas = 0
        vendas_sem_produto = 0
        offset = 0
        MAX_VENDAS = 500  # Limite máximo para não estourar memória
        
        while vendas_importadas < MAX_VENDAS:
            params['offset'] = offset
            response = requests.get(url, params=params, headers=headers)
            
            if response.status_code != 200:
                flash(f"Erro ao buscar vendas: {response.text}", "danger")
                break
            
            data = response.json()
            orders = data.get('results', [])
            
            if not orders:
                break
            
            # Processar cada venda com commit imediato
            for order in orders:
                if vendas_importadas >= MAX_VENDAS:
                    break
                    
                with engine.begin() as conn:
                    for item in order.get('order_items', []):
                        sku = item['item'].get('seller_custom_field')
                        titulo = item['item']['title']
                        
                        # Buscar produto
                        produto_row = None
                        if sku:
                            produto_row = conn.execute(
                                select(produtos.c.id, produtos.c.custo_unitario)
                                .where(produtos.c.sku == sku)
                            ).mappings().first()
                        
                        if not produto_row and titulo:
                            produto_row = conn.execute(
                                select(produtos.c.id, produtos.c.custo_unitario)
                                .where(produtos.c.nome == titulo)
                            ).mappings().first()
                        
                        if not produto_row:
                            vendas_sem_produto += 1
                            continue
                        
                        # Extrair dados
                        produto_id = produto_row['id']
                        custo_unitario = float(produto_row['custo_unitario'] or 0)
                        quantidade = item['quantity']
                        preco_unitario = item['unit_price']
                        receita_total = item['full_unit_price'] * quantidade
                        
                        # Calcular comissão (aproximação 15%)
                        comissao_ml = receita_total * 0.15
                        
                        custo_total = custo_unitario * quantidade
                        margem = receita_total - custo_total - comissao_ml
                        
                        data_venda = order['date_created'][:10]
                        numero_venda = str(order['id'])
                        ml_order_id = f"{order['id']}_{item['item']['id']}"
                        
                        # Capturar status do pedido (confirmed, ready_to_ship, shipped, delivered, etc.)
                        # Importa TODOS os status: para envio, em trânsito e finalizadas
                        ml_status = order.get('status', 'unknown')
                        
                        # Verificar se já existe (evitar duplicação)
                        venda_existente = conn.execute(
                            select(vendas.c.id)
                            .where(vendas.c.ml_order_id == ml_order_id)
                        ).first()
                        
                        if venda_existente:
                            # Pular venda duplicada
                            continue
                        
                        # Inserir venda
                        conn.execute(
                            insert(vendas).values(
                                produto_id=produto_id,
                                data_venda=data_venda,
                                quantidade=quantidade,
                                preco_venda_unitario=preco_unitario,
                                receita_total=receita_total,
                                comissao_ml=comissao_ml,
                                custo_total=custo_total,
                                margem_contribuicao=margem,
                                origem="Mercado Livre API",
                                numero_venda_ml=numero_venda,
                                lote_importacao=f"API_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                                ml_order_id=ml_order_id,
                                ml_status=ml_status,
                            )
                        )
                        
                        # Atualizar estoque
                        conn.execute(
                            update(produtos)
                            .where(produtos.c.id == produto_id)
                            .values(estoque_atual=produtos.c.estoque_atual - quantidade)
                        )
                        
                        vendas_importadas += 1
                
                # Limpar memória após cada venda
                import gc
                gc.collect()
            
            # Próxima página
            if len(orders) < 10:
                break
            offset += 10
            
            # Feedback de progresso
            print(f"API ML: {vendas_importadas} vendas importadas...")
        
        flash(f"✅ Sincronizado! {vendas_importadas} vendas importadas, {vendas_sem_produto} sem produto", "success")
        return redirect(url_for("lista_vendas"))
        
    except Exception as e:
        flash(f"Erro na sincronização: {str(e)}", "danger")
        import traceback
        traceback.print_exc()
        return redirect(url_for("ml_sincronizar_view"))


@app.route("/ml_sincronizar_hoje", methods=["POST"])
@login_required
def ml_sincronizar_hoje():
    """Sincroniza apenas as vendas de hoje"""
    hoje = datetime.now().strftime('%Y-%m-%d')
    
    try:
        access_token = refresh_ml_token()
        if not access_token:
            flash("Token expirado. Reconecte sua conta.", "danger")
            return redirect(url_for("ml_conectar"))
        
        config = get_ml_config()
        user_id = config['user_id']
        
        # Buscar vendas de hoje
        url = f"https://api.mercadolibre.com/orders/search"
        params = {
            'seller': user_id,
            'order.date_created.from': f"{hoje}T00:00:00.000-00:00",
            'order.date_created.to': f"{hoje}T23:59:59.999-00:00",
            'limit': 50,
        }
        headers = {'Authorization': f'Bearer {access_token}'}
        
        vendas_importadas = 0
        vendas_sem_produto = 0
        offset = 0
        
        while True:
            params['offset'] = offset
            response = requests.get(url, params=params, headers=headers)
            
            if response.status_code != 200:
                flash(f"Erro ao buscar vendas: {response.text}", "danger")
                break
            
            data = response.json()
            orders = data.get('results', [])
            
            if not orders:
                break
            
            # Processar cada venda
            for order in orders:
                with engine.begin() as conn:
                    for item in order.get('order_items', []):
                        sku = item['item'].get('seller_custom_field')
                        titulo = item['item']['title']
                        
                        # Buscar produto
                        produto_row = None
                        if sku:
                            produto_row = conn.execute(
                                select(produtos.c.id, produtos.c.custo_unitario)
                                .where(produtos.c.sku == sku)
                            ).mappings().first()
                        
                        if not produto_row and titulo:
                            produto_row = conn.execute(
                                select(produtos.c.id, produtos.c.custo_unitario)
                                .where(produtos.c.nome == titulo)
                            ).mappings().first()
                        
                        if not produto_row:
                            vendas_sem_produto += 1
                            continue
                        
                        # Extrair dados
                        produto_id = produto_row['id']
                        custo_unitario = float(produto_row['custo_unitario'] or 0)
                        quantidade = item['quantity']
                        preco_unitario = item['unit_price']
                        receita_total = item['full_unit_price'] * quantidade
                        comissao_ml = receita_total * 0.15
                        custo_total = custo_unitario * quantidade
                        margem = receita_total - custo_total - comissao_ml
                        
                        data_venda = order['date_created'][:10]
                        numero_venda = str(order['id'])
                        ml_order_id = f"{order['id']}_{item['item']['id']}"
                        ml_status = order.get('status', 'unknown')
                        
                        # Verificar se já existe
                        venda_existente = conn.execute(
                            select(vendas.c.id)
                            .where(vendas.c.ml_order_id == ml_order_id)
                        ).first()
                        
                        if venda_existente:
                            continue
                        
                        # Inserir venda
                        conn.execute(
                            insert(vendas).values(
                                produto_id=produto_id,
                                data_venda=data_venda,
                                quantidade=quantidade,
                                preco_venda_unitario=preco_unitario,
                                receita_total=receita_total,
                                comissao_ml=comissao_ml,
                                custo_total=custo_total,
                                margem_contribuicao=margem,
                                origem="Mercado Livre API",
                                numero_venda_ml=numero_venda,
                                lote_importacao=f"DIA_{hoje}",
                                ml_order_id=ml_order_id,
                                ml_status=ml_status,
                            )
                        )
                        
                        # Atualizar estoque
                        conn.execute(
                            update(produtos)
                            .where(produtos.c.id == produto_id)
                            .values(estoque_atual=produtos.c.estoque_atual - quantidade)
                        )
                        
                        vendas_importadas += 1
            
            if len(orders) < 50:
                break
            offset += 50
        
        flash(f"✅ Hoje: {vendas_importadas} vendas importadas, {vendas_sem_produto} sem produto", "success")
        return redirect(url_for("lista_vendas"))
        
    except Exception as e:
        flash(f"Erro na sincronização: {str(e)}", "danger")
        return redirect(url_for("ml_sincronizar_view"))


def sincronizar_automaticamente():
    """Função executada pelo scheduler a cada 5 minutos"""
    with app.app_context():
        try:
            with engine.connect() as conn:
                config_row = conn.execute(
                    select(configuracoes)
                ).mappings().first()
                
                if not config_row or config_row.get('ml_sync_auto') != 'true':
                    return
                
                if not config_row.get('access_token'):
                    return
            
            # Sincronizar hoje
            access_token = refresh_ml_token()
            if not access_token:
                print("⚠️ Sync auto: token inválido")
                return
            
            with engine.connect() as conn:
                config_row = conn.execute(
                    select(configuracoes)
                ).mappings().first()
                
                user_id = config_row['ml_user_id']
            
            hoje = datetime.now().strftime('%Y-%m-%d')
            url = f"https://api.mercadolibre.com/orders/search"
            params = {
                'seller': user_id,
                'order.date_created.from': f"{hoje}T00:00:00.000-00:00",
                'order.date_created.to': f"{hoje}T23:59:59.999-00:00",
                'limit': 50,
            }
            headers = {'Authorization': f'Bearer {access_token}'}
            
            vendas_importadas = 0
            offset = 0
            
            while offset < 200:  # Limite de segurança
                params['offset'] = offset
                response = requests.get(url, params=params, headers=headers, timeout=10)
                
                if response.status_code != 200:
                    break
                
                data = response.json()
                orders = data.get('results', [])
                
                if not orders:
                    break
                
                for order in orders:
                    with engine.begin() as conn:
                        for item in order.get('order_items', []):
                            sku = item['item'].get('seller_custom_field')
                            titulo = item['item']['title']
                            
                            produto_row = None
                            if sku:
                                produto_row = conn.execute(
                                    select(produtos.c.id, produtos.c.custo_unitario)
                                    .where(produtos.c.sku == sku)
                                ).mappings().first()
                            
                            if not produto_row and titulo:
                                produto_row = conn.execute(
                                    select(produtos.c.id, produtos.c.custo_unitario)
                                    .where(produtos.c.nome == titulo)
                                ).mappings().first()
                            
                            if not produto_row:
                                continue
                            
                            produto_id = produto_row['id']
                            custo_unitario = float(produto_row['custo_unitario'] or 0)
                            quantidade = item['quantity']
                            preco_unitario = item['unit_price']
                            receita_total = item['full_unit_price'] * quantidade
                            comissao_ml = receita_total * 0.15
                            custo_total = custo_unitario * quantidade
                            margem = receita_total - custo_total - comissao_ml
                            
                            data_venda = order['date_created'][:10]
                            numero_venda = str(order['id'])
                            ml_order_id = f"{order['id']}_{item['item']['id']}"
                            ml_status = order.get('status', 'unknown')
                            
                            venda_existente = conn.execute(
                                select(vendas.c.id)
                                .where(vendas.c.ml_order_id == ml_order_id)
                            ).first()
                            
                            if venda_existente:
                                continue
                            
                            conn.execute(
                                insert(vendas).values(
                                    produto_id=produto_id,
                                    data_venda=data_venda,
                                    quantidade=quantidade,
                                    preco_venda_unitario=preco_unitario,
                                    receita_total=receita_total,
                                    comissao_ml=comissao_ml,
                                    custo_total=custo_total,
                                    margem_contribuicao=margem,
                                    origem="Mercado Livre API Auto",
                                    numero_venda_ml=numero_venda,
                                    lote_importacao=f"AUTO_{datetime.now().strftime('%Y%m%d_%H%M')}",
                                    ml_order_id=ml_order_id,
                                    ml_status=ml_status,
                                )
                            )
                            
                            conn.execute(
                                update(produtos)
                                .where(produtos.c.id == produto_id)
                                .values(estoque_atual=produtos.c.estoque_atual - quantidade)
                            )
                            
                            vendas_importadas += 1
                
                if len(orders) < 50:
                    break
                offset += 50
            
            # Atualizar último sync
            with engine.begin() as conn:
                conn.execute(
                    update(configuracoes)
                    .values(ml_ultimo_sync=datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                )
            
            print(f"✅ Sync automática: {vendas_importadas} vendas importadas")
            
        except Exception as e:
            print(f"⚠️ Erro sync automática: {e}")


@app.route("/ml_toggle_sync_auto", methods=["POST"])
@login_required
def ml_toggle_sync_auto():
    """Ativa/desativa sincronização automática"""
    ativar = request.form.get("ativar") == "true"
    
    with engine.begin() as conn:
        conn.execute(
            update(configuracoes)
            .values(ml_sync_auto='true' if ativar else 'false')
        )
    
    if ativar:
        flash("✅ Sincronização automática ativada! Buscará vendas a cada 5 minutos.", "success")
    else:
        flash("⏸️ Sincronização automática desativada.", "info")
    
    return redirect(url_for("ml_sincronizar_view"))


# Iniciar scheduler
scheduler = BackgroundScheduler()
scheduler.add_job(func=sincronizar_automaticamente, trigger="interval", minutes=5, id='ml_sync')
scheduler.start()

# Garantir que o scheduler seja desligado ao encerrar
import atexit
atexit.register(lambda: scheduler.shutdown())


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
