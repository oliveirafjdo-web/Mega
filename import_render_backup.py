#!/usr/bin/env python
"""Importa um pacote ZIP gerado na tela /admin/backup para um banco Postgres (Render)."""

import argparse
import json
import os
import zipfile
from pathlib import Path

import pandas as pd
from sqlalchemy import MetaData, create_engine, text


def normalize_db_url(url: str) -> str:
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql+psycopg2://", 1)
    return url


def carregar_export(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")

    with zipfile.ZipFile(path, "r") as zf:
        try:
            manifest = json.loads(zf.read("manifest.json"))
        except KeyError as exc:
            raise RuntimeError("manifest.json não encontrado no ZIP gerado pelo sistema") from exc

        dfs = {}
        for name in zf.namelist():
            if not name.endswith(".csv"):
                continue
            table_name = name.replace(".csv", "")
            with zf.open(name) as f:
                dfs[table_name] = pd.read_csv(f)
    return manifest, dfs


def importar_para_postgres(engine, dfs):
    meta = MetaData()
    meta.reflect(engine)

    ordem_delete = [
        "vendas",
        "ajustes_estoque",
        "finance_transactions",
        "produtos",
        "configuracoes",
        "usuarios",
    ]
    ordem_insert = [
        "usuarios",
        "configuracoes",
        "produtos",
        "ajustes_estoque",
        "vendas",
        "finance_transactions",
    ]

    with engine.begin() as conn:
        for table_name in ordem_delete:
            if table_name in meta.tables:
                conn.execute(meta.tables[table_name].delete())
                print(f"Apagado conteúdo de {table_name}")

        for table_name in ordem_insert:
            if table_name in meta.tables and table_name in dfs:
                registros = dfs[table_name].to_dict(orient="records")
                if registros:
                    conn.execute(meta.tables[table_name].insert(), registros)
                    print(f"Inseridas {len(registros)} linhas em {table_name}")

        if engine.url.get_backend_name().startswith("postgresql"):
            for table_name in ["usuarios", "produtos", "vendas", "ajustes_estoque", "finance_transactions"]:
                if table_name in meta.tables and "id" in meta.tables[table_name].c:
                    conn.execute(
                        text(
                            f"SELECT setval(pg_get_serial_sequence('{table_name}', 'id'), "
                            f"COALESCE((SELECT MAX(id)+1 FROM {table_name}), 1), false)"
                        )
                    )
                    print(f"Sequência ajustada para {table_name}")


def main():
    parser = argparse.ArgumentParser(description="Importa backup exportado para o banco Postgres")
    parser.add_argument("export_zip", help="Caminho do arquivo ZIP gerado em /admin/backup")
    parser.add_argument(
        "--database-url",
        dest="database_url",
        help="DATABASE_URL do Postgres (senão usa variável de ambiente)",
    )
    args = parser.parse_args()

    db_url = args.database_url or os.environ.get("DATABASE_URL")
    if not db_url:
        raise SystemExit("Defina DATABASE_URL ou passe --database-url")

    db_url = normalize_db_url(db_url)
    export_path = Path(args.export_zip)

    manifest, dfs = carregar_export(export_path)
    print("Manifest carregado:", json.dumps(manifest, ensure_ascii=False, indent=2))

    engine = create_engine(db_url, future=True)
    importar_para_postgres(engine, dfs)
    print("Importação concluída com sucesso.")


if __name__ == "__main__":
    main()
