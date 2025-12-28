#!/usr/bin/env python3
"""
Script para limpar valores absurdamente grandes do banco de dados
"""
import sqlite3
import os

db_path = "metrifiy.db"

if not os.path.exists(db_path):
    print(f"❌ Banco de dados não encontrado: {db_path}")
    exit(1)

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Busca valores problemáticos (menores que -1 bilhão ou maiores que 10 bilhões)
cursor.execute("""
    SELECT id, descricao, valor FROM finance_transactions 
    WHERE valor < -1000000000 OR valor > 10000000000
""")

problematicos = cursor.fetchall()

if problematicos:
    print(f"⚠️ Encontrados {len(problematicos)} valores problemáticos:")
    for id_trans, desc, valor in problematicos:
        print(f"  ID {id_trans}: {desc} = R$ {valor:,.2f}")
    
    # Deletar os valores problemáticos
    print("\n🗑️ Deletando valores problemáticos...")
    cursor.execute("""
        DELETE FROM finance_transactions 
        WHERE valor < -1000000000 OR valor > 10000000000
    """)
    conn.commit()
    print(f"✅ {cursor.rowcount} registros deletados")
else:
    print("✅ Nenhum valor problemático encontrado")

conn.close()
