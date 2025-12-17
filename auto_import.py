"""
Script para importar dados automaticamente na inicialização do app
"""
import os
import json
from sqlalchemy import MetaData, inspect

def auto_import_data_if_empty(engine):
    """
    Verifica se o banco está vazio e importa dados automaticamente
    """
    try:
        # Verificar se as tabelas existem e estão vazias
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        
        if not tables:
            print("⚠️ Nenhuma tabela encontrada. Criando estrutura...")
            return False
        
        # Verificar se há dados nas tabelas principais
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM produtos")).scalar()
            if result > 0:
                print(f"✅ Banco já possui dados ({result} produtos encontrados)")
                return True
        
        # Se chegou aqui, precisa importar
        print("📦 Banco vazio detectado. Iniciando importação automática...")
        
        json_file = "data_export.json"
        if not os.path.exists(json_file):
            print(f"❌ Arquivo {json_file} não encontrado")
            return False
        
        print(f"📂 Carregando dados de: {json_file}")
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        metadata = MetaData()
        metadata.reflect(bind=engine)
        
        imported_count = 0
        with engine.begin() as conn:
            for table_name, rows in data.items():
                if not rows:
                    continue
                
                if table_name not in metadata.tables:
                    print(f"⚠️ Tabela {table_name} não existe")
                    continue
                
                table = metadata.tables[table_name]
                
                print(f"  → Importando {table_name}: {len(rows)} registros...")
                
                try:
                    # Inserir em lotes
                    batch_size = 500
                    for i in range(0, len(rows), batch_size):
                        batch = rows[i:i+batch_size]
                        conn.execute(table.insert(), batch)
                    
                    imported_count += len(rows)
                    print(f"    ✓ {len(rows)} registros importados")
                    
                except Exception as e:
                    print(f"    ❌ Erro ao importar {table_name}: {e}")
        
        print(f"\n✅ Importação concluída! Total: {imported_count} registros")
        return True
        
    except Exception as e:
        print(f"❌ Erro na importação automática: {e}")
        import traceback
        traceback.print_exc()
        return False

# Importar text do sqlalchemy
from sqlalchemy import text
