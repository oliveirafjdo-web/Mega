"""
Script otimizado para importar dados com baixo uso de memória
"""
import os
import json
import gc
from sqlalchemy import MetaData, inspect, text

def auto_import_data_if_empty(engine):
    """
    Verifica se o banco está vazio e importa dados automaticamente
    Otimizado para ambientes com pouca memória (Render Free = 512MB)
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
        print("📦 Banco vazio detectado. Iniciando importação otimizada...")
        
        json_file = "data_export.json"
        if not os.path.exists(json_file):
            print(f"❌ Arquivo {json_file} não encontrado")
            return False
        
        # Importar apenas tabelas pequenas primeiro (prioridade)
        priority_tables = ["usuarios", "configuracoes", "produtos", "ajustes_estoque"]
        
        print(f"📂 Carregando dados prioritários...")
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        metadata = MetaData()
        metadata.reflect(bind=engine)
        
        imported_count = 0
        
        # Importar tabelas prioritárias primeiro
        for table_name in priority_tables:
            if table_name not in data or not data[table_name]:
                continue
            
            if table_name not in metadata.tables:
                continue
            
            rows = data[table_name]
            table = metadata.tables[table_name]
            
            print(f"  → {table_name}: {len(rows)} registros...")
            
            try:
                with engine.begin() as conn:
                    # Lotes pequenos para economizar memória
                    batch_size = 100
                    for i in range(0, len(rows), batch_size):
                        batch = rows[i:i+batch_size]
                        conn.execute(table.insert(), batch)
                        gc.collect()  # Liberar memória
                
                imported_count += len(rows)
                print(f"    ✓ Importado")
                
            except Exception as e:
                print(f"    ❌ Erro: {e}")
        
        # Liberar memória das tabelas prioritárias
        for table_name in priority_tables:
            if table_name in data:
                del data[table_name]
        gc.collect()
        
        # Importar tabelas grandes (vendas e transações) em lotes muito pequenos
        large_tables = ["vendas", "finance_transactions"]
        
        for table_name in large_tables:
            if table_name not in data or not data[table_name]:
                continue
            
            if table_name not in metadata.tables:
                continue
            
            rows = data[table_name]
            total = len(rows)
            table = metadata.tables[table_name]
            
            print(f"  → {table_name}: {total} registros (lotes de 50)...")
            
            try:
                # Lotes MUITO pequenos para tabelas grandes
                batch_size = 50
                batches_done = 0
                
                for i in range(0, total, batch_size):
                    batch = rows[i:i+batch_size]
                    
                    with engine.begin() as conn:
                        conn.execute(table.insert(), batch)
                    
                    batches_done += 1
                    
                    # Progresso e limpeza de memória a cada 10 lotes
                    if batches_done % 10 == 0:
                        progress = min(i + batch_size, total)
                        print(f"    ... {progress}/{total}")
                        gc.collect()
                
                imported_count += total
                print(f"    ✓ {total} registros importados")
                
            except Exception as e:
                print(f"    ❌ Erro: {e}")
                # Continuar mesmo se falhar
        
        # Limpar dados da memória
        data.clear()
        gc.collect()
        
        print(f"\n✅ Importação concluída! Total: {imported_count} registros")
        return True
        
    except Exception as e:
        print(f"❌ Erro na importação: {e}")
        import traceback
        traceback.print_exc()
        return False
