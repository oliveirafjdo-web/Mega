import sys
import json
import os
from datetime import datetime

# This script runs the import in a separate process to avoid sharing DB connections
# Usage: python scripts/import_worker.py <caminho_arquivo> <lote_id>

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Usage: import_worker.py <caminho_arquivo> <lote_id>')
        sys.exit(2)
    caminho = sys.argv[1]
    lote = sys.argv[2]
    status_path = os.path.join('uploads', f'import_status_{lote.replace(":","-")}.json')
    try:
        # import here to avoid heavy imports in the parent process
        from app import engine, importar_vendas_ml
        resumo = importar_vendas_ml(caminho, engine, lote_id=lote)
        result = {"ok": True, "resumo": resumo, "finished_at": datetime.now().isoformat()}
        with open(status_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False)
        print('Import finished:', resumo)
    except Exception as e:
        err = {"ok": False, "error": str(e), "failed_at": datetime.now().isoformat()}
        try:
            with open(status_path, 'w', encoding='utf-8') as f:
                json.dump(err, f, ensure_ascii=False)
        except Exception:
            pass
        print('Import error:', e)
        sys.exit(1)
