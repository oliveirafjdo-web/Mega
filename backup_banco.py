#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Script para fazer backup do banco de dados"""

import os
import shutil
from datetime import datetime
from pathlib import Path

def fazer_backup():
    """Cria backup do banco de dados SQLite"""
    
    db_file = 'metrifiy.db'
    backup_dir = 'backups'
    
    # Criar diretório de backups se não existir
    os.makedirs(backup_dir, exist_ok=True)
    
    if os.path.exists(db_file):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = os.path.join(backup_dir, f'metrifiy_backup_{timestamp}.db')
        
        try:
            shutil.copy2(db_file, backup_file)
            tamanho = os.path.getsize(backup_file) / (1024*1024)  # MB
            print(f"✅ Backup criado: {backup_file} ({tamanho:.2f} MB)")
            
            # Manter apenas os últimos 10 backups
            arquivos = sorted(Path(backup_dir).glob('metrifiy_backup_*.db'))
            if len(arquivos) > 10:
                for arquivo in arquivos[:-10]:
                    arquivo.unlink()
                    print(f"🗑️ Backup antigo removido: {arquivo.name}")
            
            return backup_file
        except Exception as e:
            print(f"❌ Erro ao fazer backup: {e}")
            return None
    else:
        print(f"❌ Arquivo de banco de dados não encontrado: {db_file}")
        return None

if __name__ == "__main__":
    fazer_backup()
