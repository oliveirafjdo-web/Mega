"""Verificações rápidas de rotas.

Este script importa o módulo `app` (do projeto) e lista as rotas
registradas em `app.app.view_functions` e `app.app.url_map`.
"""

import importlib
import sys

sys.path.insert(0, r"C:\Users\Gabriel Guedes\Downloads\erp-metrifiy-repo\erp-metrifiy-repo - Copia")
app = importlib.import_module('app')

print('App carregado:', getattr(app, 'app', None))
print('Total de view_functions:', len(app.app.view_functions))

print('\n--- Endpoints (view_functions keys) ---')
for k in sorted(app.app.view_functions.keys()):
    print(k)

print('\n--- Regras de URL (url_map) ---')
for rule in sorted(app.app.url_map.iter_rules(), key=lambda r: r.rule):
    print(f'{rule.rule} -> endpoint: {rule.endpoint}  methods: {sorted(rule.methods)}')

print('\nTem endpoint `transferir_estoque`?:', 'transferir_estoque' in app.app.view_functions)
print('Regra para /estoque/transferir:' , next((r for r in app.app.url_map.iter_rules() if r.rule == '/estoque/transferir'), None))
