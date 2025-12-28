import importlib, sys
sys.path.insert(0, r"C:\Users\Gabriel Guedes\Downloads\erp-metrifiy-repo\erp-metrifiy-repo - Copia")
app = importlib.import_module('app')
print('App:', getattr(app, 'app', None))
print('Number of rules:', len(list(app.app.url_map.iter_rules())))
for rule in sorted(app.app.url_map.iter_rules(), key=lambda r: r.rule):
    print(f'{rule.rule} -> endpoint: {rule.endpoint} methods: {sorted(rule.methods)}')
print('\nHas transferir_estoque endpoint?:', 'transferir_estoque' in app.app.view_functions)
print('Rule object for /estoque/transferir:', next((r for r in app.app.url_map.iter_rules() if r.rule == '/estoque/transferir'), None))
