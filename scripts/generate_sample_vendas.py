import os
from openpyxl import Workbook

os.makedirs('uploads', exist_ok=True)
path = os.path.join('uploads', 'sample_vendas.xlsx')
wb = Workbook()
ws = wb.active
ws.title = 'Vendas BR'

# Add 5 empty rows so pandas.read_excel(..., header=5) finds header on 6th row
for _ in range(5):
    ws.append([])

# Header row (6th)
headers = [
    'N.º de venda', 'SKU', 'Título do anúncio', 'Data da venda', 'Unidades',
    'Receita por produtos (BRL)', 'Tarifa de venda e impostos (BRL)', 'Status', 'Status do envio', 'Preço', 'Estado'
]
ws.append(headers)

# Sample data rows
ws.append(['ML12345', 'TESTSKU001', 'Produto de Teste', '2025-12-30', 1, 100.0, 10.0, 'completed', 'shipped', 100.0, 'SP'])
ws.append(['ML12346', '', 'Produto Sem SKU', '2025-12-30', 2, 200.0, 20.0, 'completed', 'shipped', 100.0, 'RJ'])

wb.save(path)
print('Sample file created:', path)
