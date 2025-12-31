import sys
import os
import pandas as pd
import unicodedata

BRAZIL_UF = set(["AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS","MG","PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC","SP","SE","TO"])


def normalize_col(s):
    if s is None:
        return ''
    s = str(s)
    s = unicodedata.normalize('NFKD', s)
    return s.strip()


def guess_column(df, candidates):
    # return first column whose normalized name contains any candidate token
    for col in df.columns:
        name = normalize_col(col).lower()
        for c in candidates:
            if c in name:
                return col
    return None


def find_best_uf_column(df):
    best_col = None
    best_score = 0.0
    for col in df.columns:
        vals = df[col].dropna().astype(str).str.strip()
        if vals.empty:
            continue
        # compute fraction of values that look like UF
        def is_uf(v):
            v = v.strip().upper()
            return v in BRAZIL_UF
        score = vals.apply(is_uf).mean()
        if score > best_score:
            best_score = score
            best_col = col
    return best_col, best_score


def main():
    if len(sys.argv) < 2:
        print('Usage: preprocess_settlement.py <xlsx-path> [output-path]')
        sys.exit(2)

    path = sys.argv[1]
    out = sys.argv[2] if len(sys.argv) > 2 else None

    if not os.path.exists(path):
        print('File not found:', path)
        sys.exit(1)

    xls = pd.ExcelFile(path)
    # try to find sheet with name containing 'vendas' first
    sheet_name = None
    for s in xls.sheet_names:
        if 'vendas' in s.lower():
            sheet_name = s
            break
    if sheet_name is None:
        sheet_name = xls.sheet_names[0]

    print('Reading sheet:', sheet_name)

    # Read without header to detect where the real table starts
    raw = pd.read_excel(xls, sheet_name=sheet_name, header=None, engine='openpyxl')

    header_row = None
    import re
    header_tokens = ['n.º de venda', 'nº de venda', 'numero de venda', 'n. de venda', 'nº de venda', 'n. venda', 'número de venda']
    # scan rows for a header containing expected tokens
    for idx in range(min(120, len(raw))):
        rowvals = raw.iloc[idx].astype(str).str.lower().fillna('')
        joined = ' '.join(rowvals.tolist())
        for t in header_tokens:
            if t in joined:
                header_row = idx
                break
        if header_row is not None:
            break

    # If not found, try heuristic: find first row where many cells look like order numbers (long integers)
    if header_row is None:
        order_re = re.compile(r"^\s*\d{6,}\s*$")
        for idx in range(min(200, len(raw))):
            rowvals = raw.iloc[idx].astype(str).fillna('')
            numeric_count = sum(1 for v in rowvals if isinstance(v, str) and order_re.match(v))
            if numeric_count >= 2:  # likely a data row; header is previous non-empty row
                # try to find a plausible header above
                for j in range(max(0, idx-6), idx+1):
                    jr = raw.iloc[j].astype(str).str.lower()
                    if any('n.' in str(x) or 'nº' in str(x) or 'venda' in str(x) for x in jr):
                        header_row = j
                        break
                if header_row is None:
                    header_row = max(0, idx-1)
                break

    if header_row is None:
        header_row = 0

    print('Detected header row index:', header_row)
    df = pd.read_excel(xls, sheet_name=sheet_name, header=header_row, engine='openpyxl')

    # Guess columns
    col_n_venda = guess_column(df, ['n.º de venda', 'nº de venda', 'numero de venda', 'n. de venda', 'n. venda', 'n. venda ml', 'nº de venda'])
    col_sku = guess_column(df, ['sku'])
    col_title = guess_column(df, ['título', 'titulo', 'anúncio', 'anuncio', 'produto'])
    col_data = guess_column(df, ['data da venda', 'data', 'data_venda', 'data venda'])
    col_unidades = guess_column(df, ['unidades', 'quantidade', 'quant.'])
    col_receita = guess_column(df, ['receita por produtos', 'receita', 'total (brl)', 'receita por produtos (brl)'])
    col_tarifa = guess_column(df, ['tarifa de venda', 'tarifa', 'taxa', 'tarifa de venda e impostos'])
    col_status = guess_column(df, ['status do envio', 'status envio', 'status'])
    col_status_envio = guess_column(df, ['status do envio', 'status envio'])

    uf_col, uf_score = find_best_uf_column(df)

    print('Guessed columns:')
    print(' N.º de venda ->', col_n_venda)
    print(' SKU ->', col_sku)
    print(' Título ->', col_title)
    print(' Data ->', col_data)
    print(' Unidades ->', col_unidades)
    print(' Receita ->', col_receita)
    print(' Tarifa ->', col_tarifa)
    print(' Status ->', col_status)
    print(' UF best ->', uf_col, 'score', uf_score)

    # Build cleaned DF with expected importer columns
    cols_expected = [
        'N.º de venda','SKU','Título do anúncio','Data da venda','Unidades',
        'Receita por produtos (BRL)','Tarifa de venda e impostos (BRL)','Status','Status do envio','Preço','Estado'
    ]

    clean = pd.DataFrame()
    clean['N.º de venda'] = df[col_n_venda] if col_n_venda in df.columns else df.iloc[:,0]
    clean['SKU'] = df[col_sku] if col_sku in df.columns else ''
    clean['Título do anúncio'] = df[col_title] if col_title in df.columns else ''
    clean['Data da venda'] = df[col_data] if col_data in df.columns else ''
    clean['Unidades'] = df[col_unidades] if col_unidades in df.columns else 1
    clean['Receita por produtos (BRL)'] = df[col_receita] if col_receita in df.columns else 0.0
    clean['Tarifa de venda e impostos (BRL)'] = df[col_tarifa] if col_tarifa in df.columns else 0.0
    clean['Status'] = df[col_status] if col_status in df.columns else ''
    clean['Status do envio'] = df[col_status_envio] if col_status_envio in df.columns else ''
    # Price fallback
    price_col = guess_column(df, ['preço unitário', 'preco unitario', 'preço', 'preco'])
    clean['Preço'] = df[price_col] if price_col in df.columns else ''

    # Estado: prefer the best UF column by score; otherwise try known names
    if uf_col and uf_score > 0.05:
        clean['Estado'] = df[uf_col]
    else:
        guessed = guess_column(df, ['estado','uf','state'])
        clean['Estado'] = df[guessed] if guessed in df.columns else ''

    # Normalize strings
    for c in clean.columns:
        if clean[c].dtype == object:
            clean[c] = clean[c].astype(str).str.strip()

    # Write to new xlsx with 5 empty rows and header at 6th
    base = os.path.basename(path)
    name = os.path.splitext(base)[0]
    outname = out if out else os.path.join('uploads', f'cleaned_{name}.xlsx')

    from openpyxl import Workbook
    wb = Workbook()
    ws = wb.active
    ws.title = 'Vendas BR'

    # add 5 empty rows
    for _ in range(5):
        ws.append([])
    # header
    ws.append(list(clean.columns))
    # data
    for _, row in clean.iterrows():
        ws.append([row[col] for col in clean.columns])

    wb.save(outname)
    print('Cleaned file saved to', outname)


if __name__ == '__main__':
    main()
