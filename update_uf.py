import argparse
import pandas as pd
import re
from pathlib import Path

STATE_TO_SIGLA = {
    'acre': 'AC', 'alagoas': 'AL', 'amapa': 'AP', 'amapá': 'AP', 'amazonas': 'AM',
    'bahia': 'BA', 'ceara': 'CE', 'ceará': 'CE', 'distrito federal': 'DF',
    'espirito santo': 'ES', 'espírito santo': 'ES', 'goias': 'GO', 'goiás': 'GO',
    'maranhao': 'MA', 'maranhão': 'MA', 'mato grosso': 'MT', 'mato grosso do sul': 'MS',
    'minas gerais': 'MG', 'para': 'PA', 'pará': 'PA', 'paraiba': 'PB', 'paraíba': 'PB',
    'parana': 'PR', 'paraná': 'PR', 'pernambuco': 'PE', 'piaui': 'PI', 'piauí': 'PI',
    'rio de janeiro': 'RJ', 'rio grande do norte': 'RN', 'rio grande do sul': 'RS',
    'rondonia': 'RO', 'rondônia': 'RO', 'roraima': 'RR', 'santa catarina': 'SC',
    'sao paulo': 'SP', 'são paulo': 'SP', 'sergipe': 'SE', 'tocantins': 'TO'
}
SIGLAS_VALIDAS = set(STATE_TO_SIGLA.values())

REPLACEMENTS = {'á':'a','à':'a','ã':'a','â':'a','é':'e','ê':'e','í':'i','ó':'o','ô':'o','õ':'o','ú':'u','ç':'c'}


def normalize_uf(value):
    if pd.isna(value):
        return None
    s = str(value).strip()
    if not s:
        return None
    # já é sigla
    if len(s) == 2 and s.upper() in SIGLAS_VALIDAS:
        return s.upper()
    key = s.lower()
    if key in STATE_TO_SIGLA:
        return STATE_TO_SIGLA[key]
    # remove acentos simples
    key2 = ''.join(REPLACEMENTS.get(ch, ch) for ch in key)
    if key2 in STATE_TO_SIGLA:
        return STATE_TO_SIGLA[key2]
    # tentar última palavra (ex: 'Estado de São Paulo' -> 'sao paulo')
    parts = key2.split()
    for i in range(len(parts)):
        candidate = ' '.join(parts[i:])
        if candidate in STATE_TO_SIGLA:
            return STATE_TO_SIGLA[candidate]
    # extrair só letras e pegar 2 primeiras como fallback
    letters = re.sub(r'[^A-Za-z]', '', s)
    if len(letters) >= 2:
        return letters[:2].upper()
    return s.upper()


def find_uf_column(columns):
    candidates = ["UF", "Estado", "Estado do comprador", "Estado do Cliente", "estado", "uf"]
    cols_lower = {c.lower(): c for c in columns}
    for cand in candidates:
        if cand.lower() in cols_lower:
            return cols_lower[cand.lower()]
    # fallback: try any column named 'estado' variations
    for c in columns:
        if 'estado' in c.lower() or c.lower() == 'uf':
            return c
    return None


def process_file(path: Path, sheet_name: str | None, out_path: Path | None, report_path: Path | None):
    xls = pd.ExcelFile(path, engine='openpyxl')
    sheet = sheet_name if sheet_name in xls.sheet_names else xls.sheet_names[0] if sheet_name is None else sheet_name
    df = pd.read_excel(path, sheet_name=sheet, engine='openpyxl')

    uf_col = find_uf_column(df.columns)
    report_rows = []
    if not uf_col:
        print(f"Nenhuma coluna de UF encontrada em {path}. Nenhuma alteração será feita.")
    else:
        print(f"Coluna detectada para UF: '{uf_col}' (sheet: {sheet})")
        orig_values = df[uf_col].fillna('').astype(str)
        new_values = []
        for v in orig_values:
            norm = normalize_uf(v)
            new_values.append(norm)
            if norm is None or (len(norm) != 2 or norm not in SIGLAS_VALIDAS):
                report_rows.append({'original': v, 'converted': norm})
        df[uf_col] = new_values

    # write output
    out_file = out_path or (path.parent / (path.stem + '_uf_fixed' + path.suffix))
    df.to_excel(out_file, index=False, sheet_name=sheet, engine='openpyxl')
    print(f"Arquivo salvo: {out_file}")

    if report_path:
        rpt = pd.DataFrame(report_rows)
        rpt.to_csv(report_path, index=False)
        print(f"Relatório salvo: {report_path}")
    else:
        if report_rows:
            print(f"Foram encontrados {len(report_rows)} valores não reconhecidos. Use --report para salvar um CSV com detalhes.")
        else:
            print("Todos os valores foram convertidos para siglas válidas.")


def main():
    p = argparse.ArgumentParser(description='Corrige coluna UF em planilhas Excel (converte nomes de estados para siglas).')
    p.add_argument('file', help='Caminho para o arquivo Excel (.xlsx)')
    p.add_argument('--sheet', help='Nome da planilha (opcional)')
    p.add_argument('--out', help='Caminho do arquivo de saída (opcional)')
    p.add_argument('--report', help='Caminho do CSV de relatório (opcional)')
    args = p.parse_args()

    path = Path(args.file)
    if not path.exists():
        print('Arquivo não encontrado:', path)
        return
    out_path = Path(args.out) if args.out else None
    report_path = Path(args.report) if args.report else None

    process_file(path, args.sheet, out_path, report_path)


if __name__ == '__main__':
    main()
