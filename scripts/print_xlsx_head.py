import sys
import pandas as pd
path = sys.argv[1]
n = int(sys.argv[2]) if len(sys.argv) > 2 else 20
try:
    df = pd.read_excel(path, engine='openpyxl')
    pd.set_option('display.max_columns', None)
    print(df.head(n).to_string(index=False))
except Exception as e:
    print('ERROR:', e)
