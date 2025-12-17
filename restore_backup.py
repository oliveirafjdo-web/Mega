import shutil
from pathlib import Path
import sys

if len(sys.argv)<2:
    print('Usage: python restore_backup.py <backup-filename>')
    raise SystemExit(1)

backup = Path(sys.argv[1])
DB = Path('metrifiy.db')
if not backup.exists():
    print('Backup not found:', backup)
    raise SystemExit(1)

shutil.copy2(backup, DB)
print('Restored', DB, 'from', backup)
