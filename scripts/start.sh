#!/usr/bin/env bash
set -euo pipefail

# Diretório onde estará o banco persistente (Render Persistent Disk mount)
DATA_DIR="${DATA_DIR:-/srv/data}"
APP_DB_NAME="metrifiy.db"

mkdir -p "$DATA_DIR"

# Se não existir DB no disco persistente, copie a cópia inicial do repositório (se existir)
if [ ! -f "$DATA_DIR/$APP_DB_NAME" ]; then
  if [ -f "$(pwd)/$APP_DB_NAME" ]; then
    echo "[init] copying initial $APP_DB_NAME to $DATA_DIR"
    cp "$(pwd)/$APP_DB_NAME" "$DATA_DIR/$APP_DB_NAME"
  else
    echo "[init] no initial $APP_DB_NAME found in repo; starting with empty DB file"
    sqlite3 "$DATA_DIR/$APP_DB_NAME" "VACUUM;"
  fi
fi

# Export DATABASE_URL for apps that read it
export DATABASE_URL="sqlite:///$DATA_DIR/$APP_DB_NAME"

echo "[start] Using database at: $DATA_DIR/$APP_DB_NAME"

exec gunicorn -c gunicorn_config.py app:app
