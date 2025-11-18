#!/usr/bin/env bash
set -euo pipefail

# cd ke root project (folder yang berisi main.py)
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." &>/dev/null && pwd)"
cd "$PROJECT_ROOT"

# Load .env bila ada
if [[ -f ".env" ]]; then
  # shellcheck disable=SC2046
  export $(grep -v '^\s*#' .env | xargs -d '\n' -I {} bash -c 'if [[ "$1" == *"="* ]]; then echo "$1"; fi' _ {})
fi

# Default nilai jika tidak ada di .env
: "${SERVER_HOST:=0.0.0.0}"
: "${SERVER_PORT:=8000}"

# Pilih python
PY=python3
command -v "$PY" >/dev/null 2>&1 || PY=python
command -v "$PY" >/dev/null 2>&1 || { echo "[ERR] python tidak ditemukan"; exit 1; }

# Pastikan main.py ada
[[ -f "main.py" ]] || { echo "[ERR] main.py tidak ditemukan di $PROJECT_ROOT"; exit 1; }

# Jalankan server (tanpa reload untuk prod)
exec "$PY" -m uvicorn main:app --host "$SERVER_HOST" --port "$SERVER_PORT"
