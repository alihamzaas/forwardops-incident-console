#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

PYTHON_CANDIDATES=()
if [[ -n "${PYTHON_BIN:-}" ]]; then
  PYTHON_CANDIDATES+=("${PYTHON_BIN}")
fi
PYTHON_CANDIDATES+=("/opt/anaconda3/bin/python3" "python3" "/usr/local/bin/python3" "/usr/bin/python3")

RESOLVED_PYTHON=""
for candidate in "${PYTHON_CANDIDATES[@]}"; do
  if [[ -x "$candidate" ]] || command -v "$candidate" >/dev/null 2>&1; then
    if "$candidate" -c "import fastapi, uvicorn, pandas, pyarrow, plotly" >/dev/null 2>&1; then
      RESOLVED_PYTHON="$candidate"
      break
    fi
  fi
done

if [[ -z "$RESOLVED_PYTHON" ]]; then
  echo "Could not find a Python interpreter with the FastAPI copilot dependencies installed."
  echo "Install them with:"
  echo "  python3 -m pip install -r backend/requirements.txt"
  echo "Then run this script again. You can also use Docker Compose:"
  echo "  docker compose up --build"
  exit 1
fi

echo "Using Python: $RESOLVED_PYTHON"
exec "$RESOLVED_PYTHON" -m uvicorn backend.fastapi_app:app \
  --host "${LISTEN_HOST:-127.0.0.1}" \
  --port "${PORT:-8000}" \
  --reload
