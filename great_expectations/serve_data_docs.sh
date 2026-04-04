#!/usr/bin/env bash
set -euo pipefail

PORT="${1:-8093}"
PROJECT_HOME="${GX_HOME:-/opt/gx/project}"

CANDIDATES=(
  "${PROJECT_HOME}/gx/uncommitted/data_docs/local_site"
  "${PROJECT_HOME}/uncommitted/data_docs/local_site"
)

DOCS_DIR=""
for candidate in "${CANDIDATES[@]}"; do
  if [ -d "$candidate" ]; then
    DOCS_DIR="$candidate"
    break
  fi
done

if [ -z "$DOCS_DIR" ]; then
  DOCS_DIR="${PROJECT_HOME}/_data_docs_placeholder"
  mkdir -p "$DOCS_DIR"
  cat > "${DOCS_DIR}/index.html" <<HTML
<!doctype html>
<html>
  <head><meta charset="utf-8"><title>Great Expectations Data Docs</title></head>
  <body style="font-family: sans-serif; max-width: 720px; margin: 32px auto;">
    <h1>Data Docs are not built yet</h1>
    <p>Run a checkpoint or build docs first:</p>
    <pre>docker compose exec great-expectations bash -lc 'cd /opt/gx/project && gx docs build'</pre>
  </body>
</html>
HTML
fi

echo "Serving Data Docs from: ${DOCS_DIR} on port ${PORT}"
python -m http.server "$PORT" --directory "$DOCS_DIR"
