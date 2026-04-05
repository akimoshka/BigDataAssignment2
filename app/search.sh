#!/bin/bash
set -e

source /app/.venv/bin/activate

QUERY="$*"

if [ -z "$QUERY" ]; then
    echo "Usage: /app/search.sh <query text>"
    exit 1
fi

spark-submit \
  --master yarn \
  --deploy-mode client \
  --conf "spark.ui.showConsoleProgress=false" \
  /app/query.py "$QUERY" 2>/dev/null