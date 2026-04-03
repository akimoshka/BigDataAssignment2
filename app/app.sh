#!/bin/bash
set -e

source /app/.venv/bin/activate

echo "=== HDFS index folders ==="
hdfs dfs -ls /indexer

echo ""
echo "=== Corpus stats ==="
hdfs dfs -cat /indexer/stats/part-*

echo ""
echo "=== Query: history ==="
/app/search.sh history

echo ""
echo "=== Query: history of time ==="
/app/search.sh "history of time"

echo ""
echo "=== Query: christmas ==="
/app/search.sh christmas