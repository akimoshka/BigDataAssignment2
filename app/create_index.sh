#!/bin/bash
set -e

source /app/.venv/bin/activate

STREAMING_JAR=$(find /usr/local/hadoop/share/hadoop/tools/lib -name "hadoop-streaming*.jar" | head -n 1)

hdfs dfs -rm -r -f /tmp/indexer_stage1
hdfs dfs -rm -r -f /indexer/postings
hdfs dfs -rm -r -f /indexer/docs
hdfs dfs -rm -r -f /indexer/vocab
hdfs dfs -rm -r -f /indexer/stats

hadoop jar "$STREAMING_JAR" \
  -files mapreduce/mapper1.py,mapreduce/reducer1.py \
  -input /input/data/part-* \
  -output /tmp/indexer_stage1 \
  -mapper "python3 mapper1.py" \
  -reducer "python3 reducer1.py"

spark-submit spark/split_index.py
spark-submit spark/build_vocab.py
spark-submit spark/build_stats.py

echo "HDFS index created successfully"