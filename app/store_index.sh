#!/bin/bash
set -e

echo "Checking HDFS..."
hdfs dfs -ls / >/dev/null

echo "Checking YARN..."
yarn node -list >/dev/null

echo "Checking Cassandra..."
python -c "from cassandra.cluster import Cluster; c=Cluster(['cassandra-server']); s=c.connect(); print('Cassandra is reachable'); c.shutdown()"

echo "All services are available."