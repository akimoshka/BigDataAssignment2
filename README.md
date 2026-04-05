# Assignment 2: Simple Search Engine using Hadoop MapReduce

This repository contains a simple search engine built with Hadoop MapReduce, Spark, and Cassandra. The system indexes a text corpus, stores the index in Cassandra, and supports ranked retrieval using BM25.

## Project idea

The goal of this assignment was to implement the main parts of a very simple search engine in a distributed environment. The pipeline includes data preparation, index creation, storage, and ranked query answering.

The system works in the following order:

1. prepare text documents from the parquet dataset
2. upload them to HDFS
3. build an inverted index with Hadoop MapReduce
4. compute vocabulary and corpus statistics with Spark
5. store the index in Cassandra
6. answer user queries with BM25

## Technologies used

- Hadoop HDFS
- Hadoop Streaming
- Spark
- Cassandra
- Python

## Repository structure

```text
/app
├── mapreduce/
│   ├── mapper1.py
│   └── reducer1.py
├── spark/
│   ├── split_index.py
│   ├── build_vocab.py
│   └── build_stats.py
├── scripts/
│   └── load_index.py
├── prepare_data.py
├── prepare_input.py
├── create_index.sh
├── store_index.sh
├── index.sh
├── query.py
├── search.sh
├── app.sh
├── start-services.sh
├── requirements.txt
└── report.pdf
````

## Dataset

The dataset is based on one parquet file from the Wikipedia dataset.

Each generated document follows this naming format:

```text
<doc_id>_<doc_title>.txt
```

Example:

```text
10078432_A_Case_for_the_Court.txt
```

## How to run

### 1. Start containers

From the project root:

```bash
docker compose up -d
```

### 2. Enter the master container

```bash
docker exec -it cluster-master bash
source .venv/bin/activate
```

### 3. Check services

```bash
/app/start-services.sh
```

### 4. Run the full indexing pipeline

```bash
/app/index.sh
```

This script:

* creates the HDFS index
* computes vocabulary and statistics
* stores everything in Cassandra

### 5. Run search queries

```bash
/app/search.sh history
/app/search.sh "history of time"
/app/search.sh christmas
```

### 6. Demo run

```bash
/app/app.sh
```

This script prints:

* HDFS index folders
* corpus statistics
* sample query results

## Index structure

The final HDFS index is stored in:

* `/indexer/docs`
* `/indexer/postings`
* `/indexer/vocab`
* `/indexer/stats`

The Cassandra tables are:

* `documents`
* `vocabulary`
* `postings`
* `corpus_stats`

## Query processing

The query engine uses BM25 ranking.
For each query term, it retrieves postings, document frequency, and document length, then computes scores and returns the top 10 matching documents.

The script `query.py` supports:

* query text as command-line arguments
* query text from standard input

## Notes

The tokenizer is intentionally simple. It lowercases text and extracts alphanumeric tokens with regex. This means some numeric tokens remain in the vocabulary, which adds a bit of noise, but keeps the pipeline simple and transparent.