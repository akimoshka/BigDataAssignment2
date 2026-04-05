
# Simple Search Engine (Hadoop + Spark + Cassandra)

This project implements a simple search engine using distributed technologies.  
It builds an inverted index from a text dataset using Hadoop MapReduce, processes it with Spark, stores it in Cassandra, and supports ranked search queries using BM25.

---

## Project Overview

The system follows a full big data pipeline:

1. **Data Preparation**
   - Text documents are merged and uploaded to HDFS

2. **Indexing (Hadoop MapReduce)**
   - Builds inverted index (term → documents)
   - Computes term frequencies (TF)

3. **Post-processing (Spark)**
   - Splits index into components
   - Computes document frequency (DF)
   - Calculates corpus statistics (N, AVGDL)

4. **Storage (Cassandra)**
   - Stores documents, vocabulary, postings, and stats

5. **Search (BM25 Ranking)**
   - Processes user queries
   - Retrieves and ranks top documents

---

## Project Structure

```

/app
│
├── mapreduce/
│   ├── mapper1.py
│   └── reducer1.py
│
├── spark/
│   ├── split_index.py
│   ├── build_vocab.py
│   └── build_stats.py
│
├── scripts/
│   └── load_index.py
│
├── create_index.sh
├── store_index.sh
├── index.sh
├── query.py
├── search.sh
├── app.sh
├── requirements.txt
└── report.pdf

```

---

## Requirements

- Hadoop (HDFS + Streaming)
- Spark
- Cassandra
- Python 3

Python dependencies:

```

pip install -r requirements.txt

```

---

## Dataset Format

Each document must be in this format:

```

<doc_id>_<doc_title>.txt

```

Example:

```

10078432_A_Case_for_the_Court.txt

```

Documents are uploaded to:

```

/input/data

````

---

## How to Run

### 1. Build the Index

```bash
/app/index.sh
````

This runs the full pipeline:

* Hadoop MapReduce indexing
* Spark processing
* Cassandra storage

---

### 2. Run Search Queries

```bash
/app/search.sh <query>
```

Examples:

```bash
/app/search.sh history
/app/search.sh "history of time"
/app/search.sh christmas
```

---

### 3. Demo Script

You can also run:

```bash
/app/app.sh
```

This will:

* show HDFS index structure
* display corpus stats
* run example queries

---

## Example Output

```
Query: history of time

1   A_Briefer_History_of_Time
2   A_Brief_History_of_Time_(film)
3   A_Briefer_History_of_Time_(Schulman_book)
...
```

The results show relevant documents ranked using BM25.

---

## Design Choices

* **Simple tokenization** using regex (keeps words + numbers)
* **BM25 ranking** for better relevance
* **Separated storage** (documents, postings, vocab, stats)
* **Cassandra** for fast lookup instead of heavy joins

The system is intentionally simple but modular.

---

## Notes

* The tokenizer is basic and may include some noisy tokens (numbers), but it keeps the system lightweight.
* The focus of this project is on distributed processing rather than perfect NLP.

