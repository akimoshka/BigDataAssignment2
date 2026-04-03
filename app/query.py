import sys
import math
import re
from collections import defaultdict
from cassandra.cluster import Cluster
from pyspark import SparkContext

KEYSPACE = "search_engine"
CASSANDRA_HOST = "cassandra-server"
PORT = 9042

K1 = 1.5
B = 0.75

TOKEN_PATTERN = re.compile(r"[a-z0-9]+")


def tokenize(text):
    return TOKEN_PATTERN.findall(text.lower())


def get_corpus_stats(session):
    rows = session.execute("SELECT stat_name, stat_value FROM corpus_stats")
    stats = {}
    for row in rows:
        stats[row.stat_name] = row.stat_value
    return int(stats["N"]), float(stats["AVGDL"])


def get_df(session, term):
    row = session.execute(
        "SELECT df FROM vocabulary WHERE term = %s",
        (term,)
    ).one()
    return row.df if row else None


def get_postings(session, term):
    rows = session.execute(
        "SELECT doc_id, title, tf FROM postings WHERE term = %s",
        (term,)
    )
    return [(row.doc_id, row.title, row.tf) for row in rows]


def get_doc_length(session, doc_id):
    row = session.execute(
        "SELECT dl FROM documents WHERE doc_id = %s",
        (doc_id,)
    ).one()
    return row.dl if row else None


def bm25_score(tf, df, dl, N, avgdl, k1=K1, b=B):
    if df == 0 or avgdl == 0:
        return 0.0
    idf = math.log(N / df)
    denom = tf + k1 * (1 - b + b * (dl / avgdl))
    return idf * ((tf * (k1 + 1)) / denom)


def main():
    query = " ".join(sys.argv[1:]).strip()
    if not query:
        print("Usage: spark-submit /app/query.py <query text>")
        sys.exit(1)

    terms = tokenize(query)
    if not terms:
        print("No valid query terms found.")
        sys.exit(0)

    sc = SparkContext(appName="bm25_query")

    cluster = Cluster([CASSANDRA_HOST], port=PORT)
    session = cluster.connect(KEYSPACE)

    N, AVGDL = get_corpus_stats(session)

    all_postings = []
    for term in terms:
        df = get_df(session, term)
        if df is None or df == 0:
            continue

        postings = get_postings(session, term)
        for doc_id, title, tf in postings:
            dl = get_doc_length(session, doc_id)
            if dl is None:
                continue
            all_postings.append((doc_id, title, bm25_score(tf, df, dl, N, AVGDL)))

    if not all_postings:
        print("No matching documents found.")
        cluster.shutdown()
        sc.stop()
        return

    rdd = sc.parallelize(all_postings)

    ranked = (
        rdd.map(lambda x: (x[0], (x[1], x[2])))
           .reduceByKey(lambda a, b: (a[0], a[1] + b[1]))
           .map(lambda x: (x[0], x[1][0], x[1][1]))
           .takeOrdered(10, key=lambda x: -x[2])
    )

    for rank, (doc_id, title, score) in enumerate(ranked, start=1):
        print(f"{rank}\t{doc_id}\t{title}\t{score:.6f}")

    cluster.shutdown()
    sc.stop()


if __name__ == "__main__":
    main()