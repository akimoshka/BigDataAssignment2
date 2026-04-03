from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
import subprocess

KEYSPACE = "search_engine"
CASSANDRA_HOST = "cassandra-server"
PORT = 9042
CHUNK_SIZE = 500


def hdfs_cat(path_pattern):
    proc = subprocess.Popen(
        ["hdfs", "dfs", "-cat", path_pattern],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    for line in proc.stdout:
        yield line.rstrip("\n")
    proc.stdout.close()
    stderr = proc.stderr.read()
    ret = proc.wait()
    if ret != 0:
        raise RuntimeError(f"hdfs dfs -cat failed for {path_pattern}: {stderr}")


def chunked(iterable, size):
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def main():
    cluster = Cluster([CASSANDRA_HOST], port=PORT)
    session = cluster.connect()
    session.set_keyspace(KEYSPACE)

    # Clear old data so reruns stay clean
    session.execute("TRUNCATE documents")
    session.execute("TRUNCATE vocabulary")
    session.execute("TRUNCATE postings")
    session.execute("TRUNCATE corpus_stats")

    insert_doc = session.prepare(
        "INSERT INTO documents (doc_id, title, dl) VALUES (?, ?, ?)"
    )
    insert_vocab = session.prepare(
        "INSERT INTO vocabulary (term, df) VALUES (?, ?)"
    )
    insert_posting = session.prepare(
        "INSERT INTO postings (term, doc_id, title, tf) VALUES (?, ?, ?, ?)"
    )
    insert_stat = session.prepare(
        "INSERT INTO corpus_stats (stat_name, stat_value) VALUES (?, ?)"
    )

    # Load documents
    def docs_iter():
        for line in hdfs_cat("/indexer/docs/part-*"):
            parts = line.split("\t", 2)
            if len(parts) != 3:
                continue
            doc_id, title, dl = parts
            try:
                dl = int(dl)
            except ValueError:
                continue
            yield (doc_id, title, dl)

    for batch in chunked(docs_iter(), CHUNK_SIZE):
        execute_concurrent_with_args(session, insert_doc, batch)
    print("Loaded documents")

    # Load vocabulary
    def vocab_iter():
        for line in hdfs_cat("/indexer/vocab/part-*"):
            parts = line.split("\t", 1)
            if len(parts) != 2:
                continue
            term, df = parts
            try:
                df = int(df)
            except ValueError:
                continue
            yield (term, df)

    for batch in chunked(vocab_iter(), CHUNK_SIZE):
        execute_concurrent_with_args(session, insert_vocab, batch)
    print("Loaded vocabulary")

    # Load postings
    def postings_iter():
        for line in hdfs_cat("/indexer/postings/part-*"):
            parts = line.split("\t", 3)
            if len(parts) != 4:
                continue
            term, doc_id, title, tf = parts
            try:
                tf = int(tf)
            except ValueError:
                continue
            yield (term, doc_id, title, tf)

    for i, batch in enumerate(chunked(postings_iter(), CHUNK_SIZE), start=1):
        execute_concurrent_with_args(session, insert_posting, batch)
        if i % 50 == 0:
            print(f"Loaded {i * CHUNK_SIZE} postings...")

    print("Loaded postings")

    # Load corpus stats
    def stats_iter():
        for line in hdfs_cat("/indexer/stats/part-*"):
            parts = line.split("\t", 1)
            if len(parts) != 2:
                continue
            yield (parts[0], parts[1])

    for batch in chunked(stats_iter(), CHUNK_SIZE):
        execute_concurrent_with_args(session, insert_stat, batch)
    print("Loaded corpus stats")

    cluster.shutdown()
    print("Done")


if __name__ == "__main__":
    main()