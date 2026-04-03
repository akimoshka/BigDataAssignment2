from pyspark import SparkContext

sc = SparkContext(appName="build_vocab")

lines = sc.textFile("hdfs:///indexer/postings/part-*")

def extract_term(line):
    parts = line.split("\t", 3)
    if len(parts) != 4:
        return None
    term = parts[0]
    return (term, 1)

vocab = (
    lines.map(extract_term)
         .filter(lambda x: x is not None)
         .reduceByKey(lambda a, b: a + b)
         .map(lambda x: f"{x[0]}\t{x[1]}")
)

vocab.coalesce(1).saveAsTextFile("hdfs:///indexer/vocab")

sc.stop()