from pyspark import SparkContext

sc = SparkContext(appName="build_stats")

lines = sc.textFile("hdfs:///indexer/docs/part-*")

def parse_doc(line):
    parts = line.split("\t", 2)
    if len(parts) != 3:
        return None
    doc_id, title, dl = parts
    try:
        dl = int(dl)
    except ValueError:
        return None
    return dl

doc_lengths = lines.map(parse_doc).filter(lambda x: x is not None)

N = doc_lengths.count()
total_dl = doc_lengths.sum()
avgdl = total_dl / N if N > 0 else 0.0

stats = sc.parallelize([
    f"N\t{N}",
    f"AVGDL\t{avgdl}"
], 1)

stats.saveAsTextFile("hdfs:///indexer/stats")

sc.stop()