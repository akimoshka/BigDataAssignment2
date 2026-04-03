from pyspark import SparkContext

sc = SparkContext(appName="split_index")

lines = sc.textFile("hdfs:///tmp/indexer_stage1/part-*")

docs = lines.filter(lambda x: x.startswith("__DOC__")) \
            .map(lambda x: x.split("\t", 3)) \
            .filter(lambda p: len(p) == 4) \
            .map(lambda p: f"{p[1]}\t{p[2]}\t{p[3]}")

postings = lines.filter(lambda x: not x.startswith("__DOC__"))

postings.coalesce(1).saveAsTextFile("hdfs:///indexer/postings")
docs.coalesce(1).saveAsTextFile("hdfs:///indexer/docs")

sc.stop()