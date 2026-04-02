from pyspark import SparkContext
import os

sc = SparkContext(appName="prepare_input_data")

files = sc.wholeTextFiles("hdfs:///data/*")

def parse_record(item):
    path, text = item
    filename = os.path.basename(path)

    if filename.endswith(".txt"):
        filename = filename[:-4]

    if "_" in filename:
        doc_id, doc_title = filename.split("_", 1)
    else:
        doc_id, doc_title = filename, ""

    clean_text = text.replace("\n", " ").replace("\t", " ").strip()
    return f"{doc_id}\t{doc_title}\t{clean_text}"

output = files.map(parse_record).coalesce(1)
output.saveAsTextFile("hdfs:///input/data")

sc.stop()