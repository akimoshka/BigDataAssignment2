from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, length
import os
import unicodedata
import re

spark = SparkSession.builder \
    .appName("data preparation") \
    .master("local[*]") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .getOrCreate()

INPUT_PATH = "file:///app/a.parquet"
OUTPUT_DIR = "/app/data"
N = 1000

os.makedirs(OUTPUT_DIR, exist_ok=True)

def make_safe_title(title):
    # normalize unicode → ascii
    title = unicodedata.normalize("NFKD", str(title)).encode("ascii", "ignore").decode("ascii")
    title = title.replace(" ", "_")
    title = sanitize_filename(title)
    title = re.sub(r"[^A-Za-z0-9_\-().]", "", title)
    return title[:180] if title else "untitled"

df = spark.read.parquet(INPUT_PATH) \
    .select("id", "title", "text") \
    .filter(col("id").isNotNull()) \
    .filter(col("title").isNotNull()) \
    .filter(col("text").isNotNull()) \
    .filter(length(trim(col("text"))) > 0) \
    .limit(N)

rows = df.collect()

for row in rows:
    safe_title = make_safe_title(row["title"])
    filename = os.path.join(OUTPUT_DIR, f"{row['id']}_{safe_title}.txt")
    with open(filename, "w", encoding="utf-8") as f:
        f.write(row["text"])

print(f"Created {len(rows)} documents in {OUTPUT_DIR}")

spark.stop()