#!/usr/bin/env python3
import sys
import re
from collections import Counter

TOKEN_PATTERN = re.compile(r"[a-z0-9]+")

def tokenize(text):
    return TOKEN_PATTERN.findall(text.lower())

for line in sys.stdin:
    line = line.rstrip("\n")
    if not line:
        continue

    parts = line.split("\t", 2)
    if len(parts) != 3:
        continue

    doc_id, title, text = parts
    tokens = tokenize(text)

    if not tokens:
        continue

    tf_counter = Counter(tokens)
    dl = len(tokens)

    for term, tf in tf_counter.items():
        print(f"{term}\t{doc_id}\t{title}\t{tf}")

    print(f"__DOC__\t{doc_id}\t{title}\t{dl}")