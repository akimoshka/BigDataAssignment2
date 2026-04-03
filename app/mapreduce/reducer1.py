#!/usr/bin/env python3
import sys

current_key = None
current_sum = 0
current_doc_id = None
current_title = None

for line in sys.stdin:
    line = line.rstrip("\n")
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) != 4:
        continue

    key, doc_id, title, value = parts

    try:
        value = int(value)
    except ValueError:
        continue

    group_key = (key, doc_id, title)

    if current_key == group_key:
        current_sum += value
    else:
        if current_key is not None:
            out_key, out_doc_id, out_title = current_key
            print(f"{out_key}\t{out_doc_id}\t{out_title}\t{current_sum}")

        current_key = group_key
        current_sum = value

if current_key is not None:
    out_key, out_doc_id, out_title = current_key
    print(f"{out_key}\t{out_doc_id}\t{out_title}\t{current_sum}")