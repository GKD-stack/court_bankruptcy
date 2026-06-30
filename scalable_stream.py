import bz2
import csv
import os

import boto3
import pandas as pd
from botocore import UNSIGNED
from botocore.config import Config

BUCKET_NAME = "com-courtlistener-storage"
FILE_KEY = "bulk-data/dockets-2026-03-31.csv.bz2"
OUTPUT_FILE = "scalable_bankruptcy_dockets.parquet"
TEMP_CSV_FILE = "scalable_bankruptcy_dockets.tmp.csv"
MAX_ROWS = 5_000
KEYWORDS = (
    "bankruptcy",
    "chapter 11",
    "chapter 7",
    "distressed",
    "debtor",
    "restructuring",
)
PROGRESS_EVERY_LINES = 250_000

print("Initializing fast streaming pipeline...")

s3 = boto3.client(
    "s3",
    config=Config(signature_version=UNSIGNED),
    region_name="us-east-1",
)

print(f"Opening s3://{BUCKET_NAME}/{FILE_KEY}...")
response = s3.get_object(Bucket=BUCKET_NAME, Key=FILE_KEY)
stream = response["Body"]

matches = 0
scanned_lines = 0

print("Scanning raw CSV lines and keeping only keyword matches...")
with bz2.open(stream, mode="rt", encoding="utf-8", errors="ignore", newline="") as source:
    header = source.readline()
    if not header:
        raise RuntimeError("Source file is empty")

    with open(TEMP_CSV_FILE, "w", encoding="utf-8", newline="") as temp_csv:
        temp_csv.write(header)

        for line in source:
            scanned_lines += 1
            lowered = line.lower()

            if any(keyword in lowered for keyword in KEYWORDS):
                temp_csv.write(line)
                matches += 1

                if matches % 500 == 0 or matches == MAX_ROWS:
                    print(f"Captured {matches}/{MAX_ROWS} matches after {scanned_lines:,} lines")

                if matches >= MAX_ROWS:
                    break
            elif scanned_lines % PROGRESS_EVERY_LINES == 0:
                print(f"Scanned {scanned_lines:,} lines so far...")

print("Converting matched rows to Parquet...")
matched_rows = pd.read_csv(
    TEMP_CSV_FILE,
    dtype=str,
    engine="python",
    on_bad_lines="skip",
    quoting=csv.QUOTE_MINIMAL,
)
matched_rows.to_parquet(OUTPUT_FILE, index=False, engine="pyarrow")

print(
    f"Success! Wrote {len(matched_rows)} rows to {OUTPUT_FILE} "
    f"after scanning {scanned_lines:,} lines."
)

os.remove(TEMP_CSV_FILE)
