import csv

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore import UNSIGNED
from botocore.config import Config

BUCKET_NAME = "com-courtlistener-storage"
FILE_KEY = "bulk-data/dockets-2026-03-31.csv.bz2"
OUTPUT_FILE = "scalable_bankruptcy_dockets.parquet"
CHUNK_SIZE = 50_000
MAX_ROWS = 5_000
KEYWORDS = "bankruptcy|chapter 11|chapter 7|distressed|debtor|restructuring"

print("Initializing streaming pipeline...")

# CourtListener's public bucket can be read without AWS credentials.
s3 = boto3.client(
    "s3",
    config=Config(signature_version=UNSIGNED),
    region_name="us-east-1",
)

print(f"Opening s3://{BUCKET_NAME}/{FILE_KEY}...")
response = s3.get_object(Bucket=BUCKET_NAME, Key=FILE_KEY)
stream = response["Body"]

print("Streaming, decompressing, filtering, and writing Parquet...")
chunks = pd.read_csv(
    stream,
    compression="bz2",
    chunksize=CHUNK_SIZE,
    dtype=str,
    engine="python",
    on_bad_lines="skip",
    quoting=csv.QUOTE_NONE,
)

writer = None
rows_written = 0
source_columns = None

try:
    for chunk_index, chunk in enumerate(chunks, start=1):
        if rows_written >= MAX_ROWS:
            break

        if source_columns is None:
            source_columns = list(chunk.columns)

        text_columns = [col for col in chunk.columns if chunk[col].dtype == "object"]
        if not text_columns:
            if chunk_index % 10 == 0:
                print(f"Processed chunk {chunk_index}: no text columns found")
            continue

        mask = (
            chunk[text_columns]
            .fillna("")
            .apply(lambda series: series.str.contains(KEYWORDS, case=False, na=False))
            .any(axis=1)
        )
        filtered_chunk = chunk.loc[mask].copy()

        if filtered_chunk.empty:
            if chunk_index % 10 == 0:
                print(f"Processed chunk {chunk_index}: no matches yet")
            continue

        remaining_rows = MAX_ROWS - rows_written
        filtered_chunk = filtered_chunk.head(remaining_rows)
        table = pa.Table.from_pandas(filtered_chunk, preserve_index=False)

        if writer is None:
            writer = pq.ParquetWriter(OUTPUT_FILE, table.schema)

        writer.write_table(table)
        rows_written += len(filtered_chunk)
        print(
            f"Processed chunk {chunk_index}: wrote {len(filtered_chunk)} rows "
            f"(total {rows_written}/{MAX_ROWS})"
        )

    if writer is None:
        empty_frame = pd.DataFrame(
            {column: pd.Series(dtype="string") for column in (source_columns or [])}
        )
        empty_table = pa.Table.from_pandas(empty_frame, preserve_index=False)
        pq.write_table(empty_table, OUTPUT_FILE)
        print(f"No matches found. Wrote empty Parquet file: {OUTPUT_FILE}")
    else:
        print(f"Success! Wrote {rows_written} rows to {OUTPUT_FILE}")
finally:
    if writer is not None:
        writer.close()
