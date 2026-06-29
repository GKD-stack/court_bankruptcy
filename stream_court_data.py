import boto3
import pandas as pd
import csv
from botocore import UNSIGNED
from botocore.config import Config

# Configure AWS to access the bucket without credentials
s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED), region_name='us-east-1')

BUCKET_NAME = 'com-courtlistener-storage'
FILE_KEY = 'bulk-data/dockets-2026-03-31.csv.bz2' 
OUTPUT_FILE = 'bankruptcy_tracker_data.csv'

print("Opening network stream from S3...")
response = s3.get_object(Bucket=BUCKET_NAME, Key=FILE_KEY)
stream = response['Body']

print("Streaming, decompressing, and filtering rows for bankruptcy...")

# Fix: Added quoting=csv.QUOTE_NONE (3) to ignore malformed embedded quote marks
chunks = pd.read_csv(
    stream, 
    compression='bz2', 
    chunksize=50000, 
    engine='python',
    on_bad_lines='skip',
    quoting=csv.QUOTE_NONE
)

first_chunk = True
total_saved = 0

for i, chunk in enumerate(chunks):
    # Ensure columns exist and get all text columns to inspect for tracking metrics
    text_columns = [col for col in chunk.columns if chunk[col].dtype == 'object']
    
    # Target distressed investing / bankruptcy core keywords
    keywords = 'bankruptcy|chapter 11|chapter 7|distressed|debtor|restructuring'
    mask = chunk[text_columns].astype(str).apply(lambda x: x.str.contains(keywords, case=False, na=False)).any(axis=1)
    
    filtered_chunk = chunk[mask]
    
    if not filtered_chunk.empty:
        # Save matches directly to local CSV file
        filtered_chunk.to_csv(OUTPUT_FILE, mode='a', index=False, header=first_chunk)
        first_chunk = False
        total_saved += len(filtered_chunk)
        print(f"Processed chunk {i}: Saved {len(filtered_chunk)} matching rows... (Total: {total_saved})")
    else:
        if i % 10 == 0:
            print(f"Processed chunk {i}...")

print(f"\nSuccess! Saved {total_saved} total bankruptcy rows to {OUTPUT_FILE}.")
