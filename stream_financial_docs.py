import boto3
import pandas as pd
import csv
from botocore import UNSIGNED
from botocore.config import Config

# 1. Load our target docket IDs into memory as a fast lookup set
print("Loading target Docket IDs...")
with open('docket_ids.txt', 'r') as f:
    target_dockets = set(f.read().splitlines())

# 2. Configure S3 Stream
s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED), region_name='us-east-1')
BUCKET_NAME = 'com-courtlistener-storage'
# Note: Check S3 file structure to ensure the exact matching filename pattern
FILE_KEY = 'bulk-data/recap-documents-2026-03-31.csv.bz2' 
OUTPUT_FILE = 'bankruptcy_financial_docs.csv'

print("Opening stream for RECAP documents...")
try:
    response = s3.get_object(Bucket=BUCKET_NAME, Key=FILE_KEY)
    stream = response['Body']
except Exception as e:
    print(f"Error connecting to file. Check if filename matches exactly: {e}")
    exit()

# 3. Stream and filter
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
MAX_DOCS = 5000 # 🎯 Cap the download size to keep it extremely fast

print("Scanning for financial papers and investment triggers...")
for i, chunk in enumerate(chunks):
    # Ensure our relational key column exists
    if 'docket_id' not in chunk.columns:
        # If columns are named differently in this file, check first chunk columns
        if i == 0:
            print(f"Columns found: {list(chunk.columns)}")
            # Fallback mapper if column names vary slightly
            id_col = [col for col in chunk.columns if 'docket' in col.lower()]
            d_col = id_col[0] if id_col else 'docket_id'
        else:
            d_col = 'docket_id'
    else:
        d_col = 'docket_id'

    # Filter 1: Check if the row belongs to one of your 60,000 bankruptcies
    is_target_case = chunk[d_col].astype(str).isin(target_dockets)
    
    # Filter 2: Scan document descriptions for financial signals (Wiki parameters)
    text_cols = [col for col in chunk.columns if chunk[col].dtype == 'object']
    keywords = 'plan|disclosure|schedule|assets|liabilities|dip|collateral|confirmation'
    has_keywords = chunk[text_cols].astype(str).apply(lambda x: x.str.contains(keywords, case=False, na=False)).any(axis=1)
    
    # Combine filters
    filtered_chunk = chunk[is_target_case & has_keywords]
    
    if not filtered_chunk.empty:
        filtered_chunk.to_csv(OUTPUT_FILE, mode='a', index=False, header=first_chunk)
        first_chunk = False
        total_saved += len(filtered_chunk)
        print(f"Chunk {i}: Saved {len(filtered_chunk)} documents. Total: {total_saved}/{MAX_DOCS}")
        
    if total_saved >= MAX_DOCS:
        print(f"\n[Success] Captured {total_saved} high-value financial links. Stream closed.")
        break
