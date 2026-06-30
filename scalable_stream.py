import duckdb

print("Initializing cloud-optimized analytical engine...")
con = duckdb.connect(database=':memory:')

# Force download the correct secure extensions for the cloud environment
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("INSTALL aws; LOAD aws;")

# Correctly configure AWS to accept anonymous streams
con.execute("""
    SET s3_region='us-east-1';
    SET s3_access_key_id='';
    SET s3_secret_access_key='';
    SET s3_url_style='path';
""")

# Define target paths
s3_path = "s3://com-courtlistener-storage/bulk-data/dockets-2026-03-31.csv.bz2"
output_parquet = "scalable_bankruptcy_dockets.parquet"

print("Streaming and filtering directly from S3... (Skipping malformed text lines)")

# Use read_csv wrapper with sample_size set to prevent type-guessing failures on early rows
query = f"""
    COPY (
        SELECT * 
        FROM read_csv(
            '{s3_path}', 
            compression='bz2', 
            ignore_errors=true,
            header=true,
            sample_size=-1
        )
        WHERE lower(case_name) LIKE '%bankruptcy%'
           OR lower(case_name) LIKE '%chapter 11%'
           OR lower(case_name) LIKE '%restructuring%'
        LIMIT 5000
    ) TO '{output_parquet}' (FORMAT 'PARQUET');
"""

try:
    con.execute(query)
    print(f"Success! Scalable storage file generated: {output_parquet}")
except Exception as e:
    print(f"\n[STREAM ERROR] The execution failed in the cloud engine: {e}")
    raise e
