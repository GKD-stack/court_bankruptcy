import duckdb

print("Initializing cloud-optimized analytical engine...")
con = duckdb.connect(database=':memory:')

# Load the secure HTTP streaming extension
con.execute("INSTALL httpfs; LOAD httpfs;")

# Fix: Convert the s3:// path to a direct public https:// URL 
# This completely bypasses signature/credential checks on the GitHub runner
https_path = "https://amazonaws.com"
output_parquet = "scalable_bankruptcy_dockets.parquet"

print("Streaming and filtering directly from S3 via HTTPS...")

# Fix: Set all_varchar=True to handle messy court data without type parsing errors
query = f"""
    COPY (
        SELECT * 
        FROM read_csv(
            '{https_path}', 
            compression='bz2', 
            ignore_errors=true,
            header=true,
            all_varchar=true
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
    print(f"\n[STREAM ERROR] Execution failed: {e}")
    raise e
