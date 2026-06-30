import duckdb

print("Initializing cloud-optimized analytical engine...")
con = duckdb.connect(database=':memory:')

# Load the secure HTTP streaming extension
con.execute("INSTALL httpfs; LOAD httpfs;")

# Fix: Route directly to the us-west-2 data center where CourtListener lives
# This prevents AWS from throwing a hidden 301 redirection block
https_path = "https://amazonaws.com"
output_parquet = "scalable_bankruptcy_dockets.parquet"

print("Streaming and filtering directly from S3-us-west-2 via HTTPS...")

# Set all_varchar=True so dirty text rows don't disrupt parsing
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
