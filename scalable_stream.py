import duckdb

print("Initializing high-performance analytical engine...")
con = duckdb.connect(database=':memory:')

# Enable network extension protocols inside DuckDB
con.execute("INSTALL httpfs; LOAD httpfs;")

# Configure anonymous AWS credentials to mirror --no-sign-request
con.execute("""
    SET s3_region='us-east-1';
    SET s3_access_key_id='';
    SET s3_secret_access_key='';
""")

# Define target paths
s3_path = "s3://com-courtlistener-storage/bulk-data/dockets-2026-03-31.csv.bz2"
output_parquet = "scalable_bankruptcy_dockets.parquet"

print("Streaming and filtering directly from S3 using zero memory...")

# DuckDB processes text in parallel chunks, writing directly to highly-compressed Parquet
query = f"""
    COPY (
        SELECT * 
        FROM read_csv_auto('{s3_path}', compression='bz2', ignore_errors=true)
        WHERE lower(case_name) LIKE '%bankruptcy%'
           OR lower(case_name) LIKE '%chapter 11%'
           OR lower(case_name) LIKE '%restructuring%'
        LIMIT 10000
    ) TO '{output_parquet}' (FORMAT 'PARQUET');
"""

con.execute(query)
print(f"Success! Scalable storage file generated: {output_parquet}")
