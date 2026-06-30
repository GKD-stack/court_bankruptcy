import urllib.request
import bz2
import duckdb
import io

print("Initializing streamlined cloud architecture...")

# Target URL for CourtListener data repository
url = "https://amazonaws.com"
output_parquet = "scalable_bankruptcy_dockets.parquet"

con = duckdb.connect(database=':memory:')

print("Opening streaming data channel...")
request = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})

with urllib.request.urlopen(request) as response:
    # Fix: bz2.open directly handles the cloud response stream alignment perfectly
    with bz2.open(response, mode='rt', encoding='utf-8', errors='ignore') as decompressor:
        print("Connected! Skimming the first 200,000 text rows to extract bankruptcy targets...")
        
        # Pull text rows instantly into a memory buffer instead of raw byte chunks
        raw_text_buffer = io.StringIO()
        
        # Read the header first
        header = decompressor.readline()
        raw_text_buffer.write(header)
        
        # Pull 200,000 rows to ensure we find plenty of Chapter 11/Bankruptcy cases
        for i in range(200000):
            line = decompressor.readline()
            if not line:
                break
            raw_text_buffer.write(line)
            if i % 50000 == 0 and i > 0:
                print(f"-> Skimmed {i:,} lines from the cloud stream...")

        print("\nHanding memory stream over to the DuckDB engine...")
        raw_text_buffer.seek(0)
        
        # Register the text stream as a virtual memory table
        con.register_object('virtual_csv_stream', raw_text_buffer)

        print("Executing analytical query and compressing to Parquet...")
        query = f"""
            COPY (
                SELECT * 
                FROM read_csv('virtual_csv_stream', ignore_errors=true, header=true, all_varchar=true)
                WHERE lower(case_name) LIKE '%bankruptcy%'
                   OR lower(case_name) LIKE '%chapter 11%'
                   OR lower(case_name) LIKE '%restructuring%'
                LIMIT 5000
            ) TO '{output_parquet}' (FORMAT 'PARQUET');
        """
        
        con.execute(query)
        print(f"\n[SUCCESS] Production file generated successfully: {output_parquet}")

