import urllib.request
import bz2
import duckdb
import io

print("Initializing high-performance streaming architecture...")

# Target URL for CourtListener data repository
url = "https://amazonaws.com"
output_parquet = "scalable_bankruptcy_dockets.parquet"

# Establish connection parameters
con = duckdb.connect(database=':memory:')

print("Opening continuous network connection to S3...")
request = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})

with urllib.request.urlopen(request) as response:
    print("Stream connected. Initializing live bz2 decompressor loop...")
    
    # Python streams and decompresses the raw bytes into memory on-the-fly
    with bz2.BZ2Decompressor() as decompressor:
        
        # We process the first 100MB of raw text data to secure our subset sample
        # This keeps our GitHub memory footprint extremely light
        chunk_size = 1024 * 1024 * 50 # 50 Megabytes per network buffer
        raw_text_buffer = io.BytesIO()
        bytes_processed = 0
        max_bytes_to_process = 1024 * 1024 * 150 # Cap at 150MB of raw CSV text data

        while bytes_processed < max_bytes_to_process:
            chunk = response.read(chunk_size)
            if not chunk:
                break
                
            decompressed_data = decompressor.decompress(chunk)
            raw_text_buffer.write(decompressed_data)
            bytes_processed += len(decompressed_data)
            print(f"-> Decompressed stream buffer: {bytes_processed / (1024*1024):.1f} MB processed...")

        print("\nHanding text stream directly over to the DuckDB parser...")
        raw_text_buffer.seek(0)
        
        # Read the raw text buffer using a virtual CSV interface inside DuckDB
        csv_wrapper = io.TextIOWrapper(raw_text_buffer, encoding='utf-8', errors='ignore')
        
        # Register the text stream as a virtual memory table
        con.register_object('virtual_csv_stream', csv_wrapper)

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

