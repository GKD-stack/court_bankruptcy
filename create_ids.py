import pandas as pd

# Load your existing 60k rows
df = pd.read_csv('bankruptcy_tracker_data.csv', on_bad_lines='skip', low_memory=False)

# Clean up duplicates
df = df.drop_duplicates(subset=['id'])
df.to_csv('bankruptcy_tracker_data.csv', index=False)

# Extract unique docket IDs into a text file for our next scripts
d_ids = df['id'].dropna().astype(int).astype(str).tolist()
with open('docket_ids.txt', 'w') as f:
    f.write('\n'.join(d_ids))

print(f"Loaded {len(d_ids):,} unique Docket IDs. Ready for matching.")
