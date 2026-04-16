import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://user:password@localhost:5432/bkk_bronze')

print("Querying the Bronze database...")
query = "SELECT ingested_at, raw_data FROM bkk_raw_payloads;"
df = pd.read_sql(query, engine)

print("Flattening nested JSON...")
json_df = pd.json_normalize(df['raw_data'])

final_df = pd.concat([df[['ingested_at']], json_df], axis=1)

print(f"Exporting {len(final_df)} rows and {len(final_df.columns)} columns to CSV...")
final_df.to_csv('bkk_raw.csv', index=False)

print("Done! File saved as bkk_raw.csv")