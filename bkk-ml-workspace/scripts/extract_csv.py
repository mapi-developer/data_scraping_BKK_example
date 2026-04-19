import os
import pandas as pd
from sqlalchemy import create_engine

# Setup Database Connection
engine = create_engine('postgresql://user:password@localhost:5432/bkk_bronze')

def heal_csv(filepath):
    """Cleans the existing CSV of duplicate headers, corrupt rows, and FULL duplicate rows."""
    if not os.path.exists(filepath):
        return
        
    try:
        # Read as strings to avoid mixed-type chunking errors
        existing_df = pd.read_csv(filepath, dtype=str)
        initial_rows = len(existing_df)
        
        if existing_df.empty:
            return

        # Remove accidental header rows appended in the data
        existing_df = existing_df[existing_df['ingested_at'] != 'ingested_at']
        
        # Convert ingested_at to real datetime objects
        existing_df['ingested_at'] = pd.to_datetime(existing_df['ingested_at'], errors='coerce')
        existing_df = existing_df.dropna(subset=['ingested_at'])
        
        # THE EXACT MATCH FILTER: Drops only if 100% of the row is identical
        existing_df = existing_df.drop_duplicates()

        # Save the healed CSV back to disk
        existing_df.to_csv(filepath, index=False)
        
        rows_removed = initial_rows - len(existing_df)
        if rows_removed > 0:
            print(f"Healed {filepath}: Removed {rows_removed} perfectly identical duplicate/corrupt rows.")
            
    except Exception as e:
        print(f"Warning: Could not run healing routine on {filepath}. Error: {e}")

def extract_feed(table_name, csv_filepath):
    """Extracts incremental data from a specific Postgres table to a specific CSV."""
    print(f"--- Processing {table_name} ---")
    
    # Run healing routine first to ensure a pristine file
    heal_csv(csv_filepath)
    
    file_exists = os.path.exists(csv_filepath)
    last_ingested = None

    # Find the most recent ingestion timestamp to prevent SQL duplicates
    if file_exists:
        try:
            existing_timestamps = pd.read_csv(csv_filepath, usecols=['ingested_at'], parse_dates=['ingested_at'])
            if not existing_timestamps.empty:
                last_ingested = existing_timestamps['ingested_at'].max()
        except pd.errors.EmptyDataError:
            pass

    # Query incremental data
    if last_ingested:
        query = f"SELECT ingested_at, raw_data FROM {table_name} WHERE ingested_at > '{last_ingested}';"
    else:
        query = f"SELECT ingested_at, raw_data FROM {table_name};"

    df = pd.read_sql(query, engine)

    if df.empty:
        print(f"No new data for {table_name}. Up to date!\n")
        return

    # Flatten the JSON payload and combine with timestamps
    json_df = pd.json_normalize(df['raw_data'])
    final_df = pd.concat([df[['ingested_at']], json_df], axis=1)
    
    # THE EXACT MATCH FILTER: Prevents identical rows in the new batch from appending
    clean_index = final_df.astype(str).drop_duplicates().index
    final_df = final_df.loc[clean_index]

    # Append safely to the CSV
    final_df.to_csv(csv_filepath, mode='a', header=not file_exists, index=False)
    print(f"Appended {len(final_df)} clean rows to {csv_filepath}.\n")

# Execute extraction for BOTH sources
extract_feed('bkk_raw_payloads', '../data/raw/bkk_positions.csv')
extract_feed('bkk_trip_updates', '../data/raw/bkk_delays.csv')

print("All extraction tasks complete.")