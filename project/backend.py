## SmartLogix ETL - Clean and Load to PostgreSQL

import pandas as pd
from sqlalchemy import create_engine

# === PHASE 1: Load CSV ===
df = pd.read_csv("delhivery.csv")

# === PHASE 2: Clean & Transform ===
df.drop_duplicates(inplace=True)

# Convert to datetime
datetime_columns = ['trip_creation_time', 'od_start_time', 'od_end_time', 'cutoff_timestamp']
for col in datetime_columns:
    df[col] = pd.to_datetime(df[col], errors='coerce')

# Standardize text
text_columns = ['route_type', 'source_name', 'destination_name']
for col in text_columns:
    df[col] = df[col].astype(str).str.strip().str.title()

# Handle numerics
columns_to_fill = [
    'actual_time', 'osrm_time', 'factor',
    'actual_distance_to_destination', 'osrm_distance',
    'segment_actual_time', 'segment_osrm_time', 'segment_osrm_distance', 'segment_factor'
]
df[columns_to_fill] = df[columns_to_fill].fillna(0)
df[columns_to_fill] = df[columns_to_fill].apply(pd.to_numeric, errors='coerce')

# Derive new feature
df["time_deviation"] = df["actual_time"] - df["osrm_time"]

# Standardize column names
df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

# 💾 Save cleaned version
df.to_csv("cleaned_delhivery.csv", index=False)

# === PHASE 3: Load to PostgreSQL ===
engine = create_engine("postgresql+psycopg2://delhivery_user:temp123@localhost:5432/logistics_db")
df.to_sql("delhivery_logistics", con=engine, index=False, if_exists="replace")

print("Data cleaned and loaded successfully.")
