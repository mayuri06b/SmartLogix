## SmartLogix
import pandas as pd
from sqlalchemy import create_engine
from great_expectations.dataset import PandasDataset


# Load the CSV file
df = pd.read_csv("delhivery.csv")  # Replace with actual filename

# === ETL: Clean and Transform ===
df.drop_duplicates(inplace=True)

datetime_columns = [
    'trip_creation_time',
    'od_start_time',
    'od_end_time',
    'cutoff_timestamp'
]
for col in datetime_columns:
    df[col] = pd.to_datetime(df[col], errors='coerce')

text_columns = ['route_type', 'source_name', 'destination_name']
for col in text_columns:
    df[col] = df[col].astype(str).str.strip().str.title()

columns_to_fill = [
    'actual_time', 'osrm_time', 'factor',
    'actual_distance_to_destination', 'osrm_distance',
    'segment_actual_time', 'segment_osrm_time', 'segment_osrm_distance', 'segment_factor'
]
df[columns_to_fill] = df[columns_to_fill].fillna(0)
df[columns_to_fill] = df[columns_to_fill].apply(pd.to_numeric, errors='coerce')

df["time_deviation"] = df["actual_time"] - df["osrm_time"]
df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

# === PHASE 3: Validate Data with Great Expectations ===
# Wrap DataFrame
class CustomDataset(PandasDataset):
    pass

ge_df = CustomDataset(df)

# Define expectations
ge_df.expect_column_values_to_not_be_null("trip_uuid")
ge_df.expect_column_values_to_be_in_set("route_type", ["Carting", "Feeder", "Last-Mile", "Hub Transfer"])
ge_df.expect_column_values_to_be_between("actual_time", min_value=0, max_value=1000)
ge_df.expect_column_values_to_be_between("segment_factor", min_value=0.0, max_value=5.0)

# Validate
results = ge_df.validate()

if not results["success"]:
    print("❌ Data validation failed. Aborting load.")
    exit()
    
# === Load to PostgreSQL ===
engine = create_engine("postgresql+psycopg2://delhivery_user:temp123@localhost:5432/logistics_db")
ge_df.to_sql("delhivery_logistics", con=engine, index=False, if_exists="replace")

print("✅ Data validated and loaded successfully.")
