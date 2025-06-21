import pandas as pd
from sqlalchemy import create_engine, text
import datetime
import traceback

print("🚀 Starting ETL Script...")

try:
    # Connect to PostgreSQL
    engine = create_engine("postgresql+psycopg2://delhivery_user:temp123@localhost:5432/logistics_db")
    
    # Test connection
    with engine.connect() as test_conn:
        print("✅ Connected to PostgreSQL.")
    
    # Load cleaned CSV
    df = pd.read_csv("cleaned_delhivery.csv")
    print(f"✅ Loaded CSV with {len(df)} rows.")

    # Use begin() to ensure transaction is committed
    with engine.begin() as conn:
        
        def get_date_id(date_val):
            full_date = pd.to_datetime(date_val).date()
            result = conn.execute(text("SELECT date_id FROM dim_date WHERE full_date = :d"), {'d': full_date}).fetchone()
            if result:
                return result[0]
            day = full_date.day
            month = full_date.month
            year = full_date.year
            day_of_week = full_date.strftime('%A')
            is_weekend = day_of_week in ['Saturday', 'Sunday']
            result = conn.execute(text("""
                INSERT INTO dim_date (full_date, day, month, year, day_of_week, is_weekend)
                VALUES (:d, :day, :month, :year, :dow, :weekend)
                RETURNING date_id
            """), {
                'd': full_date, 'day': day, 'month': month, 'year': year,
                'dow': day_of_week, 'weekend': is_weekend
            }).fetchone()
            return result[0]

        def get_location_id(code, name, loc_type):
            result = conn.execute(text("""
                SELECT location_id FROM dim_location
                WHERE center_code = :code AND center_name = :name AND type = :t
            """), {'code': code, 'name': name, 't': loc_type}).fetchone()
            if result:
                return result[0]
            result = conn.execute(text("""
                INSERT INTO dim_location (center_code, center_name, type)
                VALUES (:code, :name, :t) RETURNING location_id
            """), {'code': code, 'name': name, 't': loc_type}).fetchone()
            return result[0]

        def get_vehicle_id():
            result = conn.execute(text("SELECT vehicle_id FROM dim_vehicles WHERE vehicle_type = 'Unknown'")).fetchone()
            if result:
                return result[0]
            result = conn.execute(text("INSERT INTO dim_vehicles (vehicle_type) VALUES ('Unknown') RETURNING vehicle_id")).fetchone()
            return result[0]

        print("📦 Populating dimension and fact tables...")

        count = 0
        error_count = 0
        
        for _, row in df.iterrows():
            try:
                date_id = get_date_id(row["trip_creation_time"])
                source_id = get_location_id(row["source_center"], row["source_name"], "Source")
                dest_id = get_location_id(row["destination_center"], row["destination_name"], "Destination")
                vehicle_id = get_vehicle_id()

                result = conn.execute(text("""
                    INSERT INTO fact_trips (
                        trip_uuid, route_schedule_uuid, route_type,
                        date_id, source_location_id, destination_location_id, vehicle_id,
                        actual_time, osrm_time, time_deviation,
                        actual_distance_to_destination, osrm_distance, segment_factor, is_cutoff
                    ) VALUES (
                        :trip_uuid, :rs_uuid, :route_type,
                        :date_id, :src_id, :dst_id, :veh_id,
                        :actual_time, :osrm_time, :time_deviation,
                        :actual_distance, :osrm_distance, :seg_factor, :is_cutoff
                    ) ON CONFLICT (trip_uuid) DO NOTHING
                """), {
                    'trip_uuid': row["trip_uuid"],
                    'rs_uuid': row["route_schedule_uuid"],
                    'route_type': row["route_type"],
                    'date_id': date_id,
                    'src_id': source_id,
                    'dst_id': dest_id,
                    'veh_id': vehicle_id,
                    'actual_time': row["actual_time"],
                    'osrm_time': row["osrm_time"],
                    'time_deviation': row["time_deviation"],
                    'actual_distance': row["actual_distance_to_destination"],
                    'osrm_distance': row["osrm_distance"],
                    'seg_factor': row["segment_factor"],
                    'is_cutoff': row.get("is_cutoff", False)
                })
                
                # Check if row was actually inserted
                if result.rowcount > 0:
                    count += 1
                    
                # Progress indicator
                if (count + error_count) % 1000 == 0:
                    print(f"📊 Processed {count + error_count} rows...")
                    
            except Exception as row_error:
                error_count += 1
                print(f"⚠️ Error inserting row {count + error_count}: {row['trip_uuid']}")
                print(f"   Error: {row_error}")
                
                # Stop if too many errors
                if error_count > 100:
                    print("❌ Too many errors, stopping...")
                    break

        print(f"✅ Transaction completed!")
        print(f"   - Successfully inserted: {count} rows")
        print(f"   - Errors encountered: {error_count} rows")
        print(f"   - Total processed: {count + error_count} rows")

except Exception as e:
    print("❌ Script failed:")
    traceback.print_exc()

# Verify the data was inserted
try:
    print("\n🔍 Verifying data insertion...")
    with engine.connect() as conn:
        # Check fact_trips count
        result = conn.execute(text("SELECT COUNT(*) FROM fact_trips")).fetchone()
        print(f"📊 Total rows in fact_trips: {result[0]}")
        
        # Check dimension tables
        for table in ['dim_date', 'dim_location', 'dim_vehicles']:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).fetchone()
            print(f"📊 Total rows in {table}: {result[0]}")
            
except Exception as e:
    print(f"❌ Error verifying data: {e}")