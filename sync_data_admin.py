import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.types import Integer, String, Float, DateTime
from sqlalchemy.exc import IntegrityError

# ---------- Configuration ----------
CHUNK_SIZE = 50000
TABLE_NAME = "admin"

pg_username1 = "postgres"
pg_password1 = "Root%40123"
pg_host1 = "localhost"
pg_port1 = "5432"
pg_db1 = "STATE_DB_Test"

pg_username2 = "postgres"
pg_password2 = "Root%40123"
pg_host2 = "localhost"
pg_port2 = "5432"
pg_db2 = "DB_SYNC_Test"

# PostgreSQL Source DB
pg_conn1 = f"postgresql+psycopg2://{pg_username1}:{pg_password1}@{pg_host1}:{pg_port1}/{pg_db1}"
pg_source_engine = create_engine(pg_conn1)

# PostgreSQL Target DB
pg_conn2 = f"postgresql+psycopg2://{pg_username2}:{pg_password2}@{pg_host2}:{pg_port2}/{pg_db2}"
pg_target_engine = create_engine(pg_conn2)


# ---------- dtype mapping ----------
dtype_mapping = {
    "Udise_code": Integer(),
    "State_ID": Integer(),
    "State_Name": String(200),
    "District_ID": Integer(),
    "District_name": String(200),
    "Block_ID": Integer(),
    "Block_name": String(200),
    "Cluster_ID": Integer(),
    "Cluster_name": String(200),
    "School_location_type": Integer(),
    "School_Name": String(300),
    "Latitude": Float(),
    "Longitude": Float(),
    "School_management_type": String(150),
    "Type_of_school": Integer(),
    "School_category_code": Integer(),
    "Is_Active": String(10),
    "School_classification": Integer(),
    "Minority_managed": Integer(),
    "lowest_class_in_school": Integer(),
    "Highest_class_in_school": Integer(),
    "Total_Students": Integer(),
    "Total_Students_Boys": Integer(),
    "Total_Students_girls": Integer(),
    "Total_Teachers": Integer(),
    "Total_Teachers_Male": Integer(),
    "Total_Teachers_Female": Integer(),
    "Total_Teachers_Transgender": Integer(),

    # Infrastructure / Facility related fields
    "Internet_Availability": Integer(),
    "Electricity_Availability": Integer(),
    "Smart_Classrooms_Availability": Integer(),
    "Toilet_Availability": Integer(),
    "Total_Boys_Toilet": Integer(),
    "Total_Girls_Toilet": Integer(),
    "Drinking_Water_Availability": Integer(),
    "Boundary_wall_type": Integer(),
    "Fire_extinguisher_available": Integer(),
    "year_of_establishment": DateTime(),
    "Free_uniform": Integer(),
    "Free_Textbook_primary": Integer(),
    "Free_Textbook_upper_primary": Integer(),
    "Actual_Teaching_Days": Integer(),
    "SMC_Formation_date": DateTime(),
    "SMC_Status": String(50),
    "Total_SMC_Members": Integer(),
    "Is_ICT_lab": Integer(),
    "Total_Laptops": Integer(),
    "Total_Functional_desktops": Integer(),
    "Total_Functional_laptops": Integer(),
    "Total_Functional_Tablets": Integer(),
    "Total_Functional_digital_boards": Integer(),
    "Total_Functional_projectors": Integer(),

    # Audit and Sync tracking fields
    "created_at": DateTime(),
    "updated_at": DateTime(),
    "ingested_at": DateTime(),

    # Incremental sync timestamp
    "Timestamp": DateTime(),
}


# ---------- Utility Functions ----------
def insert_with_debug(df, table, engine):
    """Fallback to row-by-row insert when bulk insert fails"""
    print(f"Inserting row-by-row due to previous error in {table}...")
    for i, row in df.iterrows():
        try:
            row_df = row.to_frame().T
            row_df.to_sql(name=table, con=engine, if_exists="append", index=False, dtype=dtype_mapping)
        except Exception as e:
            print(f"\n Error inserting row {i}: {e}")
            print(row.to_dict())
            break


def get_last_sync_time(engine):
    """Get last synced Timestamp from target table"""
    with engine.connect() as conn:
        try:
            row = conn.execute(text(f'SELECT MAX("Timestamp") FROM "{TABLE_NAME}"')).fetchone()
            return row[0] if row and row[0] else datetime(2000, 1, 1)
        except Exception:
            return datetime(2000, 1, 1)


# ---------- Incremental Sync ----------
try:
    last_sync = get_last_sync_time(pg_target_engine)
    print(f"\n Last sync Timestamp: {last_sync}")

    query = text(f'SELECT * FROM public."{TABLE_NAME}" WHERE "Timestamp" > :ts')
    school_df = pd.read_sql(query, pg_source_engine, params={"ts": last_sync})

    if school_df.empty:
        print(" No new or updated records to sync.")
    else:
        print(f" Rows to sync: {len(school_df)}")

        # Add missing audit columns if not present
        for col in ["created_at", "updated_at", "ingested_at"]:
            if col not in school_df.columns:
                school_df[col] = datetime.utcnow()

        # Convert data types safely
        for col in dtype_mapping:
            if col in school_df.columns:
                try:
                    if isinstance(dtype_mapping[col], Integer):
                        school_df[col] = pd.to_numeric(school_df[col], errors="coerce").fillna(0).astype(int)
                    elif isinstance(dtype_mapping[col], Float):
                        school_df[col] = pd.to_numeric(school_df[col], errors="coerce")
                    elif isinstance(dtype_mapping[col], String):
                        school_df[col] = school_df[col].astype(str)
                    elif isinstance(dtype_mapping[col], DateTime):
                        school_df[col] = pd.to_datetime(school_df[col], errors="coerce")
                except Exception as e:
                    print(f" Type conversion error in column {col}: {e}")

        # Insert into target DB
        try:
            school_df.to_sql(
                name=TABLE_NAME,
                con=pg_target_engine,
                if_exists="append",
                index=False,
                chunksize=CHUNK_SIZE,
                dtype=dtype_mapping
            )
            print(f" Inserted {len(school_df)} new rows into '{TABLE_NAME}'.")
        except Exception as e:
            print(f" Bulk insert failed: {e}")
            insert_with_debug(school_df, TABLE_NAME, pg_target_engine)

except Exception as e:
    print(f"\n Error during school incremental sync: {e}")
