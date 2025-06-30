import pandas as pd
from sqlalchemy import create_engine

# === 1. Configuration ===
source_db = {
    "host": "sandman.co.in",
    "port": "43306",
    "user": "rbauser",
    "password": "Rba_Data#2024",
    "database": "rba_data"
}

target_db = {
    "host": "sandman.co.in",
    "port": "43306",
    "user": "vishalscadauser",
    "password": "Vishal_scada#2023",
    "database": "vishal_scada"
}

# === 2. Table to transfer ===
table_name = "consumption_booking_test"

# === 3. Create DB Engines ===
source_engine = create_engine(
    f"mysql+pymysql://{source_db['user']}:{source_db['password']}@{source_db['host']}:{source_db['port']}/{source_db['database']}"
)

target_engine = create_engine(
    f"mysql+pymysql://{target_db['user']}:{target_db['password']}@{target_db['host']}:{target_db['port']}/{target_db['database']}"
)

# === 4. Fetch data from source ===
print(f"Fetching data from '{source_db['database']}.{table_name}'...")
df = pd.read_sql_table(table_name, con=source_engine)

# Remove duplicates based on primary key 'pkey'
existing_pkeys = pd.read_sql(f"SELECT pkey FROM {table_name}", target_engine)
df = df[~df['pkey'].isin(existing_pkeys['pkey'])]


# === 5. Insert data into target ===
print(f"Inserting into '{target_db['database']}.{table_name}'...")
df.to_sql(table_name, con=target_engine, if_exists='append', index=False)

print("✅ Data migrated successfully.")
