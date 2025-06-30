import pandas as pd
from sqlalchemy import create_engine

# === 1. Source DB (rba_data) Configuration ===
source_db = {
    "host": "sandman.co.in",
    "port": "43306",
    "user": "rbauser",
    "password": "Rba_Data#2024",
    "database": "rba_data"
}

# === 2. Target DB (mcie_scada) Configuration ===
target_db = {
    "host": "sandman.co.in",
    "port": "43306",
    "user": "mciescadauser",
    "password": "Mcie_Data#2024",
    "database": "mcie_scada"
}

# === 3. Table to transfer ===
table_name = "consumption_booking_test"

# === 4. Create Engines ===
source_engine = create_engine(
    f"mysql+pymysql://{source_db['user']}:{source_db['password']}@{source_db['host']}:{source_db['port']}/{source_db['database']}"
)

target_engine = create_engine(
    f"mysql+pymysql://{target_db['user']}:{target_db['password']}@{target_db['host']}:{target_db['port']}/{target_db['database']}"
)

# === 5. Fetch data from source ===
print(f"Fetching data from '{source_db['database']}.{table_name}'...")
df = pd.read_sql_table(table_name, con=source_engine)

# === 6. Remove already existing primary keys ===
print(f"Checking existing pkeys in '{target_db['database']}.{table_name}'...")
existing_pkeys = pd.read_sql(f"SELECT pkey FROM {table_name}", target_engine)
df = df[~df['pkey'].isin(existing_pkeys['pkey'])]

# === 7. Insert into target ===
print(f"Inserting into '{target_db['database']}.{table_name}'...")
df.to_sql(table_name, con=target_engine, if_exists='append', index=False)

print("✅ Data migrated successfully.")
