import time
import pandas as pd
import numpy as np
import json
import os
import warnings
import pymysql
from typing import List, Tuple
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import pandas as pd
from functools import lru_cache
 
warnings.filterwarnings("ignore")

def get_last_processed_timestamp(connection):
    """Get the last processed timestamp from the logger table (most recent entry)"""
    try:
        with connection.cursor() as cursor:
            # Get the most recent row by ID (descending order)
            sql = "SELECT last_timestamp FROM additive_report_dummy_logger_id ORDER BY id DESC LIMIT 1"
            cursor.execute(sql)
            result = cursor.fetchone()
            
            if result and result['last_timestamp']:
                return pd.to_datetime(result['last_timestamp'])
            else:
                return None
    except Exception as e:
        print(f"Error getting last processed timestamp: {e}")
        return None

def insert_logger_entry(connection, timestamp):
    """Insert a new row in the logger table with the current timestamp"""
    try:
        with connection.cursor() as cursor:
            sql = "INSERT INTO additive_report_dummy_logger_id (last_timestamp) VALUES (%s)"
            cursor.execute(sql, (timestamp,))
        
        connection.commit()
        print(f"New logger entry created: Last timestamp = {timestamp}")
    except Exception as e:
        print(f"Error inserting logger entry: {e}")
        connection.rollback()

def run_etl(config, engine, connection, target_table):
    """Main ETL function"""
    # Get the last processed timestamp from the logger table
    last_timestamp = get_last_processed_timestamp(connection)
    if last_timestamp:
        print(f"Last processed timestamp: {last_timestamp}")
    else:
        print("First run or no timestamp found in logger.")
    
    # Load data from database
    print("Loading source data...")
    smc_df = pd.read_sql("SELECT * FROM prepared_sand_extra_test", engine)
    df_add = pd.read_sql("SELECT * FROM additive_data_v2", engine)
    prod_data = pd.read_sql("SELECT * FROM consumption_booking_test", engine)
 
    # Convert datetime string to pandas datetime
    df_add['datetime'] = pd.to_datetime(df_add['datetime'], format='%Y-%m-%d %H:%M:%S')
 
    def date_adjust(data: pd.DataFrame, col_name: str, to_foundry: bool):
        shift_cutoff = pd.to_datetime(config["shift_time"]["A"][0]).time()
        multiplier = 1 if to_foundry else -1
 
        time_mask = data[col_name].dt.time < shift_cutoff
        data["datetime"] = data["datetime"] + pd.to_timedelta(time_mask.astype(int) * -1 * multiplier, unit='D')
        return data
 
    df_add = date_adjust(df_add, "datetime", True)
 
    # Define column pairs for cleaning
    column_pairs = [
        ("New_Sand_Set_kgs", "New_Sand_Act_Kgs"),
        ("Bentonite_Set_Kgs", "Bentonite_Act_Kgs"),
        ("Return_Sand_Set_Kgs", "Return_Sand_Act_Kgs"),
        ("Fine_Dust_Set_Kgs", "Fine_Dust_Act_Kgs"),
        ("Coal_Dust_Set_Kgs", "Coal_Dust_Act_Kgs"),
        ("Water_Dosing_Set_Litre", "Water_Dosing_Act_Litre"),
    ]
 
    def clean_actual_columns(df: pd.DataFrame, column_pairs: List[Tuple[str, str]]) -> pd.DataFrame:
        for set_col, act_col in column_pairs:
            df[set_col] = pd.to_numeric(df[set_col], errors='coerce')
            df[act_col] = pd.to_numeric(df[act_col], errors='coerce')
 
            mask = (df[set_col] != 0) & (df[act_col] <= 0)
            df.loc[mask, act_col] = np.nan
 
            temp_col = df[act_col].replace(0, np.nan)
            filled = temp_col.ffill().bfill()
            df[act_col] = df[act_col].combine_first(filled)
            df.loc[df[set_col] == 0, act_col] = 0
 
        return df
 
    df_add = clean_actual_columns(df_add, column_pairs)
 
    # Rename columns according to config
    column_renaming = config["columns_to_rename"]
    df_add.rename(columns=column_renaming, inplace=True)
    print("Renamed columns:", df_add.columns.tolist())
 
    def smc_data_preprocessing(smc_df):
        smc_df['date'] = smc_df['date'].astype(str)
        smc_df['datetime'] = pd.to_datetime(smc_df['date']) + pd.to_timedelta(smc_df['time'])
        smc_df['batch_counter'] = smc_df.groupby(config['Batch_reset']).cumcount() + 1
        smc_df['datetime'] = pd.to_datetime(smc_df['datetime'], format='%Y-%m-%d %H:%M')
        smc_df = smc_df.sort_values('datetime')
        return smc_df
 
    smc_df = smc_data_preprocessing(smc_df)
    df_add = df_add.sort_values('datetime')
 
    # Merge the datasets using datetime as the key
    matched_df = pd.merge_asof(
        smc_df,
        df_add,
        on='datetime',
        direction='nearest',
    )
 
    # Extract temporary date and time parts for processing
    matched_df['date'] = matched_df['datetime'].dt.date
    matched_df['time'] = matched_df['datetime'].dt.time
    matched_df = matched_df.sort_values(['date', 'time'])
 
    # Process product data for component lookup
    print("Processing component data...")
    prod_data['StartTime'] = pd.to_datetime(prod_data['date'] + pd.to_timedelta(prod_data['start_time']))
    prod_data['EndTime'] = pd.to_datetime(prod_data['date'] + pd.to_timedelta(prod_data['end_time']))
   
    # Fix end times that cross midnight
    midnight_crossings = prod_data['EndTime'] < prod_data['StartTime']
    prod_data.loc[midnight_crossings, 'EndTime'] += pd.Timedelta(days=1)
   
    # Create a lookup dictionary for faster component_id lookups
    component_ranges = []
    for _, row in prod_data.iterrows():
        component_ranges.append({
            'start': row['StartTime'],
            'end': row['EndTime'],
            'component_id': row['component_id'],
            'span_hours': (row['EndTime'] - row['StartTime']).total_seconds() / 3600
        })
   
    @lru_cache(maxsize=1000)
    def get_component_id(dt_str):
        dt = pd.Timestamp(dt_str)
        dt_next_day = dt + pd.Timedelta(days=1)
       
        # First try with the original datetime
        for comp_range in component_ranges:
            if comp_range['start'] <= dt <= comp_range['end']:
                return comp_range['component_id']
       
        # For multi-day components, check next day
        for comp_range in component_ranges:
            if comp_range['span_hours'] > 12:  # Only check for long-running components
                if comp_range['start'] <= dt_next_day <= comp_range['end']:
                    return comp_range['component_id']
       
        return None
 
    # Apply the component lookup function
    print("Matching components to time ranges...")
    matched_df['component_id'] = matched_df['datetime'].astype(str).apply(get_component_id)
    matched_df['mixer_name'] = config['Mixer Name']
   
    # Assign shift based on time
    def assign_shift(row):
        shift_a_start = datetime.strptime(config["shift_time"]["A"][0], "%H:%M:%S").time()
        shift_b_start = datetime.strptime(config["shift_time"]["B"][0], "%H:%M:%S").time()
       
        if shift_a_start <= row.time() < shift_b_start:
            return 'A'
        else:
            return 'B'
   
    matched_df['shift'] = matched_df['datetime'].apply(lambda dt: assign_shift(dt))
 
    # Create the timestamp column properly accounting for shift
    def compute_timestamp(row):
        base_datetime = datetime.combine(row['date'], row['time'])
       
        # Adjust for B shift times after midnight
        if row['shift'] == 'B' and row['time'] < datetime.strptime("07:00", "%H:%M").time():
            return base_datetime + timedelta(days=1)  
        else:
            return base_datetime
 
    # Create the timestamp column
    matched_df['timestamp'] = matched_df.apply(compute_timestamp, axis=1)
    
    # List of columns to select (excluding date and time, including timestamp)
    columns_to_select = [col for col in config["columns_to_select"] if col not in ['date', 'time']]
    columns_to_select.insert(0, 'timestamp')  # Add timestamp as the first column
    
    # Check if all required columns exist
    missing_columns = [col for col in columns_to_select if col not in matched_df.columns]
    if missing_columns:
        print(f"Warning: Missing columns in dataframe: {missing_columns}")
        print("Available columns:", matched_df.columns.tolist())
       
        # Fill missing columns with NaN
        for col in missing_columns:
            matched_df[col] = np.nan
 
    # Select only the needed columns
    df = matched_df[columns_to_select]
    
    # Sort by timestamp
    df = df.sort_values(by=['timestamp'])
   
    # Rename columns to match the target database schema
    output_columns = config["output_columns"].copy()
    
    # Ensure timestamp is included in output columns mapping
    if 'timestamp' not in output_columns:
        output_columns['timestamp'] = 'timestamp'
    
    # Apply column renaming
    df.rename(columns=output_columns, inplace=True)
   
    # Filter out records that already exist in the database
    if last_timestamp is not None:
        new_records = df[df['timestamp'] > last_timestamp]
        old_records = df[df['timestamp'] <= last_timestamp]
        print(f"Total records: {len(df)}")
        print(f"Records already in database: {len(old_records)}")
        print(f"New records to insert: {len(new_records)}")
        df = new_records
    
    # If no new records, return empty dataframe with the latest timestamp
    if len(df) == 0:
        print("No new records to insert.")
        return df, None
    
    # Get the latest timestamp from the new records
    latest_timestamp = df['timestamp'].max()
    
    # Show sample data
    print("Sample of new data to be inserted:")
    print(df.head())
   
    # Check column names match the target schema
    print("Final columns for DB insert:", df.columns.tolist())
    return df, latest_timestamp
 
if __name__ == "__main__":
    cd = os.getcwd()
    config_dir = os.path.join(cd, "config")
    config_file_path = os.path.join(config_dir, "config.json")
    
    # Load configuration
    with open(config_file_path, "r") as config_file:
        config = json.load(config_file)
 
    # Database connection parameters
    db_config = config["database"]
    DB_USER = db_config["user"]
    DB_PASSWORD = db_config["password"]
    DB_HOST = db_config["host"]
    DB_PORT = db_config["port"]
    DB_NAME = db_config["database_name"]
 
    # Create database engine for pandas
    engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    
    # Create a direct pymysql connection for more efficient execution and transactions
    connection = pymysql.connect(
        host=DB_HOST,
        port=int(DB_PORT),
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor
    )
    
    # Define target table
    target_table = "additive_report_dummy_rename"
   
    print("Starting ETL process...")
 
    # Main ETL loop
    try:
        while True:
            current_time = datetime.now()
            print(f"Running ETL at {current_time}")
            try:
                # Run ETL process
                df, latest_timestamp = run_etl(config, engine, connection, target_table)
                
                # Insert data to the correct table with timestamp column, but only if there are new records
                if not df.empty:
                    print(f"Writing {len(df)} new records to table: {target_table}")
                    df.to_sql(target_table, con=engine, if_exists='append', index=False)
                    
                    # Insert a new log entry with the latest timestamp
                    if latest_timestamp:
                        insert_logger_entry(connection, latest_timestamp)
                    
                    print("ETL process completed successfully.")
                else:
                    print("No new data to insert. ETL skipped.")
            except Exception as e:
                print("ETL failed:", e)
                import traceback
                traceback.print_exc()
    
            print(f"Next ETL run in 60 seconds. Press Ctrl+C to exit cleanly.")
            time.sleep(60) # Wait for 60 seconds before the next run
    except KeyboardInterrupt:
        print("ETL process terminated by user.")
    finally:
        connection.close()
        print("Database connection closed.")