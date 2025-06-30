import time
import pandas as pd
import numpy as np
import json
import os
import warnings
import pymysql
from sqlalchemy import create_engine
from datetime import datetime

warnings.filterwarnings("ignore")

def get_last_processed_timestamp(connection):
    try:
        with connection.cursor() as cursor:
            sql = "SELECT last_timestamp FROM mixer_report_test_logger_id ORDER BY id DESC LIMIT 1"
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
    try:
        with connection.cursor() as cursor:
            sql = "INSERT INTO mixer_report_test_logger_id (last_timestamp) VALUES (%s)"
            cursor.execute(sql, (timestamp,))
        connection.commit()
        print(f"Logger updated with timestamp: {timestamp}")
    except Exception as e:
        print(f"Error inserting logger entry: {e}")
        connection.rollback()

def assign_shift(dt, shift_config):
    time_only = dt.time()
    shift_a_start = datetime.strptime(shift_config["A"][0], "%H:%M:%S").time()
    shift_b_start = datetime.strptime(shift_config["B"][0], "%H:%M:%S").time()

    if shift_a_start <= time_only < shift_b_start:
        return 'A'
    else:
        return 'B'

def run_etl(config, engine, connection, target_table):
    last_timestamp = get_last_processed_timestamp(connection)
    if last_timestamp:
        print(f"Last processed timestamp: {last_timestamp}")
    else:
        print("No previous timestamp found. Processing all records.")

    print("Loading source data from 'mixer' table...")
    df = pd.read_sql("SELECT * FROM mixer ORDER BY ID DESC LIMIT 10000", engine)
    df['Date_Time'] = pd.to_datetime(df['Date_Time'])

    if last_timestamp:
        df = df[df['Date_Time'] > last_timestamp]

    if df.empty:
        print("No new records found.")
        return df, None

    df['timestamp'] = df['Date_Time']
    df['shift'] = df['Date_Time'].apply(lambda x: assign_shift(x, config['shift_time']))
    df['mixer_name'] = config['Mixer Name']
    df['component_id'] = None

    rename_map = config['columns_to_rename']
    df.rename(columns=rename_map, inplace=True)

    for col in config['columns_to_select']:
        if col not in df.columns:
            df[col] = np.nan

    df = df[config['columns_to_select']]
    df = df.sort_values(by='timestamp')
    latest_timestamp = df['timestamp'].max()

    print(f"Inserting {len(df)} new records into {target_table}...")
    df.to_sql(target_table, con=engine, if_exists='append', index=False)
    insert_logger_entry(connection, latest_timestamp)

    return df, latest_timestamp

if __name__ == "__main__":
    cd = os.getcwd()
    config_path = os.path.join(cd, "config", "config.json")

    with open(config_path) as f:
        config = json.load(f)

    db = config['database']
    engine = create_engine(f"mysql+pymysql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['database_name']}")
    connection = pymysql.connect(
        host=db['host'],
        user=db['user'],
        password=db['password'],
        database=db['database_name'],
        port=int(db['port']),
        cursorclass=pymysql.cursors.DictCursor
    )

    try:
        while True:
            print(f"Running ETL at {datetime.now()}")
            df, latest_ts = run_etl(config, engine, connection, "mixer_report_test")
            time.sleep(60)
    except KeyboardInterrupt:
        print("ETL terminated by user.")
    finally:
        connection.close()
        print("Database connection closed.")
