import pandas as pd
import numpy as np
import json
import os
import warnings
import plotly.express as px
import matplotlib.pyplot as plt
from typing import List, Tuple

from datetime import datetime, timedelta
warnings.filterwarnings("ignore")

cd=os.getcwd()
data_dir=os.path.join(cd,"data")
config_dir=os.path.join(cd,"config")

for file_name in os.listdir(config_dir):
        if file_name.endswith('.json'):
            file_path = os.path.join(config_dir, file_name)
            with open(file_path, "r") as config_file:
                config = json.load(config_file)

for file in os.listdir(data_dir):
     if file.startswith("Smc") and file.endswith(".xlsx"):
             file_path = os.path.join(data_dir, file)
             smc_df = pd.read_excel(file_path,skiprows=5)
     elif file.startswith("West") and file.endswith(".csv"):
            file_path = os.path.join(data_dir, file)
            df_add = pd.read_csv(file_path, on_bad_lines='skip')
     elif file.startswith("Consumption") and file.endswith(".xlsx"):
            file_path = os.path.join(data_dir, file)
            prod_data = pd.read_excel(file_path, skiprows=5)

df_add['datetime']=pd.to_datetime(df_add['process_date_time'],format='%Y-%m-%d %H:%M:%S')

def date_adjust(data: pd.DataFrame, col_name: str, to_foundry: bool):
    shift_cutoff = pd.to_datetime(config["shift_time"]["A"][0]).time()
    multiplier = 1 if to_foundry else -1

    time_mask = data[col_name].dt.time < shift_cutoff

    data["Datetime"] = data["datetime"] + pd.to_timedelta(time_mask.astype(int) * -1 * multiplier, unit='D')
    return data

df_add = date_adjust(df_add,"datetime",True)

column_pairs = [
    ("bond_weight_sp", "bond_weight"),
    ("water_added_sp", "water_added"),
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

df_add=clean_actual_columns(df_add,column_pairs)


def get_column_mapping(dict1):
    rename_dict = {}
    for key,value in dict1.items():
        rename_dict[value] = key    
    return rename_dict
rename_dict = get_column_mapping(config['columns_to_rename'])
df_add.rename(columns = rename_dict,inplace =True)


def smc_data_preprocessing(smc_df):
    smc_df['Date']=smc_df['Date'].astype(str)
    smc_df['Datetime']=smc_df['Date']+" "+ smc_df['Time']

    smc_df['Batch Counter'] = smc_df.groupby(config['Batch_reset']).cumcount() + 1

    smc_df['Datetime'] = pd.to_datetime(smc_df['Datetime'],format='%Y-%m-%d %H:%M')
    smc_df= smc_df.sort_values('Datetime')

    return smc_df


smc_df=smc_data_preprocessing(smc_df)


df_add = df_add.sort_values('Datetime')

matched_df =pd.merge_asof(
    smc_df,
    df_add,
    on='Datetime',
    direction='nearest',
)

matched_df=matched_df.sort_values(['Date','Time'])

prod_data['StartTime'] = pd.to_datetime(prod_data['Date'].astype(str) + ' ' + prod_data['StartTime'])

prod_data['EndTime']   = pd.to_datetime(prod_data['Date'].astype(str) + ' ' + prod_data['EndTime'])

def get_component_id(dt):
    for _, row in prod_data.iterrows():
        start = row['StartTime']
        end = row['EndTime']
        if end < start:
            end += pd.Timedelta(days=1)
        dt_check = dt
        if dt < start and end - start > pd.Timedelta(hours=12): 
            dt_check += pd.Timedelta(days=1)

        if start <= dt_check <= end:
            return row['ComponentId']
    return None


matched_df['Component ID'] = matched_df['Datetime'].apply(get_component_id)


matched_df['Mixer Name']=config['Mixer Name']


for col in config["columns_to_select"]:
    if col not in matched_df.columns:
        matched_df[col] = np.nan  # or use '' if you want empty strings

# Now select all columns as required
df = matched_df[config["columns_to_select"]]


df['Date'] = pd.to_datetime(df['Date'],format='%Y-%m-%d')
df['Time'] = pd.to_datetime(df['Time'].astype(str)).dt.time

def compute_actual_datetime(row):
    base_datetime = datetime.combine(row['Date'], row['Time'])
    
    if row['Shift'] == 'B' and row['Time'] < datetime.strptime("07:00", "%H:%M").time():
        return base_datetime + timedelta(days=1)  
    else:
        return base_datetime

df['ActualDateTime'] = df.apply(compute_actual_datetime, axis=1)
df['Date']=df['Date'].dt.date
df = df.sort_values(by=['ActualDateTime'])
df.drop(columns=['ActualDateTime'], errors='ignore', inplace=True)

df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')

df.to_excel(os.path.join(data_dir, "processed_data.xlsx"), index=False)
