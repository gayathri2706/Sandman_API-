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
     elif file.startswith("Scada") and file.endswith(".xlsx"):
            file_path = os.path.join(data_dir, file)
            scada_df = pd.read_excel(file_path,skiprows=5)



scada_df['Datetime']=pd.to_datetime(scada_df['Date'].astype(str)
+" "+ scada_df['Time'],format='%Y-%m-%d %H:%M')

def date_adjust(data: pd.DataFrame, col_name: str, to_foundry: bool):
    shift_cutoff = pd.to_datetime(config["shift_time"]["A"][0]).time()
    multiplier = 1 if to_foundry else -1

    time_mask = data[col_name].dt.time < shift_cutoff

    data["Datetime"] = data["Datetime"] + pd.to_timedelta(time_mask.astype(int) * -1 * multiplier, unit='D')
    return data

scada_df= date_adjust(scada_df,"Datetime",True)



def get_column_mapping(dict1):
    rename_dict = {}
    for key,value in dict1.items():
        rename_dict[value] = key    
    return rename_dict
rename_dict = get_column_mapping(config['columns_to_rename'])
scada_df.rename(columns = rename_dict,inplace =True)


def smc_data_preprocessing(smc_df):
    smc_df['Date']=smc_df['Date'].astype(str)
    smc_df['Datetime']=smc_df['Date']+" "+ smc_df['Time']

    smc_df['Batch Counter'] = smc_df.groupby(config['Batch_reset']).cumcount() + 1

    smc_df['Datetime'] = pd.to_datetime(smc_df['Datetime'],format='%Y-%m-%d %H:%M')

    return smc_df


smc_df=smc_data_preprocessing(smc_df)

smc_df = smc_df.sort_values('Datetime')
scada_df = scada_df.sort_values('Datetime')

matched_df = pd.merge_asof(
    smc_df,
    scada_df,
    on='Datetime',
    direction='forward'
)

matched_df['Mixer Name']=config['Mixer Name']

matched_df['Water Actual']=matched_df['Total Water (ltr)']
matched_df.rename(columns={'Date_x': 'Date', 'Time_x': 'Time'}, inplace=True)
matched_df['Recycle sand Actual']=2500
#matched_df.to_excel("matched_data.xlsx", index=False)
df = matched_df[config["columns_to_select"]]

df['Date'] = pd.to_datetime(df['Date'],format='%Y-%m-%d')
df['Time'] = pd.to_datetime(df['Time'].astype(str)).dt.time

def compute_actual_datetime(row):
    base_date = row['Date']
    time = row['Time']
    shift = row['Shift']
    base_datetime = datetime.combine(base_date, time)

    # Only shift date forward for early morning times
    if shift == 'C' and time < datetime.strptime("07:00", "%H:%M").time() and time < datetime.strptime("23:00", "%H:%M").time():
        return base_datetime + timedelta(days=1)
    else:
        return base_datetime
df['ActualDateTime'] = df.apply(compute_actual_datetime, axis=1)
df = df.sort_values(by=['ActualDateTime'])
df.drop(columns=['ActualDateTime'], errors='ignore', inplace=True)

# Optional: Format Date before saving
df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')

df.to_excel("munjal_output.xlsx", index=False)
print("Data processing complete. Output saved to 'munjal_output.xlsx'.")


