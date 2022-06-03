from datetime import date, datetime, timedelta
# from dotenv import load_dotenv
import os
import json
import pandas as pd
import requests
import time
from functools import reduce

def get_data(base_currency, quote_currency, start, resolution):

    all_data = {}



    for _ in range(5):
        start_plus_1H =  datetime.fromtimestamp(start) + timedelta(hours=2)
        end_time = time.mktime(start_plus_1H.timetuple())
        url = f"https://ftx.us/api/markets/{base_currency}/{quote_currency}/candles?resolution={resolution}&start_time={start}&end_time={end_time}"
        res = requests.get(url).json()
        if len(all_data) == 0:
            all_data = res
        else:
            all_data['result'].extend(res['result'])
        start = end_time
    return all_data

def pad_name(pad_last, df):
    old_col_names = list(df.columns)
    new_col_names = []
    for col in old_col_names:
        new_col_names.append(col+pad_last)
    df.rename(dict(zip(old_col_names, new_col_names)), axis=1, inplace=True) 
    return df

def collect_data(asset, quote_currency, staart, resolution):

    historical = get_data(asset, quote_currency, start, resolution)

    # Convert JSON to Pandas DataFrame
    df = pd.DataFrame(historical['result'])
    df = df.drop_duplicates()
    

    format = '%Y-%m-%dT%H:%M:%S+00:00'

    df['startTime'] = df['startTime'].apply(lambda x: datetime.strptime(x, format))
    df.rename({'startTime':'timestamp'}, axis=1, inplace = True)
    df.drop(['time'], axis=1, inplace=True)

    

    # col_name_change_1H = pad_name(f"_{asset}_ftx_1H", asset_1H)
    col_name_change_2H = pad_name(f"_{asset}_2020_ftx_2H", df)
    # col_name_change_4H = pad_name(f"_{asset}_2020_ftx_4H", asset_4H)
    # col_name_change_8H = pad_name(f"_{asset}_2020_ftx_8H", asset_8H)
    # col_name_change_24H = pad_name(f"_{asset}_2020_ftx_24H", asset_24H)
    # col_name_change_48H = pad_name(f"_{asset}_2020_ftx_48H", asset_48H)
    # col_name_change_72H = pad_name(f"_{asset}_2020_ftx_72H", asset_72H)

    # col_name_change_1H.to_csv(f'{asset}_ftx_1H.csv')
    col_name_change_2H.to_csv(f'{asset}_2020_ftx_2H.csv')
    # col_name_change_4H.to_csv(f'{asset}_2020_ftx_4H.csv')
    # col_name_change_8H.to_csv(f'{asset}_2020_ftx_8H.csv')
    # col_name_change_24H.to_csv(f'{asset}_2020_ftx_24H.csv')
    # col_name_change_48H.to_csv(f'{asset}_2020_ftx_48H.csv')
    # col_name_change_72H.to_csv(f'{asset}_2020_ftx_72H.csv')
    
    return 1

assets = [ 'BTC' ]
quote_currency = 'USD'
resolution= 3600
start = int(datetime(2020, 11, 26,0,0,0).timestamp())

for asset in assets:
    
    df = collect_data(asset, quote_currency, start, resolution)
