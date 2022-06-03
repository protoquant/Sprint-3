from datetime import date, datetime, timedelta
# from dotenv import load_dotenv


import os
import json
import pandas as pd
import requests
import time

def get_data(address, start):

    all_data = {}


    for _ in range(13500):
        start_plus_1H =  datetime.fromtimestamp(start) + timedelta(hours=8)
        end_time = int(time.mktime(start_plus_1H.timetuple()))
        url = f"https://api.compound.finance/api/v2/market_history/graph?asset={address}&min_block_timestamp={start}&max_block_timestamp={end_time}&num_buckets=2"
        res = requests.get(url).json()
        if len(all_data) == 0:
            all_data = res
        else:
            all_data['borrow_rates'].extend(res['borrow_rates'])
            all_data['exchange_rates'].extend(res['exchange_rates'])
            all_data['prices_usd'].extend(res['prices_usd'])
            all_data['supply_rates'].extend(res['supply_rates'])
            all_data['total_borrows_history'].extend(res['total_borrows_history'])
            all_data['total_supply_history'].extend(res['total_supply_history'])
        start = end_time
    return all_data

def pad_name(pad_last, df):
    old_col_names = list(df.columns)
    new_col_names = []
    for col in old_col_names:
        new_col_names.append(col+pad_last)
    df.rename(dict(zip(old_col_names, new_col_names)), axis=1, inplace=True) 
    return df

def collect_data(address, asset_name, start_time_stamp):

    data = get_data(address, start_time_stamp)

    ex_rates = pd.DataFrame(data['exchange_rates'])
    borrow_rates = pd.DataFrame(data['borrow_rates'])
    supply_rates = pd.DataFrame(data['supply_rates'])
    total_borrows_history = pd.DataFrame(data['total_borrows_history'])
    total_supply_history = pd.DataFrame(data['total_supply_history'])
    prices_usd = pd.DataFrame(data['prices_usd'])

    data_dict = {'timestamp': list(ex_rates['block_timestamp']),
             'block_number': list(ex_rates['block_number']),
            'exchange_rates': list(ex_rates['rate']),
             'borrow_rates': list(borrow_rates['rate']), 
             'supply_rates': list(supply_rates['rate']), 
             'total_borrows_history': list(total_borrows_history['total']), 
             'total_supply_history': list(total_supply_history['total']), 
             'prices_usd': list(prices_usd['price'])}

    all_data = pd.DataFrame(data=data_dict)

    all_data['total_borrows_history'] = all_data['total_borrows_history'].apply(lambda x: x['value'])
    all_data['total_supply_history'] = all_data['total_supply_history'].apply(lambda x: x['value'])
    all_data['prices_usd'] = all_data['prices_usd'].apply(lambda x: x['value'])


    all_data['timestamp'] = all_data['timestamp'].apply(lambda x: datetime.fromtimestamp(x))

    # col_name_change_1H = pad_name("_compound_1H", all_data)
    # col_name_change_2H = pad_name("_compound_2H", asset_2H)
    # col_name_change_4H = pad_name("_compound_4H", all_data)
    # col_name_change_8H = pad_name("_compound_8H", all_data)
    # col_name_change_24H = pad_name("_compound_24H", asset_24H)
    col_name_change_48H = pad_name("_compound_48H", all_data)
    # col_name_change_72H = pad_name("_compound_72H", asset_72H)

    # col_name_change_1H.to_csv(f'{asset_name}_compound_1H.csv')
    # col_name_change_2H.to_csv(f'{asset_name}_compound_2H.csv')
    # col_name_change_4H.to_csv(f'{asset_name}_compound_4H.csv')
    # col_name_change_8H.to_csv(f'{asset_name}_compound_8H.csv')
    # col_name_change_24H.to_csv(f'{asset_name}_compound_24H.csv')
    col_name_change_48H.to_csv(f'{asset_name}_compound_48H.csv')
    # col_name_change_72H.to_csv(f'{asset_name}_compound_72H.csv')
    
    return 1

assets = { 
          "DAI": "0x5d3a536e4d6dbd6114cc1ead35777bab948e3643",
          "BTC": "0xc11b1268c1a384e55c48c2391d8d480264a3a7f4"}

#assets = {"BTC": "0xc11b1268c1a384e55c48c2391d8d480264a3a7f4"}

start = int(datetime(2020, 11, 26,0,0,0).timestamp())

names = list(assets.keys())

for name in names:
    collect_data(assets[name], name, start)
