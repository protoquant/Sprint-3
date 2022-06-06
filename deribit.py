

!pip install websockets
from datetime import datetime
!python index.py
import pandas as pd

eth = pd.read_csv('eth_2020_master.csv')

btc.timestamp = pd.to_datetime(btc.timestamp)

def pad_name(pad_last, df):
    old_col_names = list(df.columns)
    new_col_names = []
    for col in old_col_names:
      
        new_col_names.append(col+pad_last)
    df.rename(dict(zip(old_col_names, new_col_names)), axis=1, inplace=True) 
    return df
pad_name("_deribit_1H_BTC", eth)

eth_1H = eth.set_index('timestamp').resample('1H').last()
eth_2H = eth.set_index('timestamp').resample('2H').last()
eth_4H = eth.set_index('timestamp').resample('4H').last()
eth_8H = eth.set_index('timestamp').resample('8H').last()
eth_24H = eth.set_index('timestamp').resample('24H').last()
eth_48H = eth.set_index('timestamp').resample('48H').last()
eth_72H = eth.set_index('timestamp').resample('73H').last()

eth_1H.to_csv('eth_1H_last.csv')
eth_2H.to_csv('eth_2H.csv')
eth_4H.to_csv('eth_4H.csv')
eth_8H.to_csv('eth_8H.csv')
eth_24H.to_csv('eth_24H.csv')
eth_48H.to_csv('eth_48H.csv')
eth_72H.to_csv('eth_72H.csv')




