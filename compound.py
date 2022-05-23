
# Import required packages
from datetime import datetime, timedelta
import time
import json
import pandas as pd
import requests
from pandas import json_normalize

"""call the API"""

url = ("https://api.compound.finance/api/v2/market_history/graph?asset=0x4ddc2d193948926d02f9b1fe9e1daa0718270ed5&min_block_timestamp=1577836800&max_block_timestamp=1653333494&num_buckets=100")
headers = {
    'Content-Type': 'application/json'
}

res = requests.get(url, headers=headers)
res = requests.get(url).json()

stamps = [1577836800]
start_time_stamp = 1577836800 #1/1/2020
end_time_stamp = 1653333494 #23/05/2022
asset_address = "0xccf4429db6322d5c611ee964527d42e5d685dd6a"


while stamps[-1] < end_time_stamp:
    stamp_delta = datetime.fromtimestamp(start_time_stamp) + timedelta(days=100)
    stamp = time.mktime(stamp_delta.timetuple())
    stamps.append(int(stamp))
    start_time_stamp = stamp




def get_data(address, stamps):
    no_of_stamps = len(stamps)
    all_data = {}
    for i in range(len(stamps)):
        if no_of_stamps < i:
            url = f"https://api.compound.finance/api/v2/market_history/graph?asset={address}&min_block_timestamp={stamps[i]}&max_block_timestamp={stamps[i+1]}&num_buckets=100"
        else:
            url = f"https://api.compound.finance/api/v2/market_history/graph?asset={address}&min_block_timestamp={stamps[i]}&max_block_timestamp={end_time_stamp}&num_buckets=100"
        headers = {'Content-Type': 'application/json'}
        #res = requests.get(url, headers=headers)
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
    return all_data

data = get_data(asset_address, stamps)

data

"""print the result"""

data.keys()

"""convert the exchange rate to dataframe"""

btc_df_ex_rates = pd.DataFrame(data['exchange_rates'])
btc_borrow_rates = pd.DataFrame(data['borrow_rates'])
btc_supply_rates = pd.DataFrame(data['supply_rates'])
btc_total_borrows_history = pd.DataFrame(data['total_borrows_history'])
btc_total_supply_history = pd.DataFrame(data['total_supply_history'])
btc_prices_usd = pd.DataFrame(data['prices_usd'])

list(btc_df_ex_rates['rate'])

data_dict = {'timestamp': list(btc_df_ex_rates['block_timestamp']),
             'block_number': list(btc_df_ex_rates['block_number']),
            'exchange_rates': list(btc_df_ex_rates['rate']),
             'borrow_rates': list(btc_borrow_rates['rate']), 
             'supply_rates': list(btc_supply_rates['rate']), 
             'total_borrows_history': list(btc_total_borrows_history['total']), 
             'total_supply_history': list(btc_total_supply_history['total']), 
             'prices_usd': list(btc_prices_usd['price'])}

all_BTC_data = pd.DataFrame(data=data_dict)

all_BTC_data.head()

all_BTC_data['total_borrows_history'] = all_BTC_data['total_borrows_history'].apply(lambda x: x['value'])
all_BTC_data['total_supply_history'] = all_BTC_data['total_supply_history'].apply(lambda x: x['value'])
all_BTC_data['prices_usd'] = all_BTC_data['prices_usd'].apply(lambda x: x['value'])

all_BTC_data.head()

"""change the timestamp to datetime"""

all_BTC_data['timestamp'] = all_BTC_data['timestamp'].apply(lambda x: datetime.fromtimestamp(x))

all_BTC_data.tail()

"""resample the data to get per X hour"""

all_BTC_data_1H = all_BTC_data.set_index('timestamp').resample('1H').pad()
all_BTC_data_2H = all_BTC_data.set_index('timestamp').resample('2H').pad()
all_BTC_data_4H = all_BTC_data.set_index('timestamp').resample('4H').pad()
all_BTC_data_8H = all_BTC_data.set_index('timestamp').resample('8H').pad()
all_BTC_data_24H = all_BTC_data.set_index('timestamp').resample('24H').pad()
all_BTC_data_48H = all_BTC_data.set_index('timestamp').resample('48H').pad()
all_BTC_data_72H = all_BTC_data.set_index('timestamp').resample('73H').pad()

all_BTC_data_1H.to_csv('all_BTC_data_1H.csv')
all_BTC_data_2H.to_csv('all_BTC_data_2H.csv')
all_BTC_data_4H.to_csv('all_BTC_data_4H.csv')
all_BTC_data_8H.to_csv('all_BTC_data_8H.csv')
all_BTC_data_24H.to_csv('all_BTC_data_24H.csv')
all_BTC_data_48H.to_csv('all_BTC_data_48H.csv')
all_BTC_data_72H.to_csv('all_BTC_data_72H.csv')

"""aggregate the data into X Weeks"""

all_BTC_data_1W = all_BTC_data.set_index('timestamp').resample('1W').pad()
all_BTC_data_2W = all_BTC_data.set_index('timestamp').resample('2W').pad()
all_BTC_data_4W = all_BTC_data.set_index('timestamp').resample('4W').pad()
all_BTC_data_8W = all_BTC_data.set_index('timestamp').resample('8W').pad()
all_BTC_data_16W = all_BTC_data.set_index('timestamp').resample('16W').pad()

all_BTC_data_1W.to_csv('rates_1W.csv')
all_BTC_data_2W.to_csv('rates_2W.csv')
all_BTC_data_4W.to_csv('rates_4W.csv')
all_BTC_data_8W.to_csv('rates_8W.csv')
all_BTC_data_16W.to_csv('rates_16W.csv')








"""aggregate the data in into day"""

#all_ETH_data_D = all_ETH_data.set_index('timestamp').resample('D').pad()

