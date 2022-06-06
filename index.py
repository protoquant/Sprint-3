#Import our Packages
import asyncio
import websockets
import json
import pandas as pd
import datetime as dt
import time

# connect to the API
async def call_api(msg):
   async with websockets.connect('wss://test.deribit.com/ws/api/v2') as websocket:
       await websocket.send(msg)
       while websocket.open:
           response = await websocket.recv()
           return response



def async_loop(api, message):
    return asyncio.get_event_loop().run_until_complete(api(message))


# Get historical data
def retrieve_historic_data(start, end, instrument, timeframe):
    msg = \
        {
            "jsonrpc": "2.0",
            "id": 833,
            "method": "public/get_tradingview_chart_data",
            "params": {
                "instrument_name": instrument,
                "start_timestamp": start,
                "end_timestamp": end,
                "resolution": timeframe
            }
        }
    resp = async_loop(call_api, json.dumps(msg))

    return resp


## Take the result and put it in Pandas dataframe

def json_to_dataframe(json_resp):
    res = json.loads(json_resp)

    df = pd.DataFrame(res['result'])

## Dividing the timestamps by a 1000 and convert it to date time
    df['ticks'] = df.ticks/1000
    df['timestamp'] = [dt.datetime.utcfromtimestamp(date) for date in df.ticks]

    return df


## Get data
def get_data(date1, instrument,tf=1 ):
      # find the number of days between start of the day
      n_days = (dt.datetime.now() - date1).days
      
      #intialise the master dataframe
      df_master = pd.DataFrame()

      d1 = date1
      
      #loop the timestamp and turn it to datetime
      for _ in range(n_days):
            d2 = d1 + dt.timedelta(hours=1)

            t1 = dt.datetime.timestamp(d1)*1000
            t2 = dt.datetime.timestamp(d2)*1000

            json_resp = retrieve_historic_data(t1, t2, instrument, tf)
        

            #Create a temporary df and append it to the master df
            temp_df = json_to_dataframe(json_resp)

            # Append the temporary dataframe to the Master one
            df_master = df_master.append(temp_df)

            print(f"collected data for date: {d1.isoformat()} to {d2.isoformat()}")

            # I am 
            # doing it is becasue we have some kind of limits of number of api calls we can make
            print('sleeping for 2 seconds')
            time.sleep(2)

            d1 = d2


      return df_master



if __name__ == '__main__':
    #change this to two years prior to the day you are using this script
    start = dt.datetime(2020, 11, 26, 0, 0)
    instrument = "BTC-PERPETUAL"
    tf = "120"

    df_master = get_data(start, instrument, tf)
    
    # store it in csv format
    df_master.to_csv('btc_2H_master.csv')


