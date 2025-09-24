from ingestion.data_fetch import get_data

import json




df=get_data()
df.start_socket()




# def transform_trade(trade):
#     trade['price'] = round(float(trade['price']), 2)
#     trade['quantity'] = round(float(trade['quantity']), 4)
#     from datetime import datetime
#     trade['timestamp'] = datetime.fromtimestamp(trade['timestamp']/1000).strftime('%Y-%m-%d %H:%M:%S')
#     return trade

# source.map(transform_trade(source.current_metadata))
