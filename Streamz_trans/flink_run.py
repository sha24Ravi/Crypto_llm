from kafka import KafkaProducer
from streamz import Stream
from streamz import from_kafka
import json
from datetime import datetime
import pandas as pd

class stream_data:

 def __init__(self):
       
    self.source = Stream.from_kafka(
     ['crypto_trades'],
    {
        'bootstrap.servers': 'broker:9092',
        'group.id': 'streamz'
    },
    )
    self.producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
    self.transformed_topic = "crypto_trades_transformed"

 def transform_trade(self,msg):
    # decode JSON string → dict
    trade = json.loads(msg)
    
    # format timestamp (from milliseconds to readable datetime)
    ts = datetime.fromtimestamp(trade["timestamp"] / 1000.0)

    return {
        "symbol": trade["symbol"],
        "price": round(float(trade["price"]), 1),
        "quantity": round(float(trade["quantity"]), 1),
        "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S")  # formatted string
    }

#  def start_stream(self):
#    stream = self.source.map(self.transform_trade)
#    stream.sink(print)
#    self.source.start()
#    print("Stream started. Waiting for messages...")

 def send_to_transformed_topic(self,trade):
    self.producer.send(self.transformed_topic, trade)
    print("sent data",trade)
    # self.producer.flush()  


 def summarize(self,batch):
    if not  batch :
       return
    else:
     prices = [float(trade['price']) for trade in batch]
     quantities = [float(trade['quantity']) for trade in batch]

     summary = {
        "symbol": batch[0]['symbol'],
        "min_price": min(prices),
        "max_price": max(prices),
        "avg_price": sum(prices)/len(prices),
        "total_qty": sum(quantities),
        "num_trades": len(batch)
    }

     self.send_to_transformed_topic(summary)   

 def initiate_stream(self):
   print("inside start method")
   parsed = self.source.map(self.transform_trade)
   windowed = parsed.timed_window(120)
   windowed.sink(self.summarize)
   parsed.start()
    

# def main():
#     env= create_flink_env()
#     stream=Kafka_trade_source(env=env)
#     formatted_stream = format_stream(stream)
#     formatted_stream.print()
#     kafka_sink(formatted_stream)
#     # try:
#     #  env.execute("Kafka Formatting Job")
#     # except Exception as e:
#     #  print("Job execution failed!")
#     #  traceback.print_exc()
#     # env.execute("Format Kafka Stream Job")              
#    # windowed_stream=window_funct(parsed_stream)
   
   



