import json
from kafka import KafkaProducer
import websocket
from datetime import datetime



class get_data:

    def __init__(self):
        self.kafka_topic="crypto_trades"
        self.kafka_broker="broker:9092"
        self.producer= KafkaProducer(
            bootstrap_servers=self.kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        

    def on_message(self,ws,message):
        data= json.loads(message)
        trade_event={
        "symbol": data['s'],
        "price": data['p'],
        "quantity": data['q'],
        "timestamp": data['T']

        }
        self.producer.send(self.kafka_topic,trade_event)
        
        
        
    def transform_trade(self, trade):
        trade['price'] = round(float(trade['price']), 2)
        trade['quantity'] = round(float(trade['quantity']), 2)
        ts = int(trade['timestamp'])  # ensure it's int
        trade['timestamp'] = datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d %H:%M:%S')
        return trade


    def on_error(self,ws, error):
      print("Error:", error)

    def on_close(self,ws, close_status_code, close_msg):
        print(f"WebSocket closed. Code: {close_status_code}, Msg: {close_msg}")

    def on_open(self,ws):
      print("Connected to Binance WebSocket")     

    def start_socket(self):
       URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"
       ws= websocket.WebSocketApp(
          URL,
         on_message=self.on_message,
      on_error=self.on_error,
      on_close=self.on_close,
      on_open=self.on_open

    )
       ws.run_forever()
       