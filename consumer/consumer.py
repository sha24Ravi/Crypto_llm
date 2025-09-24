from kafka import KafkaConsumer
import redis
import json



r = redis.Redis(host='redis', port=6379, db=0)

consumer = KafkaConsumer(
    'crypto_trades_transformed',
    bootstrap_servers='broker:9092',
    enable_auto_commit=True,
    group_id='streamz',
    auto_offset_reset='latest',   # only new messages after consumer starts
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    trade_summary = message.value  # your averaged message
    if not trade_summary:
        continue

    # Store latest summary
    redis_key = f"{trade_summary['symbol']}:latest"
    r.set(redis_key, json.dumps(trade_summary))
    r.expire(redis_key, 300)  # optional TTL

    # Optional: store history for charts
    r.lpush(f"{trade_summary['symbol']}:history", json.dumps(trade_summary))
    r.ltrim(f"{trade_summary['symbol']}:history", 0, 49)  # keep last 50 entries

    print(f"Pushed {trade_summary['symbol']} to Redis")