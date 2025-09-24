from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema


def Kafka_trade_source(env):
    props = {
        'bootstrap.servers': 'localhost:29093',
        'group.id': 'flink-group'
    }
    consumer = FlinkKafkaConsumer(
        topics='crypto_trades',
        deserialization_schema=SimpleStringSchema(),
        properties={'bootstrap.servers':  'kafka:29092', 'group.id': 'test-group'}
    )
    stream = env.add_source(consumer)
    return stream