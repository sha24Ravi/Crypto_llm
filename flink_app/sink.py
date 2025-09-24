from pyflink.datastream.connectors import FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
import json



def kafka_sink(stream,topic='crypto_trades'):
    return stream.map(lambda x:json.dumps(x)).add_sink(
        FlinkKafkaProducer(
            topic=topic,
            serialization_schema=SimpleStringSchema(),
            producer_config={'bootstrap.servers': 'localhost:9092'}
        )
    )

def print_sink(stream,env):
    
    stream.print()