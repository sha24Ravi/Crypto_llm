from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Configuration


def create_flink_env():
    
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars(
    "file:///C:/Users/shash/flink/lib/flink-connector-kafka-1.17.1.jar,"
    "file:///C:/Users/shash/flink/lib/kafka-clients-3.3.1.jar"
   ) 
    env.set_parallelism(1)
    return env
