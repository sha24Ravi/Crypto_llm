from flink_run import stream_data
import time

dfdtata = stream_data()
dfdtata.initiate_stream()  # starts the streamz pipeline

print("Streamz pipeline running. Press Ctrl+C to stop.")

try:
    while True:
        time.sleep(1)  # keeps main thread alive
except KeyboardInterrupt:
    print("Stopping pipeline...")