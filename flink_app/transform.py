import json
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common import Row
from pyflink.common import Time
from pyflink.common.typeinfo import Types
from datetime import datetime
def round_decimals(msg: str):
    try:
        trade = json.loads(msg)   # parse kafka JSON string

        # round only decimals
        if "price" in trade:
            trade["price"] = round(float(trade["price"]), 2)
        if "quantity" in trade:
            trade["quantity"] = round(float(trade["quantity"]), 2)

        return json.dumps(trade)  # return back as JSON string
    except Exception as e:
        print(f"Error formatting: {msg}, {e}")
        return msg  # fallback

def format_stream(stream):
    return stream.map(round_decimals, output_type=Types.STRING())


def window_funct(stream, window_time=60):

    keyed_stream = stream.key_by(lambda t: t['symbol'])
    windowed_stream = keyed_stream.window(TumblingEventTimeWindows.of(Time.seconds(60)))
    aggregated_stream = windowed_stream.reduce(
    lambda a, b: {'symbol': a['symbol'], 'count': a.get('count', 1) + b.get('count', 1)}
)                           
    return aggregated_stream
