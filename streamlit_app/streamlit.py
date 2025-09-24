import streamlit as st
import redis
import json
import pandas as pd
from llm_funct import get_insights

# Connect to Redis
r = redis.Redis(host='redis', port=6379, db=0)

symbol = st.selectbox("Select Symbol", ["BTCUSDT"])

latest_data = r.get(f"{symbol}:latest")
history = r.lrange("BTCUSDT:history", 0, 9)

if history:
    trade_summary = [json.loads(h) for h in history]
    llm_response=get_insights(trade_summary)
    st.write("### Latest 2-min Average")
    st.markdown(llm_response)