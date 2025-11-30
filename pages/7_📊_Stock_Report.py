import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from core.db import get_all_data
from strategies._all_in_one import run_all_scanners

st.set_page_config(page_title="Stock Report", layout="wide")
st.title("DETAILED STOCK REPORT")

data = get_all_data()
symbols = sorted(data.keys())
ticker = st.selectbox("Select Ticker", symbols, index=symbols.index("PLTR") if "PLTR" in symbols else 0)

if ticker not in data:
    st.error("No data")
    st.stop()

df = data[ticker].copy()
df = df.sort_index()

# === Header Metrics ===
latest = df.iloc[-1]
col1, col2, col3, col4 = st.columns(4)
col1.metric("Price", f"${latest['close']:.2f}")
col2.metric("Volume", f"{latest['volume']/1e6:.1f}M")
vol_avg = df['volume'].rolling(20).mean().iloc[-1]
col3.metric("20d Avg Vol", f"{vol_avg/1e6:.1f}M")
col4.metric("Vol x Avg", f"{latest['volume']/vol_avg:.1f}x")

# === Candlestick Chart ===
fig = go.Figure()
fig.add_trace(go.Candlestick(
    x=df.index,
    open=df['open'], high=df['high'],
    low=df['low'], close=df['close'],
    name=ticker
))
fig.update_layout(title=f"{ticker} – Full History", xaxis_rangeslider_visible=False, height=600)
st.plotly_chart(fig, use_container_width=True)

# === Key Stats ===
st.subheader("Technical Indicators")
c1, c2, c3, c4 = st.columns(4)
high_52w = df['high'].rolling(252).max().iloc[-1]
c1.metric("52w High", f"${high_52w:.2f}")
c2.metric("From 52w High", f"{(latest['close']/high_52w-1)*100:+.1f}%")

# RSI
delta = df['close'].diff()
up = delta.clip(lower=0).ewm(span=14).mean()
down = -delta.clip(upper=0).ewm(span=14).mean()
rsi = 100 - (100/(1 + up/down))
c3.metric("RSI (14)", f"{rsi.iloc[-1]:.1f}")

volatility = df['close'].pct_change().rolling(20).std().iloc[-1] * 100
c4.metric("20d Volatility", f"{volatility:.2f}%")

# === Current Strategy Signals ===
st.subheader("Active Nuclear Signals")
signals = run_all_scanners({ticker: df})

active = []
for name, sig_df in signals.items():
    if not sig_df.empty and ticker in sig_df['symbol'].values:
        active.append(name)

if active:
    for signal in active:
        st.success(f"→ {signal}")
else:
    st.info("No active signals right now")

st.caption("Fast • No external APIs • Works offline • Powered by your 52-stock nuclear universe")
