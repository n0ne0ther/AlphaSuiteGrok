import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from core.db import get_all_data
from strategies._all_in_one import run_all_scanners

st.set_page_config(page_title="Risk Dashboard", layout="wide")
st.title("RISK & OPPORTUNITY DASHBOARD")

data = get_all_data()
symbols = sorted(data.keys())

with st.spinner("Calculating risk + scanning all strategies..."):
    all_signals = run_all_scanners(data)

    # Build master dataframe
    rows = []
    for sym in symbols:
        df = data[sym].tail(252).copy()  # 1 year
        if len(df) < 60: continue
        
        latest = df.iloc[-1]
        ret = df['close'].pct_change().dropna()
        
        # Risk metrics
        volatility = ret.std() * np.sqrt(252) * 100
        sharpe = (ret.mean() * 252) / (ret.std() * np.sqrt(252)) if ret.std() != 0 else 0
        max_dd = ((df['close'].cummax() - df['close']) / df['close'].cummax()).max() * 100
        var_95 = np.percentile(ret, 5) * 100
        
        # Signal count
        sig_count = sum(1 for name, sig in all_signals.items() 
                       if not sig.empty and sym in sig['symbol'].values)
        
        rows.append({
            'symbol': sym,
            'price': latest['close'],
            'vol_M': latest['volume']/1e6,
            'volatility_%': volatility,
            'sharpe': sharpe,
            'max_drawdown_%': max_dd,
            'VaR_95_%': var_95,
            'signals': sig_count
        })

df_port = pd.DataFrame(rows).round(2)

# === RISK DASHBOARD ===
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Hottest (≥2 signals)", len(df_port[df_port['signals'] >= 2]))
col2.metric("High Risk (>50% vol)", len(df_port[df_port['volatility_%'] > 50]))
col3.metric("Best Risk/Reward (Sharpe > 1.5)", len(df_port[df_port['sharpe'] > 1.5]))
col4.metric("Danger Zone (Max DD > 70%)", len(df_port[df_port['max_drawdown_%'] > 70]))
col5.metric("Total Universe", len(df_port))

# === MAIN RISK HEATMAP ===
st.subheader("LIVE RISK & SIGNAL HEATMAP")
df_display = df_port.copy()
df_display['Risk_Level'] = pd.cut(df_display['volatility_%'], 
                                  bins=[0, 30, 60, 100, 1000], 
                                  labels=["Low", "Medium", "High", "Extreme"])

fig = px.scatter(df_display, x="volatility_%", y="signals", size="vol_M", color="sharpe",
                 hover_name="symbol", color_continuous_scale="RdYlGn",
                 size_max=60, range_color=[-2, 3],
                 labels={"volatility_%": "Annual Volatility (%)", "signals": "Active Signals"})
fig.update_layout(height=600)
st.plotly_chart(fig, use_container_width=True)

# === TOP LISTS ===
tab1, tab2, tab3, tab4 = st.tabs(["Hottest", "Best Risk/Reward", "Highest Volatility", "Worst Drawdown"])

with tab1:
    st.dataframe(df_port.sort_values("signals", ascending=False).head(15)[['symbol','price','signals','volatility_%','sharpe']], use_container_width=True)

with tab2:
    st.dataframe(df_port.sort_values("sharpe", ascending=False).head(15)[['symbol','price','sharpe','volatility_%','signals']], use_container_width=True)

with tab3:
    st.dataframe(df_port.sort_values("volatility_%", ascending=False).head(15)[['symbol','price','volatility_%','VaR_95_%','max_drawdown_%']], use_container_width=True)

with tab4:
    st.dataframe(df_port.sort_values("max_drawdown_%", ascending=False).head(15)[['symbol','price','max_drawdown_%','volatility_%']], use_container_width=True)

st.caption("Live risk metrics • 252-day lookback • Your nuclear 52-stock universe")
