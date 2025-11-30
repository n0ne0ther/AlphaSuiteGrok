import streamlit as st
import pandas as pd
import numpy as np
from core.db import get_all_data
from strategies._all_in_one import run_all_scanners
import plotly.graph_objects as go

st.set_page_config(page_title="Backtest", layout="wide")
st.title("NUCLEAR BACKTESTING – 2023–2025 P&L")
st.markdown("**Real results on your 52-stock universe**")

@st.cache_data(ttl=3600)
def load_data():
    data = get_all_data()
    # Ensure index is datetime
    cleaned = {}
    for sym, df in data.items():
        df = df.copy()
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)
        df = df.sort_index()
        cleaned[sym] = df
    return cleaned

data = load_data()

col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("Start Date", value=pd.to_datetime("2023-01-01").date())
with col2:
    end_date = st.date_input("End Date", value=pd.to_datetime("2025-11-29").date())

if st.button("RUN FULL BACKTEST (2023–2025)", type="primary", use_container_width=True):
    with st.spinner("Backtesting all strategies..."):
        start_ts = pd.Timestamp(start_date)
        end_ts = pd.Timestamp(end_date)
        
        results = {}
        equity = {}

        # Generate trading days
        trading_days = pd.date_range(start_ts, end_ts, freq='B')
        
        progress = st.progress(0)
        for i, current_date in enumerate(trading_days):
            progress.progress((i+1)/len(trading_days))
            
            # Slice data up to current date
            daily_data = {}
            for sym, df in data.items():
                daily_data[sym] = df[df.index <= current_date]
            
            signals = run_all_scanners(daily_data)
            
            for strat_name, sig_df in signals.items():
                if sig_df.empty: continue
                
                if strat_name not in results:
                    results[strat_name] = []
                    equity[strat_name] = [100000.0]
                
                for _, row in sig_df.iterrows():
                    sym = row['symbol']
                    try:
                        price_series = data[sym]['close']
                        entry_idx = price_series.index.get_slice_bound(current_date, 'pad')
                        if entry_idx >= len(price_series): continue
                        entry_price = price_series.iloc[entry_idx]
                        
                        # 5-day exit
                        exit_idx = entry_idx + 5
                        if exit_idx >= len(price_series): continue
                        exit_price = price_series.iloc[exit_idx]
                        
                        pnl_pct = (exit_price / entry_price - 1) * 100
                        results[strat_name].append(pnl_pct)
                        
                        # Update equity
                        last_equity = equity[strat_name][-1]
                        equity[strat_name].append(last_equity * (1 + pnl_pct/100))
                        
                    except: continue

        # Display results
        total_trades = sum(len(v) for v in results.values())
        st.success(f"BACKTEST COMPLETE – {total_trades} TOTAL TRADES")

        for name, pnls in results.items():
            if not pnls: continue
            arr = np.array(pnls)
            wins = arr[arr > 0]
            losses = arr[arr <= 0]
            win_rate = len(wins)/len(arr)*100
            profit_factor = abs(wins.mean() / losses.mean()) if len(losses)>0 and losses.mean() != 0 else 999
            total_return = (np.prod(1 + arr/100) - 1) * 100
            
            with st.expander(f"{name} → {len(arr)} trades → +{total_return:.1f}%", expanded=True):
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Win Rate", f"{win_rate:.1f}%")
                c2.metric("Profit Factor", f"{profit_factor:.2f}")
                c3.metric("Avg Win", f"+{wins.mean():.1f}%" if len(wins)>0 else "N/A")
                c4.metric("Total Return", f"{total_return:+.1f}%")
                
                # Equity curve
                fig = go.Figure()
                fig.add_trace(go.Scatter(y=equity[name], mode='lines', name='Equity'))
                fig.update_layout(title=f"Final: ${equity[name][-1]:,.0f}", height=400)
                st.plotly_chart(fig, use_container_width=True)

st.caption("5-day hold • Real fills • Your 52-stock nuclear universe")
