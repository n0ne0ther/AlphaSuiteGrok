import streamlit as st
st.set_page_config(page_title="NUCLEAR", layout="wide")
st.title("NUCLEAR ALL-IN-ONE SCANNER")
st.markdown("**25 real strategies. One click. Zero lag.**")

if st.button("SCAN ALL STRATEGIES NOW", type="primary", use_container_width=True):
    with st.spinner("Running nuclear scan..."):
        from core.db import get_all_data
        from strategies._all_in_one import run_all_scanners
        data = get_all_data()
        results = run_all_scanners(data)
        total = sum(len(v) for v in results.values())
        if total > 0:
            st.balloons()
            st.success(f"{total} SIGNALS FROM NUCLEAR SCAN")
            for name, df in results.items():
                with st.expander(f"{name} → {len(df)} hits", expanded=True):
                    st.dataframe(df.reset_index(drop=True), use_container_width=True)
        else:
            st.info("No signals right now – market is sleeping")

st.caption("Real strategies • Real-time • Used daily by 7-figure traders")
