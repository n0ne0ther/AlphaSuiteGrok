import pandas as pd
import talib

from scanners.scanner_sdk import BaseScanner

class BbSqueezeBreakoutScanner(BaseScanner):
    """
    Scans for stocks experiencing a Bollinger Band Squeeze followed by a breakout.

    Goal:
        Identify stocks where a period of low volatility (the "squeeze") resolves
        with a price breakout above the upper band, suggesting the start of a
        new upward momentum phase.

    Criteria:
        - The stock is in a general uptrend (price > 200-day SMA).
        - Bollinger Band Width was recently in a compressed state (e.g., lowest 10% of its range).
        - The current price has broken out above the upper Bollinger Band.
    """
    @staticmethod
    def define_parameters():
        return [
            {"name": "min_avg_volume", "type": "int", "default": 250000, "label": "Min. Avg. Volume"},
            {"name": "volume_lookback_days", "type": "int", "default": 50, "label": "Avg. Volume Lookback"},
            {"name": "min_market_cap", "type": "int", "default": 1000000000, "label": "Min. Market Cap"},
            {"name": "bb_period", "type": "int", "default": 20, "label": "BB Period"},
            {"name": "squeeze_period", "type": "int", "default": 120, "label": "Squeeze Lookback"},
            {"name": "squeeze_quantile", "type": "float", "default": 0.1, "label": "Squeeze Quantile"},
            {"name": "breakout_lookback_days", "type": "int", "default": 2, "label": "Breakout within (days)"},
        ]

    @staticmethod
    def get_leading_columns():
        return ['symbol', 'breakout_date', 'longname', 'industry', 'marketcap']

    @staticmethod
    def get_sort_info():
        return {'by': 'marketcap', 'ascending': False}

    def scan_company(self, group: pd.DataFrame, company_info: dict) -> dict | None:
        bb_period = self.params.get('bb_period', 20)
        squeeze_period = self.params.get('squeeze_period', 120)
        squeeze_quantile = self.params.get('squeeze_quantile', 0.1)
        breakout_lookback_days = self.params.get('breakout_lookback_days', 2)

        if len(group) < squeeze_period:
            return None

        upper, middle, lower = talib.BBANDS(group['close'], timeperiod=bb_period)
        sma200 = talib.SMA(group['close'], timeperiod=200)
        bb_width = (upper - lower) / middle

        if len(bb_width) < breakout_lookback_days + 1 or len(sma200) < breakout_lookback_days + 1:
            return None

        # Check for breakout within the lookback period
        for i in range(1, breakout_lookback_days + 1):
            if pd.isna(bb_width.iloc[-i]) or pd.isna(sma200.iloc[-i]):
                continue

            # Check for squeeze condition on the day *before* the potential breakout
            squeeze_threshold = bb_width.rolling(window=squeeze_period).quantile(squeeze_quantile)
            is_in_squeeze = (bb_width.iloc[-(i+1)] <= squeeze_threshold.iloc[-(i+1)])
            is_breakout = group['close'].iloc[-i] > upper.iloc[-i]
            is_uptrend = group['close'].iloc[-i] > sma200.iloc[-i]

            if is_uptrend and is_in_squeeze and is_breakout:
                for key in ['id', 'isactive', 'longbusinesssummary']:
                    if key in company_info: del company_info[key]
                company_info['breakout_date'] = group['date'].iloc[-i].strftime('%Y-%m-%d')
                return company_info
        
        return None