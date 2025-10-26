import pandas as pd
import talib

from scanners.scanner_sdk import BaseScanner

class BearishRsiDivergenceScanner(BaseScanner):
    """
    Scans for stocks showing a bearish RSI divergence.

    Goal:
        Identify potential trend exhaustion or reversal points to the downside. A bearish
        divergence occurs when price makes a new high but the RSI indicator makes a
        lower high, suggesting that the upward momentum is weakening.

    Criteria:
        - The stock is in a general uptrend (price > 200-day SMA) to find potential tops.
        - The current price has made a new high compared to a recent period.
        - The current RSI is lower than the RSI at the time of the previous high.
    """
    @staticmethod
    def define_parameters():
        return [
            {"name": "min_avg_volume", "type": "int", "default": 250000, "label": "Min. Avg. Volume"},
            {"name": "volume_lookback_days", "type": "int", "default": 50, "label": "Avg. Volume Lookback"},
            {"name": "min_market_cap", "type": "int", "default": 1000000000, "label": "Min. Market Cap"},
            {"name": "rsi_period", "type": "int", "default": 14, "label": "RSI Period"},
            {"name": "divergence_lookback", "type": "int", "default": 30, "label": "Divergence Lookback"},
            {"name": "setup_lookback_days", "type": "int", "default": 2, "label": "Setup within (days)"},
        ]

    @staticmethod
    def get_leading_columns():
        return ['symbol', 'rsi', 'divergence_date', 'longname', 'marketcap']

    @staticmethod
    def get_sort_info():
        return {'by': 'marketcap', 'ascending': False}

    def scan_company(self, group: pd.DataFrame, company_info: dict) -> dict | None:
        rsi_period = self.params.get('rsi_period', 14)
        divergence_lookback = self.params.get('divergence_lookback', 30)
        setup_lookback_days = self.params.get('setup_lookback_days', 2)

        if len(group) < divergence_lookback + rsi_period + setup_lookback_days:
            return None

        rsi = talib.RSI(group['close'], timeperiod=rsi_period)
        sma200 = talib.SMA(group['close'], timeperiod=200)

        for i in range(1, setup_lookback_days + 1):
            if len(group) < divergence_lookback + i or pd.isna(rsi.iloc[-i]) or pd.isna(sma200.iloc[-i]):
                continue

            lookback_window = group.iloc[-(divergence_lookback + i) : -i]
            if lookback_window.empty: continue

            max_price_day_idx = lookback_window['high'].idxmax()
            
            is_new_high = group['high'].iloc[-i] > lookback_window.loc[max_price_day_idx, 'high']
            is_lower_rsi = rsi.iloc[-i] < rsi.loc[max_price_day_idx]
            is_uptrend = group['close'].iloc[-i] > sma200.iloc[-i]

            if is_uptrend and is_new_high and is_lower_rsi:
                for key in ['id', 'isactive', 'longbusinesssummary', 'bookvalue']:
                    if key in company_info: del company_info[key]
                company_info['rsi'] = float(rsi.iloc[-i])
                company_info['divergence_date'] = group['date'].iloc[-i].strftime('%Y-%m-%d')
                return company_info
        
        return None