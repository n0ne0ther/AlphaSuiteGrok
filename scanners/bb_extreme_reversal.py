import pandas as pd
import talib

from scanners.scanner_sdk import BaseScanner

class BbExtremeReversalScanner(BaseScanner):
    """
    Scans for stocks showing a Bollinger Band Extreme Reversal setup.

    Goal:
        Identify mean-reversion opportunities in an uptrend. This scanner looks for
        stocks that have recently touched or dipped below their lower Bollinger Band
        and are now showing signs of reversing back up.

    Criteria:
        - The stock is in a general uptrend (price > 200-day SMA).
        - The low of the stock has touched or gone below the lower Bollinger Band
          within a recent lookback period.
        - The current price has closed back above the lower Bollinger Band.
    """
    @staticmethod
    def define_parameters():
        return [
            {"name": "min_avg_volume", "type": "int", "default": 250000, "label": "Min. Avg. Volume"},
            {"name": "volume_lookback_days", "type": "int", "default": 50, "label": "Avg. Volume Lookback"},
            {"name": "min_market_cap", "type": "int", "default": 1000000000, "label": "Min. Market Cap"},
            {"name": "bb_period", "type": "int", "default": 20, "label": "BB Period"},
            {"name": "bb_std_dev", "type": "float", "default": 2.0, "label": "BB Std. Dev."},
            {"name": "extreme_lookback", "type": "int", "default": 2, "label": "Extreme Lookback (days)"},
        ]

    @staticmethod
    def get_leading_columns():
        return ['symbol', 'setup_date', 'bb_lower', 'bb_upper', 'longname', 'industry', 'marketcap']

    @staticmethod
    def get_sort_info():
        return {'by': 'marketcap', 'ascending': False}

    def scan_company(self, group: pd.DataFrame, company_info: dict) -> dict | None:
        bb_period = self.params.get('bb_period', 20)
        bb_std_dev = self.params.get('bb_std_dev', 2.0)
        extreme_lookback = self.params.get('extreme_lookback', 2)

        if len(group) < 201:
            return None

        upper, middle, lower = talib.BBANDS(group['close'], timeperiod=bb_period, nbdevup=bb_std_dev, nbdevdn=bb_std_dev)
        sma200 = talib.SMA(group['close'], timeperiod=200)

        if pd.isna(lower.iloc[-1]) or pd.isna(sma200.iloc[-1]):
            return None

        # Check for extreme low within the lookback period (excluding today)
        is_extreme_in_lookback = (group['low'].iloc[-extreme_lookback-1:-1] <= lower.iloc[-extreme_lookback-1:-1]).any()
        is_reversed_today = group['close'].iloc[-1] > lower.iloc[-1]
        is_uptrend = group['close'].iloc[-1] > sma200.iloc[-1]

        if is_uptrend and is_extreme_in_lookback and is_reversed_today:
            for key in ['id', 'isactive', 'longbusinesssummary', 'bookvalue']:
                if key in company_info: del company_info[key]
            
            company_info['bb_lower'] = float(lower.iloc[-1])
            company_info['bb_upper'] = float(upper.iloc[-1])
            company_info['setup_date'] = group['date'].iloc[-1].strftime('%Y-%m-%d')
            return company_info

        return None