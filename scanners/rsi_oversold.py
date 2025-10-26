import pandas as pd
import talib

from scanners.scanner_sdk import BaseScanner

class RsiOversoldScanner(BaseScanner):
    """
    Scans for stocks that are in an oversold condition based on the RSI indicator.

    Goal:
        Identify potential mean-reversion or "buy the dip" opportunities. This scanner
        looks for stocks whose prices have fallen rapidly, as indicated by a low RSI,
        but are still in a long-term uptrend (above the 200-day SMA).

    Criteria:
        - The Relative Strength Index (RSI) is below a specified oversold threshold.
        - The stock is in a broader uptrend (e.g., the 200-day SMA is rising) to
          filter for pullbacks rather than new downtrends.
        - Basic liquidity and size filters are applied.
    """
    @staticmethod
    def define_parameters():
        return [
            {"name": "min_avg_volume", "type": "int", "default": 100000, "label": "Min. Avg. Volume"},
            {"name": "volume_lookback_days", "type": "int", "default": 50, "label": "Avg. Volume Lookback"},
            {"name": "min_market_cap", "type": "int", "default": 1000000000, "label": "Min. Market Cap"},
            {"name": "rsi_period", "type": "int", "default": 14, "label": "RSI Period"},
            {"name": "rsi_oversold_threshold", "type": "int", "default": 30, "label": "RSI Oversold Level"},
            {"name": "setup_lookback_days", "type": "int", "default": 2, "label": "Setup within (days)"},
        ]
    
    @staticmethod
    def get_leading_columns():
        return ['symbol', 'rsi', 'sma200', 'setup_date', 'longname', 'sector', 'marketcap']

    @staticmethod
    def get_sort_info():
        # Sort by RSI ascending to see the most oversold stocks first
        return {'by': 'rsi', 'ascending': True}

    def scan_company(self, group: pd.DataFrame, company_info: dict) -> dict | None:
        rsi_period = self.params.get('rsi_period', 14)
        rsi_oversold_threshold = self.params.get('rsi_oversold_threshold', 30)
        setup_lookback_days = self.params.get('setup_lookback_days', 2)

        if len(group) < 201 + setup_lookback_days:
            return None

        # Calculate RSI and SMA
        rsi = talib.RSI(group['close'], timeperiod=rsi_period)
        sma200 = talib.SMA(group['close'], timeperiod=200)

        # Check for oversold condition within the lookback period
        for i in range(1, setup_lookback_days + 1):
            if len(rsi) < i + 5 or len(sma200) < i + 5 or pd.isna(rsi.iloc[-i]) or pd.isna(sma200.iloc[-i]) or pd.isna(sma200.iloc[-i-5]):
                continue

            is_oversold = rsi.iloc[-i] < rsi_oversold_threshold
            is_sma200_rising = sma200.iloc[-i] > sma200.iloc[-i - 5]
            is_price_close_to_sma = group['close'].iloc[-i] > (sma200.iloc[-i] * 0.95)
            is_uptrend = is_sma200_rising and is_price_close_to_sma

            if is_oversold and is_uptrend:
                # Clean up and add calculated data
                for key in ['id', 'isactive', 'longbusinesssummary']:
                    if key in company_info: del company_info[key]

                company_info['rsi'] = float(rsi.iloc[-i])
                company_info['sma200'] = float(sma200.iloc[-i])
                company_info['setup_date'] = group['date'].iloc[-i].strftime('%Y-%m-%d')
                return company_info
        
        return None