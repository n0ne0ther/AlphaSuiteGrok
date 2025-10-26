import pandas as pd
import talib

from scanners.scanner_sdk import BaseScanner

class DeathCrossScanner(BaseScanner):
    """
    Scans for stocks where the 50-day SMA has recently crossed below the 200-day SMA.

    Goal:
        Identify a major long-term bearish trend shift. A "Death Cross" is often
        interpreted as a signal that a stock's uptrend has failed and it is entering
        a new long-term downtrend.

    Criteria:
        - The 50-day Simple Moving Average (SMA) crosses *below* the 200-day SMA.
        - The cross must have happened within a configurable lookback period.
        - The current price is still below the 50-day SMA, confirming weakness.
    """
    @staticmethod
    def define_parameters():
        return [
            {"name": "min_avg_volume", "type": "int", "default": 100000, "label": "Min. Avg. Volume"},
            {"name": "volume_lookback_days", "type": "int", "default": 50, "label": "Avg. Volume Lookback"},
            {"name": "min_market_cap", "type": "int", "default": 1000000000, "label": "Min. Market Cap"},
            {"name": "crossover_lookback_days", "type": "int", "default": 5, "label": "Crossover within (days)"},
        ]

    @staticmethod
    def get_leading_columns():
        return ['symbol', 'sma50', 'sma200', 'crossover_date', 'longname', 'sector', 'marketcap']

    @staticmethod
    def get_sort_info():
        # Sort by market cap descending
        return {'by': 'marketcap', 'ascending': False}

    def scan_company(self, group: pd.DataFrame, company_info: dict) -> dict | None:
        crossover_lookback_days = self.params.get('crossover_lookback_days', 5)

        if len(group) < 200 + crossover_lookback_days:
            return None

        sma50 = talib.SMA(group['close'], timeperiod=50)
        sma200 = talib.SMA(group['close'], timeperiod=200)

        # Check for Death Cross within the lookback period
        for i in range(1, crossover_lookback_days + 2):
            if len(sma50) < i + 1 or len(sma200) < i + 1 or pd.isna(sma50.iloc[-i]) or pd.isna(sma200.iloc[-i]):
                continue
            
            if sma50.iloc[-i] < sma200.iloc[-i] and sma50.iloc[-(i+1)] >= sma200.iloc[-(i+1)]:
                # Crossover found, check if price is still weak
                current_price = group['close'].iloc[-1]
                current_sma50 = sma50.iloc[-1]
                
                if current_sma50 is not None and current_price < current_sma50:
                    # Clean up and add calculated data
                    for key in ['id', 'isactive', 'longbusinesssummary']:
                        if key in company_info: del company_info[key]
                    
                    company_info['sma50'] = float(current_sma50)
                    company_info['sma200'] = float(sma200.iloc[-1])
                    company_info['crossover_date'] = group['date'].iloc[-i].strftime('%Y-%m-%d')
                    return company_info
                break # Exit inner loop
        
        return None