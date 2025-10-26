import pandas as pd

from scanners.scanner_sdk import BaseScanner
from typing import List

class NewHighsScanner(BaseScanner):
    """
    Scans for stocks making new 52-week highs.

    Goal:
        Identify stocks exhibiting strong upward momentum, which are often market leaders.
        This is a classic trend-following and momentum signal.

    Criteria:
        - The stock's daily high has made a new 52-week high within the lookback period.
        - Basic liquidity and size filters are applied.
    """
    @staticmethod
    def define_parameters():
        return [
            {"name": "min_avg_volume", "type": "int", "default": 250000, "label": "Min. Avg. Volume"},
            {"name": "volume_lookback_days", "type": "int", "default": 50, "label": "Avg. Volume Lookback"},
            {"name": "min_market_cap", "type": "int", "default": 1000000000, "label": "Min. Market Cap"},
            {"name": "setup_lookback_days", "type": "int", "default": 2, "label": "Setup within (days)"},
        ]

    @staticmethod
    def get_leading_columns() -> List[str]:
        return ['symbol', 'setup_date', 'pct_of_high', 'currentprice', 'fiftytwoweekhigh', 'rs_percentile', 'longname', 'industry', 'marketcap']

    @staticmethod
    def get_sort_info():
        # Sort by stocks closest to their high
        return {'by': 'pct_of_high', 'ascending': False}

    def scan_company(self, group: pd.DataFrame, company_info: dict) -> dict | None:
        setup_lookback_days = self.params.get('setup_lookback_days', 2)

        # Need at least a year of data to calculate 52-week high
        if len(group) < 252 + setup_lookback_days:
            return None

        for i in range(1, setup_lookback_days + 1):
            if len(group) < 252 + i: continue

            current_price = group['close'].iloc[-i]
            current_high = group['high'].iloc[-i]
            
            # Define the 52-week lookback window *prior* to the day being checked
            lookback_window = group.iloc[-(252 + i) : -i]
            if lookback_window.empty: continue

            previous_52w_high = lookback_window['high'].max()

            # Apply the core condition: the high of day 'i' is a new 52-week high
            if current_high >= previous_52w_high:
                # Condition met, add calculated metrics to the company info
                for key in ['id', 'isactive', 'longbusinesssummary']:
                    if key in company_info: del company_info[key]

                company_info['currentprice'] = current_price
                company_info['fiftytwoweekhigh'] = current_high # The new high is the current day's high
                company_info['pct_of_high'] = (current_price / current_high) * 100 if current_high > 0 else 0
                company_info['setup_date'] = group['date'].iloc[-i].strftime('%Y-%m-%d')
                
                return company_info # Return on the first match

        return None