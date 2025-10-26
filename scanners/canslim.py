import pandas as pd

from scanners.scanner_sdk import BaseScanner
from typing import List

class CanslimScanner(BaseScanner):
    """
    Scans for stocks that meet the core principles of the CANSLIM growth investing strategy.

    Goal:
        Identify high-growth stocks that are also market leaders, showing strong
        fundamental and technical momentum.

    Criteria (simplified):
        - C (Current Earnings): Strong quarterly earnings per share (EPS) growth.
        - A (Annual Earnings): Implied by strong recent growth.
        - N (New Highs): Stock price is near its 52-week high.
        - L (Leader): High Relative Strength (RS) percentile compared to the market.
        - S/I (Supply/Institutional Sponsorship): Basic liquidity and size filters.
    """
    @staticmethod
    def define_parameters():
        return [
            {"name": "min_avg_volume", "type": "int", "default": 250000, "label": "Min. Avg. Volume"},
            {"name": "volume_lookback_days", "type": "int", "default": 50, "label": "Avg. Volume Lookback"},
            {"name": "min_market_cap", "type": "int", "default": 1000000000, "label": "Min. Market Cap"},
            {"name": "min_eps_growth_pct", "type": "float", "default": 25.0, "label": "Min. Quarterly EPS Growth %"},
            {"name": "min_rs_percentile", "type": "int", "default": 80, "label": "Min. RS Percentile"},
            {"name": "within_pct_of_high", "type": "float", "default": 15.0, "label": "Within % of 52W High"},
        ]

    @staticmethod
    def get_leading_columns() -> List[str]:
        return ['symbol', 'earningsquarterlygrowth', 'rs_percentile', 'pct_of_high', 'longname', 'industry', 'marketcap']

    @staticmethod
    def get_sort_info():
        # Sort by the highest RS percentile to see the strongest leaders first
        return {'by': 'rs_percentile', 'ascending': False}

    def scan_company(self, group: pd.DataFrame, company_info: dict) -> dict | None:
        min_eps_growth_pct = self.params.get('min_eps_growth_pct', 25.0)
        min_rs_percentile = self.params.get('min_rs_percentile', 80)
        within_pct_of_high = self.params.get('within_pct_of_high', 15.0)

        # --- Fundamental Checks (from company_info) ---
        eps_growth = company_info.get('earningsquarterlygrowth')
        rs_percentile = company_info.get('relative_strength_percentile_252')

        if eps_growth is None or rs_percentile is None:
            return None

        is_strong_growth = eps_growth > (min_eps_growth_pct / 100.0)
        is_leader = rs_percentile > min_rs_percentile

        if not (is_strong_growth and is_leader):
            return None

        # --- Technical Check (from price history) ---
        if len(group) < 252:
            return None

        current_price = group['close'].iloc[-1]

        # Use a rolling window of the last 252 trading days for a more accurate 52-week high
        fiftytwoweekhigh = group['high'].tail(252).max()

        is_near_high = current_price >= fiftytwoweekhigh * (1 - (within_pct_of_high / 100.0))

        if is_near_high:
            for key in ['id', 'isactive', 'longbusinesssummary']:
                if key in company_info: del company_info[key]

            company_info['pct_of_high'] = (current_price / fiftytwoweekhigh) * 100 if fiftytwoweekhigh > 0 else 0
            company_info['rs_percentile'] = rs_percentile # Ensure it's in the output
            return company_info

        return None