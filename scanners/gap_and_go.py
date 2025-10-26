import pandas as pd
import talib

from scanners.scanner_sdk import BaseScanner

class GapAndGoScanner(BaseScanner):
    """
    Scans for stocks executing a "Gap and Go" pattern.

    Goal:
        Identify high-momentum stocks that have gapped up significantly at the
        market open on high volume, suggesting a strong catalyst and potential
        for continued upward movement.

    Criteria:
        - The stock is in a general uptrend (price > 200-day SMA).
        - The opening price is significantly higher than the previous day's close.
        - The volume on the gap day is significantly higher than its recent average.
    """
    @staticmethod
    def define_parameters():
        return [
            {"name": "min_avg_volume", "type": "int", "default": 500000, "label": "Min. Avg. Volume"},
            {"name": "volume_lookback_days", "type": "int", "default": 50, "label": "Avg. Volume Lookback"},
            {"name": "min_market_cap", "type": "int", "default": 500000000, "label": "Min. Market Cap"},
            {"name": "min_gap_up_pct", "type": "float", "default": 2.0, "label": "Min. Gap Up %"},
            {"name": "volume_spike_multiplier", "type": "float", "default": 1.5, "label": "Volume Spike x"},
            {"name": "gap_lookback_days", "type": "int", "default": 2, "label": "Gap within (days)"},
        ]

    @staticmethod
    def get_leading_columns():
        return ['symbol', 'gap_pct', 'gap_date', 'longname', 'marketcap']

    @staticmethod
    def get_sort_info():
        return {'by': 'gap_pct', 'ascending': False}

    def scan_company(self, group: pd.DataFrame, company_info: dict) -> dict | None:
        min_gap_up_pct = self.params.get('min_gap_up_pct', 2.0)
        volume_spike_multiplier = self.params.get('volume_spike_multiplier', 1.5)
        gap_lookback_days = self.params.get('gap_lookback_days', 2)

        if len(group) < 201 + gap_lookback_days:
            return None

        sma200 = talib.SMA(group['close'], timeperiod=200)
        avg_volume_20 = talib.SMA(group['volume'], timeperiod=20)

        # Check for gap within the lookback period
        for i in range(1, gap_lookback_days + 1):
            if len(group) < i + 1 or pd.isna(sma200.iloc[-i]) or pd.isna(avg_volume_20.iloc[-i]):
                continue
            # Gap is calculated from previous day's close to current day's open
            gap_pct = (group['open'].iloc[-i] - group['close'].iloc[-(i+1)]) / group['close'].iloc[-(i+1)]
            is_gap_up = gap_pct > (min_gap_up_pct / 100.0)
            is_high_volume = group['volume'].iloc[-i] > (avg_volume_20.iloc[-i] * volume_spike_multiplier)
            is_uptrend = group['close'].iloc[-i] > sma200.iloc[-i]

            if is_uptrend and is_gap_up and is_high_volume:
                for key in ['id', 'isactive', 'longbusinesssummary']:
                    if key in company_info: del company_info[key]
                
                company_info['gap_pct'] = gap_pct * 100
                company_info['gap_date'] = group['date'].iloc[-i].strftime('%Y-%m-%d')
                return company_info
        
        return None