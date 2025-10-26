import pandas as pd
import talib

from scanners.scanner_sdk import BaseScanner

class SellingClimaxScanner(BaseScanner):
    """
    Scans for stocks exhibiting a "Selling Climax" pattern.

    Goal:
        Identify potential capitulation or market bottoming points. This pattern
        is characterized by a sharp price drop to a new low on extremely high
        volume, followed by a close in the upper part of the day's range,
        suggesting that panic selling has been absorbed by strong buyers.

    Criteria:
        - Price makes a new low over a specified period.
        - Volume is a multiple of its recent average.
        - The close is in the upper half of the day's high-low range.
    """
    @staticmethod
    def define_parameters():
        return [
            {"name": "min_avg_volume", "type": "int", "default": 250000, "label": "Min. Avg. Volume"},
            {"name": "volume_lookback_days", "type": "int", "default": 50, "label": "Avg. Volume Lookback"},
            {"name": "min_market_cap", "type": "int", "default": 500000000, "label": "Min. Market Cap"},
            {"name": "new_low_period", "type": "int", "default": 20, "label": "New Low Period (days)"},
            {"name": "volume_spike_multiplier", "type": "float", "default": 2.5, "label": "Volume Spike x"},
            {"name": "close_reversal_pct", "type": "float", "default": 50.0, "label": "Min. Close Position in Range %"},
            {"name": "setup_lookback_days", "type": "int", "default": 2, "label": "Setup within (days)"},
        ]

    @staticmethod
    def get_leading_columns():
        return ['symbol', 'climax_date', 'close_pos_in_range', 'longname', 'marketcap']

    @staticmethod
    def get_sort_info():
        return {'by': 'marketcap', 'ascending': False}

    def scan_company(self, group: pd.DataFrame, company_info: dict) -> dict | None:
        new_low_period = self.params.get('new_low_period', 20)
        volume_spike_multiplier = self.params.get('volume_spike_multiplier', 2.5)
        close_reversal_pct = self.params.get('close_reversal_pct', 50.0)
        setup_lookback_days = self.params.get('setup_lookback_days', 2)

        if len(group) < new_low_period + 1 + setup_lookback_days:
            return None

        avg_volume_20 = talib.SMA(group['volume'], timeperiod=20)

        # Check for selling climax within the lookback period
        for i in range(1, setup_lookback_days + 1):
            if len(group) < new_low_period + i or pd.isna(avg_volume_20.iloc[-i]):
                continue

            is_new_low = group['low'].iloc[-i] < group['low'].iloc[-(new_low_period + i) : -i].min()
            is_high_volume = group['volume'].iloc[-i] > (avg_volume_20.iloc[-i] * volume_spike_multiplier)
            
            bar_range = group['high'].iloc[-i] - group['low'].iloc[-i]
            if bar_range == 0:
                continue
            
            close_position_in_range_pct = ((group['close'].iloc[-i] - group['low'].iloc[-i]) / bar_range) * 100
            is_reversal_close = close_position_in_range_pct > close_reversal_pct

            if is_new_low and is_high_volume and is_reversal_close:
                for key in ['id', 'isactive', 'longbusinesssummary']:
                    if key in company_info: del company_info[key]
                
                company_info['close_pos_in_range'] = close_position_in_range_pct
                company_info['climax_date'] = group['date'].iloc[-i].strftime('%Y-%m-%d')
                return company_info
        
        return None