import pandas as pd
import talib
import numpy as np

from scanners.scanner_sdk import BaseScanner

class WyckoffSpringScanner(BaseScanner):
    """
    Scans for stocks showing a simplified Wyckoff Spring pattern.

    Goal:
        Identify a potential "shakeout" or accumulation phase. A spring occurs
        when price briefly dips below a well-defined support level and then
        quickly reverses back above it, often on low volume. This suggests a
        lack of genuine selling pressure and that weak hands have been forced out.

    Criteria:
        - Price dips below a support level defined by recent lows.
        - Price then closes back above that same support level.
        - The volume on the spring day is not excessively high, indicating no
          strong follow-through from sellers.
    """
    @staticmethod
    def define_parameters():
        return [
            {"name": "min_avg_volume", "type": "int", "default": 250000, "label": "Min. Avg. Volume"},
            {"name": "volume_lookback_days", "type": "int", "default": 50, "label": "Avg. Volume Lookback"},
            {"name": "min_market_cap", "type": "int", "default": 1000000000, "label": "Min. Market Cap"},
            {"name": "support_period", "type": "int", "default": 60, "label": "Support Period (days)"},
            {"name": "max_volume_ratio", "type": "float", "default": 1.2, "label": "Max Volume Ratio"},
            {"name": "max_box_height_pct", "type": "float", "default": 10.0, "label": "Max Box Height %"},
            {"name": "min_close_position_pct", "type": "float", "default": 50.0, "label": "Min Close Pos in Range %"},
            {"name": "setup_lookback_days", "type": "int", "default": 2, "label": "Setup within (days)"},
        ]

    @staticmethod
    def get_leading_columns():
        return ['symbol', 'spring_date', 'support_level', 'longname', 'marketcap']

    @staticmethod
    def get_sort_info():
        return {'by': 'marketcap', 'ascending': False}

    def scan_company(self, group: pd.DataFrame, company_info: dict) -> dict | None:
        support_period = self.params.get('support_period', 60)
        max_volume_ratio = self.params.get('max_volume_ratio', 1.2)
        max_box_height_pct = self.params.get('max_box_height_pct', 10.0)
        min_close_position_pct = self.params.get('min_close_position_pct', 50.0)
        setup_lookback_days = self.params.get('setup_lookback_days', 2)

        if len(group) < support_period + 1 + setup_lookback_days:
            return None

        sma50 = talib.SMA(group['close'], timeperiod=50)
        avg_volume_20 = talib.SMA(group['volume'], timeperiod=20)

        # Check for spring within the lookback period
        for i in range(1, setup_lookback_days + 1):
            if len(group) < support_period + i or pd.isna(avg_volume_20.iloc[-i]) or pd.isna(sma50.iloc[-i]):
                continue

            # --- Define the "box" or trading range before the potential spring day ---
            range_window = group.iloc[-(support_period + i) : -i]
            if range_window.empty: continue

            # --- Add a check for a "tight box" to ensure we're in a consolidation ---
            box_high = range_window['high'].max()
            box_low = range_window['low'].min()
            box_height_pct = ((box_high - box_low) / box_low) * 100 if box_low > 0 else 0
            is_tight_box = box_height_pct > 0 and box_height_pct < max_box_height_pct

            # The support level is the lowest low of this consolidation box.
            support_level = box_low
            
            # --- Candle Shape Condition ---
            bar_range = group['high'].iloc[-i] - group['low'].iloc[-i]
            if bar_range == 0: continue # Avoid division by zero on doji candles
            
            close_pos_in_range = ((group['close'].iloc[-i] - group['low'].iloc[-i]) / bar_range) * 100
            has_long_tail = close_pos_in_range >= min_close_position_pct

            # A spring occurs when the low pierces the support and the close recovers above it.
            is_spring_action = (group['low'].iloc[-i] < support_level) and (group['close'].iloc[-i] > support_level)
            is_low_volume = (group['volume'].iloc[-i] / avg_volume_20.iloc[-i]) < max_volume_ratio

            if is_tight_box and is_spring_action and has_long_tail and is_low_volume:
                for key in ['id', 'isactive', 'longbusinesssummary']:
                    if key in company_info: del company_info[key]
                
                company_info['support_level'] = support_level
                company_info['spring_date'] = group['date'].iloc[-i].strftime('%Y-%m-%d')
                return company_info
        
        return None