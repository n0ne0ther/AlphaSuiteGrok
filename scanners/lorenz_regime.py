import pandas as pd
import numpy as np
import talib

from scanners.scanner_sdk import BaseScanner

class LorenzRegimeScanner(BaseScanner):
    """
    Scans for stocks transitioning into an uptrend regime based on a 3D state-space
    reconstruction inspired by the Lorenz attractor.

    Goal:
        This scanner aims to find stocks that are moving from an "unstable" or
        "crossover" state into a confirmed "uptrend" state, signaling a potential
        start of a new upward move.

    Criteria:
        - The stock is in a general long-term uptrend (price > 200-day SMA).
        - The Lorenz Regime, calculated from price position and momentum, has just
          flipped from 0 (unstable) to 1 (uptrend).
    """
    @staticmethod
    def define_parameters():
        return [
            {"name": "min_avg_volume", "type": "int", "default": 250000, "label": "Min. Avg. Volume"},
            {"name": "volume_lookback_days", "type": "int", "default": 50, "label": "Avg. Volume Lookback"},
            {"name": "min_market_cap", "type": "int", "default": 1000000000, "label": "Min. Market Cap"},
            {"name": "lookback_period", "type": "int", "default": 50, "label": "State Lookback"},
            {"name": "momentum_period", "type": "int", "default": 14, "label": "Momentum Period"},
            {"name": "crossover_threshold", "type": "float", "default": 0.1, "label": "Regime Threshold"},
        ]

    @staticmethod
    def get_leading_columns():
        return ['symbol', 'longname', 'industry', 'marketcap', 'lorenz_regime']

    @staticmethod
    def get_sort_info():
        return {'by': 'marketcap', 'ascending': False}

    def scan_company(self, group: pd.DataFrame, company_info: dict) -> dict | None:
        lookback = self.params.get('lookback_period', 50)
        mom_period = self.params.get('momentum_period', 14)
        threshold = self.params.get('crossover_threshold', 0.1)

        if len(group) < 201 or len(group) < lookback + 2:
            return None

        # --- State-Space Reconstruction ---
        # State X: Price position relative to its recent range
        rolling_min = group['close'].rolling(window=lookback).min()
        rolling_max = group['close'].rolling(window=lookback).max()
        rolling_range = rolling_max - rolling_min
        state_x = np.where(
            rolling_range > 0,
            2 * ((group['close'] - rolling_min) / rolling_range) - 1,
            0
        )

        # --- Regime Classification ---
        conditions = [state_x > threshold, state_x < -threshold]
        choices = [1, -1] # 1 for uptrend, -1 for downtrend
        lorenz_regime = np.select(conditions, choices, default=0) # 0 for unstable

        # --- General Trend Filter ---
        sma200 = talib.SMA(group['close'], timeperiod=200)

        # --- Setup Condition ---
        # Check the most recent full day's data
        if len(lorenz_regime) < 2 or pd.isna(sma200.iloc[-1]):
            return None

        is_crossover_to_uptrend = lorenz_regime[-1] == 1 and lorenz_regime[-2] == 0
        is_general_uptrend = group['close'].iloc[-1] > sma200.iloc[-1]

        if is_crossover_to_uptrend and is_general_uptrend:
            for key in ['id', 'isactive', 'longbusinesssummary', 'bookvalue']:
                if key in company_info: del company_info[key]
            company_info['lorenz_regime'] = lorenz_regime[-1]
            return company_info

        return None