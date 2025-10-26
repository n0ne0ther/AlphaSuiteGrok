import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import and_
import talib

from scanners.scanner_sdk import BaseScanner
from core.model import Company, Exchange

class GenericScreener(BaseScanner):
    """
    A highly flexible, user-configurable screener that allows filtering on a wide
    range of descriptive, fundamental, and technical metrics.
    """

    # A map of human-readable filter names to their corresponding SQLAlchemy model attributes.
    # 'source' indicates whether it's a direct database column ('db') or requires
    # price history calculation ('tech').
    FILTER_MAP = {
        # Descriptive
        'marketcap': {'source': 'db', 'model_attr': Company.marketcap, 'type': 'numeric'},
        'dividendyield': {'source': 'db', 'model_attr': Company.dividendyield, 'type': 'percentage'},
        'currentprice': {'source': 'db', 'model_attr': Company.currentprice, 'type': 'numeric'},
        'industry': {'source': 'db', 'model_attr': Company.industry, 'type': 'category'},
        'sector': {'source': 'db', 'model_attr': Company.sectorkey, 'type': 'category'},
        'country': {'source': 'db', 'model_attr': Company.country, 'type': 'category'},
        'recommendationkey': {'source': 'db', 'model_attr': Company.recommendationkey, 'type': 'category'},

        # Fundamental (Valuation)
        'trailingpe': {'source': 'db', 'model_attr': Company.trailingpe, 'type': 'numeric'},
        'forwardpe': {'source': 'db', 'model_attr': Company.forwardpe, 'type': 'numeric'},
        'trailingpegratio': {'source': 'db', 'model_attr': Company.trailingpegratio, 'type': 'numeric'},
        'pricetosalestrailing12months': {'source': 'db', 'model_attr': Company.pricetosalestrailing12months, 'type': 'numeric'},
        'pricetobook': {'source': 'db', 'model_attr': Company.pricetobook, 'type': 'numeric'},
        'enterprisetoebitda': {'source': 'db', 'model_attr': Company.enterprisetoebitda, 'type': 'numeric'},

        # Fundamental (Profitability & Growth)
        'returnonequity': {'source': 'db', 'model_attr': Company.returnonequity, 'type': 'percentage'},
        'returnonassets': {'source': 'db', 'model_attr': Company.returnonassets, 'type': 'percentage'},
        'profitmargins': {'source': 'db', 'model_attr': Company.profitmargins, 'type': 'percentage'},
        'grossmargins': {'source': 'db', 'model_attr': Company.grossmargins, 'type': 'percentage'},
        'operatingmargins': {'source': 'db', 'model_attr': Company.operatingmargins, 'type': 'percentage'},
        'earningsgrowth_quarterly_yoy': {'source': 'db', 'model_attr': Company.earningsgrowth_quarterly_yoy, 'type': 'percentage'},
        'revenuegrowth_quarterly_yoy': {'source': 'db', 'model_attr': Company.revenuegrowth_quarterly_yoy, 'type': 'percentage'},
        'eps_cagr_3year': {'source': 'db', 'model_attr': Company.eps_cagr_3year, 'type': 'percentage'},

        # Fundamental (Financial Health)
        'debttoequity': {'source': 'db', 'model_attr': Company.debttoequity, 'type': 'numeric'},
        'currentratio': {'source': 'db', 'model_attr': Company.currentratio, 'type': 'numeric'},
        'payoutratio': {'source': 'db', 'model_attr': Company.payoutratio, 'type': 'percentage'},

        # Technical (from DB)
        'beta': {'source': 'db', 'model_attr': Company.beta, 'type': 'numeric'},
        'price_relative_to_52week_high': {'source': 'db', 'model_attr': Company.price_relative_to_52week_high, 'type': 'percentage_raw'},
        'relative_strength_percentile_252': {'source': 'db', 'model_attr': Company.relative_strength_percentile_252, 'type': 'numeric'},
        'relative_strength_percentile_126': {'source': 'db', 'model_attr': Company.relative_strength_percentile_126, 'type': 'numeric'},
        'relative_strength_percentile_63': {'source': 'db', 'model_attr': Company.relative_strength_percentile_63, 'type': 'numeric'},
        '_52weekchange': {'source': 'db', 'model_attr': Company._52weekchange, 'type': 'percentage'},

        # Technical (Calculated On-the-fly)
        'sma': {'source': 'tech', 'type': 'numeric', 'params': ['period']},
        'ema': {'source': 'tech', 'type': 'numeric', 'params': ['period']},
        'rsi': {'source': 'tech', 'type': 'numeric', 'params': ['period']},
        'macd': {'source': 'tech', 'type': 'crossover', 'params': ['fastperiod', 'slowperiod', 'signalperiod']},
        'stoch': {'source': 'tech', 'type': 'crossover_value', 'params': ['fastk_period', 'slowk_period', 'slowd_period']},
        'bbands': {'source': 'tech', 'type': 'crossover', 'params': ['period', 'nbdevup', 'nbdevdn']},
        'adx': {'source': 'tech', 'type': 'numeric', 'params': ['period']},
    }

    @staticmethod
    def define_parameters():
        """
        This scanner's parameters are defined dynamically in the UI, so this
        method returns an empty list.
        """
        return []

    def get_leading_columns(self) -> list:
        """
        Use the output_columns defined in the UI as the leading columns.
        """
        return self.params.get('output_columns', ['symbol', 'longname', 'marketcap'])

    def get_sort_info(self) -> dict:
        """
        Sort by market cap by default for the generic screener.
        """
        return {'by': 'marketcap', 'ascending': False}

    def _get_base_query(self, db: Session):
        """
        Helper method to build the initial database query based on DB-level filters.
        This will be used by the BaseScanner's run_scan method.
        """
        market = self.params.get('market', 'us')
        filters = self.params.get('filters', [])

        # --- Stage 1: Filter candidates using the database ---
        db_filters = [f for f in filters if self.FILTER_MAP.get(f.get('name'), {}).get('source') == 'db']

        # Start with a base query
        query = db.query(Company).filter(
            Company.isactive == True,
            Company.exchange.in_(db.query(Exchange.exchange_code).filter(Exchange.country_code == market))
        )

        # Dynamically add filters
        active_filters = []
        for f in db_filters:
            filter_name = f.get('name')
            filter_op = f.get('op')
            filter_value = f.get('value')

            if not all([filter_name, filter_op, filter_value is not None]):
                continue

            filter_info = self.FILTER_MAP.get(filter_name)
            if not filter_info:
                continue

            model_attr = filter_info['model_attr']
            
            # Ensure the attribute is not None before applying comparison
            active_filters.append(model_attr != None)

            # Handle percentage values that are stored as decimals (e.g., 0.1 for 10%)
            if filter_info['type'] == 'percentage':
                filter_value = filter_value / 100.0

            if filter_op == '>':
                active_filters.append(model_attr > filter_value)
            elif filter_op == '>=':
                active_filters.append(model_attr >= filter_value)
            elif filter_op == '<':
                active_filters.append(model_attr < filter_value)
            elif filter_op == '<=':
                active_filters.append(model_attr <= filter_value)
            elif filter_op == '==':
                active_filters.append(model_attr == filter_value)
            elif filter_op == 'in':
                if isinstance(filter_value, list) and filter_value:
                    active_filters.append(model_attr.in_(filter_value))

        if active_filters:
            query = query.filter(and_(*active_filters))

        return query

    def run_scan(self, db: Session) -> pd.DataFrame:
        """
        Overrides the BaseScanner's run_scan to handle the two-stage filtering process.
        """
        # Use the custom query builder for the initial candidate selection
        candidate_query = self._get_base_query(db)
        
        # Use the base class's run_scan logic, but pass our custom query
        return super().run_scan(db, candidate_query=candidate_query)

    def scan_company(self, group: pd.DataFrame, company_info: dict) -> dict | None:
        tech_filters = [f for f in self.params.get('filters', []) if self.FILTER_MAP.get(f.get('name'), {}).get('source') == 'tech']

        # Apply each technical filter
        for f in tech_filters:
            if not self._apply_tech_filter(group, f):
                return None # Fails if any tech filter fails

        # If all filters pass (or there are no tech filters), clean up and return
        for key in ['id', 'isactive']:
            if key in company_info: del company_info[key]
        return company_info # Passes if all tech filters pass

    def _apply_tech_filter(self, group: pd.DataFrame, tech_filter: dict) -> bool:
        """Helper to apply a single technical filter to a company's price history."""
        name = tech_filter.get('name')
        op = tech_filter.get('op')
        params = tech_filter.get('value', {})
        
        if name == 'sma':
            period = params.get('period', 20)
            if len(group) < period: return False
            sma = talib.SMA(group['close'], timeperiod=period).iloc[-1]
            price = group['close'].iloc[-1]
            return eval(f"price {op} sma") if pd.notna(price) and pd.notna(sma) else False

        if name == 'ema':
            period = params.get('period', 20)
            if len(group) < period: return False
            ema = talib.EMA(group['close'], timeperiod=period).iloc[-1]
            price = group['close'].iloc[-1]
            return eval(f"price {op} ema") if pd.notna(price) and pd.notna(ema) else False

        if name == 'rsi':
            period = params.get('period', 14)
            if len(group) < period + 1: return False
            rsi = talib.RSI(group['close'], timeperiod=period).iloc[-1]
            value = params.get('value')
            return eval(f"rsi {op} value") if pd.notna(rsi) and pd.notna(value) else False

        if name == 'macd':
            fast = params.get('fastperiod', 12)
            slow = params.get('slowperiod', 26)
            signal = params.get('signalperiod', 9)
            macd_line, signal_line, _ = talib.MACD(group['close'], fastperiod=fast, slowperiod=slow, signalperiod=signal)
            if len(macd_line) < 2 or pd.isna(macd_line.iloc[-1]) or pd.isna(signal_line.iloc[-1]): return False
            if op == 'cross_above': return macd_line.iloc[-1] > signal_line.iloc[-1] and macd_line.iloc[-2] <= signal_line.iloc[-2]
            if op == 'cross_below': return macd_line.iloc[-1] < signal_line.iloc[-1] and macd_line.iloc[-2] >= signal_line.iloc[-2]

        if name == 'stoch':
            fastk = params.get('fastk_period', 14)
            slowk_p = params.get('slowk_period', 3)
            slowd_p = params.get('slowd_period', 3)
            slowk, slowd = talib.STOCH(group['high'], group['low'], group['close'], fastk_period=fastk, slowk_period=slowk_p, slowd_period=slowd_p)
            if len(slowk) < 2 or pd.isna(slowk.iloc[-1]) or pd.isna(slowd.iloc[-1]): return False
            if op == 'cross_above': return slowk.iloc[-1] > slowd.iloc[-1] and slowk.iloc[-2] <= slowd.iloc[-2]
            if op == 'cross_below': return slowk.iloc[-1] < slowd.iloc[-1] and slowk.iloc[-2] >= slowd.iloc[-2]
            if op == 'above': return slowk.iloc[-1] > params.get('value')
            if op == 'below': return slowk.iloc[-1] < params.get('value')

        if name == 'bbands':
            period = params.get('period', 20)
            dev_up = params.get('nbdevup', 2.0)
            dev_dn = params.get('nbdevdn', 2.0)
            upper, _, lower = talib.BBANDS(group['close'], timeperiod=period, nbdevup=dev_up, nbdevdn=dev_dn)
            if len(upper) < 1 or pd.isna(upper.iloc[-1]) or pd.isna(lower.iloc[-1]): return False
            price = group['close'].iloc[-1]
            if op == 'cross_above_upper': return price > upper.iloc[-1]
            if op == 'cross_below_lower': return price < lower.iloc[-1]

        return False
