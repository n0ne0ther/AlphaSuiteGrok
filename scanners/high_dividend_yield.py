import pandas as pd
from sqlalchemy.orm import Session

from scanners.scanner_sdk import BaseScanner
from core.model import Company, Exchange, object_as_dict

class HighDividendYieldScanner(BaseScanner):
    """
    Scans for stocks with a high and sustainable dividend yield.

    Goal:
        Identify potentially stable, income-generating stocks for long-term investment.
        This scanner looks for companies that pay a significant dividend, while also
        ensuring the dividend is sustainable by checking the payout ratio.

    Criteria:
        - Dividend Yield is above a specified minimum percentage.
        - Payout Ratio is within a healthy range (positive but not excessively high),
          indicating the company can afford its dividend payments from earnings.
        - Basic liquidity and size filters are applied.
    """
    @staticmethod
    def define_parameters():
        return [
            {"name": "min_avg_volume", "type": "int", "default": 100000, "label": "Min. Avg. Volume"},
            {"name": "volume_lookback_days", "type": "int", "default": 50, "label": "Avg. Volume Lookback"},
            {"name": "min_market_cap", "type": "int", "default": 1000000000, "label": "Min. Market Cap"},
            {"name": "min_dividend_yield_pct", "type": "float", "default": 3.0, "label": "Min. Dividend Yield %"},
            {"name": "max_payout_ratio_pct", "type": "float", "default": 80.0, "label": "Max. Payout Ratio %"},
        ]

    @staticmethod
    def get_leading_columns():
        return ['symbol', 'dividendyield', 'payoutratio', 'longname', 'industry', 'marketcap']

    @staticmethod
    def get_sort_info():
        # Sort by the highest dividend yield
        return {'by': 'dividendyield', 'ascending': False}

    def scan_company(self, group: pd.DataFrame, company_info: dict) -> dict | None:
        """
        For this scanner, all filtering is done in the database query.
        If a company reaches this method, it's a match.
        """
        for key in ['id', 'isactive']: del company_info[key]
        return company_info

    def run_scan(self, db: Session) -> pd.DataFrame:
        min_market_cap = self.params.get('min_market_cap', 1000000000)
        min_dividend_yield_pct = self.params.get('min_dividend_yield_pct', 3.0)
        max_payout_ratio_pct = self.params.get('max_payout_ratio_pct', 80.0)
        market = self.params.get('market', 'us')

        # 1. Build the query with all fundamental filters
        candidate_query = db.query(Company).filter(
            Company.isactive == True,
            Company.exchange.in_(db.query(Exchange.exchange_code).filter(Exchange.country_code == market)),
            Company.marketcap > min_market_cap,
            Company.dividendyield > min_dividend_yield_pct,
            Company.payoutratio > 0, # Must be positive
            Company.payoutratio < max_payout_ratio_pct # And not excessively high
        )

        # This scanner only uses DB filters, so we can pass the custom query
        # to the base run_scan method, which will handle dynamic volume filtering.
        df = super().run_scan(db, candidate_query=candidate_query)
        if not df.empty:
            df['dividendyield'] = df['dividendyield'].round(2)
            df['payoutratio'] = df['payoutratio'].round(2)
        return df