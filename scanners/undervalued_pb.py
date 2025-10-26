import pandas as pd
from sqlalchemy.orm import Session

from scanners.scanner_sdk import BaseScanner
from core.model import Company, Exchange, object_as_dict

class UndervaluedPbScanner(BaseScanner):
    """
    Scans for potentially undervalued stocks based on a low Price-to-Book (P/B) ratio.

    Goal:
        Identify stocks that may be trading for less than their book value, a classic
        indicator of potential undervaluation. This is a common screen for value investors.

    Criteria:
        - Price-to-Book (P/B) ratio is below a specified maximum threshold.
        - The company is profitable (P/E > 0) to filter out distressed companies.
        - The company has a reasonable debt-to-equity ratio.
        - Basic liquidity and size filters are applied.
    """
    @staticmethod
    def define_parameters():
        return [
            {"name": "min_avg_volume", "type": "int", "default": 100000, "label": "Min. Avg. Volume"},
            {"name": "volume_lookback_days", "type": "int", "default": 50, "label": "Avg. Volume Lookback"},
            {"name": "min_market_cap", "type": "int", "default": 500000000, "label": "Min. Market Cap"},
            {"name": "max_pb_ratio", "type": "float", "default": 1.5, "label": "Max. Price-to-Book Ratio"},
            {"name": "min_pb_ratio", "type": "float", "default": 0.1, "label": "Min. Price-to-Book Ratio"},
            {"name": "max_debt_to_equity", "type": "float", "default": 2.0, "label": "Max. Debt-to-Equity"},
        ]

    @staticmethod
    def get_leading_columns():
        return ['symbol', 'pricetobook', 'trailingpe', 'debttoequity', 'longname', 'industry', 'marketcap']

    @staticmethod
    def get_sort_info():
        # Sort by the lowest P/B ratio to see the most undervalued stocks first
        return {'by': 'pricetobook', 'ascending': True}

    def scan_company(self, group: pd.DataFrame, company_info: dict) -> dict | None:
        """
        For this scanner, all filtering is done in the database query.
        If a company reaches this method, it's a match.
        """
        for key in ['id', 'isactive']: del company_info[key]
        return company_info

    def run_scan(self, db: Session, candidate_query=None) -> pd.DataFrame:
        # This scanner only uses DB filters, so we can build a custom query
        # and pass it to the BaseScanner's run_scan method.
        min_market_cap = self.params.get('min_market_cap', 500000000)
        max_pb_ratio = self.params.get('max_pb_ratio', 1.5)
        min_pb_ratio = self.params.get('min_pb_ratio', 0.1)
        max_debt_to_equity = self.params.get('max_debt_to_equity', 2.0)
        market = self.params.get('market', 'us')

        candidate_query = db.query(Company).filter(
            Company.isactive == True,
            Company.exchange.in_(db.query(Exchange.exchange_code).filter(Exchange.country_code == market)),
            Company.marketcap > min_market_cap,
            Company.bookvalue != None,
            Company.pricetobook != None,
            Company.pricetobook > min_pb_ratio,
            Company.pricetobook > 0,
            Company.pricetobook < max_pb_ratio,
            Company.trailingpe > 0,
            Company.debttoequity != None,
            Company.debttoequity < max_debt_to_equity
        )

        # Since this scanner has no on-the-fly calculations, we can just run the query
        # and format the results directly without using the BaseScanner's full pipeline.
        # The base run_scan will handle the dynamic volume filtering.
        return super().run_scan(db, candidate_query=candidate_query)