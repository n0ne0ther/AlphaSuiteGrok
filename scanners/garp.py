import pandas as pd
from sqlalchemy.orm import Session

from scanners.scanner_sdk import BaseScanner
from core.model import Company, Exchange, object_as_dict

class GARPScanner(BaseScanner):
    """
    Scans for "Growth at a Reasonable Price" (GARP) stocks.

    Goal:
        Find companies that are showing consistent earnings growth but are not
        trading at excessively high valuations. This hybrid approach combines
        elements of both growth and value investing.

    Criteria:
        - P/E Ratio is positive but below a certain threshold (to avoid speculative or unprofitable companies).
        - PEG Ratio is below a threshold (indicating the price is reasonable relative to growth).
        - Consistent positive earnings and revenue growth over recent periods.
        - Basic liquidity and size filters are applied.
    """
    @staticmethod
    def define_parameters():
        return [
            {"name": "min_avg_volume", "type": "int", "default": 100000, "label": "Min. Avg. Volume"},
            {"name": "volume_lookback_days", "type": "int", "default": 50, "label": "Avg. Volume Lookback"},
            {"name": "min_market_cap", "type": "int", "default": 1000000000, "label": "Min. Market Cap"},
            {"name": "max_pe_ratio", "type": "float", "default": 25.0, "label": "Max. P/E Ratio"},
            {"name": "max_peg_ratio", "type": "float", "default": 1.5, "label": "Max. PEG Ratio"},
            {"name": "min_eps_growth_pct", "type": "float", "default": 10.0, "label": "Min. Quarterly EPS Growth %"},
            {"name": "min_revenue_growth_pct", "type": "float", "default": 10.0, "label": "Min. Quarterly Revenue Growth %"},
        ]

    @staticmethod
    def get_leading_columns():
        return ['symbol', 'trailingpegratio', 'trailingpe', 'earningsquarterlygrowth', 'revenuegrowth', 'longname', 'industry', 'marketcap']

    @staticmethod
    def get_sort_info():
        # Sort by the lowest PEG ratio to find the most reasonably priced growth stocks
        return {'by': 'trailingpegratio', 'ascending': True}

    def scan_company(self, group: pd.DataFrame, company_info: dict) -> dict | None:
        """
        For this scanner, all filtering is done in the database query.
        If a company reaches this method, it's a match.
        """
        for key in ['id', 'isactive']: del company_info[key]
        return company_info

    def run_scan(self, db: Session) -> pd.DataFrame:
        min_market_cap = self.params.get('min_market_cap', 1000000000)
        max_pe_ratio = self.params.get('max_pe_ratio', 25.0)
        max_peg_ratio = self.params.get('max_peg_ratio', 1.5)
        min_eps_growth_pct = self.params.get('min_eps_growth_pct', 10.0)
        min_revenue_growth_pct = self.params.get('min_revenue_growth_pct', 10.0)
        market = self.params.get('market', 'us')

        # 1. Build the query with all fundamental filters
        candidate_query = db.query(Company).filter(
            Company.isactive == True,
            Company.exchange.in_(db.query(Exchange.exchange_code).filter(Exchange.country_code == market)),
            Company.marketcap > min_market_cap,
            Company.trailingpe > 0, # Must be profitable
            Company.trailingpe < max_pe_ratio,
            Company.trailingpegratio > 0, # PEG must be positive
            Company.trailingpegratio < max_peg_ratio,
            Company.earningsquarterlygrowth > (min_eps_growth_pct / 100),
            Company.revenuegrowth > (min_revenue_growth_pct / 100)
        )

        # This scanner only uses DB filters, so we can pass the custom query
        # to the base run_scan method, which will handle dynamic volume filtering.
        return super().run_scan(db, candidate_query=candidate_query)