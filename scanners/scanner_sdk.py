"""
Defines the Software Development Kit (SDK) for creating new, self-contained
scanner modules.
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, List
import textwrap
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy.orm import Session

from core.model import PriceHistory, Company, Exchange, object_as_dict
class BaseScanner(ABC):
    """
    The abstract base class for all market scanners in AlphaSuite.
    It defines a common interface for creating custom scanners that can be
    dynamically discovered and used by the application.
    """
    def __init__(self, params: Dict[str, Any] | None = None):
        """
        Initializes the scanner with a set of parameters.

        Args:
            params (dict): A dictionary of parameters for the scanner.
        """
        self.params = params if params is not None else {}

    @staticmethod
    @abstractmethod
    def define_parameters() -> List[Dict[str, Any]]:
        """
        Defines parameters for the scanner, including their type, default value,
        and UI-friendly label. This is used to dynamically build the UI.
        """
        pass

    @classmethod
    def get_description(cls) -> str:
        """
        Returns the scanner's description from its docstring.
        This is used to display help text in the UI.
        """
        doc = cls.__doc__
        if not doc:
            return "No description available."
        return textwrap.dedent(doc).strip()

    def scan_company(self, group: pd.DataFrame, company_info: dict) -> dict | None:
        """
        The specific scanning logic for a single company's price data.
        This method is implemented by each concrete scanner class.

        Args:
            group (pd.DataFrame): The price history DataFrame for a single company.
                                  The index has been reset for talib compatibility.
            company_info (dict): A dictionary of the company's fundamental data.

        Returns:
            A dictionary with the results if the company passes the scan, otherwise None.
            This dictionary will be appended to the final results.
        """
        return None # Default implementation for scanners that don't use it.

    @staticmethod
    def get_leading_columns() -> List[str]:
        """
        Returns a list of column names that should appear first in the results.
        Defaults to a basic set if not overridden.
        """
        return ['symbol', 'longname', 'sector', 'marketcap']

    @staticmethod
    def get_sort_info() -> Dict[str, Any]:
        """
        Returns a dictionary specifying how to sort the final results.
        Defaults to sorting by marketcap descending if not overridden.
        """
        return {'by': 'marketcap', 'ascending': False}

    def run_scan(self, db: Session, candidate_query=None) -> pd.DataFrame:
        """
        Template method that orchestrates the entire scanning process.
        It handles fetching candidates, getting price history, and looping,
        while delegating the specific scan logic to the `scan_company` method.
        """
        if candidate_query is None:
            # 1. Get common and specific parameters
            market = self.params.get('market', 'us')
            min_market_cap = self.params.get('min_market_cap', 1000000000)
            
            # 2. Get candidate companies using default filters
            candidate_query = db.query(Company).filter(
                Company.isactive == True,
                Company.exchange.in_(db.query(Exchange.exchange_code).filter(Exchange.country_code == market)),
                Company.marketcap > min_market_cap,
            )

        candidates = candidate_query.all()
        if not candidates:
            return pd.DataFrame()

        candidate_map = {c.id: object_as_dict(c) for c in candidates}
        candidate_ids = list(candidate_map.keys())

        # 3. Fetch price history
        days_back = self.params.get('days_back', 500) # Default, can be overridden by scanner params
        price_df = self._get_price_history(db, candidate_ids, days_back=days_back)
        if price_df.empty:
            return pd.DataFrame()

        # 4. Calculate and filter by recent average volume (more reliable than stale DB data)
        min_avg_volume = self.params.get('min_avg_volume', 100000)
        volume_lookback = self.params.get('volume_lookback_days', 50)

        # Calculate average volume for each company
        avg_volume = price_df.groupby('company_id')['volume'].rolling(window=volume_lookback, min_periods=1).mean().reset_index(level=0, drop=True)
        price_df['avg_volume'] = avg_volume
        
        # Get the latest average volume for each company and filter
        latest_avg_volume = price_df.groupby('company_id')['avg_volume'].last()
        passing_volume_ids = latest_avg_volume[latest_avg_volume >= min_avg_volume].index
        
        price_df = price_df[price_df['company_id'].isin(passing_volume_ids)]

        # 4. Loop through companies and apply specific scan logic
        passing_stocks = []
        for company_id, group in price_df.groupby('company_id'):
            group = group.reset_index(drop=True) # Ensure contiguous index for talib
            company_info = candidate_map.get(company_id)
            if company_info:
                result = self.scan_company(group, company_info)
                if result:
                    passing_stocks.append(result)

        df = pd.DataFrame(passing_stocks)

        # 5. Format the output DataFrame
        if not df.empty:
            leading_columns = self.get_leading_columns()
            sort_info = self.get_sort_info()

            # Ensure all leading columns exist in the DataFrame before trying to reorder
            existing_leading_columns = [col for col in leading_columns if col in df.columns]
            other_columns = [col for col in df.columns if col not in existing_leading_columns]
            final_column_order = existing_leading_columns + other_columns
            df = df[final_column_order]

            if sort_info and sort_info.get('by') in df.columns:
                df = df.sort_values(by=sort_info['by'], ascending=sort_info.get('ascending', False))

        return df

    def _get_price_history(self, db: Session, company_ids: List[int], days_back: int) -> pd.DataFrame:
        """
        A helper method to efficiently fetch price history for a list of companies.

        Args:
            db: The database session.
            company_ids: A list of company IDs to fetch price data for.
            days_back: The number of calendar days of history to retrieve.

        Returns:
            A pandas DataFrame containing the price history, with columns
            ['company_id', 'date', 'open', 'high', 'low', 'close', 'adjclose', 'volume'].
            The DataFrame is sorted by company_id and date.
        """
        if not company_ids:
            return pd.DataFrame()

        start_date = datetime.utcnow() - timedelta(days=days_back)

        # Query to fetch price history including split coefficients
        query = db.query(
            PriceHistory.company_id,
            PriceHistory.date,
            PriceHistory.open,
            PriceHistory.high,
            PriceHistory.low,
            PriceHistory.close,
            PriceHistory.adjclose,
            PriceHistory.volume,
            PriceHistory.split_coefficient
        ).filter(
            PriceHistory.company_id.in_(company_ids),
            PriceHistory.date >= start_date,
        ).order_by(PriceHistory.company_id, PriceHistory.date)

        df = pd.read_sql(query.statement, db.bind)

        if not df.empty:
            # Apply split adjustments to OHLC prices
            # This ensures indicators are calculated on prices consistent with charting software
            df = df.sort_values(by=['company_id', 'date'])
            # Replace 0s with 1s in split_coefficient to avoid division by zero during cumprod
            df['split_coefficient'] = df['split_coefficient'].replace(0, 1)
            
            df['cum_split'] = df.groupby('company_id')['split_coefficient'].transform(lambda x: x.iloc[::-1].cumprod().iloc[::-1])
            for col in ['open', 'high', 'low', 'close']:
                df[col] = df[col] / df['cum_split']
        
        return df
