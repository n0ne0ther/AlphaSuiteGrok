import pandas as pd
from sqlalchemy.orm import Session

from scanners.scanner_sdk import BaseScanner
from core.model import Company, Exchange

class StrongestIndustriesScanner(BaseScanner):
    """
    Finds the strongest stocks within the strongest industries based on Relative Strength (RS) data.

    Goal:
        Find market-leading stocks that are in market-leading industry groups. This is a
        core principle of momentum investing (e.g., CANSLIM), based on the idea that
        the strongest stocks tend to come from the strongest sectors of the market.

    Criteria:
        - Identify industries with the highest average Relative Strength (RS) Ratio.
        - From those top industries, identify the stocks with the highest individual RS Ratios.
        - Apply liquidity and size filters (market cap, volume, minimum number of stocks in an industry).
    """
    @staticmethod
    def define_parameters():
        return [
            {"name": "rs_period_months", "type": "select", "default": 12, "label": "RS Period (Months)", "options": [12, 6, 3]},
            {"name": "top_n_industries", "type": "int", "default": 5, "label": "Top N Industries"},
            {"name": "top_n_stocks_per_industry", "type": "int", "default": 5, "label": "Top N Stocks per Industry"},
            {"name": "min_market_cap", "type": "int", "default": 1000000000, "label": "Min. Market Cap"},
            {"name": "min_avg_volume", "type": "int", "default": 100000, "label": "Min. Avg. Volume"},
            {"name": "volume_lookback_days", "type": "int", "default": 50, "label": "Avg. Volume Lookback"},
            {"name": "min_industry_size", "type": "int", "default": 30, "label": "Min. Stocks in Industry"},
        ]

    @staticmethod
    def get_leading_columns():
        return ['symbol', 'longname', 'industry', 'industry_rs_percentile', 'rs_percentile']

    @staticmethod
    def get_sort_info():
        # Sort by the strongest industry first, then by the strongest stock within that industry
        return {'by': ['industry_rs_percentile', 'rs_percentile'], 'ascending': [False, False]}

    def run_scan(self, db: Session, candidate_query=None) -> pd.DataFrame:
        market = self.params.get('market', 'us')
        rs_period_months = self.params.get('rs_period_months', 12)
        top_n_industries = self.params.get('top_n_industries', 5)
        top_n_stocks_per_industry = self.params.get('top_n_stocks_per_industry', 5)
        min_market_cap = self.params.get('min_market_cap', 1000000000)
        min_industry_size = self.params.get('min_industry_size', 30)

        # Map the selected period to the correct database column
        rs_column_map = {
            12: Company.relative_strength_percentile_252,
            6: Company.relative_strength_percentile_126,
            3: Company.relative_strength_percentile_63,
        }
        # Default to 12-month if an invalid period is given
        rs_column = rs_column_map.get(rs_period_months, Company.relative_strength_percentile_252)

        # 1. Get all companies in the market with necessary data
        # Note: We are not using the BaseScanner's run_scan here because this scanner's logic
        # is fundamentally different (grouping by industry first). We will manually apply volume filter later.
        company_data = db.query(
            Company.symbol, Company.longname, Company.industry, rs_column.label('rs_percentile')
        ).filter(
            Company.isactive == True,
            Company.exchange.in_(db.query(Exchange.exchange_code).filter(Exchange.country_code == market)),
            Company.industry != None,
            rs_column != None,
            Company.marketcap > min_market_cap,
        ).all()

        if not company_data:
            return pd.DataFrame()

        company_df = pd.DataFrame(company_data, columns=['symbol', 'longname', 'industry', 'rs_percentile'])

        # 2. Filter for industries with a minimum number of stocks
        industry_counts = company_df['industry'].value_counts()
        valid_industries = industry_counts[industry_counts >= min_industry_size].index.tolist()
        company_df = company_df[company_df['industry'].isin(valid_industries)]

        if company_df.empty:
            return pd.DataFrame()

        # 3. Calculate Mean RS Percentile per Industry and find the top industries.
        # Outlier filtering is no longer needed as percentiles are already a normalized rank.
        industry_rs = company_df.groupby('industry')['rs_percentile'].mean().sort_values(ascending=False)
        top_industries = industry_rs.head(top_n_industries).index.tolist()

        # 4. Find the top stocks within those top industries
        all_strongest_stocks = []
        for industry in top_industries:
            industry_df = company_df[company_df['industry'] == industry].copy()
            top_stocks = industry_df.sort_values('rs_percentile', ascending=False).head(top_n_stocks_per_industry)
            all_strongest_stocks.append(top_stocks)

        if not all_strongest_stocks: # No stocks found
            return pd.DataFrame()

        # 5. Combine results and add the industry's average RS Ratio
        final_df = pd.concat(all_strongest_stocks).reset_index(drop=True)
        
        industry_rs_df = industry_rs.reset_index()
        industry_rs_df.columns = ['industry', 'industry_rs_percentile']
        final_df = pd.merge(final_df, industry_rs_df, on='industry', how='left')

        final_df['rs_percentile'] = final_df['rs_percentile'].round(2)
        final_df['industry_rs_percentile'] = final_df['industry_rs_percentile'].round(2)

        # Format the output DataFrame
        if not final_df.empty:
            leading_columns = self.get_leading_columns()
            sort_info = self.get_sort_info()

            existing_leading_columns = [col for col in leading_columns if col in final_df.columns]
            other_columns = [col for col in final_df.columns if col not in existing_leading_columns]
            final_df = final_df[existing_leading_columns + other_columns]

            if sort_info and all(col in final_df.columns for col in sort_info.get('by', [])):
                final_df = final_df.sort_values(by=sort_info['by'], ascending=sort_info.get('ascending', False))
        return final_df