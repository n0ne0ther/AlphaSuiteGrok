#!/bin/bash
set -e
echo "Installing AlphaSuiteGrok – Nuclear Edition 2025"

sudo apt update && sudo apt install -y python3-venv python3-pip postgresql postgresql-contrib curl git

# Database
sudo -u postgres psql -c "CREATE DATABASE alphasuite;" 2>/dev/null || true
sudo -u postgres psql -c "CREATE USER \"AlphaSuite\" WITH PASSWORD '317300EdC~~';" 2>/dev/null || true
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE alphasuite TO \"AlphaSuite\";"

# Python env
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install streamlit pandas numpy plotly polygon sqlalchemy psycopg2-binary python-dotenv tqdm yfinance

# Download 52 nuclear tickers (free-tier safe)
python -c "
import os, time, pandas as pd
from polygon import RESTClient
from sqlalchemy import create_engine
from dotenv import load_dotenv
load_dotenv()
client = RESTClient(os.getenv('POLYGON_API_KEY'))
engine = create_engine(os.getenv('DATABASE_URL'))
tickers = open('sub100_universe.txt').read().split()
print(f'Downloading {len(tickers)} tickers...')
for t in tickers:
    print(f'{t}...', end=' ')
    try:
        aggs = client.get_aggs(t,1,'day','2015-01-01','2025-12-31',adjusted=True,limit=50000)
        df = pd.DataFrame([{'timestamp':a.timestamp//1000,'symbol':t,'open':a.open,'high':a.high,'low':a.low,'close':a.close,'volume':a.volume} for a in aggs])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
        df.to_sql('daily_prices', engine, if_exists='append', index=False)
        print('saved')
    except Exception as e: print('failed', e)
    time.sleep(12.5)
"

chmod +x start-alphasuite.sh
echo "Installation complete!"
echo "Run: ./start-alphasuite.sh"
echo "Open: http://localhost:9517"
