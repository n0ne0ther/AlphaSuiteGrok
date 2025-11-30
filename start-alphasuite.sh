#!/bin/bash
cd "$(dirname "$0")"
kill -9 $(lsof -t -i:9517) 2>/dev/null || true
source venv/bin/activate
export DATABASE_URL="postgresql://AlphaSuite:317300EdC~~@localhost:5432/alphasuite"
streamlit run Home.py --server.port 9517 --server.headless false
