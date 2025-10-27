import os
import logging
import pandas as pd
import yfinance as yf
from alpha_vantage.fundamentaldata import FundamentalData
from datetime import datetime, timedelta
from typing import List, Dict
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set up logging (best practice: track what's happening)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Config (best practice: use env vars, not hardcode)
ALPHA_VANTAGE_KEY = os.getenv('ALPHA_VANTAGE_KEY')
if not ALPHA_VANTAGE_KEY:
    raise ValueError("Set ALPHA_VANTAGE_KEY env var with your API key!")

# Parameters (easy to tweak)
TICKERS_LIMIT = 10  # Start small to avoid API limits
START_DATE = (datetime.now() - timedelta(days=5*365)).strftime('%Y-%m-%d')  # Last 5 years
END_DATE = datetime.now().strftime('%Y-%m-%d')

def get_sp500_tickers() -> List[str]:
    """Fetch S&P 500 tickers from Wikipedia using a browser-like User-Agent."""
    try:
        import requests
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36 StockFetcher/1.0"
        }
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        df = pd.read_html(resp.text)[0]
        tickers = df['Symbol'].tolist()[:TICKERS_LIMIT]
        logger.info(f"Fetched {len(tickers)} S&P 500 tickers.")
        return tickers
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error fetching tickers: {e}")
        raise
    except Exception as e:
        logger.error(f"Error fetching tickers: {e}")
        raise
        

def fetch_market_data(tickers: List[str], start: str, end: str) -> pd.DataFrame:
    """Fetch OHLCV data using yfinance (handles multiple tickers efficiently)."""
    try:
        data = yf.download(tickers, start=start, end=end, group_by='ticker')
        # Flatten multi-index for simplicity (one DF per ticker, but concat here)
        combined = pd.DataFrame()
        for ticker in tickers:
            ticker_data = data[ticker].dropna()  # Drop rows with all NaNs
            ticker_data['Ticker'] = ticker
            combined = pd.concat([combined, ticker_data])
        combined = combined.sort_values(['Ticker', 'Date']).reset_index(drop=True)
        logger.info(f"Market data shape: {combined.shape}")
        return combined
    except Exception as e:
        logger.error(f"Error fetching market data: {e}")
        raise

def fetch_fundamentals(tickers: List[str]) -> Dict[str, pd.DataFrame]:
    """Fetch fundamentals (P/E, EPS, earnings) using Alpha Vantage. Rate-limited, so batch if needed."""
    fd = FundamentalData(key=ALPHA_VANTAGE_KEY, output_format='pandas')
    fundamentals = {}
    
    for ticker in tickers:
        try:
            # Company overview: P/E, EPS, etc. (static-ish, quarterly updates)
            overview, _ = fd.get_company_overview(symbol=ticker)
            overview['Ticker'] = ticker
            fundamentals[f'{ticker}_overview'] = overview
            
            # Earnings history (time-series)
            earnings, _ = fd.get_earnings(symbol=ticker)
            earnings['Ticker'] = ticker
            fundamentals[f'{ticker}_earnings'] = earnings
            
            logger.info(f"Fetched fundamentals for {ticker}")
            
            # Rate limit: Sleep 12s (for free tier: 5 calls/min)
            import time
            time.sleep(12)
            
        except Exception as e:
            logger.warning(f"Error for {ticker}: {e} (skipping)")
            continue
    
    # Combine overviews into one DF for easy use
    overviews = pd.concat([fundamentals[f'{t}_overview'] for t in tickers if f'{t}_overview' in fundamentals], ignore_index=True)
    fundamentals['all_overviews'] = overviews
    return fundamentals

def validate_and_save(data_market: pd.DataFrame, data_fund: Dict[str, pd.DataFrame], tickers: List[str]):
    """Basic validation and save to CSVs (best practice: persist raw data)."""
    # Market validation
    if data_market.empty:
        raise ValueError("No market data fetched!")
    data_market = data_market.dropna(subset=['Close'])  # Ensure prices exist
    missing = data_market['Ticker'].value_counts().loc[lambda x: x < 100]  # Flag low-data tickers
    if not missing.empty:
        logger.warning(f"Low data for: {missing}")
    
    # Fundamentals validation (spot-check)
    if 'all_overviews' not in data_fund or data_fund['all_overviews'].empty:
        logger.warning("No fundamentals fetched—check API key/limits.")
    
    # Save with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    data_market.to_csv(f'market_data_{timestamp}.csv', index=False)
    data_fund['all_overviews'].to_csv(f'fundamentals_overview_{timestamp}.csv', index=False)
    
    # Save individual earnings if needed
    for key, df in data_fund.items():
        if '_earnings' in key:
            df.to_csv(f'{key}_{timestamp}.csv', index=False)
    
    logger.info(f"Data saved: market ({data_market.shape}), overviews ({data_fund['all_overviews'].shape if 'all_overviews' in data_fund else 'N/A'})")

if __name__ == "__main__":
    # Main workflow
    tickers = get_sp500_tickers()
    market_data = fetch_market_data(tickers, START_DATE, END_DATE)
    fund_data = fetch_fundamentals(tickers)
    validate_and_save(market_data, fund_data, tickers)
    
    print("Data fetch complete! Check CSVs in your folder.")
    print("\nSample Market Data:")
    print(market_data.head())
    print("\nSample Fundamentals (Overview):")
    if 'all_overviews' in fund_data:
        print(fund_data['all_overviews'][['Ticker', 'PERatio', 'EPS']].head())