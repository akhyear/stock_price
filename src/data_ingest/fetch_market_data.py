import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

# === Settings ===
TICKERS = ['A', 'ABBV', 'ABT', 'ACN', 'ADBE', 'AES', 'AFL', 'AMD', 'AOS', 'MMM']
START_DATE = (datetime.now() - timedelta(days=5*365)).strftime('%Y-%m-%d')
END_DATE = datetime.now().strftime('%Y-%m-%d')
OUTPUT_FILE = f"market_data_{END_DATE}.csv"

# === Fetch & Combine Data ===
def fetch_market_data(tickers, start, end):
    print(f"Fetching data from {start} to {end} for {len(tickers)} tickers...")

    data = yf.download(tickers, start=start, end=end, group_by='ticker', threads=True)
    combined = pd.DataFrame()

    for ticker in tickers:
        ticker_data = data[ticker].dropna().reset_index()
        ticker_data['Ticker'] = ticker
        combined = pd.concat([combined, ticker_data], ignore_index=True)

    # Ensure correct column order and sorting
    combined = combined[['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Ticker']]
    combined = combined.sort_values(['Ticker', 'Date']).reset_index(drop=True)

    return combined

# === Run & Save ===
if __name__ == "__main__":
    df = fetch_market_data(TICKERS, START_DATE, END_DATE)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\n✅ Data saved to: {OUTPUT_FILE}")
    print(f"✅ Shape: {df.shape}")
    print(df.head())