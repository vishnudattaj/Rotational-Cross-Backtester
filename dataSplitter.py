import pandas as pd
import os

output_dir = "full_history"
os.makedirs(output_dir, exist_ok=True)
stockDf = pd.read_parquet("all_stock_data.parquet", columns=["Date", "Ticker", "Open", "Close", "Volume", "Dividends", "Stock Splits"])

stockDf["Date"] = pd.to_datetime(stockDf["Date"])
stockDf["Date"] = (stockDf["Date"].dt.year * 10000 + stockDf["Date"].dt.month * 100 + stockDf["Date"].dt.day)
stockDf.sort_values("Date", inplace=True)

for ticker, group in stockDf.groupby("Ticker"):
    file_path = os.path.join(output_dir, f"{ticker}.parquet")
    group.to_parquet(file_path, index=False)