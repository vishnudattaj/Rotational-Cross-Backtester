import pandas as pd
import os
from tqdm import tqdm

directory = "updatedStocks"
all_files = [f for f in os.listdir(directory) if f.endswith('.parquet')]

data_frames = []

for filename in tqdm(all_files, desc="Processing Data"):
    ticker = filename.replace(".parquet", "")
    df = pd.read_parquet(f"{directory}/{filename}")

    valid_data = df.dropna(subset=['signal-strength']).copy()

    if not valid_data.empty:
        valid_data["risk-adj-signal"] = valid_data["signal-strength"] / valid_data["garman-klass"]
        valid_data = valid_data[['Date', 'Open', 'Close', 'risk-adj-signal', 'daily-volume', "stock-price", "garman-klass"]]
        valid_data['Ticker'] = ticker
        data_frames.append(valid_data)

totalDf = pd.concat(data_frames)
totalDf = totalDf.drop_duplicates(subset=['Date', 'Ticker'], keep='first')

reshaped = totalDf.pivot(index='Date', columns='Ticker', values=['Open', 'Close', 'risk-adj-signal', 'daily-volume', "stock-price", "garman-klass"])

reshaped.to_parquet("masterData.parquet")