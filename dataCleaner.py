import pandas as pd
import os

def crossAvg(df):
    df["daily-volume"] = (df["Volume"] * df["Close"]).rolling(window=20, min_periods=20).mean()
    df["short-term-ma"] = df["Close"].rolling(window=50, min_periods=50).mean()
    df["long-term-ma"] = df["Close"].rolling(window=200, min_periods=200).mean()

    return df

directory = "full_history"

with os.scandir(directory) as entries:
    for entry in entries:
        stockDf = pd.read_parquet(f"{directory}/{entry.name}")

        stockDf = stockDf[["Date", "Open", "Close", "Volume"]]

        stockDf = crossAvg(stockDf)
        stockDf = stockDf[stockDf["Date"] > 20119999].reset_index(drop=True)

        if stockDf.empty:
            continue

        firstDay = stockDf.iloc[0]

        if (firstDay["Date"] == 20120103) and pd.notna(firstDay["short-term-ma"]) and pd.notna(firstDay["long-term-ma"]):
            stockDf["signal-strength"] = (stockDf["short-term-ma"] - stockDf["long-term-ma"]) / stockDf["long-term-ma"]
            stockDf.to_parquet(f"updatedStocks/{entry.name.replace('.csv', '.parquet')}", index=False)