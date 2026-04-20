import pandas as pd
import numpy as np
import os

def crossAvg(df):
    log_hl = np.log(df["High"] / df["Low"]) ** 2
    log_co = np.log(df["Close"] / df["Open"]) ** 2
    gk_var = (0.5 * log_hl - (2 * np.log(2) - 1) * log_co).rolling(window=20).mean()

    df["daily-volume"] = (df["Volume"] * df["Close"]).rolling(window=20, min_periods=20).mean()
    df["garman-klass"] = np.sqrt(gk_var.clip(lower=0))
    df["short-term-ma"] = df["Close"].rolling(window=50, min_periods=50).mean()
    df["long-term-ma"] = df["Close"].rolling(window=200, min_periods=200).mean()
    df["stock-price"] = 12 / df["garman-klass"].clip(lower=0.005, upper=0.2)

    return df

directory = "full_history"

with os.scandir(directory) as entries:
    for entry in entries:
        stockDf = pd.read_parquet(f"{directory}/{entry.name}")

        stockDf = stockDf[["Date", "Open", "High", "Low", "Close", "Volume"]]

        stockDf.loc[stockDf["Open"] <= 0, "Open"] = np.nan
        stockDf.loc[stockDf["Close"] <= 0, "Close"] = np.nan
        stockDf.loc[stockDf["High"] <= 0, "High"] = np.nan
        stockDf.loc[stockDf["Low"] <= 0, "Low"] = np.nan

        stockDf = crossAvg(stockDf)
        stockDf = stockDf[stockDf["Date"] > 20119999].reset_index(drop=True)

        if stockDf.empty:
            continue

        firstDay = stockDf.iloc[0]

        if (firstDay["Date"] == 20120103) and pd.notna(firstDay["short-term-ma"]) and pd.notna(firstDay["long-term-ma"]):
            stockDf["signal-strength"] = (stockDf["short-term-ma"] - stockDf["long-term-ma"]) / stockDf["long-term-ma"]
            stockDf.to_parquet(f"updatedStocks/{entry.name.replace('.csv', '.parquet')}", index=False)