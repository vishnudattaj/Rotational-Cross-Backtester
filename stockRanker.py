import pandas as pd
import os

directory = "updatedStocks"

stats_list = []

with os.scandir(directory) as entries:
    for entry in entries:
        parquet = pd.read_parquet(f"updatedStocks/{entry.name}")

        firstDay = parquet.iloc[0]
        firstDay["Ticker"] = entry.name.replace(".parquet", "")

        if (firstDay["daily-volume"] > 1000000) and (firstDay["Close"] > 5):
            stats_list.append(firstDay)

baseline_stats = pd.DataFrame(stats_list)
baseline_stats.sort_values("signal-strength", inplace=True, ascending=False)
baseline_stats.reset_index(drop=True, inplace=True)
baseline_stats.drop(columns=["Date"], inplace=True)
baseline_stats.to_parquet("baseline_stats.parquet")