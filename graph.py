import pandas as pd
import matplotlib.pyplot as plt

try:
    df = pd.read_csv("backtest_history.csv")
    df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d')
except FileNotFoundError:
    print("Error: backtest_history.csv not found.")
    exit()

plt.style.use('dark_background')
plt.figure(figsize=(12, 6))

plt.plot(df['Date'], df['Net_Worth'], label='Strategy Net Worth', color='#00ffcc', linewidth=1.5)

df['Net_Worth_MA'] = df['Net_Worth'].rolling(window=50).mean()
plt.plot(df['Date'], df['Net_Worth_MA'], label='50-Day Trend', color='#ff3366', linestyle='--', alpha=0.7)

plt.title('Backtest Results: Risk-Adjusted Momentum Strategy', fontsize=14, pad=20)
plt.xlabel('Date', fontsize=12)
plt.ylabel('Total Value ($)', fontsize=12)
plt.grid(True, which='both', linestyle='--', alpha=0.3)
plt.legend()
plt.tight_layout()

plt.savefig("networth_curve.png", dpi=300)