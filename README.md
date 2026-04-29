# Volatility-Adjusted Momentum Engine

A quantitative framework for evaluating risk-adjusted momentum strategies with dynamic position sizing. This project implements a dual-filter approach: identifying price breakouts via moving average spreads and normalizing those signals using Garman-Klass volatility estimators to prioritize stable upward trends. In additoin, this project implements a volatility-based rotation logic, where capital allocation is inversely proportional to an asset's Garman-Klass volatility, ensuring a balanced risk contribution across the portfolio.

---

## How It Works  

The project is composed of six main scripts:  

### `dataSplitter.py`  
- **Data Partitioning:** Processes the master `all_stock_data.parquet` and splits it into individual ticker files.
- **Optimization:** Converts timestamps into YYYYMMDD integers to accelerate chronological sorting and reduce memory overhead during iterations.

### `dataCleaner.py`  
- **Garman-Klass Volatility:** Calculates high-efficiency volatility estimates using Open, High, Low, and Close prices.
- **Signal Generation:** Computes the **Signal Strength** as the percentage difference between the 50-day (short-term) and 200-day (long-term) moving averages.
- **Data Pruning:** Standardizes the timeline to start in 2012 and removes tickers with insufficient price history for the moving average windows.

### `stockRanker.py`  
- **Risk-Adjusted Ranking:** Generates the initial portfolio by dividing the **Signal Strength** by the **Garman-Klass** value.
- **Liquidity Filters:** Enforces a minimum $1,000,000 daily dollar volume and a $5 price floor to ensure tradeability.

### `master-signal.py`  
- **Matrix Construction:** Re-aggregates cleaned ticker data into a single, high-dimensional Parquet file.
- **Pivot Optimization:** Organizes data into a Date-Ticker multi-index, allowing the backtester to perform "point-in-time" lookups across the entire market simultaneously.

### `backTest.py`  
- **Execution Engine:** Simulates a daily rebalanced portfolio with a 100-slot maximum capacity.
- **Dynamic Rebalancing:** Automatically sells positions when the risk-adjusted signal turns negative and reinvests capital into the highest-ranked available candidates.
- **Cash Management:** Tracks transaction leftovers and maintains a running log of total account equity.

### `graph.py`  
- **Performance Analytics:** Parses the backtest results to generate a professional net worth curve.
- **Trend Analysis:** Overlays a 50-day moving average on the equity curve to distinguish between strategy alpha and short-term market noise.

---

## Outputs  

- `full_history/` → Individual raw ticker Parquet files.
- `updatedStocks/` → Processed files featuring GK volatility and MA signals.
- `masterData.parquet` → The consolidated "market-map" used for the simulation.
- `backtest_history.csv` → The chronological record of strategy net worth.
- `networth_curve.png` → Visual performance report.

---

## Tech Stack  

- **Python**
- **Pandas** – Multi-index data manipulation and rolling window calculations.
- **NumPy** – Vectorized log-space volatility math.
- **PyArrow** – High-speed Parquet storage integration.
- **Matplotlib** – Financial visualization and dark-mode charting.
- **tqdm** – Progress monitoring for long-duration backtests.

---

## Features  

- **Volatility Normalization:** Uses Garman-Klass logic to penalize "noisy" price movement.
- **MA Spread Tracking:** Uses the distance between the 50/200 MAs to quantify momentum velocity.
- **Liquidity Guardrails:** Built-in volume and price filters to prevent "penny stock" bias.
- **Point-in-Time Simulation:** Designed to prevent look-ahead bias by using pivoted daily data.
- **Inverse Volatility Weighting:** Positions are sized based on risk, not just stock price.
