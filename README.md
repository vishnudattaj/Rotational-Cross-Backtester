# Volatility-Adjusted Momentum Engine

A quantitative framework for evaluating risk-adjusted momentum strategies with dynamic position sizing, this project implements a dual-filter approach: identifying price breakouts via moving average spreads and normalizing those signals using Garman-Klass volatility estimators to prioritize stable upward trends. In addition, this project implements a volatility-based rotation logic, where capital allocation is inversely proportional to an asset's Garman-Klass volatility, ensuring a balanced risk contribution across the portfolio. Furthermore, it utilizes macro-momentum regime filters to isolate structural secular winners and dynamically scales equity allocations to eliminate cash drag as the portfolio compounds. In a span of 12 years, my framework grew a portfolio initially worth $70k into $285k (a 307.1% gain), delivering results highly comparable to the S&P 500 which expanded that same capital into $313k (a 347.1% gain).

---

## How It Works  

The project is composed of six main scripts:  

### `dataSplitter.py`  
- **Data Partitioning:** Processes the master `all_stock_data.parquet` and splits it into individual ticker files.
- **Optimization:** Converts timestamps into YYYYMMDD integers to accelerate chronological sorting and reduce memory overhead during iterations.

### `dataCleaner.py`  
- **Garman-Klass Volatility:** Calculates high-efficiency volatility estimates using Open, High, Low, and Close prices.
- **Signal Generation:** Computes the **Signal Strength** as the percentage difference between the 50-day (short-term) and 200-day (long-term) moving averages.
- **Macro Regime Filters:** Calculates `trend200` (199-day shift) to measure macro-momentum and weed out short-term noise, ensuring the asset is in a proven structural uptrend.
- **Data Pruning:** Standardizes the timeline to start in 2012 and removes tickers with insufficient price history for the moving average windows.

### `stockRanker.py`  
- **Risk-Adjusted Ranking:** Generates the initial portfolio by dividing the **Signal Strength** by the **Garman-Klass** value.
- **Liquidity Filters:** Enforces a minimum $1,000,000 daily dollar volume and a $5 price floor to ensure tradeability.

### `master-signal.py`  
- **Matrix Construction:** Re-aggregates cleaned ticker data into a single, high-dimensional Parquet file.
- **Pivot Optimization:** Organizes data into a Date-Ticker multi-index, allowing the backtester to perform "point-in-time" lookups across the entire market simultaneously.

### `backTest.py`  
- **Execution Engine:** Simulates a daily rebalanced portfolio with a 100-slot maximum capacity.
- **Dynamic Equity Scaling:** Transitions from fixed-dollar sizing to dynamic scaling based on total net worth, allowing position sizes to organically grow with the portfolio and entirely eliminate idle "cash drag."
- **Precision Cash Accounting:** Utilizes exact modulo arithmetic (`scaled_amount % prices`) to handle fractional remainders flawlessly, ensuring perfectly balanced cash reserves during transactions.
- **Dynamic Rebalancing:** Automatically sells positions when the risk-adjusted signal turns negative and reinvests capital into the highest-ranked available candidates.

### `graph.py`  
- **Performance Analytics:** Parses the backtest results to generate a professional net worth curve.
- **Trend Analysis:** Overlays a 50-day moving average on the equity curve to distinguish between strategy alpha and short-term market noise.

---

## Outputs  

- `full_history/` → Individual raw ticker Parquet files.
- `updatedStocks/` → Processed files featuring GK volatility and MA signals.
- `masterData.parquet` → The consolidated "market-map" used for the simulation.
- `backtest_history.csv` → The chronological record of strategy net worth.
- `strategy_vs_benchmarks_normalized.png` → Visual performance report in comparison to S&P 500.

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
- **Macro-Momentum Regime Filtering:** Uses 49/199-day absolute price shifts instead of sensitive short-term slopes to lock into long-term secular trends.
- **Liquidity Guardrails:** Built-in volume and price filters to prevent "penny stock" bias.
- **Point-in-Time Simulation:** Designed to prevent look-ahead bias by using pivoted daily data.
- **Inverse Volatility Weighting:** Positions are sized based on risk, not just stock price.
- **Zero Cash Drag Architecture:** Purchasing power automatically scales based on current total net worth, compounding returns efficiently without leaving dead capital.
- **Flawless Transaction Accounting:** Employs modulo-based ledger logic to perfectly calculate actual cash spent versus fractional remainders.
