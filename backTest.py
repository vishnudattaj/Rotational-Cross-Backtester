import pandas as pd
from tqdm import tqdm

def stockChooser(date, investments, master_df, index=0):
    if date not in master_df.index:
        return None

    day_signals = master_df.loc[date]
    available_signals = day_signals.drop(labels=investments, errors='ignore').dropna()
    try:
        top_ticker = available_signals[available_signals].sort_values(ascending=False).index[index]
    except IndexError:
        top_ticker = None

    return top_ticker

def buyStocks(investments, master, holdings, date, amount):
    prices = master.loc[date, investments]
    share_counts = amount // prices
    holdings.update(share_counts.to_dict())

    if isinstance(share_counts, pd.Series):
        zero_share_list = share_counts[share_counts == 0].index.tolist()
    else:
        zero_share_list = [investments] if share_counts == 0 else []

    return (amount % prices).sum(), zero_share_list


def sellStocks(investments, open_prices, holdings, date):
    if not investments:
        return 0

    prices = open_prices.loc[date, investments]
    if isinstance(prices, float):
        prices = pd.Series({investments: prices})

    total_cash_gained = 0

    for ticker in investments:
        if ticker in holdings:
            qty = holdings[ticker]
            price = prices[ticker]

            total_cash_gained += qty * price
            del holdings[ticker]

    return total_cash_gained

def sellSignals(investments, master, date):
    signals = master.loc[date, investments]
    negative_signals = signals[signals < 0]

    return negative_signals.index.tolist()


master_df = pd.read_parquet("masterData.parquet")
topStocks = pd.read_parquet("baseline_stats.parquet").head(70)

current_cash = len(topStocks["Ticker"].tolist()) * 1000
tickers = master_df.columns.get_level_values(1).unique().tolist()
dates = master_df.index
holdings = {}
setBuy = False
setSell = False
networth = []

open_prices = master_df['Open'].ffill()
close_prices = master_df['Close'].ffill()
signals = master_df['risk-adj-signal'].fillna(0)
volume = master_df['daily-volume'].ffill()

for i in tqdm(range(len(dates)), desc="Backtesting Strategy"):
    index = dates[i]

    if not list(holdings.keys()):
        investments = topStocks["Ticker"].tolist()
        leftover_change, zeroStock = buyStocks(investments, open_prices, holdings, index, 1000)
        current_cash = current_cash - (len(investments) * 1000) + leftover_change
        networth.append(len(investments) * 1000)

        day_p_start = open_prices.loc[index]
        day_volume_start = volume.loc[index]
        validTickers = day_p_start[(day_p_start > 5) & (day_volume_start > 2000000)].index.tolist()
        updateSignals = signals.loc[index, validTickers]

        while current_cash >= 1000:
            investment = stockChooser(index, list(holdings.keys()), updateSignals)

            if investment:
                leftover_change, zeroStock = buyStocks([investment], open_prices, holdings, index, 1000)
                current_cash = current_cash + leftover_change - 1000
            else:
                break

            if zeroStock:
                break

    if i > 0:
        if setBuy:
            leftover_change, zeroStock = buyStocks(investmentList, open_prices, holdings, index, 1000)
            current_cash = current_cash + leftover_change - (1000 * len(investmentList))

            setBuy = False

        if setSell:
            max_portfolio_size = 100
            current_count = len(holdings)
            slots_available = max_portfolio_size - current_count
            current_cash += sellStocks(sellList, open_prices, holdings, index)

            possibleBuys = int(current_cash // 1000)
            if possibleBuys > 0:
                day_p = close_prices.loc[index]
                day_s = signals.loc[index]
                day_volume = volume.loc[index]

                potential = day_s[(day_p > 5) & (day_s > 0) & (day_volume > 2000000)].drop(labels=holdings.keys(), errors='ignore')
                num_to_pick = min(possibleBuys, slots_available)
                top_picks = potential.sort_values(ascending=False).head(num_to_pick)

                if not top_picks.empty:
                    investmentList = top_picks.index.tolist()
                    setBuy = True

            setSell = False

        sellList = sellSignals(list(holdings.keys()), signals, index)
        if sellList:
            setSell = True

        current_tickers = list(holdings.keys())
        if current_tickers:
            market_value = sum(holdings[t] * close_prices.at[index, t] for t in holdings)
        else:
            market_value = 0

        networth.append(current_cash + market_value)
    holdings = {ticker: qty for ticker, qty in holdings.items() if qty > 0}

results_df = pd.DataFrame({
    'Date': dates,
    'Net_Worth': networth
})

results_df.to_csv("backtest_history.csv", index=False)

if holdings:
    holdings_df = pd.DataFrame.from_dict(holdings, orient='index', columns=['Shares'])
    holdings_df.index.name = 'Ticker'
    holdings_df.to_csv("final_holdings_inspect.csv")
else:
    print("No holdings to save.")