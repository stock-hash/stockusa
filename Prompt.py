import os
import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import plotly.graph_objects as go
from scipy.optimize import minimize

# ==========================================
# SETTINGS
# ==========================================

OUTPUT_FOLDER = "docs"
OUTPUT_HTML = os.path.join(OUTPUT_FOLDER, "MultiUniverse_Institutional_Dashboard.html")
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

LOOKBACK_YEARS = 10
VOL_TARGET = 0.15
TOP_N = 20
BOTTOM_N = 20

# ==========================================
# UNIVERSE
# ==========================================

major_assets = [
    "SPY","QQQ","IWM","DIA","VTI",
    "XLF","XLK","XLE","XLV","XLI",
    "TQQQ","SOXL","SPXL","UPRO",
    "IBIT","ETHA","BTC-USD","ETH-USD",
    "AAPL","MSFT","NVDA","AMZN","META",
    "GOOGL","TSLA","AVGO","AMD","JPM"
]

tickers = list(set(major_assets))
print("Universe Size:", len(tickers))

# ==========================================
# DOWNLOAD 10 YEARS DATA
# ==========================================

prices = yf.download(
    tickers,
    period=f"{LOOKBACK_YEARS}y",
    auto_adjust=True,
    threads=True,
    progress=False
)["Close"]

prices = prices.dropna(axis=1, how="all").dropna()

returns = prices.pct_change().dropna()

# ==========================================
# MACRO REGIME FILTER (SPY Trend)
# ==========================================

spy = prices["SPY"]
spy_ma200 = spy.rolling(200).mean()
macro_regime = np.where(spy > spy_ma200, 1, 0)

# ==========================================
# FACTOR SCORES
# ==========================================

momentum_12m = prices.pct_change(252)
volatility = returns.rolling(63).std()
sharpe = returns.rolling(252).mean() / returns.rolling(252).std()

latest_scores = pd.DataFrame({
    "Momentum": momentum_12m.iloc[-1],
    "Volatility": volatility.iloc[-1],
    "Sharpe": sharpe.iloc[-1]
}).dropna()

latest_scores["Momentum_z"] = (latest_scores["Momentum"] - latest_scores["Momentum"].mean()) / latest_scores["Momentum"].std()
latest_scores["Sharpe_z"] = (latest_scores["Sharpe"] - latest_scores["Sharpe"].mean()) / latest_scores["Sharpe"].std()
latest_scores["LowRisk_z"] = -(latest_scores["Volatility"] - latest_scores["Volatility"].mean()) / latest_scores["Volatility"].std()

latest_scores["TotalScore"] = (
    latest_scores["Momentum_z"] * 0.5 +
    latest_scores["Sharpe_z"] * 0.3 +
    latest_scores["LowRisk_z"] * 0.2
)

latest_scores = latest_scores.sort_values("TotalScore", ascending=False)

# ==========================================
# LONG / SHORT SELECTION
# ==========================================

long_assets = latest_scores.head(TOP_N).index.tolist()
short_assets = latest_scores.tail(BOTTOM_N).index.tolist()

# ==========================================
# PORTFOLIO OPTIMIZER (Sharpe Maximization)
# ==========================================

selected = long_assets
ret_sel = returns[selected]

def neg_sharpe(weights):
    port_ret = np.dot(ret_sel.mean(), weights)
    port_vol = np.sqrt(np.dot(weights.T, np.dot(ret_sel.cov(), weights)))
    return -(port_ret / port_vol)

constraints = ({'type': 'eq', 'fun': lambda w: np.sum(w) - 1})
bounds = tuple((0, 1) for _ in selected)
init_guess = np.array(len(selected) * [1. / len(selected)])

opt = minimize(neg_sharpe, init_guess, bounds=bounds, constraints=constraints)
weights = opt.x

weights_df = pd.DataFrame({
    "Ticker": selected,
    "Weight": weights
}).sort_values("Weight", ascending=False)

# ==========================================
# LONG/SHORT RETURNS
# ==========================================

long_returns = returns[long_assets].mean(axis=1)
short_returns = returns[short_assets].mean(axis=1)

strategy_returns = long_returns - short_returns

# Apply Macro Regime Filter
strategy_returns = strategy_returns * macro_regime[-len(strategy_returns):]

# ==========================================
# VOLATILITY TARGETING
# ==========================================

rolling_vol = strategy_returns.rolling(21).std()
scaling = VOL_TARGET / (rolling_vol * np.sqrt(252))
strategy_scaled = strategy_returns * scaling.shift(1)
strategy_scaled = strategy_scaled.dropna()

# ==========================================
# 10-YEAR BACKTEST METRICS
# ==========================================

equity_curve = (1 + strategy_scaled).cumprod()

cagr = equity_curve.iloc[-1] ** (252/len(equity_curve)) - 1
sharpe_ratio = strategy_scaled.mean() / strategy_scaled.std() * np.sqrt(252)
max_dd = (equity_curve / equity_curve.cummax() - 1).min()

# ==========================================
# VISUALIZATION
# ==========================================

fig = go.Figure()
fig.add_trace(go.Scatter(
    x=equity_curve.index,
    y=equity_curve,
    mode="lines",
    name="Strategy Equity Curve"
))

fig.update_layout(
    template="plotly_dark",
    title="10-Year Institutional Long/Short Strategy",
    xaxis_title="Date",
    yaxis_title="Equity"
)

plot_html = fig.to_html(full_html=False)

# ==========================================
# DASHBOARD OUTPUT
# ==========================================

html = f"""
<html>
<head>
<title>Institutional Quant System</title>
<style>
body {{font-family: Arial; background:#0f172a; color:white;}}
h1 {{color:#38bdf8;}}
table {{border-collapse: collapse; width:100%;}}
th, td {{padding:8px; border:1px solid #334155;}}
th {{background:#1e293b;}}
tr:nth-child(even){{background:#1e293b;}}
</style>
</head>
<body>

<h1>Institutional Multi-Asset Quant System</h1>
<p>Date: {datetime.datetime.now()}</p>

<h2>Performance Metrics</h2>
<ul>
<li>CAGR: {round(cagr*100,2)}%</li>
<li>Sharpe Ratio: {round(sharpe_ratio,2)}</li>
<li>Max Drawdown: {round(max_dd*100,2)}%</li>
<li>Macro Regime (1=Risk On): {macro_regime[-1]}</li>
</ul>

<h2>Optimized Long Portfolio Weights</h2>
{weights_df.to_html(index=False)}

<h2>Equity Curve</h2>
{plot_html}

</body>
</html>
"""

with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
    f.write(html)

print("Institutional Dashboard Created:", OUTPUT_HTML)
