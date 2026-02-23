import os
import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import plotly.express as px

# ==========================================
# SETTINGS
# ==========================================

OUTPUT_FOLDER = "docs"
OUTPUT_HTML = os.path.join(OUTPUT_FOLDER, "StockMarket_Opportunity_Dashboard.html")

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

LOOKBACK = "1y"
MIN_LIQUIDITY = 20_000_000

# ==========================================
# 1️⃣ UNIVERSE
# ==========================================

sp500 = pd.read_csv(
    "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"
)

sp500_tickers = sp500["Symbol"].str.replace(".", "-", regex=False).tolist()

extra_assets = [
    "QQQ","IWM","DIA","XLF","XLK","XLE","XLV","XLI",
    "TQQQ","SOXL","SPXL",
    "IBIT","ETHA","BTC-USD","ETH-USD",
    "AAPL","MSFT","NVDA","AMZN","META",
    "GOOGL","TSLA","AVGO","AMD","JPM"
]

tickers = list(set(sp500_tickers + extra_assets))

print("Universe Size:", len(tickers))

# ==========================================
# 2️⃣ DOWNLOAD DATA
# ==========================================

prices = yf.download(
    tickers,
    period=LOOKBACK,
    group_by="ticker",
    auto_adjust=True,
    threads=True,
    progress=False
)

if prices.empty:
    print("Download failed")
    exit()

benchmark = yf.download("SPY", period=LOOKBACK, auto_adjust=True, progress=False)["Close"]
benchmark_ret = benchmark.pct_change()

data = []

# ==========================================
# 3️⃣ OPPORTUNITY ENGINE
# ==========================================

for ticker in tickers:
    try:
        df = prices[ticker].dropna()

        if df.empty or len(df) < 200:
            continue

        close = df["Close"]
        volume = df["Volume"]
        returns = close.pct_change().dropna()

        avg_dollar_vol = (close * volume).mean()
        if avg_dollar_vol < MIN_LIQUIDITY:
            continue

        # Trend
        ma50 = close.rolling(50).mean().iloc[-1]
        ma200 = close.rolling(200).mean().iloc[-1]
        trend = ma50 > ma200

        # Breakout (near 52-week high)
        high_52w = close.max()
        breakout = close.iloc[-1] > high_52w * 0.95

        # Pullback in uptrend
        pullback = trend and close.iloc[-1] < ma50

        # Momentum
        mom6 = close.pct_change(126).iloc[-1]
        mom12 = close.pct_change(252).iloc[-1]

        # Relative Strength vs SPY
        rel = returns.mean() - benchmark_ret.mean()

        # Risk
        vol = returns.std()

        score = (
            mom6 * 0.4 +
            mom12 * 0.3 +
            rel * 0.2 -
            vol * 0.1
        )

        # Signal Classification
        if trend and breakout and rel > 0:
            signal = "STRONG BUY"
        elif trend and pullback:
            signal = "BUY PULLBACK"
        elif not trend and rel < 0:
            signal = "AVOID"
        else:
            signal = "WATCH"

        data.append({
            "Ticker": ticker,
            "Signal": signal,
            "Trend": trend,
            "Breakout": breakout,
            "Pullback": pullback,
            "Momentum6M": round(mom6, 2),
            "Momentum12M": round(mom12, 2),
            "RelStrength": round(rel, 4),
            "Volatility": round(vol, 4),
            "Score": round(score, 4)
        })

    except:
        continue

df = pd.DataFrame(data)

if df.empty:
    print("No qualifying stocks found")
    exit()

df = df.sort_values("Score", ascending=False)

# ==========================================
# 4️⃣ OPPORTUNITY SUMMARY
# ==========================================

strong_buys = df[df["Signal"] == "STRONG BUY"]
pullbacks = df[df["Signal"] == "BUY PULLBACK"]
avoids = df[df["Signal"] == "AVOID"]

# ==========================================
# 5️⃣ VISUAL MAP
# ==========================================

fig = px.scatter(
    df.head(150),
    x="Momentum6M",
    y="RelStrength",
    color="Signal",
    hover_name="Ticker",
    size="Volatility",
    title="Market Opportunity Map"
)

plot_html = fig.to_html(full_html=False)

# ==========================================
# 6️⃣ DASHBOARD OUTPUT
# ==========================================

html = f"""
<html>
<head>
<title>Stock Market Opportunity Dashboard</title>
<style>
body {{font-family: Arial; background:#0f172a; color:white;}}
h1 {{color:#38bdf8;}}
h2 {{color:#facc15;}}
table {{border-collapse: collapse; width:100%;}}
th, td {{padding:8px; border:1px solid #334155;}}
th {{background:#1e293b;}}
tr:nth-child(even){{background:#1e293b;}}
.buy {{color:#22c55e; font-weight:bold;}}
.avoid {{color:#ef4444; font-weight:bold;}}
.watch {{color:#facc15; font-weight:bold;}}
</style>
</head>
<body>

<h1>📊 Stock Market Opportunity Dashboard</h1>
<p>Date: {datetime.datetime.now()}</p>

<h2>🔥 Strong Buy Opportunities ({len(strong_buys)})</h2>
{strong_buys.head(20).to_html(index=False)}

<h2>📉 Buy the Pullback ({len(pullbacks)})</h2>
{pullbacks.head(20).to_html(index=False)}

<h2>⚠ Avoid List ({len(avoids)})</h2>
{avoids.head(20).to_html(index=False)}

<h2>📈 Market Opportunity Map</h2>
{plot_html}

<h2>📊 Top 30 Ranked Overall</h2>
{df.head(30).to_html(index=False)}

</body>
</html>
"""

with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
    f.write(html)

print("Opportunity Dashboard Created:", OUTPUT_HTML)
