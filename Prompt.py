import os
import shutil
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
LOOKBACK = "1y"
MIN_LIQUIDITY = 20_000_000

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# ==========================================
# CLEAN OLD FILE + CACHE
# ==========================================
if os.path.exists(OUTPUT_HTML):
    os.remove(OUTPUT_HTML)

cache_dir = os.path.expanduser("~/.cache/yfinance")
if os.path.exists(cache_dir):
    shutil.rmtree(cache_dir)

# ==========================================
# 1️⃣ UNIVERSE
# ==========================================
sp500 = pd.read_csv("https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv")
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
# 2️⃣ DOWNLOAD DATA SAFELY
# ==========================================
prices = yf.download(
    tickers,
    period=LOOKBACK,
    auto_adjust=True,
    threads=True,
    progress=False
)

if prices.empty:
    print("Download failed")
    exit()

# Determine valid tickers from actual download (handles delisted)
if isinstance(prices.columns, pd.MultiIndex):
    valid_tickers = prices.columns.get_level_values(0).unique().tolist()
else:
    valid_tickers = tickers

# Safe benchmark
try:
    benchmark = yf.download("SPY", period=LOOKBACK, auto_adjust=True, progress=False)["Close"]
    benchmark_ret = benchmark.pct_change().dropna()
except:
    benchmark_ret = pd.Series(dtype=float)

data = []

# ==========================================
# 3️⃣ OPPORTUNITY ENGINE (ROBUST)
# ==========================================
for ticker in valid_tickers:
    try:
        # Handle multi-index
        if isinstance(prices.columns, pd.MultiIndex):
            if ticker not in prices.columns.get_level_values(0):
                continue
            df_t = prices[ticker].dropna()
        else:
            df_t = prices.dropna()

        if df_t.empty or len(df_t) < 200:
            continue

        close = df_t["Close"]
        volume = df_t["Volume"]
        returns = close.pct_change().dropna()

        if len(returns) < 150:
            continue

        avg_dollar_vol = (close * volume).mean()
        if avg_dollar_vol < MIN_LIQUIDITY:
            continue

        # Trend & breakout
        ma50 = close.rolling(50).mean().iloc[-1]
        ma200 = close.rolling(200).mean().iloc[-1]
        trend = ma50 > ma200
        high_52w = close.rolling(252).max().iloc[-1]
        breakout = close.iloc[-1] > high_52w * 0.95
        pullback = trend and close.iloc[-1] < ma50

        # Momentum
        mom6 = close.pct_change(126).iloc[-1]
        mom12 = close.pct_change(252).iloc[-1]

        # Relative Strength
        rel = 0
        if not benchmark_ret.empty:
            rel = returns.mean() - benchmark_ret.mean()

        # Risk
        vol = returns.std()

        # Score
        score = (
            mom6 * 0.4 +
            mom12 * 0.3 +
            rel * 0.2 -
            vol * 0.1
        )

        # Signal
        if trend and breakout and rel > 0:
            signal = "🚀 STRONG BUY"
        elif trend and pullback:
            signal = "🟢 BUY PULLBACK"
        elif not trend and rel < 0:
            signal = "🔴 AVOID"
        else:
            signal = "🟡 WATCH"

        data.append({
            "Ticker": ticker,
            "Signal": signal,
            "Momentum6M": float(mom6),
            "Momentum12M": float(mom12),
            "RelStrength": float(rel),
            "Volatility": float(vol),
            "Liquidity": int(avg_dollar_vol),
            "Score": float(score)
        })

    except:
        continue

df = pd.DataFrame(data)

if df.empty:
    print("No qualifying stocks found")
    exit()

# Force Score numeric & drop invalid
df["Score"] = pd.to_numeric(df["Score"], errors="coerce")
df = df.dropna(subset=["Score"])
df = df.sort_values("Score", ascending=False).reset_index(drop=True)

# ==========================================
# 4️⃣ ADD EXTERNAL LINKS
# ==========================================
def make_links(t):
    finviz = f"https://finviz.com/quote.ashx?t={t}"
    forecast = f"https://stockanalysis.com/stocks/{t.lower()}/forecast/"
    return f'<a href="{finviz}" target="_blank">{t}</a>', f'<a href="{forecast}" target="_blank">Forecast</a>'

df["Finviz"], df["Forecast"] = zip(*df["Ticker"].apply(make_links))

# ==========================================
# 5️⃣ SUMMARY
# ==========================================
strong_buys = df[df["Signal"].str.contains("STRONG")]
pullbacks = df[df["Signal"].str.contains("PULLBACK")]
avoids = df[df["Signal"].str.contains("AVOID")]

# ==========================================
# 6️⃣ VISUAL MAP
# ==========================================
fig = px.scatter(
    df.head(150),
    x="Momentum6M",
    y="RelStrength",
    color="Signal",
    hover_name="Ticker",
    size="Liquidity",
    title="Market Opportunity Map"
)
plot_html = fig.to_html(full_html=False)

# ==========================================
# 7️⃣ DASHBOARD OUTPUT
# ==========================================
html = f"""
<html>
<head>
<title>Stock Market Opportunity Dashboard</title>
<style>
body {{font-family:Arial;background:#0f172a;color:white;}}
h1 {{color:#38bdf8;}}
h2 {{color:#facc15;}}
table {{border-collapse:collapse;width:100%;}}
th,td {{padding:8px;border:1px solid #334155;text-align:center;}}
th {{background:#1e293b;}}
tr:nth-child(even){{background:#1e293b;}}
a {{color:#38bdf8;text-decoration:none;}}
</style>
</head>
<body>

<h1>📊 Stock Market Opportunity Dashboard</h1>
<p>Date: {datetime.datetime.now()}</p>

<h2>🔥 Strong Buy Opportunities ({len(strong_buys)})</h2>
{strong_buys.head(20)[["Finviz","Signal","Score","Momentum6M","RelStrength","Forecast"]].to_html(index=False, escape=False)}

<h2>📉 Buy the Pullback ({len(pullbacks)})</h2>
{pullbacks.head(20)[["Finviz","Signal","Score","Momentum6M","RelStrength","Forecast"]].to_html(index=False, escape=False)}

<h2>⚠ Avoid List ({len(avoids)})</h2>
{avoids.head(20)[["Finviz","Signal","Score","Momentum6M","RelStrength","Forecast"]].to_html(index=False, escape=False)}

<h2>📈 Market Opportunity Map</h2>
{plot_html}

<h2>🏆 Top 30 Ranked Overall</h2>
{df.head(30)[["Finviz","Signal","Score","Momentum6M","RelStrength","Forecast"]].to_html(index=False, escape=False)}

</body>
</html>
"""

with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
    f.write(html)

print("✅ Opportunity Dashboard Created:", OUTPUT_HTML)
