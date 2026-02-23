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
OUTPUT_HTML = os.path.join(OUTPUT_FOLDER, "MultiUniverse_Institutional_Dashboard.html")

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

LOOKBACK = "1y"
MIN_LIQUIDITY = 20_000_000  # Avg Dollar Volume Filter

# ==========================================
# 1️⃣ UNIVERSE BUILDING (ROBUST)
# ==========================================

sp500 = pd.read_csv(
    "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"
)
sp500_tickers = sp500["Symbol"].str.replace(".", "-", regex=False).tolist()

nasdaq100 = pd.read_csv(
    "https://raw.githubusercontent.com/datasets/nasdaq-listings/master/data/nasdaq-listed-symbols.csv"
)
nasdaq_tickers = nasdaq100["Symbol"].tolist()[:100]

major_etfs = [
    "SPY","QQQ","VTI","IWM","DIA",
    "XLF","XLK","XLE","XLV","XLI",
    "TLT","GLD","VNQ","ARKK"
]

leveraged_etfs = [
    "TQQQ","SQQQ","SOXL","SOXS",
    "SPXL","SPXS","UPRO","SDOW",
    "LABU","LABD","FNGU","FNGD"
]

crypto_etfs = [
    "IBIT","FBTC","ARKB","BITO",
    "ETHA","ETHE"
]

crypto_spot = [
    "BTC-USD","ETH-USD","SOL-USD","XRP-USD"
]

mega_caps = [
    "AAPL","MSFT","NVDA","AMZN","GOOGL",
    "META","TSLA","AVGO","AMD","JPM"
]

tickers = list(set(
    sp500_tickers +
    nasdaq_tickers +
    major_etfs +
    leveraged_etfs +
    crypto_etfs +
    crypto_spot +
    mega_caps
))

print("Total Universe Size:", len(tickers))

# ==========================================
# 2️⃣ BULK DATA DOWNLOAD (STABLE)
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
    print("Download failed.")
    exit()

data = []

# ==========================================
# 3️⃣ FACTOR CALCULATION (PRICE-BASED)
# ==========================================

for ticker in tickers:
    try:
        df = prices[ticker].dropna()

        if df.empty or len(df) < 200:
            continue

        close = df["Close"]
        volume = df["Volume"]

        returns = close.pct_change().dropna()

        if len(returns) < 200:
            continue

        # Liquidity filter
        avg_dollar_vol = (close * volume).mean()
        if avg_dollar_vol < MIN_LIQUIDITY:
            continue

        # Momentum
        mom_6m = close.pct_change(126).iloc[-1]
        mom_12m = close.pct_change(252).iloc[-1]

        # Volatility
        vol = returns.std()

        # Trend
        ma50 = close.rolling(50).mean().iloc[-1]
        ma200 = close.rolling(200).mean().iloc[-1]
        trend = 1 if ma50 > ma200 else 0

        # Risk-adjusted return
        sharpe = returns.mean() / returns.std()

        data.append({
            "Ticker": ticker,
            "Momentum6M": mom_6m,
            "Momentum12M": mom_12m,
            "Volatility": vol,
            "Trend": trend,
            "Sharpe": sharpe,
            "Liquidity": avg_dollar_vol
        })

    except:
        continue

df = pd.DataFrame(data)

if df.empty:
    print("No qualifying securities after filtering.")
    exit()

# ==========================================
# 4️⃣ FACTOR ENGINE (INSTITUTIONAL STYLE)
# ==========================================

for col in ["Momentum6M","Momentum12M","Volatility","Sharpe"]:
    df[col+"_z"] = (df[col] - df[col].mean()) / df[col].std()

df["MomentumScore"] = df["Momentum6M_z"] + df["Momentum12M_z"]
df["LowRisk"] = -df["Volatility_z"]
df["Quality"] = df["Sharpe_z"]

df["TotalScore"] = (
    df["MomentumScore"] * 0.50 +
    df["Quality"] * 0.30 +
    df["LowRisk"] * 0.20 +
    df["Trend"] * 0.10
)

df = df.sort_values("TotalScore", ascending=False)

# ==========================================
# 5️⃣ VISUALIZATION
# ==========================================

fig = px.scatter(
    df.head(150),
    x="MomentumScore",
    y="Quality",
    size="Liquidity",
    hover_name="Ticker",
    title="Institutional Multi-Asset Momentum Map"
)

plot_html = fig.to_html(full_html=False)

# ==========================================
# 6️⃣ DASHBOARD OUTPUT
# ==========================================

html = f"""
<html>
<head>
<title>Institutional Multi-Universe Dashboard</title>
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

<h1>Institutional Multi-Asset Quant Model</h1>
<p>Date: {datetime.datetime.now()}</p>

<h2>Top 30 Ranked Assets</h2>
{df.head(30).to_html(index=False)}

<h2>Factor Map</h2>
{plot_html}

</body>
</html>
"""

with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
    f.write(html)

print("Dashboard Created:", OUTPUT_HTML)
