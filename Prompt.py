import os
import yfinance as yf
import pandas as pd
import numpy as np
from scipy.stats import zscore
import datetime
from jinja2 import Template
import plotly.express as px

# ==============================
# SETTINGS
# ==============================

MIN_MARKET_CAP = 10_000_000_000

OUTPUT_FOLDER = "docs"
OUTPUT_HTML = os.path.join(OUTPUT_FOLDER, "MultiUniverse_Institutional_Dashboard.html")

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# ==============================
# 1️⃣ UNIVERSE BUILDING
# ==============================

# S&P 500 Constituents
sp500 = pd.read_csv(
    "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"
)
sp500_tickers = sp500["Symbol"].str.replace(".", "-", regex=False).tolist()

# Nasdaq 100 (Top 100 Nasdaq Listings)
nasdaq100 = pd.read_csv(
    "https://raw.githubusercontent.com/datasets/nasdaq-listings/master/data/nasdaq-listed-symbols.csv"
)
nasdaq_tickers = nasdaq100["Symbol"].tolist()[:100]

# Major ETFs
etf_tickers = [
    "SPY","QQQ","VTI","IWM","DIA",
    "XLF","XLK","XLE","XLV","XLI",
    "TLT","GLD","VNQ","ARKK"
]

tickers = list(set(sp500_tickers + nasdaq_tickers + etf_tickers))

print(f"Total Universe Size: {len(tickers)}")

# ==============================
# 2️⃣ DATA DOWNLOAD
# ==============================

benchmark = yf.Ticker("SPY").history(period="1y")["Close"].pct_change()

data = []

for ticker in tickers:
    try:
        stock = yf.Ticker(ticker)
        info = stock.info

        if info.get("marketCap", 0) < MIN_MARKET_CAP:
            continue

        hist = stock.history(period="1y")
        if hist.empty:
            continue

        returns = hist["Close"].pct_change().dropna()

        if len(returns) < 200:
            continue

        ret6 = returns[-126:].mean() / returns[-126:].std()
        ret12 = returns.mean() / returns.std()
        rel_strength = returns.mean() - benchmark.mean()

        ma50 = hist["Close"].rolling(50).mean().iloc[-1]
        ma200 = hist["Close"].rolling(200).mean().iloc[-1]

        data.append({
            "Ticker": ticker,
            "Sector": info.get("sector","ETF"),
            "ForwardPE": info.get("forwardPE"),
            "PEG": info.get("pegRatio"),
            "ROE": info.get("returnOnEquity"),
            "DebtEquity": info.get("debtToEquity"),
            "RevenueGrowth": info.get("revenueGrowth"),
            "EarningsGrowth": info.get("earningsGrowth"),
            "Momentum6M": ret6,
            "Momentum12M": ret12,
            "RelStrength": rel_strength,
            "Trend": 1 if ma50 > ma200 else 0,
            "Volatility": returns.std()
        })

    except:
        continue

df = pd.DataFrame(data).dropna()

if df.empty:
    print("No qualifying securities found.")
    exit()

# ==============================
# 3️⃣ SECTOR NEUTRAL Z-SCORE
# ==============================

factor_cols = [
    "ForwardPE","PEG","ROE","DebtEquity",
    "RevenueGrowth","EarningsGrowth",
    "Momentum6M","Momentum12M",
    "RelStrength","Volatility"
]

for col in factor_cols:
    df[col+"_z"] = df.groupby("Sector")[col].transform(
        lambda x: zscore(x, nan_policy="omit")
    )

# ==============================
# 4️⃣ FACTOR MODEL
# ==============================

df["Value"] = -df["ForwardPE_z"] - df["PEG_z"]
df["Quality"] = df["ROE_z"] - df["DebtEquity_z"] + df["RevenueGrowth_z"]
df["Momentum"] = df["Momentum6M_z"] + df["Momentum12M_z"] + df["RelStrength_z"] + df["Trend"]
df["LowRisk"] = -df["Volatility_z"]

df["TotalScore"] = (
    df["Value"] * 0.30 +
    df["Quality"] * 0.25 +
    df["Momentum"] * 0.20 +
    df["LowRisk"] * 0.10 +
    df["EarningsGrowth_z"] * 0.15
)

# ==============================
# 5️⃣ CYCLIC TRAP FILTER
# ==============================

df["CyclicRisk"] = np.where(
    (df["ForwardPE"] > 25) &
    (df["EarningsGrowth"] < 0),
    "⚠ Risk",
    "OK"
)

df = df[df["CyclicRisk"] == "OK"]
df = df.sort_values("TotalScore", ascending=False)

# ==============================
# 6️⃣ VISUALIZATION
# ==============================

fig = px.scatter(
    df.head(150),
    x="Value",
    y="Quality",
    size="Momentum",
    hover_name="Ticker",
    title="Multi-Universe Institutional Factor Map"
)

plot_html = fig.to_html(full_html=False)

# ==============================
# 7️⃣ DASHBOARD GENERATION
# ==============================

template = Template("""
<html>
<head>
<title>Multi-Universe Institutional Dashboard</title>
<style>
body {font-family: Arial; background:#0f172a; color:white;}
h1 {color:#38bdf8;}
table {border-collapse: collapse; width:100%;}
th, td {padding:8px; border:1px solid #334155;}
th {background:#1e293b;}
tr:nth-child(even){background:#1e293b;}
</style>
</head>
<body>

<h1>Institutional Multi-Universe VQM Model</h1>
<p>Date: {{date}}</p>

<h2>Top 30 Ranked Securities</h2>
{{table}}

<h2>Factor Map</h2>
{{plot}}

</body>
</html>
""")

html_out = template.render(
    date=datetime.datetime.now(),
    table=df.head(30).to_html(index=False),
    plot=plot_html
)

with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
    f.write(html_out)

print("Dashboard Created:", OUTPUT_HTML)
