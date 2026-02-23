import os
import shutil
import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import plotly.express as px
from time import sleep
import re

# ================= SETTINGS =================
OUTPUT_FOLDER = "docs"
OUTPUT_HTML = os.path.join(OUTPUT_FOLDER, "StockMarket_Opportunity_Dashboard.html")
LOOKBACK = "1y"
MIN_LIQUIDITY = 20_000_000
CHUNK_SIZE = 200  # chunk size for large downloads

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# ================ CLEAN OLD FILE + CACHE =================
if os.path.exists(OUTPUT_HTML):
    os.remove(OUTPUT_HTML)

cache_dir = os.path.expanduser("~/.cache/yfinance")
if os.path.exists(cache_dir):
    shutil.rmtree(cache_dir)

# ================= FULL MARKET UNIVERSE =================
nasdaq = pd.read_csv("https://raw.githubusercontent.com/datasets/nasdaq-listings/master/data/nasdaq-listed-symbols.csv")
nyse = pd.read_csv("https://raw.githubusercontent.com/datasets/nyse-other-listings/master/data/nyse-listed.csv")
amex = pd.read_csv("https://raw.githubusercontent.com/datasets/nyse-other-listings/master/data/other-listed.csv")

nasdaq_tickers = nasdaq["Symbol"].str.strip().tolist()
nyse_tickers = nyse["ACT Symbol"].str.strip().tolist()
amex_tickers = amex["ACT Symbol"].str.strip().tolist()

extra_assets = [
    "SPY","QQQ","IWM","DIA","VTI",
    "XLF","XLK","XLE","XLV","XLI",
    "TQQQ","SOXL","SPXL","UPRO",
    "IBIT","ETHA","BTC-USD","ETH-USD",
    "AAPL","MSFT","NVDA","AMZN","META",
    "GOOGL","TSLA","AVGO","AMD","JPM"
]

tickers = list(set(nasdaq_tickers + nyse_tickers + amex_tickers + extra_assets))
print("Full Market Universe Size:", len(tickers))

# ================= SANITIZE TICKERS =================
def sanitize_ticker(t):
    t = t.upper().strip()
    # Remove invalid Yahoo characters
    t = re.sub(r'[^A-Z0-9\-\.]', '', t)
    return t

tickers = [sanitize_ticker(t) for t in tickers if sanitize_ticker(t)]
print("Sanitized tickers count:", len(tickers))

# ================= CHUNKED DOWNLOAD =================
def chunked_download(ticker_list, period="1y", chunk_size=CHUNK_SIZE):
    combined = {}
    for i in range(0, len(ticker_list), chunk_size):
        batch = ticker_list[i:i+chunk_size]
        print(f"Downloading chunk {i}-{i+len(batch)}")
        try:
            part = yf.download(batch, period=period, auto_adjust=True, threads=True, progress=False)
            if isinstance(part, pd.DataFrame) and isinstance(part.columns, pd.MultiIndex):
                for t in batch:
                    if t in part.columns.get_level_values(0):
                        combined[t] = part[t].dropna()
            elif isinstance(part, pd.DataFrame):
                for t in batch:
                    if t in part.columns:
                        combined[t] = part.dropna()
        except Exception as e:
            print("Chunk error:", e)
        sleep(1)
    return combined

prices_data = chunked_download(tickers)

# ================= SAFE BENCHMARK =================
try:
    benchmark = yf.download("SPY", period=LOOKBACK, auto_adjust=True, progress=False)["Close"]
    benchmark_ret = benchmark.pct_change().dropna()
except:
    benchmark_ret = pd.Series(dtype=float)

# ================= OPPORTUNITY ENGINE =================
data = []
failed_tickers = []

for ticker in tickers:
    if ticker not in prices_data:
        failed_tickers.append(ticker)
        continue
    df_t = prices_data[ticker]
    if df_t.empty or len(df_t) < 150:
        failed_tickers.append(ticker)
        continue

    try:
        close = df_t["Close"]
        volume = df_t["Volume"]
        returns = close.pct_change().dropna()
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
        score = mom6*0.4 + mom12*0.3 + rel*0.2 - vol*0.1

        # Signal
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
            "Momentum6M": float(mom6),
            "Momentum12M": float(mom12),
            "RelStrength": float(rel),
            "Volatility": float(vol),
            "Liquidity": int(avg_dollar_vol),
            "Score": float(score)
        })
    except:
        failed_tickers.append(ticker)
        continue

df = pd.DataFrame(data)
if failed_tickers:
    print(f"⚠ Skipped/failed tickers: {len(failed_tickers)}")

if df.empty:
    print("No qualifying stocks found, dashboard will be empty.")
else:
    df["Score"] = pd.to_numeric(df["Score"], errors="coerce")
    df = df.dropna(subset=["Score"]).sort_values("Score", ascending=False).reset_index(drop=True)

    # ================= ADD LINKS =================
    def make_links(t):
        finviz = f"https://finviz.com/quote.ashx?t={t}"
        forecast = f"https://stockanalysis.com/stocks/{t.lower()}/forecast/"
        return f'<a href="{finviz}" target="_blank">{t}</a>', f'<a href="{forecast}" target="_blank">Forecast</a>'

    df["Finviz"], df["Forecast"] = zip(*df["Ticker"].apply(make_links))

    # ================= SUMMARY =================
    strong_buys = df[df["Signal"].str.contains("STRONG")]
    pullbacks = df[df["Signal"].str.contains("PULLBACK")]
    avoids = df[df["Signal"].str.contains("AVOID")]

    # ================= VISUAL MAP =================
    fig = px.scatter(
        df.head(150),
        x="Momentum6M",
        y="RelStrength",
        color="Signal",
        hover_data=["Momentum6M","Momentum12M","RelStrength","Volatility","Score"],
        hover_name="Ticker",
        size="Liquidity",
        title="Market Opportunity Map"
    )
    plot_html = fig.to_html(full_html=False)

    # ================= DASHBOARD =================
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
    .STRONG {{color:#22c55e;font-weight:bold;}}
    .PULLBACK {{color:#facc15;font-weight:bold;}}
    .AVOID {{color:#ef4444;font-weight:bold;}}
    .WATCH {{color:#fcd34d;font-weight:bold;}}
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
