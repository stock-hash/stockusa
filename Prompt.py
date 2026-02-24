import os
import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import plotly.express as px
import re
import time
import random

# ================= SETTINGS =================
OUTPUT_FOLDER = "docs"
OUTPUT_HTML = os.path.join(OUTPUT_FOLDER, "StockMarket_Opportunity_Dashboard.html")

LOOKBACK = "2y"
MIN_LIQUIDITY = 20_000_000
MIN_PRICE = 5

CHUNK_SIZE = 25              # smaller = safer
REQUEST_SLEEP = 1.5          # delay between batches
MAX_RETRIES = 4

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# ================= LOAD UNIVERSE =================
def load_universe():
    print("Loading US stock universe...")

    urls = [
        "https://raw.githubusercontent.com/datasets/nasdaq-listings/master/data/nasdaq-listed-symbols.csv",
        "https://raw.githubusercontent.com/datasets/nyse-other-listings/master/data/nyse-listed.csv",
        "https://raw.githubusercontent.com/datasets/nyse-other-listings/master/data/other-listed.csv",
    ]

    tickers = []

    for url in urls:
        try:
            df = pd.read_csv(url)
            for col in ["Symbol", "ACT Symbol"]:
                if col in df.columns:
                    tickers += df[col].dropna().tolist()
        except:
            pass

    return list(set(tickers))


# ================= SANITIZE =================
def sanitize_ticker(t):
    if not isinstance(t, str):
        return None

    t = t.strip().upper()

    if any(x in t for x in [".W", ".U", ".R", "^", "/", "="]):
        return None

    if not re.match(r"^[A-Z\-.]+$", t):
        return None

    t = t.replace(".", "-")

    if len(t) > 6:
        return None

    return t


raw = load_universe()
tickers = list(set(filter(None, [sanitize_ticker(t) for t in raw])))

print("Initial Universe:", len(tickers))


# ================= SAFE DOWNLOAD =================
def download_batch(batch):
    for attempt in range(MAX_RETRIES):
        try:
            data = yf.download(
                batch,
                period=LOOKBACK,
                auto_adjust=True,
                progress=False,
                threads=False,
                group_by="ticker"
            )

            if data is None or len(data) == 0:
                return {}

            result = {}

            if isinstance(data.columns, pd.MultiIndex):
                for t in batch:
                    if t in data.columns.levels[0]:
                        df = data[t].dropna()
                        if not df.empty:
                            result[t] = df
            else:
                df = data.dropna()
                if not df.empty:
                    result[batch[0]] = df

            return result

        except Exception as e:
            sleep_time = REQUEST_SLEEP * (2 ** attempt) + random.uniform(0, 1)
            print(f"Retry {attempt+1} — sleeping {round(sleep_time,2)}s")
            time.sleep(sleep_time)

    return {}


def sequential_download(tickers):
    all_data = {}
    batches = [tickers[i:i+CHUNK_SIZE] for i in range(0, len(tickers), CHUNK_SIZE)]

    for i, batch in enumerate(batches):
        batch_data = download_batch(batch)
        all_data.update(batch_data)

        print(f"Downloaded batch {i+1}/{len(batches)} | Total stocks: {len(all_data)}")

        time.sleep(REQUEST_SLEEP)

    return all_data


prices_data = sequential_download(tickers)

print("Successfully downloaded:", len(prices_data))

if len(prices_data) == 0:
    print("Download failed (rate limited or blocked).")
    exit()


# ================= BENCHMARK =================
try:
    spy = yf.download("SPY", period=LOOKBACK, auto_adjust=True, progress=False)
    benchmark_ret = spy["Close"].pct_change().dropna()
except:
    benchmark_ret = pd.Series(dtype=float)


# ================= OPPORTUNITY ENGINE =================
results = []

for ticker, df_t in prices_data.items():

    if len(df_t) < 252:
        continue

    close = df_t["Close"]
    volume = df_t["Volume"]

    if close.iloc[-1] < MIN_PRICE:
        continue

    avg_dollar_vol = (close * volume).mean()
    if avg_dollar_vol < MIN_LIQUIDITY:
        continue

    returns = close.pct_change().dropna()

    ma50 = close.rolling(50).mean().iloc[-1]
    ma200 = close.rolling(200).mean().iloc[-1]

    trend = ma50 > ma200
    mom6 = close.pct_change(126).iloc[-1]
    mom12 = close.pct_change(252).iloc[-1]

    rel = returns.mean() - benchmark_ret.mean() if not benchmark_ret.empty else 0
    vol = returns.std()

    score = mom6*0.4 + mom12*0.3 + rel*0.2 - vol*0.1

    if np.isnan(score):
        continue

    if trend and mom6 > 0 and rel > 0:
        signal = "STRONG BUY"
    elif trend and mom6 > 0:
        signal = "BUY PULLBACK"
    elif not trend and rel < 0:
        signal = "AVOID"
    else:
        signal = "WATCH"

    results.append({
        "Ticker": ticker,
        "Signal": signal,
        "Momentum6M": round(mom6,4),
        "Momentum12M": round(mom12,4),
        "RelStrength": round(rel,4),
        "Volatility": round(vol,4),
        "Liquidity": int(avg_dollar_vol),
        "Score": round(score,4)
    })


df = pd.DataFrame(results)

if df.empty:
    print("No qualifying stocks found.")
    exit()

df = df.sort_values("Score", ascending=False).reset_index(drop=True)


# ================= LINKS =================
def make_links(t):
    return (
        f'<a href="https://finviz.com/quote.ashx?t={t}" target="_blank">{t}</a>',
        f'<a href="https://stockanalysis.com/stocks/{t.lower()}/forecast/" target="_blank">Forecast</a>'
    )

df["Finviz"], df["Forecast"] = zip(*df["Ticker"].apply(make_links))

strong_buys = df[df["Signal"]=="STRONG BUY"]
pullbacks = df[df["Signal"]=="BUY PULLBACK"]
avoids = df[df["Signal"]=="AVOID"]


# ================= VISUAL =================
fig = px.scatter(
    df.head(200),
    x="Momentum6M",
    y="RelStrength",
    color="Signal",
    size="Liquidity",
    hover_name="Ticker",
    title="Market Opportunity Map"
)

plot_html = fig.to_html(full_html=False)


# ================= MODERN HTML =================
html = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Institutional Quant Market Scanner</title>
<style>
body {{
    font-family: Inter, Arial;
    background: #0f172a;
    color: #e2e8f0;
    padding: 40px;
}}
h1 {{
    color: #38bdf8;
    font-size: 32px;
}}
h2 {{
    color: #facc15;
    margin-top: 60px;
}}
.card {{
    background: #1e293b;
    padding: 20px;
    border-radius: 12px;
    margin-top: 20px;
    box-shadow: 0 4px 20px rgba(0,0,0,0.4);
}}
table {{
    border-collapse: collapse;
    width: 100%;
}}
th, td {{
    padding: 10px;
    border-bottom: 1px solid #334155;
    text-align: center;
}}
th {{
    background: #111827;
}}
tr:hover {{
    background: #1f2937;
}}
a {{
    color: #38bdf8;
    text-decoration: none;
    font-weight: 600;
}}
</style>
</head>
<body>

<h1>📊 Institutional Quant Opportunity Scanner</h1>
<p>{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>

<div class="card">
<h2>🔥 Strong Buy ({len(strong_buys)})</h2>
{strong_buys.head(20)[["Finviz","Score","Momentum6M","RelStrength","Forecast"]].to_html(index=False, escape=False)}
</div>

<div class="card">
<h2>📉 Pullbacks ({len(pullbacks)})</h2>
{pullbacks.head(20)[["Finviz","Score","Momentum6M","RelStrength","Forecast"]].to_html(index=False, escape=False)}
</div>

<div class="card">
<h2>⚠ Avoid ({len(avoids)})</h2>
{avoids.head(20)[["Finviz","Score","Momentum6M","RelStrength","Forecast"]].to_html(index=False, escape=False)}
</div>

<div class="card">
<h2>📈 Market Opportunity Map</h2>
{plot_html}
</div>

<div class="card">
<h2>🏆 Top 30 Overall</h2>
{df.head(30)[["Finviz","Score","Momentum6M","RelStrength","Forecast"]].to_html(index=False, escape=False)}
</div>

</body>
</html>
"""

with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
    f.write(html)

print("✅ Dashboard Created:", OUTPUT_HTML)
