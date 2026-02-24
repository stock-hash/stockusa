import os
import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import plotly.express as px
import re
import time
import random

pd.options.mode.use_inf_as_na = True

# ================= SETTINGS =================
OUTPUT_FOLDER = "docs"
OUTPUT_HTML = os.path.join(OUTPUT_FOLDER, "StockMarket_Reversal_Dashboard.html")

LOOKBACK = "1y"
MIN_LIQUIDITY = 20_000_000
MIN_PRICE = 5
EARNINGS_BUFFER_DAYS = 7

CHUNK_SIZE = 25
REQUEST_SLEEP = 1.5
MAX_RETRIES = 4

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# ================= LOAD UNIVERSE =================
def load_universe():
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
print("Universe:", len(tickers))

# ================= DOWNLOAD =================
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

        except:
            sleep_time = REQUEST_SLEEP * (2 ** attempt) + random.uniform(0, 1)
            time.sleep(sleep_time)

    return {}


def sequential_download(tickers):
    all_data = {}
    batches = [tickers[i:i+CHUNK_SIZE] for i in range(0, len(tickers), CHUNK_SIZE)]

    for i, batch in enumerate(batches):
        batch_data = download_batch(batch)
        all_data.update(batch_data)
        print(f"Downloaded {i+1}/{len(batches)} | Stocks: {len(all_data)}")
        time.sleep(REQUEST_SLEEP)

    return all_data


prices_data = sequential_download(tickers)

if len(prices_data) == 0:
    print("Download failed.")
    exit()

# ================= RSI FUNCTION =================
def compute_rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.rolling(period).mean()
    avg_loss = loss.rolling(period).mean()

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


# ================= REVERSAL ENGINE =================
results = []
today = datetime.datetime.now()

for ticker, df in prices_data.items():
    try:
        if len(df) < 100:
            continue

        close = df["Close"]
        volume = df["Volume"]

        if close.iloc[-1] < MIN_PRICE:
            continue

        avg_dollar_vol = float((close * volume).mean())
        if avg_dollar_vol < MIN_LIQUIDITY:
            continue

        # Earnings filter
        try:
            info = yf.Ticker(ticker)
            earnings = info.calendar.loc["Earnings Date"][0]
            days_to_earnings = (earnings - today).days
            if 0 <= days_to_earnings <= EARNINGS_BUFFER_DAYS:
                continue
        except:
            days_to_earnings = None

        # Indicators
        rsi = compute_rsi(close).iloc[-1]
        ma10 = close.rolling(10).mean().iloc[-1]
        ma20 = close.rolling(20).mean().iloc[-1]
        std20 = close.rolling(20).std().iloc[-1]

        lower_band = ma20 - 2 * std20

        five_day_return = close.pct_change(5).iloc[-1]
        volume_spike = volume.iloc[-1] > volume.rolling(20).mean().iloc[-1]

        # Reversal Conditions
        oversold = rsi < 35 and close.iloc[-1] < lower_band
        turning_up = five_day_return > 0 and close.iloc[-1] > ma10

        if oversold and turning_up:

            score = (35 - rsi) + (five_day_return * 100)

            results.append({
                "Ticker": ticker,
                "RSI": round(rsi,2),
                "5D Return %": round(five_day_return*100,2),
                "Volume Spike": volume_spike,
                "Liquidity": int(avg_dollar_vol),
                "DaysToEarnings": days_to_earnings,
                "Score": round(score,2)
            })

    except:
        continue


df = pd.DataFrame(results)

if df.empty:
    print("No reversal candidates found.")
    exit()

df = df.sort_values("Score", ascending=False).reset_index(drop=True)

# ================= LINKS =================
def make_links(t):
    finviz = f'https://finviz.com/quote.ashx?t={t}'
    forecast = f'https://stockanalysis.com/stocks/{t.lower()}/forecast/'
    return f'<a href="{finviz}" target="_blank">{t}</a>', f'<a href="{forecast}" target="_blank">Forecast</a>'

df["Ticker"], df["Forecast"] = zip(*df["Ticker"].apply(make_links))

# ================= VISUAL =================
fig = px.scatter(
    df.head(200),
    x="RSI",
    y="5D Return %",
    size="Liquidity",
    hover_name="Ticker",
    title="Reversal Opportunity Map"
)

plot_html = fig.to_html(full_html=False)

# ================= DASHBOARD =================
html = f"""
<html>
<head>
<title>Market Reversal Scanner</title>
<style>
body {{ background:#0b1220; color:white; font-family:Arial; padding:40px; }}
.card {{ background:#111827; padding:25px; border-radius:12px; margin-top:40px; }}
table {{ width:100%; border-collapse:collapse; }}
th, td {{ padding:10px; border-bottom:1px solid #1f2937; text-align:center; }}
th {{ background:#1f2937; }}
a {{ color:#38bdf8; text-decoration:none; font-weight:600; }}
</style>
</head>
<body>

<h1>🔄 Institutional Reversal Scanner</h1>
<p>{today.strftime("%Y-%m-%d %H:%M:%S")}</p>

<div class="card">
<h2>🔥 Top Reversal Candidates</h2>
{df.head(40).to_html(index=False, escape=False)}
</div>

<div class="card">
<h2>📈 Reversal Map</h2>
{plot_html}
</div>

</body>
</html>
"""

with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
    f.write(html)

print("✅ Reversal Dashboard Created:", OUTPUT_HTML)
