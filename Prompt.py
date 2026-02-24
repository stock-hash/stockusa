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

LOOKBACK = "2y"
MIN_LIQUIDITY = 20_000_000
MIN_PRICE = 5
EARNINGS_BUFFER_DAYS = 7

CHUNK_SIZE = 20
REQUEST_SLEEP = 2
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

        except Exception:
            sleep_time = REQUEST_SLEEP * (2 ** attempt) + random.uniform(0, 1)
            time.sleep(sleep_time)

    return {}


def sequential_download(tickers):
    all_data = {}
    batches = [tickers[i:i+CHUNK_SIZE] for i in range(0, len(tickers), CHUNK_SIZE)]

    for i, batch in enumerate(batches):
        batch_data = download_batch(batch)
        all_data.update(batch_data)
        print(f"Downloaded {i+1}/{len(batches)} | Total stocks: {len(all_data)}")
        time.sleep(REQUEST_SLEEP)

    return all_data


prices_data = sequential_download(tickers)
print("Successfully downloaded:", len(prices_data))

if len(prices_data) == 0:
    print("Download failed.")
    exit()

# ================= BENCHMARK =================
spy = yf.download("SPY", period=LOOKBACK, auto_adjust=True, progress=False)
benchmark_ret = spy["Close"].pct_change().dropna()

# ================= REVERSAL ENGINE =================
results = []
today = datetime.datetime.now()

for ticker, df_t in prices_data.items():
    try:
        if len(df_t) < 252:
            continue

        close = df_t["Close"]
        volume = df_t["Volume"]

        if close.iloc[-1] < MIN_PRICE:
            continue

        avg_dollar_vol = float((close * volume).mean())
        if avg_dollar_vol < MIN_LIQUIDITY:
            continue

        # Earnings filter safe
        days_to_earnings = None
        try:
            cal = yf.Ticker(ticker).calendar
            if cal is not None and not cal.empty:
                earnings_date = cal.iloc[0][0]
                if isinstance(earnings_date, pd.Timestamp):
                    days_to_earnings = (earnings_date - today).days
                    if 0 <= days_to_earnings <= EARNINGS_BUFFER_DAYS:
                        continue
        except:
            pass

        returns = close.pct_change().dropna()
        ma20 = close.rolling(20).mean().iloc[-1]
        ma50 = close.rolling(50).mean().iloc[-1]
        ma200 = close.rolling(200).mean().iloc[-1]

        rsi = 100 - (100 / (1 + returns.rolling(14).mean().iloc[-1] / 
                             returns.rolling(14).std().iloc[-1] if returns.rolling(14).std().iloc[-1] != 0 else 1))

        mom3 = close.pct_change(63).iloc[-1]
        mom6 = close.pct_change(126).iloc[-1]

        rel = returns.mean() - benchmark_ret.mean()
        vol = returns.std()

        # ===== REVERSAL CONDITIONS =====
        oversold = rsi < 35
        above_ma20 = close.iloc[-1] > ma20
        improving_momentum = mom3 > mom6

        if not (oversold and above_ma20 and improving_momentum):
            continue

        # ===== REVERSAL SCORE =====
        score = (
            abs(mom6) * 0.3 +
            (35 - rsi) * 0.3 +
            rel * 0.2 -
            vol * 0.2
        )

        if pd.isna(score):
            continue

        results.append({
            "Ticker": ticker,
            "Signal": "REVERSAL",
            "Score": round(float(score),4),
            "RSI": round(float(rsi),2),
            "Momentum3M": round(float(mom3*100),2),
            "Momentum6M": round(float(mom6*100),2),
            "RelStrength": round(float(rel*100),2),
            "Volatility": round(float(vol*100),2),
            "Liquidity": int(avg_dollar_vol),
            "DaysToEarnings": days_to_earnings
        })

    except Exception:
        continue

df = pd.DataFrame(results)

if df.empty:
    print("No reversal stocks found.")
    exit()

df = df.sort_values("Score", ascending=False).reset_index(drop=True)

# ================= ADD LINKS =================
df["Ticker"] = df["Ticker"].apply(
    lambda x: f'<a href="https://finviz.com/quote.ashx?t={x}" target="_blank">{x}</a>'
)

df["Forecast"] = df["Ticker"].str.extract(r'>(.*?)<')[0].apply(
    lambda x: f'<a href="https://stockanalysis.com/stocks/{x.lower()}/forecast/" target="_blank">Forecast</a>'
)

# ================= DASHBOARD =================
fig = px.scatter(
    df.head(300),
    x="Momentum3M",
    y="RelStrength",
    size="Liquidity",
    hover_name=df["Ticker"].str.extract(r'>(.*?)<')[0],
    title="Reversal Strength Map"
)

plot_html = fig.to_html(full_html=False)

html = f"""
<html>
<head>
<title>Institutional Reversal Scanner</title>
<style>
body {{ background:#0b1220; color:white; font-family:Arial; padding:40px; }}
.card {{ background:#111827; padding:25px; border-radius:12px; margin-top:40px; }}
table {{ width:100%; border-collapse:collapse; }}
th, td {{ padding:10px; border-bottom:1px solid #1f2937; text-align:center; }}
th {{ background:#1f2937; }}
a {{ color:#38bdf8; text-decoration:none; font-weight:bold; }}
</style>
</head>
<body>

<h1>📊 Institutional Reversal Market Scanner</h1>
<p>{today.strftime("%Y-%m-%d %H:%M:%S")}</p>

<div class="card">
<h2>🔥 Top Reversal Stocks</h2>
{df.head(30).to_html(index=False, escape=False)}
</div>

<div class="card">
<h2>📈 Reversal Opportunity Map</h2>
{plot_html}
</div>

</body>
</html>
"""

with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
    f.write(html)

print("✅ Reversal Dashboard Created:", OUTPUT_HTML)
