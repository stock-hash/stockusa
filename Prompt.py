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
OUTPUT_HTML = os.path.join(OUTPUT_FOLDER, "StockMarket_Opportunity_Dashboard.html")

LOOKBACK = "2y"
MIN_LIQUIDITY = 20_000_000
MIN_PRICE = 5

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
try:
    spy = yf.download("SPY", period=LOOKBACK, auto_adjust=True, progress=False)
    benchmark_ret = spy["Close"].pct_change().dropna()
except:
    benchmark_ret = pd.Series(dtype=float)

# ================= OPPORTUNITY ENGINE =================
results = []

for ticker, df_t in prices_data.items():
    try:
        if len(df_t) < 252:
            continue

        if not {"Close", "Volume"}.issubset(df_t.columns):
            continue

        close = df_t["Close"].squeeze()
        volume = df_t["Volume"].squeeze()

        if close.iloc[-1] < MIN_PRICE:
            continue

        avg_dollar_vol = float((close * volume).mean())
        if avg_dollar_vol < MIN_LIQUIDITY:
            continue

        returns = close.pct_change().dropna()

        ma50 = float(close.rolling(50).mean().iloc[-1])
        ma200 = float(close.rolling(200).mean().iloc[-1])

        mom6 = float(close.pct_change(126).iloc[-1])
        mom12 = float(close.pct_change(252).iloc[-1])

        rel = float(returns.mean() - benchmark_ret.mean()) if not benchmark_ret.empty else 0
        vol = float(returns.std())

        score = (
            mom6 * 0.35 +
            mom12 * 0.30 +
            rel * 0.20 -
            vol * 0.15
        )

        if pd.isna(score):
            continue

        # ===== CLEAR SIGNAL RULES =====
        if ma50 > ma200 and mom6 > 0.10 and rel > 0:
            signal = "STRONG BUY"
        elif ma50 > ma200 and mom6 > 0:
            signal = "BUY"
        elif ma50 < ma200 and rel < 0:
            signal = "AVOID"
        else:
            signal = "WATCH"

        results.append({
            "Ticker": ticker,
            "Signal": signal,
            "Score": round(score,4),
            "Momentum6M": round(mom6*100,2),
            "Momentum12M": round(mom12*100,2),
            "RelStrength": round(rel*100,2),
            "Volatility": round(vol*100,2),
            "Liquidity": int(avg_dollar_vol)
        })

    except Exception:
        continue

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

# ================= SUMMARY STATS =================
strong_count = len(df[df["Signal"]=="STRONG BUY"])
buy_count = len(df[df["Signal"]=="BUY"])
avoid_count = len(df[df["Signal"]=="AVOID"])
watch_count = len(df[df["Signal"]=="WATCH"])

# ================= VISUAL =================
fig = px.scatter(
    df.head(300),
    x="Momentum6M",
    y="RelStrength",
    color="Signal",
    size="Liquidity",
    hover_name="Ticker",
    title="Market Strength Positioning Map"
)

plot_html = fig.to_html(full_html=False)

# ================= PROFESSIONAL DASHBOARD =================
html = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Institutional Market Scanner</title>
<style>
body {{
    font-family: Inter, Arial;
    background: #0b1220;
    color: #e2e8f0;
    padding: 40px;
}}
h1 {{
    color: #38bdf8;
    font-size: 34px;
}}
.summary {{
    display:flex;
    gap:20px;
    margin-top:20px;
}}
.badge {{
    padding:15px 25px;
    border-radius:10px;
    font-weight:bold;
    font-size:18px;
}}
.strong {{ background:#14532d; }}
.buy {{ background:#1e3a8a; }}
.watch {{ background:#78350f; }}
.avoid {{ background:#7f1d1d; }}

.card {{
    background:#111827;
    padding:25px;
    border-radius:14px;
    margin-top:40px;
}}

table {{
    width:100%;
    border-collapse:collapse;
}}
th, td {{
    padding:10px;
    border-bottom:1px solid #1f2937;
    text-align:center;
}}
th {{ background:#1f2937; }}

a {{
    color:#38bdf8;
    text-decoration:none;
    font-weight:600;
}}
</style>
</head>
<body>

<h1>📊 Institutional Quant Market Scanner</h1>
<p>{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>

<div class="summary">
<div class="badge strong">🔥 Strong Buy: {strong_count}</div>
<div class="badge buy">📈 Buy: {buy_count}</div>
<div class="badge watch">👀 Watch: {watch_count}</div>
<div class="badge avoid">⚠ Avoid: {avoid_count}</div>
</div>

<div class="card">
<h2>🏆 Top 300 Ranked</h2>
{df.head(300)[["Finviz","Signal","Score","Momentum6M","RelStrength","Forecast"]].to_html(index=False, escape=False)}
</div>

<div class="card">
<h2>📈 Market Strength Map</h2>
{plot_html}
</div>

</body>
</html>
"""

with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
    f.write(html)

print("✅ Professional Dashboard Created:", OUTPUT_HTML)

