import os
import shutil
import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import plotly.express as px
import re
import concurrent.futures
import time

# ================= SETTINGS =================
OUTPUT_FOLDER = "docs"
OUTPUT_HTML = os.path.join(OUTPUT_FOLDER, "StockMarket_Opportunity_Dashboard.html")

LOOKBACK = "2y"
MIN_LIQUIDITY = 20_000_000
MIN_PRICE = 5
CHUNK_SIZE = 100
MAX_WORKERS = 3

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# ================= CLEAN PREVIOUS =================
if os.path.exists(OUTPUT_HTML):
    os.remove(OUTPUT_HTML)

cache_dir = os.path.expanduser("~/.cache/yfinance")
if os.path.exists(cache_dir):
    shutil.rmtree(cache_dir)

# ================= LOAD FULL US MARKET =================
def load_universe():
    nasdaq = pd.read_csv("https://raw.githubusercontent.com/datasets/nasdaq-listings/master/data/nasdaq-listed-symbols.csv")
    nyse = pd.read_csv("https://raw.githubusercontent.com/datasets/nyse-other-listings/master/data/nyse-listed.csv")
    amex = pd.read_csv("https://raw.githubusercontent.com/datasets/nyse-other-listings/master/data/other-listed.csv")

    tickers = []

    if "Symbol" in nasdaq.columns:
        tickers += nasdaq["Symbol"].dropna().tolist()

    if "ACT Symbol" in nyse.columns:
        tickers += nyse["ACT Symbol"].dropna().tolist()

    if "ACT Symbol" in amex.columns:
        tickers += amex["ACT Symbol"].dropna().tolist()

    return list(set(tickers))


# ================= SANITIZE =================
def sanitize_ticker(t):
    if pd.isna(t) or not isinstance(t, str):
        return None

    t = t.strip().upper()

    # Remove special securities
    if any(x in t for x in [".W", ".U", ".R", "^", "/", "="]):
        return None

    # Only allow A-Z and dash
    if not re.match(r"^[A-Z\-.]+$", t):
        return None

    # Convert BRK.B → BRK-B for Yahoo
    t = t.replace(".", "-")

    if len(t) > 6:
        return None

    return t


raw_tickers = load_universe()
tickers = list(set(filter(None, [sanitize_ticker(t) for t in raw_tickers])))

print("Final Universe:", len(tickers))


# ================= SAFE DOWNLOAD =================
def download_batch(batch):
    result = {}

    try:
        data = yf.download(
            batch,
            period=LOOKBACK,
            auto_adjust=True,
            progress=False,
            threads=False
        )

        if data is None or data.empty:
            return result

        if isinstance(data.columns, pd.MultiIndex):
            for t in batch:
                if t in data.columns.get_level_values(0):
                    df = data[t].dropna()
                    if {"Close", "Volume"}.issubset(df.columns):
                        result[t] = df

        else:
            df = data.dropna()
            if {"Close", "Volume"}.issubset(df.columns):
                result[batch[0]] = df

    except Exception as e:
        print("Download error:", e)

    time.sleep(1)
    return result


def chunked_parallel_download(ticker_list):
    all_data = {}
    batches = [ticker_list[i:i + CHUNK_SIZE] for i in range(0, len(ticker_list), CHUNK_SIZE)]

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(download_batch, batch) for batch in batches]

        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            batch_data = future.result()
            all_data.update(batch_data)
            print(f"Downloaded batch {i+1}/{len(batches)}")

    return all_data


prices_data = chunked_parallel_download(tickers)
print("Successfully downloaded:", len(prices_data))


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

    try:
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
            "Momentum6M": round(mom6, 4),
            "Momentum12M": round(mom12, 4),
            "RelStrength": round(rel, 4),
            "Volatility": round(vol, 4),
            "Liquidity": int(avg_dollar_vol),
            "Score": round(score, 4)
        })

    except:
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


strong_buys = df[df["Signal"] == "STRONG BUY"]
pullbacks = df[df["Signal"] == "BUY PULLBACK"]
avoids = df[df["Signal"] == "AVOID"]


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


# ================= PROFESSIONAL HTML =================
html = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Quant Market Scanner</title>
<style>
body {{font-family:Inter,Arial;background:#0b1220;color:#e2e8f0;padding:40px;}}
h1 {{color:#38bdf8;}}
h2 {{color:#facc15;margin-top:50px;}}
table {{border-collapse:collapse;width:100%;margin-top:15px;}}
th,td {{padding:8px;border:1px solid #1e293b;text-align:center;}}
th {{background:#1e293b;}}
tr:nth-child(even){{background:#111827;}}
a {{color:#38bdf8;text-decoration:none;font-weight:600;}}
.badge {{padding:4px 8px;border-radius:6px;font-weight:bold;}}
.STRONG {{background:#065f46;color:#10b981;}}
.PULLBACK {{background:#78350f;color:#facc15;}}
.AVOID {{background:#7f1d1d;color:#ef4444;}}
.WATCH {{background:#1e293b;color:#94a3b8;}}
</style>
</head>
<body>

<h1>📊 Institutional Quant Opportunity Scanner</h1>
<p>{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>

<h2>🔥 Strong Buy ({len(strong_buys)})</h2>
{strong_buys.head(20)[["Finviz","Signal","Score","Momentum6M","RelStrength","Forecast"]].to_html(index=False, escape=False)}

<h2>📉 Pullbacks ({len(pullbacks)})</h2>
{pullbacks.head(20)[["Finviz","Signal","Score","Momentum6M","RelStrength","Forecast"]].to_html(index=False, escape=False)}

<h2>⚠ Avoid ({len(avoids)})</h2>
{avoids.head(20)[["Finviz","Signal","Score","Momentum6M","RelStrength","Forecast"]].to_html(index=False, escape=False)}

<h2>📈 Market Opportunity Map</h2>
{plot_html}

<h2>🏆 Top 30 Overall</h2>
{df.head(30)[["Finviz","Signal","Score","Momentum6M","RelStrength","Forecast"]].to_html(index=False, escape=False)}

</body>
</html>
"""

with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
    f.write(html)

print("✅ Dashboard Created:", OUTPUT_HTML)
