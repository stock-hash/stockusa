#!/usr/bin/env python3
"""
generate_report_v51.py — One-shot scanner for GitHub Actions
Runs a single scan cycle, generates TopBottom_Universal.html
Uses v5.1 logic: 5m+15m+30m strict, 60m booster
"""
import yfinance as yf
import pandas as pd
import numpy as np
import warnings, time, random, os, sys
from datetime import datetime, timedelta, date

warnings.filterwarnings("ignore")

try:
    import pytz
    ET = pytz.timezone("US/Eastern")
except ImportError:
    print("ERROR: pip install pytz")
    sys.exit(1)


# ═══════════════════════════════════════════════════════
# STOCK UNIVERSE (same as v5 scanner)
# ═══════════════════════════════════════════════════════

DOW_30 = [
    "AAPL","MSFT","AMZN","NVDA","UNH","V","JNJ","WMT","JPM","PG",
    "MA","HD","MRK","CVX","KO","DIS","MCD","CSCO","ABT","VZ",
    "NKE","INTC","SHW","DOW","MMM","TRV","AXP","BA","CAT","GS"
]

SP500_TOP = [
    "GOOGL","META","BRK-B","TSLA","XOM","LLY","ABBV",
    "PEP","COST","AVGO","TMO","ADBE","CRM","TXN",
    "NFLX","AMD","NEE","QCOM","ISRG","INTU","AMAT","BKNG",
    "GE","SYK","BLK","GILD","VRTX","REGN",
    "PANW","KLAC","SNPS","CDNS",
    "PGR","CME","CI","ICE",
    "FDX","EMR","ITW","CTAS","ORLY",
    "FICO","CPRT","CEG","CARR","GWW",
    "AXON","DECK","GEHC","FSLR","URI"
]

TOP_ETFS = [
    "SPY","QQQ","IWM","DIA","XLK","XLE","XLF","XLV","XLI",
    "XLU","XLB","XLY","XLP","XLRE","XLC","SMH","SOXX",
    "GDX","GLD","SLV","TLT","HYG","ARKK"
]

LEVERAGED_3X = [
    "TQQQ","SQQQ","SPXL","SPXS","TNA","TZA","SOXL","SOXS",
    "FNGU","FNGD","TECL","TECS","FAS","FAZ","UPRO","SPXU"
]

ALL_STOCKS = list(dict.fromkeys(DOW_30 + SP500_TOP + TOP_ETFS + LEVERAGED_3X))


# ═══════════════════════════════════════════════════════
# TECHNICAL INDICATORS
# ═══════════════════════════════════════════════════════

def safe_val(s, default=0):
    try:
        v = s.iloc[-1]
        return default if pd.isna(v) else float(v)
    except Exception:
        return default

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_g = gain.rolling(window=period, min_periods=period).mean()
    avg_l = loss.rolling(window=period, min_periods=period).mean()
    rs = avg_g / avg_l
    return 100 - (100 / (1 + rs))

def calculate_macd_hist(series, fast=12, slow=26, signal=9):
    ef = series.ewm(span=fast, adjust=False).mean()
    es = series.ewm(span=slow, adjust=False).mean()
    ml = ef - es
    sl = ml.ewm(span=signal, adjust=False).mean()
    return ml - sl

def calculate_stochastic_k(high, low, close, k_period=14):
    ll = low.rolling(window=k_period).min()
    hh = high.rolling(window=k_period).max()
    return 100 * (close - ll) / (hh - ll)

def calculate_williams_r(high, low, close, period=14):
    hh = high.rolling(window=period).max()
    ll = low.rolling(window=period).min()
    return -100 * (hh - close) / (hh - ll)

def calculate_mfi(high, low, close, volume, period=14):
    tp = (high + low + close) / 3.0
    mf = tp * volume
    pmf = mf.where(tp > tp.shift(1), 0.0).rolling(period).sum()
    nmf = mf.where(tp <= tp.shift(1), 0.0).rolling(period).sum()
    mfr = pmf / nmf.where(nmf > 0, np.nan)
    return 100 - (100 / (1 + mfr))

def calculate_cmf(high, low, close, volume, period=20):
    clv = ((close - low) - (high - close)) / (high - low).where((high - low) > 0, np.nan)
    return (clv * volume).rolling(period).sum() / volume.rolling(period).sum()

def calculate_vwap(high, low, close, volume):
    tp = (high + low + close) / 3.0
    return (tp * volume).cumsum() / volume.cumsum()

def calculate_bollinger(series, period=20, std_dev=2):
    sma = series.rolling(window=period).mean()
    std = series.rolling(window=period).std()
    return sma + std_dev * std, sma, sma - std_dev * std


# ═══════════════════════════════════════════════════════
# SAFE DOWNLOAD
# ═══════════════════════════════════════════════════════

def safe_download(ticker, period, interval, max_retries=2):
    for attempt in range(1, max_retries + 1):
        try:
            time.sleep(0.8 + random.uniform(0.3, 1.0))
            d = yf.download(ticker, period=period, interval=interval,
                            progress=False, auto_adjust=True, threads=False)
            if d is not None and not d.empty:
                if isinstance(d.columns, pd.MultiIndex):
                    d.columns = d.columns.get_level_values(0)
                return d
        except Exception:
            if attempt < max_retries:
                time.sleep(attempt * 2)
    return None


# ═══════════════════════════════════════════════════════
# MARKET REGIME (SPY)
# ═══════════════════════════════════════════════════════

def get_market_regime():
    try:
        spy = safe_download("SPY", "5d", "5m")
        if spy is None or len(spy) < 30:
            return "NEUTRAL", 50.0
        close = spy["Close"]
        rsi = calculate_rsi(close)
        mh = calculate_macd_hist(close)
        ma20 = close.rolling(20).mean()
        sr = safe_val(rsi, 50)
        sm = safe_val(mh, 0)
        sp = safe_val(close, 0)
        sa = safe_val(ma20, 0)
        bs = (1 if sr > 55 else 0) + (1 if sr > 65 else 0) + \
             (1 if sm > 0 else 0) + (1 if sp > sa else 0)
        br = (1 if sr < 45 else 0) + (1 if sr < 35 else 0) + \
             (1 if sm < 0 else 0) + (1 if sp < sa else 0)
        regime = "BULLISH" if bs >= 3 else ("BEARISH" if br >= 3 else "NEUTRAL")
        return regime, sr
    except Exception:
        return "NEUTRAL", 50.0


# ═══════════════════════════════════════════════════════
# FULL ANALYSIS (one timeframe)
# ═══════════════════════════════════════════════════════

def analyze_single(ticker, interval, period):
    data = safe_download(ticker, period, interval)
    if data is None or len(data) < 30:
        return None

    c = data["Close"]
    h = data["High"]
    lo = data["Low"]
    v = data["Volume"]
    o = data["Open"]

    rv = safe_val(calculate_rsi(c), 50)
    mh = safe_val(calculate_macd_hist(c), 0)
    mh_prev = safe_val(calculate_macd_hist(c).shift(1), 0)
    sk = safe_val(calculate_stochastic_k(h, lo, c), 50)
    sd = safe_val(calculate_stochastic_k(h, lo, c).rolling(3).mean(), 50)
    wr = safe_val(calculate_williams_r(h, lo, c), -50)
    mfi = safe_val(calculate_mfi(h, lo, c, v), 50)
    cmf = safe_val(calculate_cmf(h, lo, c, v), 0)
    vwap = safe_val(calculate_vwap(h, lo, c, v), 0)
    bb_up, bb_mid, bb_lo = calculate_bollinger(c)
    bb_u = safe_val(bb_up, 0)
    bb_l = safe_val(bb_lo, 0)
    cl = safe_val(c, 0)
    op = safe_val(o, 0)
    hi = safe_val(h, 0)
    low_v = safe_val(lo, 0)

    vol_avg = safe_val(v.rolling(20).mean(), 1)
    cur_vol = safe_val(v, 0)
    vol_spike = cur_vol > 1.5 * vol_avg

    # Volume direction
    pc = c.diff()
    bv = v.where(pc > 0, 0.0).rolling(10).sum()
    sv = v.where(pc < 0, 0.0).rolling(10).sum()
    tv = bv + sv
    vd = safe_val(bv / tv.where(tv > 0, np.nan), 0.5)

    # OBV rising
    sign = np.where(c > c.shift(1), 1, np.where(c < c.shift(1), -1, 0))
    obv = (pd.Series(sign, index=c.index) * v).cumsum()
    obv_rising = safe_val(obv, 0) > safe_val(obv.shift(5), 0)

    # Market structure
    higher_low = low_v > safe_val(lo.shift(1), 0)
    lower_high = hi < safe_val(h.shift(1), 0)

    # Candle info
    body = abs(cl - op)
    rng = hi - low_v
    lower_wick = min(cl, op) - low_v
    upper_wick = hi - max(cl, op)

    # ── BOTTOM SCORING (15 conditions) ──
    bottom = 0
    if mh > mh_prev and mh_prev < 0: bottom += 15
    if sk > sd and sk < 30: bottom += 15
    elif sk < 20: bottom += 8
    if vwap > 0 and cl > vwap and cl < vwap * 1.005: bottom += 15
    if rv < 30: bottom += 10
    elif rv < 35: bottom += 5
    if bb_l > 0 and cl <= bb_l * 1.003 and cl > bb_l: bottom += 10
    if wr < -80: bottom += 10
    if vd > 0.55: bottom += 10
    if higher_low: bottom += 15
    if vol_spike and vd > 0.5: bottom += 5
    if mfi < 20: bottom += 12
    elif mfi < 30: bottom += 6
    if obv_rising and rv < 40: bottom += 10
    if cmf > 0 and rv < 40: bottom += 10
    if rng > 0 and lower_wick > 2 * body and body < rng * 0.35: bottom += 10
    try:
        if c.iloc[-2] < o.iloc[-2] and cl > op and cl > o.iloc[-2] and op < c.iloc[-2]:
            bottom += 12
    except Exception:
        pass

    # ── TOP SCORING (15 conditions) ──
    top = 0
    if mh < mh_prev and mh_prev > 0: top += 15
    if sk < sd and sk > 70: top += 15
    elif sk > 80: top += 8
    if vwap > 0 and cl < vwap and cl > vwap * 0.995: top += 15
    if rv > 70: top += 10
    elif rv > 65: top += 5
    if bb_u > 0 and cl >= bb_u * 0.997 and cl < bb_u: top += 10
    if wr > -20: top += 10
    if vd < 0.45: top += 10
    if lower_high: top += 15
    if vol_spike and vd < 0.5: top += 5
    if mfi > 80: top += 12
    elif mfi > 70: top += 6
    if not obv_rising and rv > 60: top += 10
    if cmf < 0 and rv > 60: top += 10
    if rng > 0 and upper_wick > 2 * body and body < rng * 0.35: top += 10
    try:
        if c.iloc[-2] > o.iloc[-2] and cl < op and cl < o.iloc[-2] and op > c.iloc[-2]:
            top += 12
    except Exception:
        pass

    signal = None
    score = 0
    if bottom > top and bottom >= 40:
        signal = "BOTTOM"
        score = min(bottom, 100)
    elif top > bottom and top >= 40:
        signal = "TOP"
        score = min(top, 100)

    return {
        "signal": signal, "score": score,
        "rsi": round(rv, 1), "macd_h": round(mh, 3),
        "stoch_k": round(sk, 1), "wr": round(wr, 1),
        "mfi": round(mfi, 1), "cmf": round(cmf, 3),
        "vwap": round(vwap, 2), "price": round(cl, 2),
        "vol_spike": vol_spike, "vol_dir": round(vd, 2),
        "bottom_score": bottom, "top_score": top,
    }


# ═══════════════════════════════════════════════════════
# MULTI-TIMEFRAME CHECK (v5.1: 5m+15m+30m strict, 60m booster)
# ═══════════════════════════════════════════════════════

def multi_timeframe_check(ticker):
    """Run v5.1 MTF confirmation: 5m, 15m, 30m strict + 60m booster."""

    # ── 5m analysis ──
    r5 = analyze_single(ticker, "5m", "5d")
    if r5 is None or r5["signal"] is None:
        return None

    signal = r5["signal"]
    c5 = r5["score"]

    # ── 15m STRICT ──
    r15 = analyze_single(ticker, "15m", "5d")
    if r15 is None or r15["signal"] != signal:
        return None
    c15 = r15["score"]

    # ── 30m STRICT ──
    r30 = analyze_single(ticker, "30m", "1mo")
    if r30 is None or r30["signal"] != signal:
        return None
    c30 = r30["score"]

    # ── 60m BOOSTER ──
    r60 = analyze_single(ticker, "60m", "1mo")
    mtf_bonus = 0
    mtf_status = "5m+15m+30m"
    c60 = 0

    if r60 is not None and r60["signal"] == signal:
        mtf_bonus = 10
        mtf_status = "5m+15m+30m+60m"
        c60 = r60["score"]
    elif r60 is not None and r60["signal"] is not None:
        mtf_bonus = -5
        c60 = r60["score"]

    avg_c = round((c5 + c15 + c30) / 3) + mtf_bonus
    if mtf_status == "5m+15m+30m+60m":
        avg_c = round((c5 + c15 + c30 + c60) / 4) + mtf_bonus
    avg_c = max(0, min(avg_c, 100))

    return {
        "ticker": ticker,
        "signal": signal,
        "c5": c5, "c15": c15, "c30": c30, "c60": c60,
        "avg_c": avg_c,
        "mtf_status": mtf_status,
        "mtf_bonus": mtf_bonus,
        "price": r5["price"],
        "rsi": r5["rsi"],
        "stoch_k": r5["stoch_k"],
        "wr": r5["wr"],
        "mfi": r5["mfi"],
        "cmf": r5["cmf"],
        "vwap": r5["vwap"],
        "vol_spike": r5["vol_spike"],
        "vol_dir": r5["vol_dir"],
    }


# ═══════════════════════════════════════════════════════
# QUICK FILTER (Pass 1)
# ═══════════════════════════════════════════════════════

def batch_quick_filter(stock_list):
    """Download all stocks in batch, filter by 2-of-3 extreme."""
    print(f"  Quick-filtering {len(stock_list)} stocks...")
    filtered = []
    try:
        bd = yf.download(stock_list, period="5d", interval="5m",
                         progress=False, auto_adjust=True, threads=False,
                         group_by="ticker")
        if bd is None or bd.empty:
            return []
        for ticker in stock_list:
            try:
                if isinstance(bd.columns, pd.MultiIndex):
                    if ticker not in bd.columns.get_level_values(0):
                        continue
                    df = bd[ticker].dropna(how="all")
                else:
                    df = bd.dropna(how="all")
                if df is None or df.empty or len(df) < 20:
                    continue
                c = df["Close"]
                h = df["High"]
                lo = df["Low"]
                rv = safe_val(calculate_rsi(c), 50)
                sk = safe_val(calculate_stochastic_k(h, lo, c), 50)
                wv = safe_val(calculate_williams_r(h, lo, c), -50)
                t_cnt = (1 if rv > 65 else 0) + (1 if sk > 75 else 0) + (1 if wv > -25 else 0)
                b_cnt = (1 if rv < 35 else 0) + (1 if sk < 25 else 0) + (1 if wv < -75 else 0)
                if t_cnt >= 2 or b_cnt >= 2:
                    filtered.append(ticker)
            except Exception:
                continue
    except Exception as e:
        print(f"  Batch error: {e}")
    return filtered


# ═══════════════════════════════════════════════════════
# HTML REPORT GENERATOR (Bloomberg-style dark theme)
# ═══════════════════════════════════════════════════════

def generate_html_report(alerts, regime, spy_rsi, scan_time, total_stocks, filtered_count):
    """Generate self-contained HTML report file."""

    bottom_alerts = [a for a in alerts if a["signal"] == "BOTTOM"]
    top_alerts = [a for a in alerts if a["signal"] == "TOP"]

    def make_row(a):
        sig = a["signal"]
        bg = "#1a3a1a" if sig == "BOTTOM" else "#3a1a1a"
        badge_bg = "#00c853" if sig == "BOTTOM" else "#ff1744"
        badge_txt = "BOTTOM" if sig == "BOTTOM" else "TOP"
        bar_w = min(a["avg_c"], 100)
        bar_color = "#4caf50" if a["avg_c"] >= 60 else ("#ff9800" if a["avg_c"] >= 45 else "#f44336")
        mtf = a["mtf_status"]
        mtf_color = "#00e676" if "60m" in mtf else "#ffab00"

        return f"""<tr style="background:{bg};">
<td style="font-weight:bold;font-size:16px;">{a['ticker']}</td>
<td><span style="background:{badge_bg};color:#fff;padding:3px 10px;border-radius:12px;font-weight:bold;font-size:12px;">{badge_txt}</span></td>
<td>${a['price']:.2f}</td>
<td>
  <div style="background:#333;border-radius:6px;height:18px;width:120px;position:relative;">
    <div style="background:{bar_color};height:18px;width:{bar_w}%;border-radius:6px;text-align:center;color:#fff;font-size:11px;line-height:18px;">{a['avg_c']}</div>
  </div>
</td>
<td style="font-size:12px;">{a['c5']}/{a['c15']}/{a['c30']}/{a['c60']}</td>
<td><span style="background:{mtf_color};color:#000;padding:2px 8px;border-radius:8px;font-size:11px;font-weight:bold;">{mtf}</span></td>
<td>{a['rsi']:.1f}</td>
<td>{a['stoch_k']:.1f}</td>
<td>{a['wr']:.1f}</td>
<td>{a['mfi']:.1f}</td>
<td>{a['cmf']:.3f}</td>
<td>{"YES" if a['vol_spike'] else "no"}</td>
</tr>"""

    alert_rows = ""
    for a in sorted(alerts, key=lambda x: x["avg_c"], reverse=True):
        alert_rows += make_row(a)

    regime_color = "#00e676" if regime == "BULLISH" else ("#ff1744" if regime == "BEARISH" else "#ffab00")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta http-equiv="refresh" content="600">
<title>Scanner v5.1 Dashboard</title>
<style>
  body {{ background:#0a0a0a; color:#e0e0e0; font-family:'Segoe UI',sans-serif; margin:0; padding:20px; }}
  h1 {{ color:#64b5f6; margin:0 0 5px 0; font-size:24px; }}
  .subtitle {{ color:#999; font-size:13px; margin-bottom:20px; }}
  .stats {{ display:flex; gap:20px; margin-bottom:20px; flex-wrap:wrap; }}
  .stat-card {{ background:#1a1a2e; border:1px solid #333; border-radius:10px; padding:15px 25px; min-width:140px; }}
  .stat-label {{ color:#888; font-size:11px; text-transform:uppercase; letter-spacing:1px; }}
  .stat-value {{ font-size:28px; font-weight:bold; margin-top:4px; }}
  table {{ width:100%; border-collapse:collapse; font-size:13px; }}
  th {{ background:#1a1a2e; color:#64b5f6; text-align:left; padding:10px 8px; border-bottom:2px solid #333;
       position:sticky; top:0; }}
  td {{ padding:8px; border-bottom:1px solid #222; }}
  tr:hover {{ background:#1a1a3e !important; }}
  .footer {{ color:#555; font-size:11px; margin-top:20px; text-align:center; }}
</style>
</head>
<body>

<h1>Scanner v5.1 — Multi-Timeframe Dashboard</h1>
<div class="subtitle">Generated: {scan_time} | Auto-refresh: 10 min</div>

<div class="stats">
  <div class="stat-card">
    <div class="stat-label">Market Regime</div>
    <div class="stat-value" style="color:{regime_color};">{regime}</div>
  </div>
  <div class="stat-card">
    <div class="stat-label">SPY RSI</div>
    <div class="stat-value">{spy_rsi:.1f}</div>
  </div>
  <div class="stat-card">
    <div class="stat-label">Universe</div>
    <div class="stat-value">{total_stocks}</div>
  </div>
  <div class="stat-card">
    <div class="stat-label">Passed Filter</div>
    <div class="stat-value">{filtered_count}</div>
  </div>
  <div class="stat-card">
    <div class="stat-label">Confirmed Signals</div>
    <div class="stat-value" style="color:#00e676;">{len(alerts)}</div>
  </div>
  <div class="stat-card">
    <div class="stat-label">BOTTOM</div>
    <div class="stat-value" style="color:#00c853;">{len(bottom_alerts)}</div>
  </div>
  <div class="stat-card">
    <div class="stat-label">TOP</div>
    <div class="stat-value" style="color:#ff1744;">{len(top_alerts)}</div>
  </div>
</div>

<table>
<tr>
  <th>Ticker</th><th>Signal</th><th>Price</th><th>Confidence</th>
  <th>5m/15m/30m/60m</th><th>MTF Status</th>
  <th>RSI</th><th>Stoch</th><th>W%R</th><th>MFI</th><th>CMF</th><th>Vol Spike</th>
</tr>
{alert_rows}
</table>

{"<p style='color:#555;text-align:center;margin-top:40px;font-size:16px;'>No confirmed signals this scan cycle</p>" if len(alerts) == 0 else ""}

<div class="footer">
  Scanner v5.1 | MTF: 5m+15m+30m (STRICT) + 60m (BOOSTER) | GitHub Actions Automated
</div>
</body>
</html>"""
    return html


# ═══════════════════════════════════════════════════════
# MAIN — One-shot scan for GitHub Actions
# ═══════════════════════════════════════════════════════

def main():
    scan_time = datetime.now(ET).strftime("%Y-%m-%d %I:%M:%S %p ET")
    print(f"\n{'='*60}")
    print(f"  SCANNER v5.1 — GitHub Actions One-Shot")
    print(f"  {scan_time}")
    print(f"  Universe: {len(ALL_STOCKS)} stocks")
    print(f"{'='*60}\n")

    # Step 1: Market Regime
    regime, spy_rsi = get_market_regime()
    print(f"  Market Regime: {regime} (SPY RSI={spy_rsi:.1f})")

    # Step 2: Quick Filter (Pass 1)
    filtered = batch_quick_filter(ALL_STOCKS)
    print(f"  Quick Filter: {len(filtered)} passed out of {len(ALL_STOCKS)}")

    # Step 3: Full MTF analysis on filtered stocks
    alerts = []
    for i, ticker in enumerate(filtered):
        print(f"  [{i+1}/{len(filtered)}] Analyzing {ticker}...")
        try:
            result = multi_timeframe_check(ticker)
            if result is not None and result["avg_c"] >= 45:
                alerts.append(result)
                print(f"    *** {result['signal']} confirmed! "
                      f"c={result['avg_c']} MTF={result['mtf_status']}")
        except Exception as e:
            print(f"    Error: {e}")
            continue

    print(f"\n  Confirmed signals: {len(alerts)}")

    # Step 4: Generate HTML report
    html = generate_html_report(alerts, regime, spy_rsi, scan_time,
                                len(ALL_STOCKS), len(filtered))
    os.makedirs("docs", exist_ok=True)
    with open("TopBottom_Universal.html", "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  Report saved: TopBottom_Universal.html ({len(html)} bytes)")

    # Also save a JSON snapshot for debugging
    import json
    snapshot = {
        "scan_time": scan_time,
        "regime": regime,
        "spy_rsi": spy_rsi,
        "total_stocks": len(ALL_STOCKS),
        "filtered": len(filtered),
        "alerts": len(alerts),
        "signals": [{
            "ticker": a["ticker"],
            "signal": a["signal"],
            "avg_c": a["avg_c"],
            "mtf_status": a["mtf_status"],
            "price": a["price"],
        } for a in alerts],
    }
    with open("docs/latest_scan.json", "w") as f:
        json.dump(snapshot, f, indent=2)
    print(f"  Snapshot saved: latest_scan.json")
    print(f"\n{'='*60}")
    print(f"  SCAN COMPLETE")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
