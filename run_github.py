#!/usr/bin/env python3
"""
================================================================
MARKET SCANNER v5.3 - GITHUB ACTIONS RUNNER (FULL FEATURES)
================================================================
Generates a self-contained HTML dashboard for GitHub Pages with:
  - Live alerts with scores, setup types, MTF status
  - 30-day rolling history (accumulated across runs)
  - Interactive charts (Plotly) for each alerted ticker
  - Options chain + strategies for each alerted ticker
  - All data embedded as JSON - NO API calls needed

Output: docs/TopBottom_Universal.html
History: docs/scan_history.json (rolling 30 days)
================================================================
"""

import os, sys, json, time, random, logging, warnings, math
from datetime import datetime, timedelta, date
from html import escape

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("github_runner")

script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, script_dir)

try:
    import yfinance as yf
    import pandas as pd
    import numpy as np
except ImportError as e:
    logger.error("Missing package: %s", e)
    sys.exit(1)

try:
    from market_scanner_v5 import (
        ALL_STOCKS, SECTOR_MAP,
        get_eastern_now, get_market_regime, check_sector_strength,
        get_stock_sector, batch_quick_scan, analyze_stock_v5,
        get_time_quality, get_options_pcr,
        sector_strength_cache, US_MARKET_HOLIDAYS,
        MARKET_OPEN_HOUR, MARKET_OPEN_MINUTE,
        MARKET_CLOSE_HOUR, MARKET_CLOSE_MINUTE,
    )
    SCANNER_IMPORTED = True
    logger.info("Scanner imported: %d stocks", len(ALL_STOCKS))
except ImportError as e:
    logger.error("Cannot import scanner: %s", e)
    SCANNER_IMPORTED = False
    ALL_STOCKS = []

# v5.3: Import peewee memory patch to fix _tz_kv errors
_HAS_PATCH = False
try:
    from market_scanner_v5 import safe_yf_download as _scanner_dl
    from market_scanner_v5 import _patch_yf_tz_cache, _reinit_tz_mem_cache
    _patch_yf_tz_cache()
    _HAS_PATCH = True
    logger.info("v5.3 peewee memory patch applied")
except Exception as e:
    logger.info("v5.3 patch not available: %s", e)

def patched_yf_download(*args, **kwargs):
    if _HAS_PATCH:
        _reinit_tz_mem_cache()
        return _scanner_dl(*args, **kwargs)
    return yf.download(*args, **kwargs)

def safe_int(val):
    try:
        if val is None:
            return 0
        fv = float(val)
        if fv != fv:  # NaN check
            return 0
        return int(fv)
    except (ValueError, TypeError, OverflowError):
        return 0

def safe_float(val, default=0.0):
    try:
        if val is None:
            return default
        fv = float(val)
        if fv != fv:  # NaN check
            return default
        return fv
    except (ValueError, TypeError):
        return default


# ================================================================
# TECHNICAL INDICATORS (for chart data)
# ================================================================

def calc_rsi(close, period=14):
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    ag = gain.rolling(period, min_periods=period).mean()
    al = loss.rolling(period, min_periods=period).mean()
    rs = ag / al
    return 100 - (100 / (1 + rs))

def calc_macd(close, fast=12, slow=26, signal=9):
    ef = close.ewm(span=fast, adjust=False).mean()
    es = close.ewm(span=slow, adjust=False).mean()
    ml = ef - es
    sl = ml.ewm(span=signal, adjust=False).mean()
    return ml, sl, ml - sl

def calc_ema(close, period):
    return close.ewm(span=period, adjust=False).mean()

def calc_bollinger(close, period=20, std_dev=2):
    sma = close.rolling(period).mean()
    std = close.rolling(period).std()
    return sma + std_dev * std, sma, sma - std_dev * std

def calc_vwap(high, low, close, volume):
    tp = (high + low + close) / 3.0
    return (tp * volume).cumsum() / volume.cumsum()

def s2l(s):
    """Series to list with None for NaN."""
    return [None if pd.isna(v) else round(float(v), 4) for v in s]


# ================================================================
# EMAIL (optional)
# ================================================================

def send_email(subject, html_body):
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    addr = os.getenv("EMAIL_ADDRESS", "")
    pwd = os.getenv("EMAIL_PASSWORD", "")
    rcpt = os.getenv("RECIPIENT_EMAILS", "")
    if not addr or not pwd or not rcpt:
        logger.info("Email not configured")
        return
    try:
        with smtplib.SMTP(os.getenv("SMTP_SERVER","smtp.gmail.com"), 587, timeout=60) as srv:
            srv.starttls()
            srv.login(addr, pwd)
            for r in [x.strip() for x in rcpt.split(",") if x.strip()]:
                msg = MIMEMultipart("alternative")
                msg["Subject"] = subject
                msg["From"] = addr
                msg["To"] = r
                msg.attach(MIMEText(html_body, "html"))
                srv.sendmail(addr, [r], msg.as_string())
        logger.info("Email sent: %s", subject)
    except Exception as e:
        logger.error("Email failed: %s", e)


# ================================================================
# SCAN CYCLE
# ================================================================

def run_single_scan():
    if not SCANNER_IMPORTED:
        return [], "UNKNOWN", 50.0

    now = get_eastern_now()
    logger.info("=" * 60)
    logger.info("GITHUB SCAN - %s", now.strftime("%Y-%m-%d %I:%M %p ET"))
    logger.info("Universe: %d stocks", len(ALL_STOCKS))
    logger.info("=" * 60)

    regime, spy_rsi = get_market_regime()
    logger.info("Regime: %s (SPY RSI=%.1f)", regime, spy_rsi)

    filtered = batch_quick_scan(ALL_STOCKS)
    logger.info("Pass 1: %d/%d passed", len(filtered), len(ALL_STOCKS))

    if not filtered:
        return [], regime, spy_rsi

    checked = set()
    for t in filtered:
        sec = get_stock_sector(t)
        if sec not in checked and sec != "SPY":
            check_sector_strength(sec)
            checked.add(sec)
            time.sleep(0.5)

    alerts = []
    for ticker in filtered:
        try:
            sec = get_stock_sector(ticker)
            ss = sector_strength_cache.get(sec, {}).get("strength", "NEUTRAL")
            r5 = analyze_stock_v5(ticker, "5m", regime, ss)
            if r5 is None:
                continue
            sig = r5["signal"]
            conf = r5["confidence"]
            mtf = "5m"
            c15 = c30 = c60 = 0
            try:
                r15 = analyze_stock_v5(ticker, "15m", regime, ss)
                if r15 and r15["signal"] == sig:
                    mtf = "5m+15m"; c15 = r15["confidence"]
                    r30 = analyze_stock_v5(ticker, "30m", regime, ss)
                    if r30 and r30["signal"] == sig:
                        mtf = "5m+15m+30m"; c30 = r30["confidence"]
                        r60 = analyze_stock_v5(ticker, "60m", regime, ss)
                        if r60 and r60["signal"] == sig:
                            mtf = "5m+15m+30m+60m"; c60 = r60["confidence"]
            except Exception:
                pass
            if "15m" not in mtf:
                continue
            confs = [conf, c15]
            if c30 > 0: confs.append(c30)
            if c60 > 0: confs.append(c60)
            avg_c = int(sum(confs) / len(confs))
            try:
                pcr = get_options_pcr(ticker)
            except Exception:
                pcr = 1.0
            now_et = get_eastern_now().strftime("%Y-%m-%d %I:%M:%S %p ET")
            alert = {
                "ticker": ticker, "signal": sig, "confidence": avg_c,
                "alert_price": round(r5["cl"], 2),
                "rsi": round(r5["rsi"], 1),
                "mfi": round(r5.get("mfi", 0), 1),
                "cmf": round(r5.get("cmf", 0), 3),
                "pcr": round(pcr, 2),
                "regime": regime, "sector_strength": ss,
                "setup_type": r5.get("setup_type", "REVERSAL"),
                "trend_3d": round(r5.get("trend_3d", 0), 2),
                "trend_5d": round(r5.get("trend_5d", 0), 2),
                "volume_expansion": round(r5.get("volume_expansion", 1.0), 2),
                "signals": r5.get("signals", []),
                "mtf_status": mtf,
                "time": now_et, "date": now.strftime("%Y-%m-%d"),
            }
            alerts.append(alert)
            logger.info("  * %s: %s c=%d mtf=%s", ticker, sig, avg_c, mtf)
        except Exception as e:
            logger.warning("  Error %s: %s", ticker, e)
    logger.info("Scan complete: %d signals", len(alerts))
    return alerts, regime, spy_rsi


# ================================================================
# CHART DATA FETCHER
# ================================================================

def fetch_chart_data(ticker):
    """Fetch 5-day intraday OHLCV + indicators for a ticker."""
    try:
        data = patched_yf_download(ticker, period="5d", interval="5m", progress=False, auto_adjust=True, threads=False)
        if data is None or data.empty:
            return None
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)
        cl = data["Close"]; hi = data["High"]; lo = data["Low"]; vo = data["Volume"]
        rsi = calc_rsi(cl)
        ml, ms, mh = calc_macd(cl)
        bbu, bbm, bbl = calc_bollinger(cl)
        ema9 = calc_ema(cl, 9)
        ema21 = calc_ema(cl, 21)
        vwap = calc_vwap(hi, lo, cl, vo)
        return {
            "ticker": ticker,
            "timestamps": [str(i) for i in data.index],
            "open": s2l(data["Open"]), "high": s2l(hi), "low": s2l(lo),
            "close": s2l(cl), "volume": s2l(vo),
            "rsi": s2l(rsi), "macd_line": s2l(ml), "macd_signal": s2l(ms), "macd_hist": s2l(mh),
            "bb_upper": s2l(bbu), "bb_middle": s2l(bbm), "bb_lower": s2l(bbl),
            "ema9": s2l(ema9), "ema21": s2l(ema21), "vwap": s2l(vwap),
        }
    except Exception as e:
        logger.warning("Chart data error %s: %s", ticker, e)
        return None


# ================================================================
# OPTIONS DATA FETCHER
# ================================================================

def fetch_options_data(ticker, signal="BOTTOM"):
    """Fetch options chain + compute strategies."""
    try:
        if _HAS_PATCH:
            _reinit_tz_mem_cache()
        t = yf.Ticker(ticker)
        hist = t.history(period="5d")
        if hist.empty:
            return None
        spot = round(float(hist["Close"].iloc[-1]), 2)
        exps = t.options
        if not exps:
            return {"ticker": ticker, "spot": spot, "error": "No options available"}
        now = datetime.now()
        target_exp = None
        for exp in exps:
            dte = (datetime.strptime(exp, "%Y-%m-%d") - now).days
            if 7 <= dte <= 45:
                target_exp = exp; break
        if target_exp is None:
            target_exp = exps[0]
        exp_dt = datetime.strptime(target_exp, "%Y-%m-%d")
        dte = (exp_dt - now).days
        T = max(dte / 365.0, 0.01)
        chain = t.option_chain(target_exp)
        cdf = chain.calls; pdf = chain.puts
        # IV
        all_ivs = []
        for df in [cdf, pdf]:
            if "impliedVolatility" in df.columns:
                all_ivs.extend(df["impliedVolatility"].dropna().tolist())
        avg_iv = round(float(np.mean(all_ivs)) * 100, 1) if all_ivs else 0
        # PCR
        cv = safe_float(cdf["volume"].sum()) if "volume" in cdf.columns else 0
        pv = safe_float(pdf["volume"].sum()) if "volume" in pdf.columns else 0
        pcr = round(float(pv / cv), 2) if cv and cv > 0 and not pd.isna(cv) else 1.0
        # Greeks helper
        def ncdf(x):
            return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))
        def npdf(x):
            return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)
        def bsg(S, K, T2, r, sig, ic=True):
            if T2 <= 0 or sig <= 0 or S <= 0 or K <= 0:
                return {"price": 0, "delta": 0, "gamma": 0, "theta": 0, "vega": 0}
            d1 = (math.log(S/K) + (r + 0.5*sig**2)*T2) / (sig*math.sqrt(T2))
            d2 = d1 - sig * math.sqrt(T2)
            if ic:
                pr = S*ncdf(d1) - K*math.exp(-r*T2)*ncdf(d2); dl = ncdf(d1)
            else:
                pr = K*math.exp(-r*T2)*ncdf(-d2) - S*ncdf(-d1); dl = ncdf(d1) - 1.0
            gm = npdf(d1) / (S * sig * math.sqrt(T2))
            tc = -(S * npdf(d1) * sig) / (2 * math.sqrt(T2))
            if ic:
                th = (tc - r*K*math.exp(-r*T2)*ncdf(d2)) / 365
            else:
                th = (tc + r*K*math.exp(-r*T2)*ncdf(-d2)) / 365
            vg = S * npdf(d1) * math.sqrt(T2) / 100
            return {"price": round(pr, 2), "delta": round(dl, 4), "gamma": round(gm, 6),
                    "theta": round(th, 4), "vega": round(vg, 4)}
        r = 0.05
        def build_chain(df, ic):
            rows = []
            for _, row in df.iterrows():
                strike = float(row["strike"])
                iv = safe_float(row.get("impliedVolatility", 0.3), 0.3)
                g = bsg(spot, strike, T, r, iv, ic)
                rows.append({
                    "strike": round(strike, 2),
                    "bid": round(safe_float(row.get("bid", 0)), 2),
                    "ask": round(safe_float(row.get("ask", 0)), 2),
                    "volume": safe_int(row.get("volume", 0)),
                    "oi": safe_int(row.get("openInterest", 0)),
                    "iv": round(iv * 100, 1),
                    "delta": g["delta"], "gamma": g["gamma"],
                    "theta": g["theta"], "vega": g["vega"],
                    "itm": (strike < spot) if ic else (strike > spot),
                })
            return rows
        calls = build_chain(cdf, True)
        puts = build_chain(pdf, False)
        # Unusual
        unusual = []
        for ol, ot in [(calls, "CALL"), (puts, "PUT")]:
            for o in ol:
                if safe_int(o.get("oi",0)) > 0 and safe_int(o.get("volume",0)) > 0 and safe_int(o.get("volume",0)) / max(safe_int(o.get("oi",0)),1) > 2:
                    unusual.append({"strike": o["strike"], "type": ot, "volume": o["volume"],
                                    "oi": o["oi"], "ratio": round(o["volume"]/o["oi"], 1)})
        unusual.sort(key=lambda x: x["ratio"], reverse=True)
        # Strategies
        strategies = []
        if signal == "BOTTOM":
            ac = next((c for c in calls if c["strike"] >= spot), None)
            oc = next((c for c in calls if c["strike"] >= spot * 1.03), None)
            if ac and oc:
                cost = ac["ask"] - oc["bid"]
                strategies.append({"name": "Bull Call Spread", "legs": [
                    {"action": "BUY", "type": "CALL", "strike": ac["strike"], "price": ac["ask"]},
                    {"action": "SELL", "type": "CALL", "strike": oc["strike"], "price": oc["bid"]}],
                    "cost": round(cost, 2), "max_profit": round(oc["strike"]-ac["strike"]-cost, 2),
                    "breakeven": round(ac["strike"]+cost, 2)})
            if ac:
                strategies.append({"name": "Long Call", "legs": [
                    {"action": "BUY", "type": "CALL", "strike": ac["strike"], "price": ac["ask"]}],
                    "cost": round(ac["ask"], 2), "max_profit": "Unlimited",
                    "breakeven": round(ac["strike"]+ac["ask"], 2)})
        else:
            ap = next((p for p in reversed(puts) if p["strike"] <= spot), None)
            op2 = next((p for p in reversed(puts) if p["strike"] <= spot * 0.97), None)
            if ap and op2:
                cost = ap["ask"] - op2["bid"]
                strategies.append({"name": "Bear Put Spread", "legs": [
                    {"action": "BUY", "type": "PUT", "strike": ap["strike"], "price": ap["ask"]},
                    {"action": "SELL", "type": "PUT", "strike": op2["strike"], "price": op2["bid"]}],
                    "cost": round(cost, 2), "max_profit": round(ap["strike"]-op2["strike"]-cost, 2),
                    "breakeven": round(ap["strike"]-cost, 2)})
            if ap:
                strategies.append({"name": "Long Put", "legs": [
                    {"action": "BUY", "type": "PUT", "strike": ap["strike"], "price": ap["ask"]}],
                    "cost": round(ap["ask"], 2), "max_profit": round(ap["strike"]-ap["ask"], 2),
                    "breakeven": round(ap["strike"]-ap["ask"], 2)})
        # Payoff for first strategy
        payoff = []
        if strategies:
            st = strategies[0]
            for i in range(61):
                px = spot * 0.85 + i * (spot * 0.30 / 60)
                pnl_v = 0
                for leg in st["legs"]:
                    ic2 = leg["type"] == "CALL"; k = leg["strike"]; pm = leg["price"]
                    if leg["action"] == "BUY":
                        pnl_v += (max(0, px-k) - pm) if ic2 else (max(0, k-px) - pm)
                    else:
                        pnl_v += (pm - max(0, px-k)) if ic2 else (pm - max(0, k-px))
                payoff.append({"price": round(px, 2), "pnl": round(pnl_v * 100, 2)})
        return {
            "ticker": ticker, "spot": spot, "signal": signal,
            "expiration": target_exp, "dte": dte, "avg_iv": avg_iv, "pcr": pcr,
            "calls": calls[:15], "puts": puts[-15:],
            "unusual": unusual[:8], "strategies": strategies, "payoff": payoff,
        }
    except Exception as e:
        logger.warning("Options error %s: %s", ticker, e)
        return {"ticker": ticker, "error": str(e)}


# ================================================================
# HISTORY MANAGER (rolling 30 days)
# ================================================================

def load_history():
    p = os.path.join(script_dir, "docs", "scan_history.json")
    if os.path.exists(p):
        try:
            with open(p, "r") as f:
                data = json.load(f)
            return data if isinstance(data, list) else []
        except Exception:
            return []
    return []

def save_history(history):
    cutoff = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    history = [h for h in history if h.get("date", "") >= cutoff]
    p = os.path.join(script_dir, "docs", "scan_history.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w") as f:
        json.dump(history, f, indent=1)
    return history


# ================================================================
# HTML GENERATOR
# ================================================================

def generate_html(alerts, regime, spy_rsi, history, charts, options_data):
    now = get_eastern_now() if SCANNER_IMPORTED else datetime.utcnow()
    scan_time = now.strftime("%Y-%m-%d %I:%M:%S %p ET")
    scan_date = now.strftime("%Y-%m-%d")
    alerts_sorted = sorted(alerts, key=lambda a: a.get("confidence", 0), reverse=True)
    total = len(alerts_sorted)
    bottoms = sum(1 for a in alerts_sorted if a["signal"] == "BOTTOM")
    tops = total - bottoms
    num_stocks = len(ALL_STOCKS) if ALL_STOCKS else 260
    regime_cls = "badge-bull" if regime == "BULLISH" else "badge-bear" if regime == "BEARISH" else "badge-neutral"
    hist_total = len(history)
    hist_wins = sum(1 for h in history if h.get("result") == "WIN")
    hist_losses = sum(1 for h in history if h.get("result") == "LOSS")

    # Convert data to JSON strings
    alerts_json = json.dumps(alerts_sorted)
    history_json = json.dumps(history[-200:])  # last 200 entries max
    charts_json = json.dumps(charts)
    options_json = json.dumps(options_data)

    html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta http-equiv="refresh" content="900">
<title>Market Scanner v5.3 - """ + scan_date + """</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
*{margin:0;padding:0;box-sizing:border-box}
:root{--bg:#0a0e17;--card:#111827;--border:#1e293b;--text:#e2e8f0;--dim:#64748b;--green:#22c55e;--red:#ef4444;--blue:#3b82f6;--cyan:#06b6d4;--purple:#a855f7;--orange:#f97316;--yellow:#eab308}
body{font-family:'Segoe UI',system-ui,sans-serif;background:var(--bg);color:var(--text);font-variant-numeric:tabular-nums;min-height:100vh}
a{color:var(--cyan);text-decoration:none}
.topbar{display:flex;align-items:center;justify-content:space-between;padding:12px 24px;background:#0d1320;border-bottom:1px solid var(--border);flex-wrap:wrap;gap:10px;position:sticky;top:0;z-index:100}
.topbar h1{font-size:18px;font-weight:700;color:var(--cyan)}
.topbar small{font-size:11px;color:var(--dim)}
.badge{padding:3px 10px;border-radius:20px;font-size:11px;font-weight:700;display:inline-block}
.badge-bull{background:#22c55e22;color:var(--green)}.badge-bear{background:#ef444422;color:var(--red)}.badge-neutral{background:#eab30822;color:var(--yellow)}
.meta{display:flex;align-items:center;gap:14px;font-size:12px;color:var(--dim)}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:14px;padding:18px 24px}
.card{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:18px}
.card .label{font-size:11px;color:var(--dim);text-transform:uppercase;letter-spacing:1px;margin-bottom:6px}
.card .value{font-size:26px;font-weight:700}
.card .sub{font-size:12px;color:var(--dim);margin-top:4px}
.tabs{display:flex;gap:0;padding:0 24px;border-bottom:1px solid var(--border);background:#0d1320}
.tab{padding:12px 24px;cursor:pointer;font-size:14px;font-weight:600;color:var(--dim);border-bottom:3px solid transparent;transition:all .2s}
.tab:hover{color:var(--text)}.tab.active{color:var(--cyan);border-bottom-color:var(--cyan)}
.tc{display:none;padding:20px 24px}.tc.active{display:block}
.toolbar{display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;flex-wrap:wrap;gap:10px}
.filters{display:flex;gap:6px;flex-wrap:wrap}
.fbtn{padding:6px 14px;border-radius:20px;border:1px solid var(--border);background:transparent;color:var(--dim);cursor:pointer;font-size:12px;font-weight:600;transition:all .2s}
.fbtn:hover,.fbtn.active{background:var(--cyan);color:#000;border-color:var(--cyan)}
table{width:100%;border-collapse:collapse;font-size:13px}
thead th{background:#0d1320;color:var(--dim);font-size:11px;text-transform:uppercase;letter-spacing:.5px;padding:10px 12px;cursor:pointer;text-align:left;border-bottom:1px solid var(--border)}
thead th:hover{color:var(--cyan)}
tbody tr{border-bottom:1px solid #1e293b44;transition:background .15s}
tbody tr:hover{background:#1e293b55}
td{padding:10px 12px;white-space:nowrap}
.ticker{font-weight:700;color:var(--cyan);cursor:pointer}
.sig{padding:3px 10px;border-radius:4px;font-size:11px;font-weight:700}
.sig-b{background:#22c55e22;color:var(--green)}.sig-t{background:#ef444422;color:var(--red)}
.st{padding:3px 8px;border-radius:4px;font-size:10px;font-weight:700}
.st-r{background:#3b82f622;color:var(--blue)}.st-l{background:#a855f722;color:var(--purple)}.st-h{background:#f9731622;color:var(--orange)}
.sb{display:flex;align-items:center;gap:6px}.sb .bar{width:50px;height:6px;background:#1e293b;border-radius:3px;overflow:hidden}.sb .bar .fill{height:100%;border-radius:3px}
.ppos{color:var(--green)}.pneg{color:var(--red)}.pz{color:var(--dim)}
.ibtn{background:none;border:none;color:var(--dim);cursor:pointer;font-size:16px;padding:4px;transition:color .2s}.ibtn:hover{color:var(--cyan)}
.empty{text-align:center;padding:60px;color:var(--dim)}.empty .ico{font-size:48px;margin-bottom:12px}
.footer{text-align:center;padding:20px;color:var(--dim);font-size:11px;border-top:1px solid var(--border);margin-top:20px}
.chart-container{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:16px;margin-bottom:16px}
.opt-cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:16px;margin-bottom:20px}
.opt-card{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:16px}
.opt-card h3{color:var(--cyan);margin-bottom:10px;font-size:15px}
.legs{width:100%;font-size:12px;margin-bottom:10px}
.legs th{padding:5px 8px;color:var(--dim);font-size:10px;border-bottom:1px solid var(--border)}
.legs td{padding:5px 8px}
.ss{display:grid;grid-template-columns:1fr 1fr;gap:4px;font-size:12px}
.ss .k{color:var(--dim)}.ss .v{font-weight:700;text-align:right}
.chain-grid{display:grid;grid-template-columns:1fr 1fr;gap:16px}
.sum-row{display:flex;gap:16px;margin-bottom:16px;flex-wrap:wrap}
.sum-s{background:var(--card);border:1px solid var(--border);border-radius:8px;padding:12px 20px;text-align:center}
.sum-s .num{font-size:22px;font-weight:700}.sum-s .lbl{font-size:11px;color:var(--dim);text-transform:uppercase;margin-top:2px}
@media(max-width:768px){.cards{grid-template-columns:1fr 1fr}.chain-grid{grid-template-columns:1fr}td,th{padding:6px 8px;font-size:12px}.topbar{flex-direction:column;text-align:center}}
</style>
</head>
<body>
<div class="topbar">
  <div><h1>&#9889; Market Scanner v5.3</h1><small>12-Indicator - Multi-Timeframe - GitHub Actions</small></div>
  <div class="meta"><span class="badge """ + regime_cls + '\">' + regime + """</span><span>Scanned: <b>""" + scan_time + """</b></span><span>""" + str(num_stocks) + """ stocks</span></div>
</div>
<div class="cards">
  <div class="card"><div class="label">Today's Alerts</div><div class="value">""" + str(total) + """</div><div class="sub">&#9650; """ + str(bottoms) + """ Bottom / &#9660; """ + str(tops) + """ Top</div></div>
  <div class="card"><div class="label">Market Regime</div><div class="value">""" + regime + """</div><div class="sub">SPY RSI: """ + str(round(spy_rsi, 1)) + """</div></div>
  <div class="card"><div class="label">30-Day History</div><div class="value">""" + str(hist_total) + """</div><div class="sub">W:""" + str(hist_wins) + """ / L:""" + str(hist_losses) + """</div></div>
  <div class="card"><div class="label">Platform</div><div class="value" style="font-size:18px">GitHub Actions</div><div class="sub">Auto-refreshes every 15 min</div></div>
</div>
<div class="tabs">
  <div class="tab active" onclick="showTab('live')">&#128225; Live Alerts</div>
  <div class="tab" onclick="showTab('history')">&#128202; History (30d)</div>
  <div class="tab" onclick="showTab('charts')">&#128200; Charts</div>
  <div class="tab" onclick="showTab('options')">&#9881; Options</div>
</div>

<!-- LIVE TAB -->
<div class="tc active" id="tab-live">
  <div class="toolbar">
    <div class="filters" id="fbar">
      <button class="fbtn active" onclick="flt('ALL')">ALL """ + str(total) + """</button>
      <button class="fbtn" onclick="flt('BOTTOM')">&#9650; BOTTOM """ + str(bottoms) + """</button>
      <button class="fbtn" onclick="flt('TOP')">&#9660; TOP """ + str(tops) + """</button>
    </div>
  </div>
  <div style="overflow-x:auto"><table><thead><tr>
    <th onclick="srt('ticker')">Ticker</th><th onclick="srt('signal')">Signal</th>
    <th onclick="srt('alert_price')">Alert $</th><th onclick="srt('confidence')">Score</th>
    <th onclick="srt('setup_type')">Setup</th><th onclick="srt('mtf_status')">MTF</th>
    <th>Sector</th><th onclick="srt('rsi')">RSI</th><th onclick="srt('trend_3d')">Trend 3d</th>
    <th>Vol Exp</th><th>PCR</th><th>Signals</th><th>&#128200;</th><th>&#9881;</th>
  </tr></thead><tbody id="liveBody"></tbody></table></div>
  <div class="empty" id="liveEmpty" style="display:none"><div class="ico">&#128225;</div><p>No signals detected.</p></div>
</div>

<!-- HISTORY TAB -->
<div class="tc" id="tab-history">
  <div class="sum-row" id="histSum"></div>
  <div style="overflow-x:auto"><table><thead><tr>
    <th>Date</th><th>Ticker</th><th>Signal</th><th>Alert $</th><th>Score</th><th>Setup</th><th>MTF</th><th>Regime</th><th>Sector</th><th>Trend 3d</th>
  </tr></thead><tbody id="histBody"></tbody></table></div>
  <div class="empty" id="histEmpty" style="display:none"><div class="ico">&#128202;</div><p>No history data yet.</p></div>
</div>

<!-- CHARTS TAB -->
<div class="tc" id="tab-charts">
  <div class="toolbar">
    <div class="filters" id="chartBtns"></div>
    <span style="color:var(--dim);font-size:12px">Click ticker to view chart</span>
  </div>
  <div id="chartArea">
    <div class="empty"><div class="ico">&#128200;</div><p>Select a ticker above to view its chart.</p></div>
  </div>
</div>

<!-- OPTIONS TAB -->
<div class="tc" id="tab-options">
  <div class="toolbar">
    <div class="filters" id="optBtns"></div>
    <span style="color:var(--dim);font-size:12px">Click ticker to view options</span>
  </div>
  <div id="optArea">
    <div class="empty"><div class="ico">&#9881;</div><p>Select a ticker above to view options data.</p></div>
  </div>
</div>

<div class="footer">
  Generated by Market Scanner v5.3 on GitHub Actions &#8226; """ + scan_time + """ &#8226; """ + str(num_stocks) + """ stocks scanned<br>
  <a href="https://stock-hash.github.io/stockusa/">stock-hash.github.io/stockusa</a>
</div>

<script>
var alerts=""" + alerts_json + """;
var history=""" + history_json + """;
var charts=""" + charts_json + """;
var optData=""" + options_json + """;
var cf='ALL',sc='confidence',sd=-1;

function showTab(n){document.querySelectorAll('.tc').forEach(function(t){t.classList.remove('active')});document.querySelectorAll('.tab').forEach(function(t){t.classList.remove('active')});document.getElementById('tab-'+n).classList.add('active');var m={live:0,history:1,charts:2,options:3};document.querySelectorAll('.tab')[m[n]].classList.add('active');if(n==='charts')initChartBtns();if(n==='options')initOptBtns();if(n==='history')renderHistory();}
function sigB(s){return s==='BOTTOM'?'<span class=\"sig sig-b\">&#9650; BOTTOM</span>':'<span class=\"sig sig-t\">&#9660; TOP</span>';}
function stB(s){if(!s)return'-';var c=s.indexOf('LEADER')>=0?'st-l':s.indexOf('HYBRID')>=0?'st-h':'st-r';return'<span class=\"st '+c+'\">'+s+'</span>';}
function scB(v){v=parseInt(v)||0;var c=v>=80?'var(--green)':v>=60?'var(--cyan)':v>=45?'var(--yellow)':'var(--red)';return'<div class=\"sb\">'+v+'<div class=\"bar\"><div class=\"fill\" style=\"width:'+v+'%;background:'+c+'\"></div></div></div>';}
function fp(v){if(!v||v===0)return'<span class=\"pz\">0.00%</span>';return v>0?'<span class=\"ppos\">+'+v.toFixed(2)+'%</span>':'<span class=\"pneg\">'+v.toFixed(2)+'%</span>';}
function et(at){if(!at)return'-';var m=at.match(/(\\d{1,2}:\\d{2}:\\d{2}\\s*[AP]M)/i);return m?m[1]+' ET':at;}

function renderAlerts(){
  var d=alerts.slice();
  if(cf!=='ALL')d=d.filter(function(a){return a.signal===cf;});
  if(sc)d.sort(function(a,b){var va=a[sc],vb=b[sc];if(typeof va==='string')return sd*va.localeCompare(vb);return sd*((va||0)-(vb||0));});
  var body=document.getElementById('liveBody');
  if(!d.length){body.innerHTML='';document.getElementById('liveEmpty').style.display='';return;}
  document.getElementById('liveEmpty').style.display='none';
  var h='';
  for(var i=0;i<d.length;i++){var a=d[i];var sg=(a.signals||[]).join(', ');
    h+='<tr><td class=\"ticker\" onclick=\"showChart(\\''+a.ticker+'\\')\">'+ a.ticker+'</td><td>'+sigB(a.signal)+'</td><td>$'+(a.alert_price||0).toFixed(2)+'</td><td>'+scB(a.confidence)+'</td><td>'+stB(a.setup_type)+'</td><td style=\"color:var(--dim);font-size:12px\">'+(a.mtf_status||'-')+'</td><td style=\"font-size:12px\">'+(a.sector_strength||'-')+'</td><td>'+(a.rsi||0).toFixed(1)+'</td><td>'+fp(a.trend_3d)+'</td><td>'+(a.volume_expansion||1).toFixed(1)+'x</td><td>'+(a.pcr||1).toFixed(2)+'</td><td style=\"color:var(--dim);font-size:11px;max-width:160px;overflow:hidden;text-overflow:ellipsis\">'+sg+'</td><td><button class=\"ibtn\" onclick=\"showChart(\\''+a.ticker+'\\')\" title=\"Chart\">&#128200;</button></td><td><button class=\"ibtn\" onclick=\"showOpt(\\''+a.ticker+'\\')\" title=\"Options\">&#9881;</button></td></tr>';}
  body.innerHTML=h;}
function flt(t){cf=t;document.querySelectorAll('#fbar .fbtn').forEach(function(b){b.classList.remove('active')});event.target.closest('.fbtn').classList.add('active');renderAlerts();}
function srt(c){if(sc===c)sd*=-1;else{sc=c;sd=-1;}renderAlerts();}

function renderHistory(){
  var d=history.slice().reverse();
  document.getElementById('histSum').innerHTML='<div class=\"sum-s\"><div class=\"num\">'+d.length+'</div><div class=\"lbl\">Total (30d)</div></div>';
  var body=document.getElementById('histBody');
  if(!d.length){body.innerHTML='';document.getElementById('histEmpty').style.display='';return;}
  document.getElementById('histEmpty').style.display='none';
  var h='';
  for(var i=0;i<d.length;i++){var a=d[i];
    h+='<tr><td style=\"color:var(--dim)\">'+(a.date||'-')+'</td><td class=\"ticker\" onclick=\"showChart(\\''+a.ticker+'\\')\">'+ a.ticker+'</td><td>'+sigB(a.signal)+'</td><td>$'+(a.alert_price||0).toFixed(2)+'</td><td>'+scB(a.confidence)+'</td><td>'+stB(a.setup_type)+'</td><td style=\"font-size:12px\">'+(a.mtf_status||'-')+'</td><td style=\"font-size:12px\">'+(a.regime||'-')+'</td><td style=\"font-size:12px\">'+(a.sector_strength||'-')+'</td><td>'+fp(a.trend_3d)+'</td></tr>';}
  body.innerHTML=h;}

// Charts
function initChartBtns(){
  var tickers=Object.keys(charts);
  if(!tickers.length){document.getElementById('chartBtns').innerHTML='<span style=\"color:var(--dim)\">No chart data</span>';return;}
  var h='';for(var i=0;i<tickers.length;i++){h+='<button class=\"fbtn\" onclick=\"renderChart(\\''+tickers[i]+'\\')\">'+ tickers[i]+'</button>';}
  document.getElementById('chartBtns').innerHTML=h;
  renderChart(tickers[0]);}
function showChart(t){showTab('charts');setTimeout(function(){renderChart(t);},100);}
function renderChart(t){
  var d=charts[t];if(!d){document.getElementById('chartArea').innerHTML='<div class=\"empty\"><p>No chart data for '+t+'</p></div>';return;}
  var area=document.getElementById('chartArea');
  area.innerHTML='<div class="chart-container"><div id="priceChart" style="height:400px"></div></div><div class="chart-container"><div id="rsiChart" style="height:200px"></div></div><div class="chart-container"><div id="macdChart" style="height:200px"></div></div>';
  var ts=d.timestamps.map(function(x){return x;});
  var layout={paper_bgcolor:'#111827',plot_bgcolor:'#111827',font:{color:'#64748b'},xaxis:{gridcolor:'#1e293b'},yaxis:{gridcolor:'#1e293b'},margin:{l:50,r:20,t:40,b:40},legend:{font:{color:'#64748b'}}};
  Plotly.newPlot('priceChart',[
    {x:ts,y:d.close,type:'scatter',name:'Close',line:{color:'#06b6d4',width:2}},
    {x:ts,y:d.bb_upper,type:'scatter',name:'BB Upper',line:{color:'#64748b',width:1,dash:'dot'}},
    {x:ts,y:d.bb_lower,type:'scatter',name:'BB Lower',line:{color:'#64748b',width:1,dash:'dot'},fill:'tonexty',fillcolor:'rgba(100,116,139,0.05)'},
    {x:ts,y:d.ema9,type:'scatter',name:'EMA9',line:{color:'#22c55e',width:1}},
    {x:ts,y:d.ema21,type:'scatter',name:'EMA21',line:{color:'#ef4444',width:1}},
    {x:ts,y:d.vwap,type:'scatter',name:'VWAP',line:{color:'#eab308',width:1,dash:'dash'}},
  ],Object.assign({},layout,{title:{text:t+' - Price + Indicators',font:{color:'#e2e8f0'}}}),{responsive:true,displayModeBar:false});
  Plotly.newPlot('rsiChart',[{x:ts,y:d.rsi,type:'scatter',name:'RSI',line:{color:'#a855f7',width:2}}],Object.assign({},layout,{title:{text:'RSI',font:{color:'#e2e8f0'}},shapes:[{type:'line',y0:70,y1:70,x0:0,x1:1,xref:'paper',line:{color:'#ef4444',dash:'dash',width:1}},{type:'line',y0:30,y1:30,x0:0,x1:1,xref:'paper',line:{color:'#22c55e',dash:'dash',width:1}}]}),{responsive:true,displayModeBar:false});
  Plotly.newPlot('macdChart',[
    {x:ts,y:d.macd_line,type:'scatter',name:'MACD',line:{color:'#06b6d4',width:2}},
    {x:ts,y:d.macd_signal,type:'scatter',name:'Signal',line:{color:'#f97316',width:1}},
    {x:ts,y:d.macd_hist,type:'bar',name:'Histogram',marker:{color:d.macd_hist.map(function(v){return v>=0?'#22c55e44':'#ef444444';})}}
  ],Object.assign({},layout,{title:{text:'MACD',font:{color:'#e2e8f0'}}}),{responsive:true,displayModeBar:false});
  document.querySelectorAll('#chartBtns .fbtn').forEach(function(b){b.classList.remove('active');if(b.textContent===t)b.classList.add('active');});}

// Options
function initOptBtns(){
  var tickers=Object.keys(optData);
  if(!tickers.length){document.getElementById('optBtns').innerHTML='<span style=\"color:var(--dim)\">No options data</span>';return;}
  var h='';for(var i=0;i<tickers.length;i++){h+='<button class=\"fbtn\" onclick=\"renderOpt(\\''+tickers[i]+'\\')\">'+ tickers[i]+'</button>';}
  document.getElementById('optBtns').innerHTML=h;
  renderOpt(tickers[0]);}
function showOpt(t){showTab('options');setTimeout(function(){renderOpt(t);},100);}
function renderOpt(t){
  var d=optData[t];
  if(!d||d.error){document.getElementById('optArea').innerHTML='<div class=\"empty\"><p>No options data for '+t+(d&&d.error?' - '+d.error:'')+'</p></div>';return;}
  var area=document.getElementById('optArea');
  var h='<div style=\"display:flex;align-items:center;gap:16px;margin-bottom:16px;flex-wrap:wrap\"><h2 style=\"color:var(--cyan)\">'+t+'</h2><span style=\"font-size:20px;font-weight:700\">$'+d.spot+'</span><span class=\"badge '+(d.signal==='BOTTOM'?'badge-bull':'badge-bear')+'\">'+(d.signal==='BOTTOM'?'BULLISH':'BEARISH')+'</span><span style=\"color:var(--dim)\">Exp: '+d.expiration+' ('+d.dte+'d)</span><span style=\"color:var(--dim)\">IV: '+d.avg_iv+'%</span><span style=\"color:var(--dim)\">PCR: '+d.pcr+'</span></div>';
  if(d.strategies&&d.strategies.length){
    h+='<h3 style=\"color:var(--dim);margin-bottom:10px;font-size:13px\">STRATEGIES</h3><div class=\"opt-cards\">';
    for(var i=0;i<d.strategies.length;i++){var s=d.strategies[i];
      h+='<div class=\"opt-card\"><h3>'+s.name+'</h3><table class=\"legs\"><thead><tr><th>Action</th><th>Type</th><th>Strike</th><th>Price</th></tr></thead><tbody>';
      for(var j=0;j<s.legs.length;j++){var l=s.legs[j];var ac=l.action==='BUY'?'color:var(--green)':'color:var(--red)';
        h+='<tr><td style=\"'+ac+';font-weight:700\">'+l.action+'</td><td>'+l.type+'</td><td>$'+l.strike+'</td><td>$'+l.price.toFixed(2)+'</td></tr>';}
      h+='</tbody></table><div class=\"ss\"><span class=\"k\">Cost</span><span class=\"v\">$'+(typeof s.cost==='number'?s.cost.toFixed(2):s.cost)+'</span><span class=\"k\">Max Profit</span><span class=\"v\" style=\"color:var(--green)\">'+(typeof s.max_profit==='number'?'$'+s.max_profit.toFixed(2):s.max_profit)+'</span><span class=\"k\">Breakeven</span><span class=\"v\">$'+(typeof s.breakeven==='number'?s.breakeven.toFixed(2):s.breakeven)+'</span></div></div>';}
    h+='</div>';}
  if(d.payoff&&d.payoff.length){h+='<div class="chart-container"><div id="payoffChart" style="height:280px"></div></div>';}
  if(d.unusual&&d.unusual.length){
    h+='<h3 style=\"color:var(--dim);margin:16px 0 10px;font-size:13px\">UNUSUAL ACTIVITY</h3><table><thead><tr><th>Strike</th><th>Type</th><th>Volume</th><th>OI</th><th>Ratio</th></tr></thead><tbody>';
    for(var i=0;i<d.unusual.length;i++){var u=d.unusual[i];h+='<tr><td>$'+u.strike+'</td><td><span class=\"badge '+(u.type==='CALL'?'badge-bull':'badge-bear')+'\">'+u.type+'</span></td><td>'+u.volume.toLocaleString()+'</td><td>'+u.oi.toLocaleString()+'</td><td style=\"color:var(--orange);font-weight:700\">'+u.ratio+'x</td></tr>';}
    h+='</tbody></table>';}
  if(d.calls||d.puts){
    h+='<h3 style=\"color:var(--dim);margin:16px 0 10px;font-size:13px\">OPTIONS CHAIN</h3><div class=\"chain-grid\">';
    h+='<div><h4 style=\"color:var(--green);margin-bottom:6px\">CALLS</h4><div style=\"max-height:300px;overflow-y:auto\"><table><thead><tr><th>Strike</th><th>Bid</th><th>Ask</th><th>IV</th><th>Delta</th><th>Vol</th></tr></thead><tbody>';
    for(var i=0;i<(d.calls||[]).length;i++){var c=d.calls[i];h+='<tr'+(c.itm?' style=\"background:#22c55e08\"':'')+'><td>$'+c.strike+'</td><td>'+c.bid.toFixed(2)+'</td><td>'+c.ask.toFixed(2)+'</td><td>'+c.iv+'%</td><td>'+c.delta.toFixed(2)+'</td><td>'+c.volume+'</td></tr>';}
    h+='</tbody></table></div></div>';
    h+='<div><h4 style=\"color:var(--red);margin-bottom:6px\">PUTS</h4><div style=\"max-height:300px;overflow-y:auto\"><table><thead><tr><th>Strike</th><th>Bid</th><th>Ask</th><th>IV</th><th>Delta</th><th>Vol</th></tr></thead><tbody>';
    for(var i=0;i<(d.puts||[]).length;i++){var p=d.puts[i];h+='<tr'+(p.itm?' style=\"background:#ef444408\"':'')+'><td>$'+p.strike+'</td><td>'+p.bid.toFixed(2)+'</td><td>'+p.ask.toFixed(2)+'</td><td>'+p.iv+'%</td><td>'+p.delta.toFixed(2)+'</td><td>'+p.volume+'</td></tr>';}
    h+='</tbody></table></div></div></div>';}
  area.innerHTML=h;
  if(d.payoff&&d.payoff.length){
    var px=d.payoff.map(function(p){return p.price;});var pn=d.payoff.map(function(p){return p.pnl;});
    Plotly.newPlot('payoffChart',[{x:px,y:pn,type:'scatter',fill:'tozeroy',line:{color:'#06b6d4',width:2},fillcolor:'rgba(6,182,212,0.1)'}],{paper_bgcolor:'#111827',plot_bgcolor:'#111827',title:{text:'Payoff at Expiration',font:{color:'#64748b',size:13}},xaxis:{title:'Stock Price',color:'#64748b',gridcolor:'#1e293b'},yaxis:{title:'P&L ($)',color:'#64748b',gridcolor:'#1e293b',zeroline:true,zerolinecolor:'#334155'},margin:{l:50,r:20,t:40,b:40},shapes:[{type:'line',x0:d.spot,x1:d.spot,y0:0,y1:1,yref:'paper',line:{color:'#eab308',dash:'dash',width:1}}]},{responsive:true,displayModeBar:false});}
  document.querySelectorAll('#optBtns .fbtn').forEach(function(b){b.classList.remove('active');if(b.textContent===t)b.classList.add('active');});}

renderAlerts();
</script>
</body></html>"""
    return html


# ================================================================
# MAIN
# ================================================================

def main():
    logger.info("=" * 65)
    logger.info("MARKET SCANNER v5.3 - GITHUB ACTIONS (FULL FEATURES)")
    logger.info("=" * 65)

    # Run scan
    alerts, regime, spy_rsi = run_single_scan()

    # Fetch chart data for alerted tickers
    charts = {}
    for a in alerts:
        t = a["ticker"]
        if t not in charts:
            logger.info("  Fetching chart: %s", t)
            cd = fetch_chart_data(t)
            if cd:
                charts[t] = cd
            time.sleep(0.5)
    logger.info("Chart data: %d tickers", len(charts))

    # Fetch options data for alerted tickers
    options = {}
    for a in alerts:
        t = a["ticker"]
        if t not in options:
            logger.info("  Fetching options: %s", t)
            od = fetch_options_data(t, a.get("signal", "BOTTOM"))
            if od:
                options[t] = od
            time.sleep(0.5)
    logger.info("Options data: %d tickers", len(options))

    # Load and update history
    history = load_history()
    for a in alerts:
        history.append(a)
    history = save_history(history)
    logger.info("History: %d total entries (30d rolling)", len(history))

    # Generate HTML
    html = generate_html(alerts, regime, spy_rsi, history, charts, options)

    # Write output
    docs_dir = os.path.join(script_dir, "docs")
    os.makedirs(docs_dir, exist_ok=True)
    output_path = os.path.join(docs_dir, "TopBottom_Universal.html")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    logger.info("Dashboard: %s (%d bytes)", output_path, len(html))

    # Write JSON
    json_path = os.path.join(docs_dir, "latest_scan.json")
    scan_data = {
        "scan_time": get_eastern_now().strftime("%Y-%m-%d %I:%M:%S %p ET") if SCANNER_IMPORTED else "",
        "regime": regime, "spy_rsi": spy_rsi,
        "total_alerts": len(alerts),
        "stocks_scanned": len(ALL_STOCKS) if ALL_STOCKS else 260,
        "alerts": alerts,
    }
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(scan_data, f, indent=2)

    # Email
    if alerts:
        try:
            et_now = get_eastern_now().strftime("%Y-%m-%d") if SCANNER_IMPORTED else ""
            send_email("v5.3 GitHub - " + str(len(alerts)) + " signals - " + et_now, html)
        except Exception:
            pass

    logger.info("GitHub Actions scan complete!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
