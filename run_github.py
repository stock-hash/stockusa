#!/usr/bin/env python3
"""
MARKET SCANNER v5 — GITHUB ACTIONS RUNNER
Runs a single scan cycle, reads/writes JSON files in docs/data/
"""

import yfinance as yf
import pandas as pd
import numpy as np
import json, os, sys, time, random, warnings, logging, smtplib
from datetime import datetime, timedelta, date
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

warnings.filterwarnings("ignore")

try:
    import pytz
    ET = pytz.timezone("US/Eastern")
except ImportError:
    print("ERROR: pip install pytz")
    sys.exit(1)

# ═══════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════

EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
RECIPIENT_EMAILS = os.getenv("RECIPIENT_EMAILS", "")
RECIPIENT_EMAIL = [a.strip() for a in RECIPIENT_EMAILS.split(",") if a.strip()]

DATA_DIR = os.path.join(os.path.dirname(__file__), "docs", "data")
os.makedirs(DATA_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════
# STOCK UNIVERSE (same as market_scanner_v5.py)
# ═══════════════════════════════════════════════════════

DOW_30 = ["AAPL","MSFT","AMZN","NVDA","UNH","V","JNJ","WMT","JPM","PG",
"MA","HD","MRK","CVX","KO","DIS","MCD","CSCO","ABT","VZ",
"NKE","INTC","SHW","DOW","MMM","TRV","AXP","BA","CAT","GS"]

SP500_TOP = ["GOOGL","GOOG","META","BRK-B","TSLA","XOM","LLY","ABBV",
"PEP","COST","AVGO","TMO","DHR","ACN","ADBE","CRM","TXN",
"NFLX","AMD","CMCSA","NEE","PFE","PM","UPS","RTX","HON",
"LOW","QCOM","UNP","SPGI","ISRG","INTU","AMAT","BKNG",
"GE","SYK","BLK","MDLZ","GILD","ADI","VRTX","REGN",
"LRCX","PANW","KLAC","AMT","SCHW","SNPS","CDNS","ZTS",
"PGR","CB","CME","CI","MO","DUK","SO","ICE","CL",
"MCK","SLB","EOG","APD","PH","CMG","ITW","CTAS","ORLY",
"WELL","FDX","PSA","EMR","MSI","AJG","ROP","SRE","TDG",
"PAYX","MCHP","KEYS","HCA","FAST","KMB","RSG","ODFL",
"FICO","CPRT","CEG","CARR","GWW","VRSK","DXCM","EW",
"A","IDXX","LNG","OTIS","MPWR","TT","IR","AXON",
"DECK","GEHC","HUBB","TRGP","PWR","FSLR","EME",
"XYL","ROK","AME","MLM","VMC","URI","WST","PODD",
"TER","ENTG","ONTO","LSCC","WMS"]

TOP_ETFS = ["SPY","QQQ","IWM","DIA","XLK","XLE","XLF","XLV","XLI",
"XLU","XLB","XLY","XLP","XLRE","XLC","SMH","SOXX"]

LEVERAGED = ["TQQQ","SQQQ","SPXL","SPXS","TNA","TZA","SOXL","SOXS",
"FNGU","FNGD","TECL","TECS","LABU","LABD","FAS","FAZ"]

ALL_STOCKS = list(dict.fromkeys(DOW_30 + SP500_TOP + TOP_ETFS + LEVERAGED))

SECTOR_MAP = {
"AAPL":"XLK","MSFT":"XLK","NVDA":"XLK","AVGO":"XLK","CSCO":"XLK","INTC":"XLK",
"ADBE":"XLK","CRM":"XLK","ACN":"XLK","TXN":"XLK","AMD":"XLK","QCOM":"XLK",
"AMAT":"XLK","LRCX":"XLK","KLAC":"XLK","SNPS":"XLK","CDNS":"XLK","PANW":"XLK",
"INTU":"XLK","FSLR":"XLK","ADI":"XLK",
"GOOGL":"XLC","GOOG":"XLC","META":"XLC","NFLX":"XLC","CMCSA":"XLC","DIS":"XLC","VZ":"XLC",
"AMZN":"XLY","TSLA":"XLY","HD":"XLY","MCD":"XLY","LOW":"XLY","NKE":"XLY",
"BKNG":"XLY","CMG":"XLY","ORLY":"XLY","DECK":"XLY",
"JPM":"XLF","V":"XLF","MA":"XLF","GS":"XLF","BLK":"XLF","AXP":"XLF",
"SCHW":"XLF","SPGI":"XLF","CB":"XLF","CME":"XLF","ICE":"XLF","PGR":"XLF",
"BRK-B":"XLF","TRV":"XLF","AJG":"XLF",
"UNH":"XLV","JNJ":"XLV","LLY":"XLV","ABBV":"XLV","MRK":"XLV","PFE":"XLV",
"TMO":"XLV","DHR":"XLV","ABT":"XLV","SYK":"XLV","ISRG":"XLV","VRTX":"XLV",
"REGN":"XLV","GILD":"XLV","HCA":"XLV","ZTS":"XLV","MCK":"XLV","CI":"XLV",
"CAT":"XLI","BA":"XLI","HON":"XLI","UPS":"XLI","RTX":"XLI","GE":"XLI",
"FDX":"XLI","EMR":"XLI","ITW":"XLI","CTAS":"XLI","PH":"XLI","UNP":"XLI",
"XOM":"XLE","CVX":"XLE","SLB":"XLE","EOG":"XLE","LNG":"XLE","TRGP":"XLE",
"WMT":"XLP","PG":"XLP","KO":"XLP","PEP":"XLP","COST":"XLP","PM":"XLP",
"MDLZ":"XLP","CL":"XLP","MO":"XLP","KMB":"XLP",
"NEE":"XLU","DUK":"XLU","SO":"XLU","CEG":"XLU","SRE":"XLU",
"APD":"XLB","SHW":"XLB","MLM":"XLB","VMC":"XLB",
"AMT":"XLRE","WELL":"XLRE","PSA":"XLRE","OTIS":"XLRE",
}

US_MARKET_HOLIDAYS = [
date(2025,1,1),date(2025,1,20),date(2025,2,17),date(2025,4,18),date(2025,5,26),
date(2025,6,19),date(2025,7,4),date(2025,9,1),date(2025,11,27),date(2025,12,25),
date(2026,1,1),date(2026,1,19),date(2026,2,16),date(2026,4,3),date(2026,5,25),
date(2026,6,19),date(2026,7,3),date(2026,9,7),date(2026,11,26),date(2026,12,25),
]


# ═══════════════════════════════════════════════════════
# TECHNICAL INDICATORS
# ═══════════════════════════════════════════════════════

def get_eastern_now():
    return datetime.now(ET)

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

def calculate_macd(series, fast=12, slow=26, signal=9):
    ef = series.ewm(span=fast, adjust=False).mean()
    es = series.ewm(span=slow, adjust=False).mean()
    ml = ef - es
    sl = ml.ewm(span=signal, adjust=False).mean()
    return ml, sl, ml - sl

def calculate_stochastic(high, low, close, k_period=14, d_period=3):
    ll = low.rolling(window=k_period).min()
    hh = high.rolling(window=k_period).max()
    k = 100 * (close - ll) / (hh - ll)
    return k, k.rolling(window=d_period).mean()

def calculate_bollinger(series, period=20, std_dev=2):
    sma = series.rolling(window=period).mean()
    std = series.rolling(window=period).std()
    return sma + std_dev * std, sma, sma - std_dev * std

def calculate_williams_r(high, low, close, period=14):
    hh = high.rolling(window=period).max()
    ll = low.rolling(window=period).min()
    return -100 * (hh - close) / (hh - ll)

def detect_volume_spike(volume, threshold=1.5):
    return volume > (threshold * volume.rolling(window=20).mean())

def calculate_vwap(high, low, close, volume):
    tp = (high + low + close) / 3.0
    return (tp * volume).cumsum() / volume.cumsum()

def calculate_mfi(high, low, close, volume, period=14):
    tp = (high + low + close) / 3.0
    mf = tp * volume
    pmf = mf.where(tp > tp.shift(1), 0.0).rolling(period).sum()
    nmf = mf.where(tp <= tp.shift(1), 0.0).rolling(period).sum()
    mfr = pmf / nmf.where(nmf > 0, np.nan)
    return 100 - (100 / (1 + mfr))

def calculate_obv(close, volume):
    sign = np.where(close > close.shift(1), 1, np.where(close < close.shift(1), -1, 0))
    return (pd.Series(sign, index=close.index) * volume).cumsum()

def calculate_cmf(high, low, close, volume, period=20):
    clv = ((close - low) - (high - close)) / (high - low).where((high - low) > 0, np.nan)
    return (clv * volume).rolling(period).sum() / volume.rolling(period).sum()

def calculate_volume_direction(close, volume, period=10):
    try:
        pc = close.diff()
        bv = volume.where(pc > 0, 0.0).rolling(period).sum()
        sv = volume.where(pc < 0, 0.0).rolling(period).sum()
        t = bv + sv
        return safe_val(bv / t.where(t > 0, np.nan), 0.5)
    except Exception:
        return 0.5


# ═══════════════════════════════════════════════════════
# JSON FILE I/O (replaces SQLite)
# ═══════════════════════════════════════════════════════

def read_json(filename, default=None):
    path = os.path.join(DATA_DIR, filename)
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return default if default is not None else []

def write_json(filename, data):
    path = os.path.join(DATA_DIR, filename)
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)
    logger.info("  Written: %s", path)


# ═══════════════════════════════════════════════════════
# BATCH QUICK SCAN (Pass 1)
# ═══════════════════════════════════════════════════════

def batch_quick_scan(stock_list):
    logger.info("  Batch scanning %d stocks...", len(stock_list))
    filtered = []
    try:
        bd = yf.download(stock_list, period="5d", interval="5m",
                         progress=False, auto_adjust=True, threads=False, group_by="ticker")
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
                if c.dropna().empty:
                    continue
                rv = safe_val(calculate_rsi(c), 50)
                sv = safe_val(calculate_stochastic(h, lo, c)[0], 50)
                wv = safe_val(calculate_williams_r(h, lo, c), -50)
                top = (1 if rv > 65 else 0) + (1 if sv > 75 else 0) + (1 if wv > -25 else 0)
                bot = (1 if rv < 35 else 0) + (1 if sv < 25 else 0) + (1 if wv < -75 else 0)
                if top >= 2 or bot >= 2:
                    filtered.append(ticker)
            except Exception:
                continue
    except Exception as e:
        logger.error("  Batch error: %s", e)
    return filtered


# ═══════════════════════════════════════════════════════
# SIMPLIFIED ANALYSIS (Pass 2)
# ═══════════════════════════════════════════════════════

def analyze_stock(ticker):
    """Simplified v5 analysis — returns signal dict or None."""
    try:
        time.sleep(0.5 + random.uniform(0.2, 0.8))
        data = yf.download(ticker, period="5d", interval="5m",
                           progress=False, auto_adjust=True, threads=False)
        if data is None or data.empty or len(data) < 30:
            return None
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)

        cl = data["Close"]; hi = data["High"]; lo = data["Low"]
        vo = data["Volume"]; op = data["Open"]

        rsi = calculate_rsi(cl)
        _, _, macd_hist = calculate_macd(cl)
        sk, sd = calculate_stochastic(hi, lo, cl)
        bbu, _, bbl = calculate_bollinger(cl)
        wr = calculate_williams_r(hi, lo, cl)
        vd = calculate_volume_direction(cl, vo)
        mfi = calculate_mfi(hi, lo, cl, vo)
        obv = calculate_obv(cl, vo)
        cmf = calculate_cmf(hi, lo, cl, vo)

        rv = safe_val(rsi, 50)
        mh = safe_val(macd_hist, 0)
        mhp = safe_val(macd_hist.shift(1), 0)
        skv = safe_val(sk, 50)
        sdv = safe_val(sd, 50)
        wrv = safe_val(wr, -50)
        clv = safe_val(cl, 0)
        mfv = safe_val(mfi, 50)
        cmfv = safe_val(cmf, 0)
        obv_rising = safe_val(obv, 0) > safe_val(obv.shift(5), 0) if len(obv) > 5 else False

        # Bottom scoring
        bs = 0
        bsg = []
        if mh > mhp and mhp < 0: bs += 15; bsg.append("MACD+")
        if skv > sdv and skv < 30: bs += 15; bsg.append("StochX")
        elif skv < 20: bs += 8
        if rv < 30: bs += 10; bsg.append("RSI<30")
        elif rv < 35: bs += 5
        if wrv < -80: bs += 10; bsg.append("WR<-80")
        if vd > 0.55: bs += 10; bsg.append("BuyVol")
        if mfv < 20: bs += 12; bsg.append("MFI<20")
        elif mfv < 30: bs += 6
        if obv_rising and rv < 40: bs += 10; bsg.append("OBV+")
        if cmfv > 0 and rv < 40: bs += 10; bsg.append("CMF+")

        # Top scoring
        ts = 0
        tsg = []
        if mh < mhp and mhp > 0: ts += 15; tsg.append("MACD-")
        if skv < sdv and skv > 70: ts += 15; tsg.append("StochX")
        elif skv > 80: ts += 8
        if rv > 70: ts += 10; tsg.append("RSI>70")
        elif rv > 65: ts += 5
        if wrv > -20: ts += 10; tsg.append("WR>-20")
        if vd < 0.45: ts += 10; tsg.append("SellVol")
        if mfv > 80: ts += 12; tsg.append("MFI>80")
        elif mfv > 70: ts += 6
        if not obv_rising and rv > 60: ts += 10; tsg.append("OBV-")
        if cmfv < 0 and rv > 60: ts += 10; tsg.append("CMF-")

        sig = None; score = 0; sigs = []
        if bs > ts and bs >= 40:
            sig = "BOTTOM"; score = min(bs, 100); sigs = bsg
        elif ts > bs and ts >= 40:
            sig = "TOP"; score = min(ts, 100); sigs = tsg

        if sig is None:
            return None

        sector = SECTOR_MAP.get(ticker, "SPY")
        now_et = get_eastern_now().strftime("%Y-%m-%d %I:%M:%S %p ET")

        return {
            "ticker": ticker, "signal": sig, "confidence": score,
            "alert_price": round(clv, 2), "alert_time": now_et,
            "date": get_eastern_now().strftime("%Y-%m-%d"),
            "rsi": round(rv, 1), "mfi": round(mfv, 1), "cmf": round(cmfv, 3),
            "regime": "NEUTRAL", "sector_strength": "NEUTRAL",
            "setup_type": "REVERSAL", "signals_text": ", ".join(sigs),
            "current_price": round(clv, 2), "pnl": 0, "pnl_pct": 0,
            "status": "PENDING", "result": "PENDING",
            "mtf_status": "5m", "sector_override": 0,
            "trend_3d": 0, "trend_5d": 0, "volume_expansion": 1.0,
            "time_elapsed": "just now", "pcr": 1.0,
        }
    except Exception:
        return None


# ═══════════════════════════════════════════════════════
# UPDATE LIVE PRICES FOR EXISTING ALERTS
# ═══════════════════════════════════════════════════════

def update_live_prices(alerts):
    """Fetch current prices and update P&L for existing alerts."""
    if not alerts:
        return alerts
    tickers = list(set(a["ticker"] for a in alerts))
    prices = {}
    for ticker in tickers:
        try:
            d = yf.download(ticker, period="1d", interval="1d",
                            progress=False, auto_adjust=True, threads=False)
            if d is not None and not d.empty:
                if isinstance(d.columns, pd.MultiIndex):
                    d.columns = d.columns.get_level_values(0)
                val = d["Close"].iloc[-1]
                if hasattr(val, 'iloc'):
                    val = val.iloc[0]
                prices[ticker] = round(float(val), 2)
        except Exception:
            pass

    now = get_eastern_now()
    for a in alerts:
        cp = prices.get(a["ticker"], 0)
        ap = float(a.get("alert_price", 0) or 0)
        if cp > 0 and ap > 0:
            a["current_price"] = cp
            a["pnl"] = round(cp - ap, 2)
            a["pnl_pct"] = round((cp - ap) / ap * 100, 2)
            sig = a.get("signal", "")
            pnl = a["pnl"]
            if abs(a["pnl_pct"]) < 0.05:
                a["status"] = "FLAT"
            elif sig == "BOTTOM":
                a["status"] = "WIN" if pnl > 0 else "LOSS"
            elif sig == "TOP":
                a["status"] = "WIN" if pnl < 0 else "LOSS"
        # Time elapsed
        try:
            at = a.get("alert_time", "")
            if at and "ET" in at:
                at_clean = at.replace(" ET", "").strip()
                alert_dt = datetime.strptime(at_clean, "%Y-%m-%d %I:%M:%S %p")
                diff = now.replace(tzinfo=None) - alert_dt
                mins = int(diff.total_seconds() / 60)
                if mins < 60:
                    a["time_elapsed"] = str(mins) + "m ago"
                else:
                    a["time_elapsed"] = str(mins // 60) + "h " + str(mins % 60) + "m ago"
        except Exception:
            pass
    return alerts


# ═══════════════════════════════════════════════════════
# EMAIL
# ═══════════════════════════════════════════════════════

def send_email(subject, html_content):
    if not EMAIL_ADDRESS or not EMAIL_PASSWORD or not RECIPIENT_EMAIL:
        logger.warning("Email not configured.")
        return
    try:
        with smtplib.SMTP(SMTP_SERVER, 587, timeout=60) as server:
            server.starttls()
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            for r in RECIPIENT_EMAIL:
                msg = MIMEMultipart("alternative")
                msg["Subject"] = subject
                msg["From"] = EMAIL_ADDRESS
                msg["To"] = r
                msg.attach(MIMEText(html_content, "html"))
                server.sendmail(EMAIL_ADDRESS, [r], msg.as_string())
        logger.info("Email sent: %s", subject)
    except Exception as e:
        logger.error("Email error: %s", e)


# ═══════════════════════════════════════════════════════
# MAIN — SINGLE SCAN CYCLE
# ═══════════════════════════════════════════════════════

def main():
    now = get_eastern_now()
    logger.info("=" * 55)
    logger.info("SCANNER GITHUB — %s", now.strftime("%Y-%m-%d %I:%M %p ET"))
    logger.info("Stocks: %d", len(ALL_STOCKS))
    logger.info("=" * 55)

    # Check market hours
    if now.weekday() >= 5:
        logger.info("WEEKEND — skip")
        return
    if now.date() in US_MARKET_HOLIDAYS:
        logger.info("HOLIDAY — skip")
        return
    mkt_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    mkt_close = now.replace(hour=16, minute=5, second=0, microsecond=0)
    if now < mkt_open or now > mkt_close:
        logger.info("Outside market hours — skip")
        return

    today_str = now.strftime("%Y-%m-%d")

    # Read existing alerts
    all_alerts = read_json("alerts_today.json", [])
    # Filter to today only
    all_alerts = [a for a in all_alerts if a.get("date") == today_str]
    existing_keys = set(a["ticker"] + "_" + a["signal"] for a in all_alerts)

    # PASS 1: Quick scan
    logger.info("PASS 1: Batch scan %d stocks...", len(ALL_STOCKS))
    filtered = batch_quick_scan(ALL_STOCKS)
    logger.info("PASS 1: %d/%d passed", len(filtered), len(ALL_STOCKS))

    # PASS 2: Full analysis
    new_alerts = []
    if filtered:
        logger.info("PASS 2: Analyzing %d stocks...", len(filtered))
        for ticker in filtered:
            key = ticker + "_BOTTOM"
            key2 = ticker + "_TOP"
            result = analyze_stock(ticker)
            if result:
                ak = result["ticker"] + "_" + result["signal"]
                if ak not in existing_keys:
                    new_alerts.append(result)
                    existing_keys.add(ak)
                    logger.info("  ★ NEW: %s %s @ $%.2f (score=%d)",
                                result["ticker"], result["signal"],
                                result["alert_price"], result["confidence"])

    # Merge new alerts
    all_alerts.extend(new_alerts)

    # Update live prices for ALL alerts
    logger.info("Updating live prices for %d alerts...", len(all_alerts))
    all_alerts = update_live_prices(all_alerts)

    # Write today's alerts
    write_json("alerts_today.json", all_alerts)

    # Update weekly data
    week_alerts = read_json("alerts_week.json", [])
    week_cutoff = (now - timedelta(days=7)).strftime("%Y-%m-%d")
    week_alerts = [a for a in week_alerts if a.get("date", "") >= week_cutoff]
    # Add new alerts to weekly
    for a in new_alerts:
        week_alerts.append(a)
    # Update prices in weekly for today's alerts
    for wa in week_alerts:
        if wa.get("date") == today_str:
            for ta in all_alerts:
                if ta.get("ticker") == wa.get("ticker") and ta.get("alert_time") == wa.get("alert_time"):
                    wa["current_price"] = ta.get("current_price", 0)
                    wa["pnl"] = ta.get("pnl", 0)
                    wa["pnl_pct"] = ta.get("pnl_pct", 0)
                    wa["status"] = ta.get("status", "PENDING")
    write_json("alerts_week.json", week_alerts)

    # Status file
    wins = sum(1 for a in all_alerts if a.get("status") == "WIN")
    losses = sum(1 for a in all_alerts if a.get("status") == "LOSS")
    wr = round(wins / (wins + losses) * 100, 1) if (wins + losses) > 0 else 0
    write_json("status.json", {
        "last_scan": now.strftime("%Y-%m-%d %I:%M:%S %p ET"),
        "date": today_str,
        "total_stocks": len(ALL_STOCKS),
        "alerts_today": len(all_alerts),
        "new_this_scan": len(new_alerts),
        "win_rate": wr,
        "wins": wins,
        "losses": losses,
        "regime": "NEUTRAL",
        "data_source": "yahoo",
    })

    # Send email for new alerts
    if new_alerts:
        rows = ""
        for a in new_alerts:
            c = "#fce8e6" if a["signal"] == "TOP" else "#e6f4ea"
            rows += '<tr style="background:%s">' % c
            rows += '<td><b>%s</b></td><td>%s</td><td>$%.2f</td><td>%d</td><td>%s</td><td>%s</td></tr>' % (
                a["ticker"], a["signal"], a["alert_price"], a["confidence"],
                a["signals_text"], a["alert_time"])
        html = '<html><body><h2>Scanner Alert — %d signal(s)</h2>' % len(new_alerts)
        html += '<table border="1" cellpadding="5"><tr style="background:#2c3e50;color:white">'
        html += '<th>Ticker</th><th>Signal</th><th>Price</th><th>Score</th><th>Signals</th><th>Time</th></tr>'
        html += rows + '</table></body></html>'
        send_email("Scanner Alert — %d signal(s) — %s" % (len(new_alerts), now.strftime("%I:%M %p ET")), html)

    logger.info("Scan complete: %d total alerts, %d new", len(all_alerts), len(new_alerts))


if __name__ == "__main__":
    main()
