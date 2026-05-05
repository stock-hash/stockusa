#!/usr/bin/env python3
"""
DOW30 Scanner — GitHub Actions Edition
=======================================
Self-contained scanner. No Flask. No database. No WebSocket.
Runs directly on GitHub's free servers every 5 minutes.

Within each 5-minute run it scans TWICE (60 s apart), giving you
an effective scan frequency of ~2-3 minutes during market hours.

GitHub Actions will:
  1. Run this script automatically every 5 minutes, 24/7 — for FREE
  2. Email you the moment a confirmed Top or Bottom fires
  3. Save scan results to docs/data/ so your GitHub Pages dashboard
     updates automatically

Required GitHub Secrets (Settings → Secrets → Actions):
  RESEND_API_KEY   — get free at resend.com (3,000 emails/month free)
  ALERT_EMAIL      — the email address alerts should go to

Optional GitHub Secrets:
  SCAN_LOOPS       — how many scans per run (default: 2)
  LOOP_SLEEP       — seconds between scans (default: 60)

GitHub automatically provides:
  GITHUB_REPOSITORY — used to build your dashboard URL automatically
"""

import os
import sys
import json
import math
import time
import urllib.request
import logging
from datetime import datetime, timezone

try:
    import yfinance as yf
    import pandas as pd
except ImportError:
    sys.exit("ERROR: Run  pip install yfinance pandas  first.")

# ── Logging ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("dow30")

# ── Config (set via GitHub Secrets or environment) ─────────────────────────────

RESEND_API_KEY  = os.environ.get("RESEND_API_KEY",  "")
ALERT_EMAIL     = os.environ.get("ALERT_EMAIL",     "")
SCAN_LOOPS      = int(os.environ.get("SCAN_LOOPS",  "2"))   # scans per GitHub run
LOOP_SLEEP      = int(os.environ.get("LOOP_SLEEP",  "60"))  # seconds between scans

# Auto-detect GitHub Pages URL from the repo name GitHub injects automatically
_repo = os.environ.get("GITHUB_REPOSITORY", "")   # e.g. "johnsmith/dow30-tracker"
if _repo and "/" in _repo:
    _user, _name = _repo.split("/", 1)
    DASHBOARD_URL = f"https://{_user}.github.io/{_name}/"
else:
    DASHBOARD_URL = "https://github.com"   # fallback if run locally

# ── File paths ─────────────────────────────────────────────────────────────────

BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
DATA_DIR     = os.path.join(BASE_DIR, "docs", "data")
LATEST_FILE  = os.path.join(DATA_DIR, "latest.json")
HISTORY_FILE = os.path.join(DATA_DIR, "history.json")
MAX_HISTORY  = 500   # keep the last 500 signal events

os.makedirs(DATA_DIR, exist_ok=True)

# ── Dow 30 symbols ─────────────────────────────────────────────────────────────

DOW_30 = [
    {"symbol": "AAPL",  "name": "Apple Inc."},
    {"symbol": "AMGN",  "name": "Amgen Inc."},
    {"symbol": "AXP",   "name": "American Express"},
    {"symbol": "BA",    "name": "Boeing Co."},
    {"symbol": "CAT",   "name": "Caterpillar Inc."},
    {"symbol": "CRM",   "name": "Salesforce Inc."},
    {"symbol": "CSCO",  "name": "Cisco Systems"},
    {"symbol": "CVX",   "name": "Chevron Corp."},
    {"symbol": "DIS",   "name": "Walt Disney Co."},
    {"symbol": "DOW",   "name": "Dow Inc."},
    {"symbol": "GS",    "name": "Goldman Sachs"},
    {"symbol": "HD",    "name": "Home Depot Inc."},
    {"symbol": "HON",   "name": "Honeywell Intl."},
    {"symbol": "IBM",   "name": "IBM Corp."},
    {"symbol": "INTC",  "name": "Intel Corp."},
    {"symbol": "JNJ",   "name": "Johnson & Johnson"},
    {"symbol": "JPM",   "name": "JPMorgan Chase"},
    {"symbol": "KO",    "name": "Coca-Cola Co."},
    {"symbol": "MCD",   "name": "McDonald's Corp."},
    {"symbol": "MMM",   "name": "3M Company"},
    {"symbol": "MRK",   "name": "Merck & Co."},
    {"symbol": "MSFT",  "name": "Microsoft Corp."},
    {"symbol": "NKE",   "name": "Nike Inc."},
    {"symbol": "PG",    "name": "Procter & Gamble"},
    {"symbol": "TRV",   "name": "Travelers Cos."},
    {"symbol": "UNH",   "name": "UnitedHealth Group"},
    {"symbol": "V",     "name": "Visa Inc."},
    {"symbol": "VZ",    "name": "Verizon Comms."},
    {"symbol": "WBA",   "name": "Walgreens Boots"},
    {"symbol": "WMT",   "name": "Walmart Inc."},
]

# ── Technical indicator helpers ────────────────────────────────────────────────

def compute_ema(values, period):
    result = [math.nan] * len(values)
    if len(values) < period:
        return result
    k = 2.0 / (period + 1)
    result[period - 1] = sum(values[:period]) / period
    for i in range(period, len(values)):
        result[i] = values[i] * k + result[i - 1] * (1 - k)
    return result

def compute_rsi(closes, period=14):
    rsi = [math.nan] * len(closes)
    if len(closes) < period + 1:
        return rsi
    gains  = [max(closes[i] - closes[i-1], 0.0) for i in range(1, len(closes))]
    losses = [max(closes[i-1] - closes[i], 0.0) for i in range(1, len(closes))]
    avg_g  = sum(gains[:period])  / period
    avg_l  = sum(losses[:period]) / period
    rsi[period] = 100.0 if avg_l == 0 else 100.0 - 100.0 / (1 + avg_g / avg_l)
    for i in range(period + 1, len(closes)):
        avg_g = (avg_g * (period - 1) + gains[i-1])  / period
        avg_l = (avg_l * (period - 1) + losses[i-1]) / period
        rsi[i] = 100.0 if avg_l == 0 else 100.0 - 100.0 / (1 + avg_g / avg_l)
    return rsi

def compute_macd(closes, fast=12, slow=26, signal=9):
    ema_f = compute_ema(closes, fast)
    ema_s = compute_ema(closes, slow)
    macd  = [f - s if not (math.isnan(f) or math.isnan(s)) else math.nan
             for f, s in zip(ema_f, ema_s)]
    valid = [(i, v) for i, v in enumerate(macd) if not math.isnan(v)]
    sig_l = [math.nan] * len(macd)
    if len(valid) >= signal:
        ema_sig = compute_ema([v for _, v in valid], signal)
        for j, (i, _) in enumerate(valid):
            if not math.isnan(ema_sig[j]):
                sig_l[i] = ema_sig[j]
    return macd, sig_l

def score_number(status):
    return {"confirmed_top": 8, "confirmed_bottom": 8,
            "pending_top":   5, "pending_bottom":   5}.get(status, 2)

# ── Yahoo Finance fetch ────────────────────────────────────────────────────────

def fetch_ohlcv(symbol, period, interval, retries=2):
    for attempt in range(retries + 1):
        try:
            df = yf.Ticker(symbol).history(
                period=period, interval=interval, auto_adjust=True
            )
            if df is None or df.empty:
                return []
            candles = []
            for ts, row in df.iterrows():
                if any(pd.isna([row["Open"], row["High"], row["Low"], row["Close"]])):
                    continue
                candles.append({
                    "time":   int(ts.timestamp()),
                    "open":   round(float(row["Open"]),  4),
                    "high":   round(float(row["High"]),  4),
                    "low":    round(float(row["Low"]),   4),
                    "close":  round(float(row["Close"]), 4),
                    "volume": int(row["Volume"]) if not pd.isna(row["Volume"]) else 0,
                })
            return candles
        except Exception as exc:
            if attempt < retries:
                log.warning(f"  Retry {attempt+1} for {symbol} {interval}: {exc}")
                time.sleep(2)
            else:
                log.warning(f"  Failed {symbol} {interval}: {exc}")
                return []

# ── Swing detection ────────────────────────────────────────────────────────────

def detect_swing(candles, lookback=3):
    result = {"high_idx": -1, "high_price": None, "low_idx": -1, "low_price": None}
    n = len(candles)
    if n < lookback * 2 + 1:
        return result
    for i in range(n - 2, lookback - 1, -1):
        c = candles[i]
        if result["high_idx"] == -1:
            if all(candles[i-k]["high"] < c["high"] and candles[i+k]["high"] < c["high"]
                   for k in range(1, lookback + 1) if i-k >= 0 and i+k < n):
                result["high_idx"]  = i
                result["high_price"] = c["high"]
        if result["low_idx"] == -1:
            if all(candles[i-k]["low"] > c["low"] and candles[i+k]["low"] > c["low"]
                   for k in range(1, lookback + 1) if i-k >= 0 and i+k < n):
                result["low_idx"]  = i
                result["low_price"] = c["low"]
        if result["high_idx"] != -1 and result["low_idx"] != -1:
            break
    return result

# ── Email alert (HTML formatted) ───────────────────────────────────────────────

def send_email(symbol, name, signal_type, price, change_pct, rsi_val=None, macd_val=None):
    if not RESEND_API_KEY or not ALERT_EMAIL:
        log.info("  Email skipped — RESEND_API_KEY or ALERT_EMAIL not set.")
        return

    type_labels = {
        "top":          ("▼ Confirmed TOP",  "#f87171", "⚠ SELL SIGNAL"),
        "bottom":       ("▲ Confirmed BOTTOM","#4ade80", "✅ BUY SIGNAL"),
        "price_target": ("🎯 Price Target Hit","#818cf8","🎯 PRICE TARGET"),
    }
    label, color, tag = type_labels.get(signal_type, (signal_type, "#94a3b8", "SIGNAL"))
    chg_sign = "+" if change_pct >= 0 else ""
    rsi_str  = f"{rsi_val:.1f}" if rsi_val is not None and not math.isnan(rsi_val) else "N/A"
    macd_str = f"{macd_val:+.4f}" if macd_val is not None and not math.isnan(macd_val) else "N/A"
    now_str  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    rsi_note = ""
    if rsi_val and not math.isnan(rsi_val):
        if rsi_val >= 70:   rsi_note = " (Overbought ⚠)"
        elif rsi_val <= 30: rsi_note = " (Oversold ✅)"

    html = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"/></head>
<body style="background:#0f172a;color:#f1f5f9;font-family:system-ui,sans-serif;padding:24px;margin:0;">
  <div style="max-width:480px;margin:0 auto;">
    <div style="background:#1e293b;border-radius:12px;padding:24px;border:1px solid #334155;">

      <div style="font-size:11px;font-weight:700;letter-spacing:.1em;color:{color};
                  text-transform:uppercase;margin-bottom:8px;">{tag}</div>

      <h1 style="margin:0 0 4px;font-size:26px;font-weight:900;color:#f8fafc;">
        {symbol}
      </h1>
      <div style="font-size:13px;color:#94a3b8;margin-bottom:16px;">{name}</div>

      <div style="background:#0f172a;border-radius:8px;padding:14px 16px;margin-bottom:16px;">
        <div style="font-size:22px;font-weight:900;color:{color};margin-bottom:4px;">
          {label}
        </div>
        <div style="font-size:28px;font-weight:900;color:#f8fafc;">${price:.2f}</div>
        <div style="font-size:13px;color:{'#4ade80' if change_pct>=0 else '#f87171'};">
          {chg_sign}{change_pct:.2f}% change
        </div>
      </div>

      <table style="width:100%;border-collapse:collapse;font-size:12px;margin-bottom:16px;">
        <tr style="border-bottom:1px solid #334155;">
          <td style="padding:8px 0;color:#64748b;">RSI (14)</td>
          <td style="padding:8px 0;text-align:right;font-weight:700;">{rsi_str}{rsi_note}</td>
        </tr>
        <tr style="border-bottom:1px solid #334155;">
          <td style="padding:8px 0;color:#64748b;">MACD</td>
          <td style="padding:8px 0;text-align:right;font-weight:700;">{macd_str}</td>
        </tr>
        <tr>
          <td style="padding:8px 0;color:#64748b;">Signal time</td>
          <td style="padding:8px 0;text-align:right;">{now_str}</td>
        </tr>
      </table>

      <a href="{DASHBOARD_URL}"
         style="display:block;text-align:center;background:#4f46e5;color:#fff;
                border-radius:8px;padding:12px;font-weight:700;text-decoration:none;
                font-size:13px;">
        View Live Dashboard →
      </a>

    </div>
    <div style="text-align:center;font-size:10px;color:#475569;margin-top:12px;">
      DOW30 Tracker · GitHub Actions Edition · Alerts sent by Resend
    </div>
  </div>
</body></html>"""

    plain = (
        f"DOW30 Alert: {symbol} — {label} @ ${price:.2f}\n\n"
        f"Change:   {chg_sign}{change_pct:.2f}%\n"
        f"RSI(14):  {rsi_str}{rsi_note}\n"
        f"MACD:     {macd_str}\n"
        f"Time:     {now_str}\n\n"
        f"Dashboard: {DASHBOARD_URL}"
    )

    payload = json.dumps({
        "from":    "DOW30 Tracker <alerts@resend.dev>",
        "to":      [ALERT_EMAIL],
        "subject": f"🔔 DOW30: {symbol} {label} @ ${price:.2f}",
        "html":    html,
        "text":    plain,
    }).encode()

    try:
        req = urllib.request.Request(
            "https://api.resend.com/emails",
            data=payload,
            headers={
                "Authorization":  f"Bearer {RESEND_API_KEY}",
                "Content-Type":   "application/json",
            },
        )
        urllib.request.urlopen(req, timeout=15)
        log.info(f"  Email sent → {ALERT_EMAIL}")
    except Exception as exc:
        log.warning(f"  Email failed: {exc}")

# ── Data persistence ───────────────────────────────────────────────────────────

def load_previous():
    """Load the last saved state to detect new signal transitions."""
    if not os.path.exists(LATEST_FILE):
        return {}
    try:
        with open(LATEST_FILE) as f:
            data = json.load(f)
        return {s["symbol"]: s for s in data.get("stocks", [])}
    except Exception:
        return {}

def load_history():
    """Load existing signal history log."""
    if not os.path.exists(HISTORY_FILE):
        return []
    try:
        with open(HISTORY_FILE) as f:
            return json.load(f)
    except Exception:
        return []

def save_latest(stocks, scan_time):
    tops    = sum(1 for s in stocks if s["status"] == "confirmed_top")
    bottoms = sum(1 for s in stocks if s["status"] == "confirmed_bottom")
    pending = sum(1 for s in stocks if s["status"].startswith("pending"))
    with open(LATEST_FILE, "w") as f:
        json.dump({
            "scannedAt":        scan_time.isoformat(),
            "topsToday":        tops,
            "bottomsToday":     bottoms,
            "pendingSignals":   pending,
            "totalAlertsToday": tops + bottoms,
            "stocks":           stocks,
        }, f, indent=2)

def save_history(history):
    with open(HISTORY_FILE, "w") as f:
        json.dump(history[-MAX_HISTORY:], f, indent=2)

# ── Single scan pass ───────────────────────────────────────────────────────────

def run_scan(previous, history):
    """
    Scan all 30 Dow stocks once.
    Returns (results, history, new_signals_count).
    """
    scan_time   = datetime.now(timezone.utc)
    results     = []
    new_signals = 0

    log.info(f"── Scan started {scan_time.strftime('%Y-%m-%d %H:%M:%S UTC')} ──")

    for stock in DOW_30:
        sym  = stock["symbol"]
        name = stock["name"]
        log.info(f"  {sym}…")

        try:
            c15 = fetch_ohlcv(sym, "5d",  "15m")
            c60 = fetch_ohlcv(sym, "10d", "60m")

            if not c15:
                results.append(_idle(sym, name, scan_time))
                continue

            latest  = c15[-1]
            prev    = c15[-2] if len(c15) > 1 else latest
            price   = latest["close"]
            change  = round(price - prev["close"], 4)
            chg_pct = round(change / prev["close"] * 100, 4) if prev["close"] else 0.0

            # RSI + MACD for email
            closes  = [c["close"] for c in c15]
            rsi_arr = compute_rsi(closes)
            macd_arr, _ = compute_macd(closes)
            rsi_now  = next((v for v in reversed(rsi_arr)  if not math.isnan(v)), None)
            macd_now = next((v for v in reversed(macd_arr) if not math.isnan(v)), None)

            # Swing detection + multi-timeframe confirmation
            sw = detect_swing(c15)
            status = "idle"
            if c60:
                recent  = c60[-3:] if len(c60) >= 3 else c60
                avg_dir = sum(c["close"] - c["open"] for c in recent) / len(recent)
                if sw["high_idx"] != -1 and avg_dir < 0:
                    status = "confirmed_top"
                elif sw["low_idx"] != -1 and avg_dir > 0:
                    status = "confirmed_bottom"
                elif sw["high_idx"] != -1:
                    status = "pending_top"
                elif sw["low_idx"] != -1:
                    status = "pending_bottom"

            # Detect NEW signal transitions → send email, log history
            prev_status = previous.get(sym, {}).get("status", "")

            if status == "confirmed_top" and prev_status != "confirmed_top":
                log.info(f"  *** CONFIRMED TOP  {sym} @ ${price:.2f}")
                send_email(sym, name, "top", price, chg_pct, rsi_now, macd_now)
                history.append(_history_row(sym, name, "top", price, chg_pct, scan_time))
                new_signals += 1

            elif status == "confirmed_bottom" and prev_status != "confirmed_bottom":
                log.info(f"  *** CONFIRMED BOTTOM  {sym} @ ${price:.2f}")
                send_email(sym, name, "bottom", price, chg_pct, rsi_now, macd_now)
                history.append(_history_row(sym, name, "bottom", price, chg_pct, scan_time))
                new_signals += 1

            results.append({
                "symbol":    sym,
                "name":      name,
                "price":     price,
                "change":    change,
                "changePct": chg_pct,
                "status":    status,
                "score":     score_number(status),
                "swingHigh": sw["high_price"],
                "swingLow":  sw["low_price"],
                "lastUpdated": scan_time.isoformat(),
            })

        except Exception as exc:
            log.warning(f"  Error {sym}: {exc}")
            results.append(_idle(sym, name, scan_time))

        time.sleep(0.5)   # be polite to Yahoo Finance

    save_latest(results, scan_time)
    save_history(history)

    tops    = sum(1 for s in results if s["status"] == "confirmed_top")
    bottoms = sum(1 for s in results if s["status"] == "confirmed_bottom")
    log.info(f"── Scan done. Tops: {tops}  Bottoms: {bottoms}  New: {new_signals} ──\n")

    # Return updated state map so next loop uses it for transition detection
    return {s["symbol"]: s for s in results}, history, new_signals

def _idle(sym, name, scan_time):
    return {
        "symbol": sym, "name": name, "price": 0.0,
        "change": 0.0, "changePct": 0.0, "status": "idle",
        "score": 2, "swingHigh": None, "swingLow": None,
        "lastUpdated": scan_time.isoformat(),
    }

def _history_row(sym, name, sig_type, price, chg_pct, scan_time):
    return {
        "symbol":      sym,
        "name":        name,
        "signal_type": sig_type,
        "price":       price,
        "change_pct":  chg_pct,
        "note":        "",
        "fired_at":    scan_time.isoformat(),
    }

# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("=" * 55)
    log.info("  DOW30 Scanner — GitHub Actions Edition")
    log.info(f"  Loops per run : {SCAN_LOOPS}")
    log.info(f"  Sleep between : {LOOP_SLEEP}s")
    log.info(f"  Dashboard URL : {DASHBOARD_URL}")
    log.info(f"  Email alerts  : {'Enabled → ' + ALERT_EMAIL if RESEND_API_KEY and ALERT_EMAIL else 'DISABLED (set RESEND_API_KEY + ALERT_EMAIL)'}")
    log.info("=" * 55)

    previous = load_previous()
    history  = load_history()

    for loop_num in range(1, SCAN_LOOPS + 1):
        log.info(f"\n[Loop {loop_num}/{SCAN_LOOPS}]")
        previous, history, _ = run_scan(previous, history)

        if loop_num < SCAN_LOOPS:
            log.info(f"Sleeping {LOOP_SLEEP}s before next scan…")
            time.sleep(LOOP_SLEEP)

    log.info("All loops complete. GitHub Actions will push data files and exit.")
