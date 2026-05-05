#!/usr/bin/env python3
"""
DOW 30 TOP/BOTTOM ALERT TRACKER — Single Python File Edition
=============================================================
A fully self-contained trading dashboard in one Python file.
Produces the same output as the Node.js + React version.

INSTALL REQUIREMENTS (run once):
    pip install flask yfinance pandas

RUN:
    python dow30_tracker.py
    Then open: http://localhost:5000

OPTIONAL — Email alerts:
    Set these environment variables before running:
        RESEND_API_KEY=re_xxxxxxxxxxxx
        ALERT_EMAIL=you@youremail.com

OPTIONAL — Custom database path:
    DATABASE_PATH=/path/to/alerts.db  (default: dow30.db in current folder)

FEATURES:
  - Scans all 30 Dow Jones stocks every 5 minutes
  - Detects swing highs (tops) and swing lows (bottoms) on 15m + 60m charts
  - Multi-timeframe confirmation (15m signal must be confirmed by 60m direction)
  - RSI (14-period, Wilder's smoothing)
  - MACD (12/26/9 EMA)
  - Pivot Point support/resistance levels (PP, R1-R3, S1-S3)
  - 30-day daily candlestick chart
  - 2-day intraday 15m chart
  - Alert history stored in SQLite
  - Custom watchlist (add any ticker beyond the Dow 30)
  - Dark-themed dashboard — same look and feel as the main app
"""

import os
import json
import math
import time
import copy
import sqlite3
import logging
import threading
from datetime import datetime, timedelta

try:
    import yfinance as yf
    import pandas as pd
except ImportError:
    print("\n[ERROR] Missing dependencies. Run this first:\n")
    print("    pip install flask yfinance pandas\n")
    raise SystemExit(1)

try:
    from flask import Flask, jsonify, request
    from flask_socketio import SocketIO
except ImportError:
    print("\n[ERROR] Flask or Flask-SocketIO not installed. Run:\n")
    print("    pip install flask flask-socketio yfinance pandas\n")
    raise SystemExit(1)

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("dow30")

# ── Dow 30 symbols ────────────────────────────────────────────────────────────

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

DOW_30_SET = {s["symbol"] for s in DOW_30}

# ── Database ───────────────────────────────────────────────────────────────────

DB_PATH = os.environ.get("DATABASE_PATH", "dow30.db")

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol      TEXT    NOT NULL,
                type        TEXT    NOT NULL,
                price       REAL    NOT NULL,
                alerted_at  TEXT    NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS price_alerts (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol       TEXT    NOT NULL,
                target_price REAL    NOT NULL,
                direction    TEXT    NOT NULL CHECK(direction IN ('above','below')),
                note         TEXT    DEFAULT '',
                triggered    INTEGER NOT NULL DEFAULT 0,
                triggered_at TEXT    DEFAULT NULL,
                created_at   TEXT    NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS scanner_history (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol      TEXT    NOT NULL,
                name        TEXT    NOT NULL DEFAULT '',
                signal_type TEXT    NOT NULL,
                price       REAL    NOT NULL,
                change_pct  REAL    NOT NULL DEFAULT 0.0,
                note        TEXT    DEFAULT '',
                fired_at    TEXT    NOT NULL
            )
        """)
        conn.commit()
    log.info(f"Database ready: {DB_PATH}")

def save_alert(symbol: str, alert_type: str, price: float):
    with get_db() as conn:
        conn.execute(
            "INSERT INTO alerts (symbol, type, price, alerted_at) VALUES (?, ?, ?, ?)",
            (symbol, alert_type, round(price, 4), datetime.utcnow().isoformat())
        )
        conn.commit()

def get_alerts(limit: int = 50):
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM alerts ORDER BY alerted_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

def get_alert_summary():
    today = datetime.utcnow().strftime("%Y-%m-%d")
    with get_db() as conn:
        tops    = conn.execute("SELECT COUNT(*) FROM alerts WHERE type='top'    AND alerted_at LIKE ?", (today+"%",)).fetchone()[0]
        bottoms = conn.execute("SELECT COUNT(*) FROM alerts WHERE type='bottom' AND alerted_at LIKE ?", (today+"%",)).fetchone()[0]
        total   = conn.execute("SELECT COUNT(*) FROM alerts WHERE alerted_at LIKE ?", (today+"%",)).fetchone()[0]
    return {"topsToday": tops, "bottomsToday": bottoms, "totalAlertsToday": total}

# ── Scanner History ─────────────────────────────────────────────────────────────

def save_scanner_history(symbol: str, name: str, signal_type: str, price: float, change_pct: float, note: str = ""):
    with get_db() as conn:
        conn.execute(
            "INSERT INTO scanner_history (symbol, name, signal_type, price, change_pct, note, fired_at) VALUES (?,?,?,?,?,?,?)",
            (symbol, name, signal_type, round(price, 4), round(change_pct, 4), note, datetime.utcnow().isoformat())
        )
        conn.commit()

def get_scanner_history(symbol=None, signal_type=None, date_from=None, date_to=None, limit=500):
    clauses, params = [], []
    if symbol:
        clauses.append("symbol = ?"); params.append(symbol.upper())
    if signal_type and signal_type != "all":
        clauses.append("signal_type = ?"); params.append(signal_type)
    if date_from:
        clauses.append("fired_at >= ?"); params.append(date_from)
    if date_to:
        clauses.append("fired_at <= ?"); params.append(date_to + "T23:59:59")
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    params.append(limit)
    with get_db() as conn:
        rows = conn.execute(
            f"SELECT * FROM scanner_history {where} ORDER BY fired_at DESC LIMIT ?", params
        ).fetchall()
    return [dict(r) for r in rows]

# ── Price alerts CRUD ───────────────────────────────────────────────────────────

def save_price_alert(symbol: str, target: float, direction: str, note: str = "") -> int:
    with get_db() as conn:
        cur = conn.execute(
            "INSERT INTO price_alerts (symbol, target_price, direction, note, created_at) VALUES (?, ?, ?, ?, ?)",
            (symbol.upper(), round(target, 4), direction, note, datetime.utcnow().isoformat())
        )
        conn.commit()
        return cur.lastrowid

def get_price_alerts(active_only: bool = False) -> list:
    with get_db() as conn:
        if active_only:
            rows = conn.execute(
                "SELECT * FROM price_alerts WHERE triggered=0 ORDER BY created_at DESC"
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM price_alerts ORDER BY triggered ASC, created_at DESC LIMIT 200"
            ).fetchall()
        return [dict(r) for r in rows]

def delete_price_alert(alert_id: int):
    with get_db() as conn:
        conn.execute("DELETE FROM price_alerts WHERE id=?", (alert_id,))
        conn.commit()

def check_and_trigger_price_alerts(symbol: str, price: float) -> list:
    """Check active price alerts for symbol. Returns list of triggered alert dicts."""
    triggered = []
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM price_alerts WHERE symbol=? AND triggered=0", (symbol.upper(),)
        ).fetchall()
        for row in rows:
            r = dict(row)
            hit = (r["direction"] == "above" and price >= r["target_price"]) or \
                  (r["direction"] == "below" and price <= r["target_price"])
            if hit:
                conn.execute(
                    "UPDATE price_alerts SET triggered=1, triggered_at=? WHERE id=?",
                    (datetime.utcnow().isoformat(), r["id"])
                )
                triggered.append(r)
        if triggered:
            conn.commit()
    return triggered

# ── Indicator math ─────────────────────────────────────────────────────────────

def compute_ema(values: list, period: int) -> list:
    """Standard EMA using multiplier 2/(period+1)."""
    result = [math.nan] * len(values)
    if len(values) < period:
        return result
    k = 2.0 / (period + 1)
    # Seed with simple average of first `period` values
    seed = sum(values[:period]) / period
    result[period - 1] = seed
    for i in range(period, len(values)):
        result[i] = values[i] * k + result[i - 1] * (1 - k)
    return result


def compute_rsi(closes: list, period: int = 14) -> list:
    """RSI with Wilder's smoothing. Returns list aligned to closes."""
    rsi = [math.nan] * len(closes)
    if len(closes) < period + 1:
        return rsi

    gains = [max(closes[i] - closes[i-1], 0.0) for i in range(1, len(closes))]
    losses = [max(closes[i-1] - closes[i], 0.0) for i in range(1, len(closes))]

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    if avg_loss == 0:
        rsi[period] = 100.0
    else:
        rsi[period] = 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)

    for i in range(period + 1, len(closes)):
        avg_gain = (avg_gain * (period - 1) + gains[i - 1]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i - 1]) / period
        if avg_loss == 0:
            rsi[i] = 100.0
        else:
            rsi[i] = 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)

    return rsi


def compute_macd(closes: list, fast: int = 12, slow: int = 26, signal: int = 9):
    """MACD = EMA(fast) - EMA(slow), Signal = EMA(signal) of MACD."""
    ema_fast = compute_ema(closes, fast)
    ema_slow = compute_ema(closes, slow)

    macd_line = [
        f - s if not (math.isnan(f) or math.isnan(s)) else math.nan
        for f, s in zip(ema_fast, ema_slow)
    ]

    # Map valid MACD values to compute signal EMA
    valid_pairs = [(i, v) for i, v in enumerate(macd_line) if not math.isnan(v)]
    signal_line = [math.nan] * len(macd_line)

    if len(valid_pairs) >= signal:
        valid_vals = [v for _, v in valid_pairs]
        ema_sig = compute_ema(valid_vals, signal)
        for j, (i, _) in enumerate(valid_pairs):
            if not math.isnan(ema_sig[j]):
                signal_line[i] = ema_sig[j]

    histogram = [
        m - s if not (math.isnan(m) or math.isnan(s)) else math.nan
        for m, s in zip(macd_line, signal_line)
    ]

    return macd_line, signal_line, histogram


def compute_pivots(high: float, low: float, close: float) -> dict:
    """Classic pivot points from previous day's HLC."""
    pp = (high + low + close) / 3
    return {
        "pp": pp,
        "r1": 2 * pp - low,
        "r2": pp + (high - low),
        "r3": high + 2 * (pp - low),
        "s1": 2 * pp - high,
        "s2": pp - (high - low),
        "s3": low - 2 * (high - pp),
    }

# ── Yahoo Finance helpers ──────────────────────────────────────────────────────

def fetch_ohlcv(symbol: str, period: str, interval: str) -> list:
    """Fetch OHLCV data from Yahoo Finance. Returns list of dicts with unix 'time' key."""
    try:
        df = yf.Ticker(symbol).history(period=period, interval=interval, auto_adjust=True)
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
        log.warning(f"Fetch failed {symbol} {interval}: {exc}")
        return []

# ── Swing detection ────────────────────────────────────────────────────────────

def detect_swing(candles: list, lookback: int = 3) -> dict:
    """
    Returns the most recent swing high and swing low indexes/prices.
    A swing high: candle[i].high > all neighbors within `lookback`.
    A swing low:  candle[i].low  < all neighbors within `lookback`.
    """
    result = {"high_idx": -1, "high_price": None, "low_idx": -1, "low_price": None}
    n = len(candles)
    if n < lookback * 2 + 1:
        return result

    for i in range(n - 2, lookback - 1, -1):
        c = candles[i]
        if result["high_idx"] == -1:
            is_high = all(
                candles[i - k]["high"] < c["high"] and candles[i + k]["high"] < c["high"]
                for k in range(1, lookback + 1)
                if i - k >= 0 and i + k < n
            )
            if is_high:
                result["high_idx"] = i
                result["high_price"] = c["high"]

        if result["low_idx"] == -1:
            is_low = all(
                candles[i - k]["low"] > c["low"] and candles[i + k]["low"] > c["low"]
                for k in range(1, lookback + 1)
                if i - k >= 0 and i + k < n
            )
            if is_low:
                result["low_idx"] = i
                result["low_price"] = c["low"]

        if result["high_idx"] != -1 and result["low_idx"] != -1:
            break

    return result

# ── In-memory scanner state ────────────────────────────────────────────────────

_state_lock = threading.Lock()
_scanner_states: dict = {}   # symbol -> state dict
_custom_symbols: list = []
_last_scan_time: str = ""
_next_scan_in: int = 0
_scan_running: bool = False

SCAN_INTERVAL = 300  # 5 minutes

def _default_state(symbol: str, name: str) -> dict:
    return {
        "symbol": symbol,
        "name": name,
        "price": 0.0,
        "change": 0.0,
        "changePct": 0.0,
        "status": "idle",
        "pendingConfirmations": 0,
        "swingHigh": None,
        "swingLow": None,
        "lastUpdated": "",
    }


def scan_symbol(symbol: str, name: str):
    """Fetch data for one symbol, detect swings, update state, fire alerts."""
    candles_15m = fetch_ohlcv(symbol, "5d",  "15m")
    candles_60m = fetch_ohlcv(symbol, "10d", "60m")

    if not candles_15m:
        return

    # Price info from latest 15m candle
    latest = candles_15m[-1]
    prev   = candles_15m[-2] if len(candles_15m) > 1 else latest
    price  = latest["close"]
    change = round(price - prev["close"], 4)
    chg_pct = round(change / prev["close"] * 100, 4) if prev["close"] else 0.0

    # Check custom price-level alerts
    triggered_pa = check_and_trigger_price_alerts(symbol, price)
    for pa in triggered_pa:
        dir_label = "ABOVE" if pa["direction"] == "above" else "BELOW"
        log.info(f"PRICE ALERT {symbol} crossed {dir_label} ${pa['target_price']:.2f} (now ${price:.2f})")
        note_str = f"{dir_label} ${pa['target_price']:.2f}" + (f" — {pa['note']}" if pa.get("note") else "")
        save_scanner_history(symbol, name, "price_target", price, chg_pct, note_str)
        _send_email_alert(
            symbol,
            f"PRICE TARGET HIT — {dir_label} ${pa['target_price']:.2f}",
            price
        )
        socketio.emit("price_alert", {
            "symbol":    symbol,
            "alertId":   pa["id"],
            "target":    pa["target_price"],
            "direction": pa["direction"],
            "price":     price,
            "note":      pa.get("note", ""),
            "timestamp": datetime.utcnow().isoformat(),
        })

    # Detect swings on 15m
    sw_15m = detect_swing(candles_15m)

    # Simple multi-timeframe confirmation:
    # if swing high on 15m, check if 60m recent momentum is also bearish (close < open)
    status = "idle"
    swing_high_price = sw_15m["high_price"]
    swing_low_price  = sw_15m["low_price"]

    if candles_60m:
        recent_60m = candles_60m[-3:] if len(candles_60m) >= 3 else candles_60m
        avg_60m_dir = sum(c["close"] - c["open"] for c in recent_60m) / len(recent_60m)

        if sw_15m["high_idx"] != -1 and avg_60m_dir < 0:
            status = "confirmed_top"
        elif sw_15m["low_idx"] != -1 and avg_60m_dir > 0:
            status = "confirmed_bottom"
        elif sw_15m["high_idx"] != -1:
            status = "pending_top"
        elif sw_15m["low_idx"] != -1:
            status = "pending_bottom"

    # Fire alert on confirmed signals (only once per transition)
    with _state_lock:
        prev_state = _scanner_states.get(symbol, {})
        prev_status = prev_state.get("status", "")

        if status == "confirmed_top" and prev_status != "confirmed_top":
            save_alert(symbol, "top", price)
            save_scanner_history(symbol, name, "top", price, chg_pct)
            log.info(f"ALERT TOP  {symbol} @ ${price:.2f}")
            _send_email_alert(symbol, "TOP", price)
            socketio.emit("signal", {
                "symbol": symbol, "name": name, "type": "top",
                "price": price, "changePct": chg_pct,
                "timestamp": datetime.utcnow().isoformat(),
            })

        elif status == "confirmed_bottom" and prev_status != "confirmed_bottom":
            save_alert(symbol, "bottom", price)
            save_scanner_history(symbol, name, "bottom", price, chg_pct)
            log.info(f"ALERT BOT  {symbol} @ ${price:.2f}")
            _send_email_alert(symbol, "BOTTOM", price)
            socketio.emit("signal", {
                "symbol": symbol, "name": name, "type": "bottom",
                "price": price, "changePct": chg_pct,
                "timestamp": datetime.utcnow().isoformat(),
            })

        _scanner_states[symbol] = {
            "symbol":               symbol,
            "name":                 name,
            "price":                price,
            "change":               change,
            "changePct":            chg_pct,
            "status":               status,
            "pendingConfirmations": 1 if status.startswith("pending") else 0,
            "swingHigh":            swing_high_price,
            "swingLow":             swing_low_price,
            "lastUpdated":          datetime.utcnow().isoformat(),
        }


def _send_email_alert(symbol: str, alert_type: str, price: float):
    """Send email via Resend API if configured."""
    api_key = os.environ.get("RESEND_API_KEY", "")
    to_email = os.environ.get("ALERT_EMAIL", "")
    if not api_key or not to_email:
        return
    try:
        import urllib.request
        import urllib.parse
        payload = json.dumps({
            "from": "alerts@resend.dev",
            "to": [to_email],
            "subject": f"DOW30 Alert: {symbol} {alert_type} @ ${price:.2f}",
            "text": f"{symbol} has triggered a confirmed {alert_type} signal at ${price:.2f}.\n\nTime: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
        }).encode()
        req = urllib.request.Request(
            "https://api.resend.com/emails",
            data=payload,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=10)
        log.info(f"Email sent → {to_email} ({symbol} {alert_type})")
    except Exception as exc:
        log.warning(f"Email send failed: {exc}")


def _run_scanner():
    """Background thread: scan all symbols every SCAN_INTERVAL seconds."""
    global _last_scan_time, _next_scan_in, _scan_running

    all_stocks = DOW_30.copy()

    while True:
        _scan_running = True
        with _state_lock:
            custom = list(_custom_symbols)

        combined = all_stocks + [{"symbol": s, "name": s} for s in custom if s not in DOW_30_SET]

        log.info(f"Scanning {len(combined)} symbols…")
        for stock in combined:
            try:
                scan_symbol(stock["symbol"], stock["name"])
            except Exception as exc:
                log.warning(f"Scan error {stock['symbol']}: {exc}")
            time.sleep(0.8)   # respect Yahoo Finance rate limits

        _last_scan_time = datetime.utcnow().strftime("%I:%M:%S %p")
        _scan_running = False
        log.info("Scan complete. Sleeping 5 minutes.")
        socketio.emit("scan_status", {
            "scanning": False,
            "lastScanTime": _last_scan_time,
            "nextScanIn": SCAN_INTERVAL,
        })

        # Countdown timer — push live countdown to all connected browsers
        for remaining in range(SCAN_INTERVAL, 0, -1):
            _next_scan_in = remaining
            # Push every 5 s to avoid flooding; always push last 10 s
            if remaining % 5 == 0 or remaining <= 10:
                socketio.emit("scan_status", {
                    "scanning": False,
                    "lastScanTime": _last_scan_time,
                    "nextScanIn": remaining,
                })
            time.sleep(1)

        socketio.emit("scan_status", {"scanning": True, "lastScanTime": _last_scan_time, "nextScanIn": 0})

# ── History endpoint helper ────────────────────────────────────────────────────

def build_history(symbol: str) -> dict:
    """Fetch 30-day daily + 2-day 15m + compute RSI, MACD, pivots."""
    daily_raw   = fetch_ohlcv(symbol, "45d", "1d")
    intraday_raw = fetch_ohlcv(symbol, "5d",  "15m")

    # Keep last 30 trading days
    daily = daily_raw[-30:] if len(daily_raw) > 30 else daily_raw

    # Compute RSI + MACD on all available daily data for accuracy
    closes_all  = [c["close"] for c in daily_raw]
    rsi_all     = compute_rsi(closes_all)
    macd_l, sig_l, hist_l = compute_macd(closes_all)

    offset = len(daily_raw) - len(daily)

    rsi_out = []
    for i, d in enumerate(daily):
        v = rsi_all[offset + i]
        if not math.isnan(v):
            rsi_out.append({"time": d["time"], "value": round(v, 2)})

    macd_out = []
    for i, d in enumerate(daily):
        m = macd_l[offset + i]
        s = sig_l[offset + i]
        h = hist_l[offset + i]
        if not (math.isnan(m) or math.isnan(s) or math.isnan(h)):
            macd_out.append({
                "time":      d["time"],
                "macd":      round(m, 4),
                "signal":    round(s, 4),
                "histogram": round(h, 4),
            })

    # Pivot from second-to-last daily candle (yesterday's completed day)
    pivots = None
    if len(daily) >= 2:
        ref = daily[-2]
        pivots = compute_pivots(ref["high"], ref["low"], ref["close"])
        pivots = {k: round(v, 4) for k, v in pivots.items()}

    # Intraday: last 2 distinct trading days
    if intraday_raw:
        seen_days: set = set()
        day_list = []
        for c in intraday_raw:
            day_str = datetime.utcfromtimestamp(c["time"]).strftime("%Y-%m-%d")
            seen_days.add(day_str)
        last_two = sorted(seen_days)[-2:]
        day_list = [
            c for c in intraday_raw
            if datetime.utcfromtimestamp(c["time"]).strftime("%Y-%m-%d") in last_two
        ]
    else:
        day_list = []

    return {
        "symbol":   symbol,
        "daily":    daily,
        "intraday": day_list,
        "rsi":      rsi_out,
        "macd":     macd_out,
        "pivots":   pivots,
    }

# ── Flask app ──────────────────────────────────────────────────────────────────

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading", logger=False, engineio_logger=False)

# ─── Routes ───────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return HTML_TEMPLATE

@app.route("/api/status")
def api_status():
    return jsonify({
        "lastScanTime": _last_scan_time or "Initializing…",
        "nextScanIn":   _next_scan_in,
        "scanning":     _scan_running,
        "apiOk":        True,
    })

@app.route("/api/stocks")
def api_stocks():
    with _state_lock:
        states = list(_scanner_states.values())
    # Add any Dow 30 symbols not yet scanned
    scanned = {s["symbol"] for s in states}
    for stock in DOW_30:
        if stock["symbol"] not in scanned:
            states.append(_default_state(stock["symbol"], stock["name"]))
    return jsonify(states)

@app.route("/api/stocks/<symbol>")
def api_stock(symbol):
    sym = symbol.upper()
    with _state_lock:
        state = _scanner_states.get(sym)
    if state:
        return jsonify(state)
    for stock in DOW_30:
        if stock["symbol"] == sym:
            return jsonify(_default_state(sym, stock["name"]))
    return jsonify({"error": "Not found"}), 404

@app.route("/api/stocks/<symbol>/history")
def api_history(symbol):
    try:
        data = build_history(symbol.upper())
        return jsonify(data)
    except Exception as exc:
        log.error(f"History error {symbol}: {exc}")
        return jsonify({"error": str(exc)}), 500

@app.route("/api/alerts")
def api_alerts():
    return jsonify(get_alerts(50))

@app.route("/api/alerts/summary")
def api_alert_summary():
    return jsonify(get_alert_summary())

@app.route("/api/custom/symbols", methods=["GET", "POST"])
def api_custom_symbols():
    global _custom_symbols
    if request.method == "POST":
        data = request.get_json(force=True)
        syms = [s.upper().strip() for s in (data.get("symbols") or []) if s.strip()]
        with _state_lock:
            _custom_symbols = [s for s in syms if s not in DOW_30_SET]
        return jsonify({"symbols": _custom_symbols})
    with _state_lock:
        syms = list(_custom_symbols)
    return jsonify({"symbols": syms})

@app.route("/api/custom/stocks")
def api_custom_stocks():
    with _state_lock:
        custom = list(_custom_symbols)
        states = [_scanner_states[s] for s in custom if s in _scanner_states]
    return jsonify(states)

@app.route("/api/price-alerts", methods=["GET", "POST"])
def api_price_alerts():
    if request.method == "POST":
        data      = request.get_json(force=True)
        symbol    = data.get("symbol", "").upper().strip()
        note      = data.get("note", "").strip()
        direction = data.get("direction", "above")
        try:
            target = float(data.get("target", 0))
        except (TypeError, ValueError):
            return jsonify({"error": "target must be a number"}), 400
        if not symbol or target <= 0:
            return jsonify({"error": "symbol and a positive target price are required"}), 400
        if direction not in ("above", "below"):
            return jsonify({"error": "direction must be 'above' or 'below'"}), 400
        alert_id = save_price_alert(symbol, target, direction, note)
        return jsonify({"id": alert_id, "symbol": symbol, "target_price": target,
                        "direction": direction, "note": note, "triggered": 0}), 201
    return jsonify(get_price_alerts(active_only=False))

@app.route("/api/price-alerts/active")
def api_active_price_alerts():
    return jsonify(get_price_alerts(active_only=True))

@app.route("/api/price-alerts/<int:alert_id>", methods=["DELETE"])
def api_delete_price_alert(alert_id: int):
    delete_price_alert(alert_id)
    return jsonify({"deleted": alert_id})

@app.route("/api/scanner-history")
def api_scanner_history():
    symbol    = request.args.get("symbol", "").strip().upper() or None
    sig_type  = request.args.get("type",   "all")
    date_from = request.args.get("from",   "") or None
    date_to   = request.args.get("to",     "") or None
    try:
        limit = min(int(request.args.get("limit", 500)), 2000)
    except (TypeError, ValueError):
        limit = 500
    rows = get_scanner_history(symbol, sig_type, date_from, date_to, limit)
    return jsonify(rows)

@app.route("/api/scanner-history/clear", methods=["DELETE"])
def api_clear_scanner_history():
    with get_db() as conn:
        conn.execute("DELETE FROM scanner_history")
        conn.commit()
    return jsonify({"cleared": True})

@app.route("/api/scanner-history/export")
def api_export_scanner_history():
    from flask import Response
    rows  = get_scanner_history(limit=10000)
    lines = ["id,symbol,name,signal_type,price,change_pct,note,fired_at"]
    for r in rows:
        note = r.get("note", "").replace('"', '""')
        lines.append(
            f'{r["id"]},{r["symbol"]},"{r["name"]}",{r["signal_type"]},'
            f'{r["price"]},{r["change_pct"]},"{note}",{r["fired_at"]}'
        )
    return Response(
        "\n".join(lines),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=scanner_history.csv"}
    )

# ── HTML + JS frontend ─────────────────────────────────────────────────────────

HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>DOW30 Tracker</title>
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;700;900&display=swap" rel="stylesheet"/>
<script src="https://cdn.tailwindcss.com"></script>
<script src="https://cdn.socket.io/4.7.5/socket.io.min.js"></script>
<script src="https://unpkg.com/lightweight-charts@4.2.3/dist/lightweight-charts.standalone.production.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  *{font-family:'JetBrains Mono',Menlo,monospace;}
  :root{--bg:#020817;--card:#0f172a;--border:#1e293b;--muted:#64748b;--fg:#f8fafc;}
  body{background:var(--bg);color:var(--fg);margin:0;}
  .card{background:var(--card);border:1px solid var(--border);border-radius:8px;}
  .badge{display:inline-flex;align-items:center;gap:4px;font-size:10px;font-weight:700;padding:2px 6px;border-radius:4px;}
  .badge-idle{background:#1e293b;color:#64748b;}
  .badge-top{background:rgba(239,68,68,.15);color:#f87171;}
  .badge-bottom{background:rgba(34,197,94,.15);color:#4ade80;}
  .badge-pending{background:rgba(249,115,22,.15);color:#fb923c;}
  .score-circle{width:32px;height:32px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-weight:900;font-size:13px;border:1px solid;}
  .modal-backdrop{position:fixed;inset:0;background:rgba(0,0,0,.75);z-index:50;display:flex;align-items:center;justify-content:center;padding:16px;}
  .modal{background:var(--card);border:1px solid var(--border);border-radius:12px;width:100%;max-width:900px;max-height:92vh;display:flex;flex-direction:column;}
  .tab-btn{padding:8px 14px;font-size:13px;font-weight:600;border-bottom:2px solid transparent;color:var(--muted);cursor:pointer;transition:color .15s,border-color .15s;}
  .tab-btn.active{color:#f8fafc;border-bottom-color:#6366f1;}
  .period-btn{padding:4px 12px;font-size:11px;font-weight:600;border-radius:6px;border:1px solid var(--border);color:var(--muted);cursor:pointer;background:transparent;}
  .period-btn.active{border-color:rgba(99,102,241,.5);color:#818cf8;background:rgba(99,102,241,.1);}
  table{width:100%;border-collapse:collapse;font-size:11px;}
  th{padding:6px 10px;text-align:right;color:var(--muted);font-weight:600;font-size:9px;text-transform:uppercase;letter-spacing:.05em;border-bottom:1px solid var(--border);}
  th:first-child{text-align:left;}
  td{padding:5px 10px;text-align:right;border-bottom:1px solid rgba(30,41,59,.5);}
  td:first-child{text-align:left;color:var(--muted);}
  tr:hover{background:rgba(255,255,255,.03);}
  ::-webkit-scrollbar{width:4px;height:4px;}
  ::-webkit-scrollbar-track{background:var(--bg);}
  ::-webkit-scrollbar-thumb{background:#334155;border-radius:2px;}

  /* ── Ticker tape ── */
  .ticker-wrap{overflow:hidden;background:#060f1e;border-bottom:1px solid var(--border);height:30px;display:flex;align-items:center;position:relative;}
  .ticker-wrap::before,.ticker-wrap::after{content:'';position:absolute;top:0;width:48px;height:100%;z-index:2;pointer-events:none;}
  .ticker-wrap::before{left:0;background:linear-gradient(90deg,#060f1e,transparent);}
  .ticker-wrap::after{right:0;background:linear-gradient(270deg,#060f1e,transparent);}
  .ticker-track{display:flex;align-items:center;white-space:nowrap;animation:ticker-scroll 80s linear infinite;width:max-content;}
  .ticker-track:hover{animation-play-state:paused;}
  @keyframes ticker-scroll{0%{transform:translateX(0)}100%{transform:translateX(-50%)}}
  .ticker-item{display:inline-flex;align-items:center;gap:6px;padding:0 18px;border-right:1px solid var(--border);font-size:11px;font-weight:600;cursor:pointer;}
  .ticker-item:hover{background:rgba(255,255,255,.04);}
  .ticker-sym{color:#cbd5e1;letter-spacing:.03em;}
  .ticker-price{color:#f8fafc;}
  .ticker-up{color:#4ade80;}
  .ticker-dn{color:#f87171;}
  .ticker-dot{width:5px;height:5px;border-radius:50%;flex-shrink:0;}

  /* ── Leaderboard ── */
  .lb-sort-btn{background:rgba(255,255,255,.05);border:1px solid var(--border);color:var(--muted);border-radius:5px;padding:3px 9px;font-size:10px;font-weight:700;cursor:pointer;font-family:inherit;transition:background .12s,color .12s;}
  .lb-sort-btn:hover{background:rgba(255,255,255,.09);color:#f8fafc;}
  .lb-sort-btn.active{background:rgba(99,102,241,.15);border-color:rgba(99,102,241,.4);color:#818cf8;}
  #leaderboard-body table thead th{position:sticky;top:0;background:var(--card);z-index:1;border-bottom:1px solid var(--border);padding:7px 10px;color:var(--muted);font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.06em;white-space:nowrap;}
  #leaderboard-body table tbody td{padding:8px 10px;border-bottom:1px solid rgba(30,41,59,.5);}
  #leaderboard-body table tbody tr:last-child td{border-bottom:none;}

  /* ── Scanner History ── */
  .sh-filter-input{background:var(--card);border:1px solid var(--border);border-radius:6px;padding:5px 10px;color:var(--fg);font-family:inherit;font-size:11px;outline:none;}
  .sh-filter-input:focus{border-color:rgba(99,102,241,.5);}
  .sh-btn{border-radius:6px;padding:5px 12px;font-size:11px;font-weight:700;cursor:pointer;font-family:inherit;transition:background .12s,color .12s;}
  .sh-btn-refresh{background:rgba(255,255,255,.06);border:1px solid var(--border);color:var(--muted);}
  .sh-btn-refresh:hover{background:rgba(255,255,255,.1);color:#f8fafc;}
  .sh-btn-csv{background:rgba(74,222,128,.1);border:1px solid rgba(74,222,128,.25);color:#4ade80;}
  .sh-btn-csv:hover{background:rgba(74,222,128,.18);}
  .sh-btn-clear{background:rgba(248,113,113,.08);border:1px solid rgba(248,113,113,.25);color:#f87171;}
  .sh-btn-clear:hover{background:rgba(248,113,113,.16);}
  #sh-body table thead th{position:sticky;top:0;background:var(--card);z-index:1;border-bottom:1px solid var(--border);padding:7px 10px;color:var(--muted);font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.06em;white-space:nowrap;}
  #sh-body table tbody td{padding:8px 10px;border-bottom:1px solid rgba(30,41,59,.5);}
  #sh-body table tbody tr:last-child td{border-bottom:none;}

  /* ── Sound toggle button ── */
  .sound-btn{background:rgba(255,255,255,.06);border:1px solid var(--border);color:var(--muted);border-radius:6px;padding:4px 10px;font-size:11px;font-weight:700;cursor:pointer;display:flex;align-items:center;gap:5px;font-family:inherit;transition:background .15s,color .15s;}
  .sound-btn:hover{background:rgba(255,255,255,.1);color:#f8fafc;}
  .sound-btn.on{border-color:rgba(99,102,241,.4);color:#818cf8;background:rgba(99,102,241,.12);}

  /* ── Toast notification ── */
  @keyframes toast-in {from{transform:translateY(80px);opacity:0} to{transform:translateY(0);opacity:1}}
  @keyframes toast-out{from{transform:translateY(0);opacity:1} to{transform:translateY(80px);opacity:0}}
  .toast{position:fixed;bottom:24px;right:24px;z-index:9999;background:var(--card);border-radius:12px;padding:14px 18px;display:flex;align-items:flex-start;gap:12px;box-shadow:0 12px 40px rgba(0,0,0,.7);cursor:pointer;font-family:inherit;max-width:300px;animation:toast-in .3s ease forwards;}
  .toast.out{animation:toast-out .3s ease forwards;}
</style>
</head>
<body>

<!-- ── Header ── -->
<header style="background:var(--card);border-bottom:1px solid var(--border);padding:0 20px;height:56px;display:flex;align-items:center;justify-content:space-between;">
  <div style="display:flex;align-items:center;gap:10px;">
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#6366f1" stroke-width="2.5"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>
    <span style="font-weight:900;font-size:16px;letter-spacing:.05em;">DOW30 TRACKER</span>
    <span style="font-size:10px;color:var(--muted);">Python Edition</span>
  </div>
  <div style="display:flex;align-items:center;gap:10px;">
    <div id="api-status" style="display:flex;align-items:center;gap:6px;font-size:11px;color:#64748b;">
      <span id="ws-dot" style="width:6px;height:6px;background:#64748b;border-radius:50%;transition:background .3s;"></span>
      <span id="ws-label">Connecting…</span>
    </div>
    <button id="sound-btn" class="sound-btn on" onclick="toggleSound()" title="Toggle sound alerts">
      <span id="sound-icon">🔔</span> <span id="sound-label">Sound ON</span>
    </button>
  </div>
</header>

<!-- ── Ticker tape ── -->
<div class="ticker-wrap" id="ticker-wrap">
  <div class="ticker-track" id="ticker-track">
    <!-- populated by JS -->
    <span style="padding:0 20px;font-size:11px;color:var(--muted);">Loading prices…</span>
  </div>
</div>

<!-- ── Status bar ── -->
<div style="background:#0a1628;border-bottom:1px solid var(--border);padding:6px 20px;display:flex;align-items:center;justify-content:space-between;font-size:11px;color:var(--muted);">
  <div id="scan-status">Initializing scanner…</div>
  <div id="next-scan"></div>
</div>

<!-- ── Main ── -->
<main style="max-width:1400px;margin:0 auto;padding:20px;display:grid;grid-template-columns:1fr 320px;gap:20px;">

  <!-- Stock grid -->
  <div>
    <!-- Summary cards -->
    <div id="summary" style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:20px;">
      <div class="card" style="padding:16px;"><div style="font-size:12px;color:var(--muted);">Tops Today</div><div id="tops-today" style="font-size:28px;font-weight:900;color:#f87171;">0</div></div>
      <div class="card" style="padding:16px;"><div style="font-size:12px;color:var(--muted);">Bottoms Today</div><div id="bots-today" style="font-size:28px;font-weight:900;color:#4ade80;">0</div></div>
      <div class="card" style="padding:16px;"><div style="font-size:12px;color:var(--muted);">Pending</div><div id="pending-count" style="font-size:28px;font-weight:900;color:#fb923c;">0</div></div>
      <div class="card" style="padding:16px;"><div style="font-size:12px;color:var(--muted);">Total Alerts</div><div id="total-alerts" style="font-size:28px;font-weight:900;">0</div></div>
    </div>

    <!-- Section header -->
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px;">
      <h2 style="font-size:18px;font-weight:900;">Dow 30 Scanner Matrix</h2>
      <span style="font-size:10px;color:var(--muted);">📊 to view history · click card for detail</span>
    </div>

    <div id="stock-grid" style="display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:12px;">
      <!-- Stock cards injected here -->
    </div>

    <!-- Custom watchlist -->
    <div style="margin-top:28px;">
      <h2 style="font-size:16px;font-weight:900;margin-bottom:12px;">Custom Watchlist</h2>
      <div style="display:flex;gap:8px;margin-bottom:10px;">
        <input id="custom-input" placeholder="NVDA, TSLA, AMZN" style="flex:1;background:var(--card);border:1px solid var(--border);border-radius:6px;padding:6px 10px;color:var(--fg);font-family:inherit;font-size:12px;" onkeydown="if(event.key==='Enter')addCustom()"/>
        <button onclick="addCustom()" style="background:rgba(99,102,241,.15);border:1px solid rgba(99,102,241,.3);color:#818cf8;padding:6px 14px;border-radius:6px;cursor:pointer;font-size:12px;font-weight:700;">Add</button>
      </div>
      <div id="custom-grid" style="display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:12px;"></div>
    </div>

    <!-- ── Leaderboard ── -->
    <div style="margin-top:32px;">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px;flex-wrap:wrap;gap:10px;">
        <h2 style="font-size:16px;font-weight:900;">🏆 Symbol Leaderboard</h2>
        <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap;">
          <span style="font-size:10px;color:var(--muted);margin-right:4px;">Sort by:</span>
          <button class="lb-sort-btn active" id="lbsort-changePct" onclick="setLbSort('changePct')">Change %</button>
          <button class="lb-sort-btn" id="lbsort-price"     onclick="setLbSort('price')">Price</button>
          <button class="lb-sort-btn" id="lbsort-score"     onclick="setLbSort('score')">Score</button>
          <button class="lb-sort-btn" id="lbsort-signal"    onclick="setLbSort('signal')">Signal</button>
          <button class="lb-sort-btn" id="lbsort-change"    onclick="setLbSort('change')">Change $</button>
        </div>
      </div>
      <div class="card" style="overflow:hidden;">
        <div id="leaderboard-body" style="max-height:540px;overflow-y:auto;overflow-x:auto;">
          <div style="padding:20px;text-align:center;color:var(--muted);font-size:12px;">Waiting for first scan…</div>
        </div>
      </div>
    </div>

    <!-- ── Scanner History ── -->
    <div style="margin-top:32px;">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px;flex-wrap:wrap;gap:10px;">
        <h2 style="font-size:16px;font-weight:900;">📋 Scanner History</h2>
        <div style="display:flex;gap:6px;align-items:center;">
          <button class="sh-btn sh-btn-csv"   onclick="exportHistoryCSV()">⬇ Export CSV</button>
          <button class="sh-btn sh-btn-clear" onclick="clearHistory()">🗑 Clear All</button>
        </div>
      </div>

      <!-- Filter bar -->
      <div class="card" style="padding:10px 14px;margin-bottom:10px;display:flex;gap:8px;align-items:center;flex-wrap:wrap;">
        <input id="sh-symbol" class="sh-filter-input" placeholder="Filter symbol…" style="width:110px;"
               oninput="loadScannerHistory()"/>
        <select id="sh-type" class="sh-filter-input" onchange="loadScannerHistory()"
                style="background:var(--card);cursor:pointer;">
          <option value="all">All Types</option>
          <option value="top">▼ Confirmed Top</option>
          <option value="bottom">▲ Confirmed Bottom</option>
          <option value="price_target">🎯 Price Target</option>
        </select>
        <input type="date" id="sh-from" class="sh-filter-input" onchange="loadScannerHistory()" style="width:130px;color-scheme:dark;"/>
        <span style="color:var(--muted);font-size:11px;">→</span>
        <input type="date" id="sh-to"   class="sh-filter-input" onchange="loadScannerHistory()" style="width:130px;color-scheme:dark;"/>
        <button class="sh-btn sh-btn-refresh" onclick="loadScannerHistory()">🔄 Refresh</button>
        <span id="sh-count" style="margin-left:auto;font-size:10px;color:var(--muted);"></span>
      </div>

      <!-- Table -->
      <div class="card" style="overflow:hidden;">
        <div id="sh-body" style="max-height:520px;overflow-y:auto;overflow-x:auto;">
          <div style="padding:20px;text-align:center;color:var(--muted);font-size:12px;">Loading…</div>
        </div>
      </div>
    </div>
  </div>

  <!-- Right sidebar -->
  <div style="display:flex;flex-direction:column;gap:16px;">
    <!-- Recent alerts -->
    <div class="card">
      <div style="padding:12px 14px;border-bottom:1px solid var(--border);font-size:11px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.08em;">Recent Alerts</div>
      <div id="alerts-list" style="max-height:300px;overflow-y:auto;">
        <div style="padding:16px;text-align:center;color:var(--muted);font-size:12px;">No alerts today</div>
      </div>
    </div>
    <!-- Price alerts panel -->
    <div class="card">
      <div style="padding:12px 14px;border-bottom:1px solid var(--border);font-size:11px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.08em;display:flex;align-items:center;justify-content:space-between;">
        <span>🎯 Price Alerts</span>
        <span id="pa-count" style="background:rgba(99,102,241,.15);color:#818cf8;border-radius:10px;padding:1px 7px;font-size:10px;">0 active</span>
      </div>
      <!-- Add alert form -->
      <div style="padding:12px 14px;border-bottom:1px solid var(--border);">
        <div style="display:flex;gap:6px;margin-bottom:6px;">
          <input id="pa-symbol" placeholder="Symbol" maxlength="6"
            style="width:72px;background:var(--bg);border:1px solid var(--border);border-radius:5px;padding:5px 7px;color:var(--fg);font-family:inherit;font-size:11px;text-transform:uppercase;"
            oninput="this.value=this.value.toUpperCase()"
            onkeydown="if(event.key==='Enter')addPriceAlert()"/>
          <input id="pa-target" placeholder="Target $" type="number" step="0.01" min="0.01"
            style="flex:1;background:var(--bg);border:1px solid var(--border);border-radius:5px;padding:5px 7px;color:var(--fg);font-family:inherit;font-size:11px;"
            onkeydown="if(event.key==='Enter')addPriceAlert()"/>
          <select id="pa-dir"
            style="background:var(--bg);border:1px solid var(--border);border-radius:5px;padding:5px 5px;color:var(--fg);font-family:inherit;font-size:11px;">
            <option value="above">↑ Above</option>
            <option value="below">↓ Below</option>
          </select>
        </div>
        <div style="display:flex;gap:6px;">
          <input id="pa-note" placeholder="Note (optional)"
            style="flex:1;background:var(--bg);border:1px solid var(--border);border-radius:5px;padding:5px 7px;color:var(--fg);font-family:inherit;font-size:11px;"
            onkeydown="if(event.key==='Enter')addPriceAlert()"/>
          <button onclick="addPriceAlert()"
            style="background:rgba(99,102,241,.15);border:1px solid rgba(99,102,241,.3);color:#818cf8;padding:5px 12px;border-radius:5px;cursor:pointer;font-size:11px;font-weight:700;white-space:nowrap;">
            + Set
          </button>
        </div>
      </div>
      <!-- Alert list -->
      <div id="pa-list" style="max-height:260px;overflow-y:auto;">
        <div style="padding:14px;text-align:center;color:var(--muted);font-size:11px;">No price alerts set</div>
      </div>
    </div>

    <!-- About -->
    <div class="card" style="padding:14px;">
      <div style="font-size:10px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px;">How It Works</div>
      <div style="font-size:10px;color:var(--muted);line-height:1.6;">
        Scans every 5 min · 15m + 60m timeframes · Swing detection with multi-TF confirmation ·
        Click 📊 on any card to see 30-day chart, RSI (14), MACD (12/26/9), and Pivot S/R levels.
        Click 🎯 on any card to set a price target alert.
      </div>
    </div>
  </div>
</main>

<!-- ── History Modal ── -->
<div id="modal" class="modal-backdrop" style="display:none;" onclick="if(event.target===this)closeModal()">
  <div class="modal">
    <!-- Modal header -->
    <div style="padding:14px 16px;border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;flex-shrink:0;">
      <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;">
        <span id="modal-symbol" style="font-weight:900;font-size:18px;"></span>
        <span id="modal-price" style="font-size:13px;font-weight:600;"></span>
        <span id="modal-rsi-badge"></span>
        <span id="modal-macd-badge"></span>
      </div>
      <button onclick="closeModal()" style="background:none;border:none;color:var(--muted);cursor:pointer;font-size:18px;padding:4px;">✕</button>
    </div>
    <!-- Pivot level badges -->
    <div id="pivot-bar" style="padding:8px 16px;border-bottom:1px solid var(--border);display:flex;gap:6px;flex-wrap:wrap;flex-shrink:0;"></div>
    <!-- Tabs -->
    <div style="display:flex;border-bottom:1px solid var(--border);padding:0 12px;flex-shrink:0;">
      <button class="tab-btn active" id="tab-chart" onclick="switchTab('chart')">📈 Charts</button>
      <button class="tab-btn" id="tab-table" onclick="switchTab('table')">📋 Table</button>
    </div>
    <!-- Period selector (charts only) -->
    <div id="period-bar" style="padding:10px 16px;display:flex;gap:8px;flex-shrink:0;">
      <button class="period-btn active" id="period-30d" onclick="switchPeriod('30d')">30 Day Daily</button>
      <button class="period-btn" id="period-2d" onclick="switchPeriod('2d')">2-Day Intraday (15m)</button>
    </div>
    <!-- Body -->
    <div id="modal-body" style="flex:1;overflow-y:auto;padding:16px;display:flex;flex-direction:column;gap:16px;min-height:0;">
      <div id="modal-loading" style="padding:40px;text-align:center;color:var(--muted);">Loading…</div>
    </div>
  </div>
</div>

<script>
// ── WebSocket (Socket.IO) ──────────────────────────────────────────────────

const socket = io({ transports: ['websocket', 'polling'] });

socket.on('connect', () => {
  const dot   = document.getElementById('ws-dot');
  const label = document.getElementById('ws-label');
  if (dot)   { dot.style.background = '#4ade80'; dot.style.boxShadow = '0 0 6px #4ade80'; }
  if (label) { label.style.color = '#4ade80'; label.textContent = '⚡ LIVE'; }
});

socket.on('disconnect', () => {
  const dot   = document.getElementById('ws-dot');
  const label = document.getElementById('ws-label');
  if (dot)   { dot.style.background = '#f87171'; dot.style.boxShadow = 'none'; }
  if (label) { label.style.color = '#f87171'; label.textContent = 'Reconnecting…'; }
});

// Confirmed swing signal pushed instantly from scanner thread
socket.on('signal', (data) => {
  // Immediately notify — no waiting for next 15-second poll
  if (_audioEnabled) playAlertSound(data.type);
  showToast(data.symbol, data.type, data.price);
  fireBrowserNotif(
    data.symbol,
    data.type === 'top' ? `▼ Confirmed Top @ $${Number(data.price).toFixed(2)}` : `▲ Confirmed Bottom @ $${Number(data.price).toFixed(2)}`
  );
  // Refresh grid + alerts so card reflects new status immediately
  refreshStocks();
  refreshAlerts();
});

// Price target hit pushed instantly from scanner thread
socket.on('price_alert', (data) => {
  if (_audioEnabled) playAlertSound('price');
  const sub = `${data.direction === 'above' ? '↑ Above' : '↓ Below'} $${Number(data.target).toFixed(2)}${data.note ? ' · ' + data.note : ''}`;
  showToast(data.symbol, 'price', data.target, sub);
  fireBrowserNotif(data.symbol, `🎯 Price target hit — ${sub}`);
  loadPriceAlerts();
});

// Live scan countdown + scanning state pushed from scanner thread
socket.on('scan_status', (data) => {
  const el = document.getElementById('scan-status');
  const nx = document.getElementById('next-scan');
  if (el) el.textContent = data.scanning ? '⟳ Scanning all symbols…' : `Last scan: ${data.lastScanTime}`;
  if (nx) {
    const mins = Math.floor(data.nextScanIn / 60);
    const secs = data.nextScanIn % 60;
    nx.textContent = data.nextScanIn > 0 ? `Next scan in: ${mins}:${String(secs).padStart(2,'0')}` : '';
  }
});

// Browser notification helper
function fireBrowserNotif(title, body) {
  if (!('Notification' in window)) return;
  if (Notification.permission === 'granted') {
    new Notification(title, { body, icon: '' });
  } else if (Notification.permission !== 'denied') {
    Notification.requestPermission().then(p => {
      if (p === 'granted') new Notification(title, { body, icon: '' });
    });
  }
}

// ── State ──────────────────────────────────────────────────────────────────

let _historyData = null;
let _currentTab = 'chart';
let _currentPeriod = '30d';
let _lwChart = null;
let _lwSeries = null;
let _rsiChart = null;
let _macdChart = null;

// ── Utilities ──────────────────────────────────────────────────────────────

function fmtDate(unixSec) {
  return new Date(unixSec * 1000).toLocaleDateString('en-US', {month:'short', day:'numeric', year:'2-digit'});
}
function fmtTime(unixSec) {
  return new Date(unixSec * 1000).toLocaleTimeString('en-US', {hour:'numeric', minute:'2-digit', hour12:true});
}
function fmtNum(n, dec=2) { return isNaN(n) ? '—' : Number(n).toFixed(dec); }
function fmtVol(n) {
  if (n >= 1e6) return (n/1e6).toFixed(1)+'M';
  if (n >= 1e3) return (n/1e3).toFixed(0)+'K';
  return String(n);
}

function scoreColor(status) {
  if (status === 'confirmed_top' || status === 'confirmed_bottom') return ['#22c55e','rgba(34,197,94,.15)','rgba(34,197,94,.4)'];
  if (status === 'pending_top'   || status === 'pending_bottom')   return ['#f59e0b','rgba(245,158,11,.15)','rgba(245,158,11,.4)'];
  return ['#64748b','rgba(100,116,139,.1)','rgba(100,116,139,.2)'];
}

function scoreNumber(status) {
  if (status === 'confirmed_top' || status === 'confirmed_bottom') return 8;
  if (status === 'pending_top'   || status === 'pending_bottom')   return 5;
  return 3;
}

function statusBadge(status) {
  const map = {
    'confirmed_top':    ['badge-top',     '▼ Confirmed Top'],
    'confirmed_bottom': ['badge-bottom',  '▲ Confirmed Bottom'],
    'pending_top':      ['badge-pending', '⏳ Pending Top'],
    'pending_bottom':   ['badge-pending', '⏳ Pending Bottom'],
    'idle':             ['badge-idle',    '· Idle'],
  };
  const [cls, label] = map[status] || ['badge-idle', '· Idle'];
  return `<span class="badge ${cls}">${label}</span>`;
}

// ── Stock card rendering ──────────────────────────────────────────────────

function renderCard(stock, isCustom=false) {
  const [color, bg, border] = scoreColor(stock.status);
  const score = scoreNumber(stock.status);
  const price  = stock.price  > 0 ? `$${stock.price.toFixed(2)}`  : '$--';
  const change = stock.price  > 0 ? `${stock.changePct >= 0 ? '+' : ''}${stock.changePct.toFixed(2)}%` : '--';
  const chgColor = stock.changePct >= 0 ? '#4ade80' : '#f87171';

  return `
  <div class="card" style="padding:14px;cursor:pointer;transition:background .15s;" onmouseenter="this.style.background='#131f35'" onmouseleave="this.style.background=''" >
    <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:10px;">
      <div style="display:flex;gap:8px;align-items:flex-start;">
        <div class="score-circle" style="color:${color};background:${bg};border-color:${border};flex-shrink:0;">${score}</div>
        <div>
          <div style="font-weight:900;font-size:14px;">${stock.symbol}</div>
          <div style="font-size:10px;color:var(--muted);max-width:90px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${stock.name}</div>
        </div>
      </div>
      <div style="text-align:right;">
        <div style="font-weight:600;font-size:13px;">${price}</div>
        <div style="font-size:11px;color:${chgColor};">${change}</div>
        <button onclick="openHistory('${stock.symbol}')" style="margin-top:4px;background:none;border:none;cursor:pointer;font-size:14px;" title="View 30-day history, RSI & MACD">📊</button>
      </div>
    </div>
    <div style="display:flex;align-items:center;justify-content:space-between;margin-top:2px;">
      <div onclick="openHistory('${stock.symbol}')" style="flex:1;">${statusBadge(stock.status)}</div>
      <button onclick="event.stopPropagation();prefillPriceAlert('${stock.symbol}','${stock.price > 0 ? stock.price.toFixed(2) : ''}')"
        style="background:none;border:none;cursor:pointer;font-size:13px;opacity:.5;padding:2px 3px;line-height:1;" title="Set price alert">🎯</button>
    </div>
  </div>`;
}

// ── Data fetching ──────────────────────────────────────────────────────────

async function refreshStocks() {
  try {
    const [stocks, summary] = await Promise.all([
      fetch('/api/stocks').then(r => r.json()),
      fetch('/api/alerts/summary').then(r => r.json()),
    ]);

    // Summary
    document.getElementById('tops-today').textContent = summary.topsToday;
    document.getElementById('bots-today').textContent = summary.bottomsToday;
    document.getElementById('total-alerts').textContent = summary.totalAlertsToday;

    // Check for new confirmed signals → play sound + show toast
    checkNewSignals(stocks);

    let pending = 0;
    const grid = document.getElementById('stock-grid');
    grid.innerHTML = '';
    for (const s of stocks) {
      if (s.status === 'pending_top' || s.status === 'pending_bottom') pending++;
      grid.innerHTML += renderCard(s);
    }
    document.getElementById('pending-count').textContent = pending;

    // Update leaderboard with the same data (no extra fetch)
    renderLeaderboard(stocks);
  } catch(e) { console.error('refreshStocks', e); }
}

async function refreshAlerts() {
  try {
    const alerts = await fetch('/api/alerts').then(r => r.json());
    const el = document.getElementById('alerts-list');
    if (!alerts.length) {
      el.innerHTML = '<div style="padding:16px;text-align:center;color:var(--muted);font-size:12px;">No alerts yet</div>';
      return;
    }
    el.innerHTML = alerts.slice(0,10).map(a => {
      const isTop = a.type === 'top';
      const t = new Date(a.alerted_at + 'Z').toLocaleTimeString('en-US',{hour:'numeric',minute:'2-digit'});
      return `<div style="padding:10px 14px;border-bottom:1px solid var(--border);display:flex;justify-content:space-between;align-items:center;cursor:pointer;" onclick="openHistory('${a.symbol}')">
        <div>
          <div style="display:flex;gap:8px;align-items:center;">
            <span style="font-weight:700;font-size:13px;">${a.symbol}</span>
            <span class="badge ${isTop ? 'badge-top' : 'badge-bottom'}">${isTop ? '▼ TOP' : '▲ BOT'}</span>
          </div>
          <div style="font-size:10px;color:var(--muted);margin-top:2px;">${t}</div>
        </div>
        <span style="font-size:12px;font-weight:600;">$${Number(a.price).toFixed(2)}</span>
      </div>`;
    }).join('');
  } catch(e) { console.error('refreshAlerts', e); }
}

async function refreshStatus() {
  try {
    const s = await fetch('/api/status').then(r => r.json());
    const el = document.getElementById('scan-status');
    el.textContent = s.scanning
      ? '⟳ Scanning…'
      : `Last scan: ${s.lastScanTime}`;
    const nx = document.getElementById('next-scan');
    const mins = Math.floor(s.nextScanIn / 60);
    const secs = s.nextScanIn % 60;
    nx.textContent = s.nextScanIn > 0 ? `Next scan in: ${mins}:${String(secs).padStart(2,'0')}` : '';
  } catch(e) {}
}

// ── Custom watchlist ───────────────────────────────────────────────────────

async function addCustom() {
  const input = document.getElementById('custom-input');
  const raw = input.value.trim();
  if (!raw) return;
  const syms = raw.toUpperCase().split(/[\s,]+/).filter(s => /^[A-Z]{1,6}$/.test(s));
  if (!syms.length) { alert('No valid ticker symbols found.'); return; }
  input.value = '';
  await fetch('/api/custom/symbols', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({symbols: syms}),
  });
  refreshCustom();
}

async function refreshCustom() {
  try {
    const stocks = await fetch('/api/custom/stocks').then(r => r.json());
    const grid = document.getElementById('custom-grid');
    grid.innerHTML = stocks.map(s => renderCard(s, true)).join('');
  } catch(e) {}
}

// ── History modal ──────────────────────────────────────────────────────────

async function openHistory(symbol) {
  document.getElementById('modal').style.display = 'flex';
  document.getElementById('modal-symbol').textContent = symbol;
  document.getElementById('modal-price').textContent = '';
  document.getElementById('modal-rsi-badge').textContent = '';
  document.getElementById('modal-macd-badge').textContent = '';
  document.getElementById('pivot-bar').innerHTML = '';
  document.getElementById('modal-loading').style.display = 'block';
  document.getElementById('modal-body').innerHTML = '<div id="modal-loading" style="padding:40px;text-align:center;color:var(--muted);">Loading ' + symbol + ' history…</div>';

  _historyData = null;
  _currentTab = 'chart';
  _currentPeriod = '30d';
  document.getElementById('tab-chart').classList.add('active');
  document.getElementById('tab-table').classList.remove('active');
  document.getElementById('period-bar').style.display = 'flex';
  document.getElementById('period-30d').classList.add('active');
  document.getElementById('period-2d').classList.remove('active');

  try {
    const data = await fetch('/api/stocks/' + symbol + '/history').then(r => r.json());
    _historyData = data;

    // Header stats
    if (data.daily && data.daily.length > 0) {
      const last  = data.daily[data.daily.length - 1];
      const prev2 = data.daily.length > 1 ? data.daily[data.daily.length - 2] : last;
      const chg   = ((last.close - prev2.close) / prev2.close * 100).toFixed(2);
      document.getElementById('modal-price').innerHTML =
        `<span style="font-weight:700;">$${last.close.toFixed(2)}</span>
         <span style="font-size:11px;color:${chg>=0?'#4ade80':'#f87171'};">${chg>=0?'+':''}${chg}%</span>`;
    }

    if (data.rsi && data.rsi.length) {
      const rv = data.rsi[data.rsi.length-1].value;
      const rColor = rv >= 70 ? '#f87171' : rv <= 30 ? '#4ade80' : '#f59e0b';
      const rBg    = rv >= 70 ? 'rgba(239,68,68,.15)' : rv <= 30 ? 'rgba(34,197,94,.15)' : 'rgba(245,158,11,.1)';
      document.getElementById('modal-rsi-badge').innerHTML =
        `<span class="badge" style="color:${rColor};background:${rBg};">RSI ${rv.toFixed(1)}</span>`;
    }

    if (data.macd && data.macd.length) {
      const mv = data.macd[data.macd.length-1].macd;
      const mColor = mv >= 0 ? '#4ade80' : '#f87171';
      const mBg    = mv >= 0 ? 'rgba(34,197,94,.15)' : 'rgba(239,68,68,.15)';
      document.getElementById('modal-macd-badge').innerHTML =
        `<span class="badge" style="color:${mColor};background:${mBg};">MACD ${mv>=0?'+':''}${mv.toFixed(3)}</span>`;
    }

    if (data.pivots) {
      const p = data.pivots;
      const pivotBar = document.getElementById('pivot-bar');
      pivotBar.innerHTML = [
        ['R3', p.r3, '#dc2626','rgba(220,38,38,.1)'],
        ['R2', p.r2, '#ef4444','rgba(239,68,68,.1)'],
        ['R1', p.r1, '#f87171','rgba(248,113,113,.1)'],
        ['PP', p.pp, '#f59e0b','rgba(245,158,11,.12)'],
        ['S1', p.s1, '#4ade80','rgba(74,222,128,.1)'],
        ['S2', p.s2, '#22c55e','rgba(34,197,94,.1)'],
        ['S3', p.s3, '#16a34a','rgba(22,163,74,.1)'],
      ].map(([label, val, color, bg]) =>
        `<span class="badge" style="color:${color};background:${bg};font-weight:700;">${label} $${val.toFixed(2)}</span>`
      ).join('');
    }

    renderModal();
  } catch(e) {
    document.getElementById('modal-body').innerHTML =
      '<div style="padding:40px;text-align:center;color:#f87171;">Failed to load data. Market may be closed.</div>';
  }
}

function closeModal() {
  document.getElementById('modal').style.display = 'none';
  destroyCharts();
  _historyData = null;
}

function destroyCharts() {
  if (_lwChart) { try { _lwChart.remove(); } catch(e){} _lwChart = null; _lwSeries = null; }
  if (_rsiChart) { _rsiChart.destroy(); _rsiChart = null; }
  if (_macdChart) { _macdChart.destroy(); _macdChart = null; }
}

function switchTab(tab) {
  _currentTab = tab;
  document.getElementById('tab-chart').classList.toggle('active', tab==='chart');
  document.getElementById('tab-table').classList.toggle('active', tab==='table');
  document.getElementById('period-bar').style.display = tab==='chart' ? 'flex' : 'none';
  destroyCharts();
  renderModal();
}

function switchPeriod(period) {
  _currentPeriod = period;
  document.getElementById('period-30d').classList.toggle('active', period==='30d');
  document.getElementById('period-2d').classList.toggle('active', period==='2d');
  destroyCharts();
  renderModal();
}

function renderModal() {
  if (!_historyData) return;
  const body = document.getElementById('modal-body');

  if (_currentTab === 'table') {
    body.innerHTML = renderTable();
    return;
  }

  // Chart view
  if (_currentPeriod === '30d') {
    body.innerHTML = `
      <div style="background:rgba(0,0,0,.2);border:1px solid var(--border);border-radius:8px;padding:12px;">
        <div style="font-size:9px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px;">30-Day Daily Candlestick · Pivot S/R Levels</div>
        <div id="lw-chart" style="height:260px;"></div>
      </div>
      <div style="background:rgba(0,0,0,.2);border:1px solid var(--border);border-radius:8px;padding:12px;">
        <div style="font-size:9px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px;">RSI (14) — Overbought &gt;70 · Oversold &lt;30</div>
        <canvas id="rsi-chart" height="100"></canvas>
      </div>
      <div style="background:rgba(0,0,0,.2);border:1px solid var(--border);border-radius:8px;padding:12px;">
        <div style="font-size:9px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px;">MACD (12/26/9) — Histogram · MACD Line · Signal Line</div>
        <canvas id="macd-chart" height="110"></canvas>
      </div>`;

    requestAnimationFrame(() => {
      renderLwChart(_historyData.daily, _historyData.pivots);
      renderRsiChart(_historyData.rsi);
      renderMacdChart(_historyData.macd);
    });

  } else {
    // 2-day intraday
    const intra = _historyData.intraday || [];
    body.innerHTML = `
      <div style="background:rgba(0,0,0,.2);border:1px solid var(--border);border-radius:8px;padding:12px;">
        <div style="font-size:9px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px;">2-Day Intraday (15m candles)</div>
        <div id="lw-chart" style="height:280px;"></div>
      </div>
      ${intra.length ? '<div style="overflow:auto;border:1px solid var(--border);border-radius:8px;">' + renderIntradayTable(intra) + '</div>' : ''}`;

    requestAnimationFrame(() => renderLwChart(intra, null, true));
  }
}

// ── Lightweight Charts (candlestick) ───────────────────────────────────────

function renderLwChart(data, pivots, isIntraday=false) {
  const container = document.getElementById('lw-chart');
  if (!container || !data || !data.length) return;

  _lwChart = LightweightCharts.createChart(container, {
    width: container.clientWidth,
    height: container.clientHeight,
    layout: { background: {color:'transparent'}, textColor:'#64748b', fontFamily:"'JetBrains Mono',Menlo,monospace", fontSize:10 },
    grid: { vertLines:{color:'#1e293b'}, horzLines:{color:'#1e293b'} },
    crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
    rightPriceScale: { borderColor:'#1e293b', textColor:'#64748b' },
    timeScale: { borderColor:'#1e293b', timeVisible:isIntraday, secondsVisible:false },
    handleScroll: true, handleScale: true,
  });

  _lwSeries = _lwChart.addCandlestickSeries({
    upColor:'#22c55e', downColor:'#ef4444', borderVisible:false, wickUpColor:'#22c55e', wickDownColor:'#ef4444',
  });

  _lwSeries.setData(data.map(c => ({ time: c.time, open: c.open, high: c.high, low: c.low, close: c.close })));

  // Draw pivot lines
  if (pivots) {
    const lines = [
      {price:pivots.r3, color:'#dc2626cc', label:'R3', style:1},
      {price:pivots.r2, color:'#ef4444cc', label:'R2', style:2},
      {price:pivots.r1, color:'#f87171cc', label:'R1', style:2},
      {price:pivots.pp, color:'#f59e0bcc', label:' PP', style:0},
      {price:pivots.s1, color:'#4ade80cc', label:'S1', style:2},
      {price:pivots.s2, color:'#22c55ecc', label:'S2', style:2},
      {price:pivots.s3, color:'#16a34acc', label:'S3', style:1},
    ];
    for (const l of lines) {
      _lwSeries.createPriceLine({ price:l.price, color:l.color, lineWidth:1, lineStyle:l.style, axisLabelVisible:true, title:l.label });
    }
  }

  _lwChart.timeScale().fitContent();

  // Responsive resize
  const ro = new ResizeObserver(([entry]) => {
    if (_lwChart) _lwChart.applyOptions({width: entry.contentRect.width});
  });
  ro.observe(container);
}

// ── Chart.js helpers ───────────────────────────────────────────────────────

const CHART_DEFAULTS = {
  color: '#64748b',
  plugins: { legend:{ labels:{ color:'#64748b', font:{size:9}, boxWidth:8 } }, tooltip:{ backgroundColor:'#0f172a', borderColor:'#1e293b', borderWidth:1, titleFont:{size:10}, bodyFont:{size:10}, titleColor:'#94a3b8', bodyColor:'#cbd5e1' } },
  scales: {
    x: { grid:{color:'#1e293b'}, ticks:{color:'#64748b',font:{size:9},maxTicksLimit:8}, border:{color:'#1e293b'} },
    y: { grid:{color:'#1e293b'}, ticks:{color:'#64748b',font:{size:9}},       border:{color:'transparent'} },
  },
};

function renderRsiChart(rsiData) {
  const ctx = document.getElementById('rsi-chart');
  if (!ctx || !rsiData || !rsiData.length) return;
  _rsiChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels: rsiData.map(r => fmtDate(r.time)),
      datasets: [{
        label:'RSI', data: rsiData.map(r => r.value),
        borderColor:'#f59e0b', borderWidth:1.5, pointRadius:0, tension:0.2,
      }],
    },
    options: {
      ...CHART_DEFAULTS,
      responsive:true, maintainAspectRatio:true,
      scales: {
        ...CHART_DEFAULTS.scales,
        y: { ...CHART_DEFAULTS.scales.y, min:0, max:100,
             ticks:{...CHART_DEFAULTS.scales.y.ticks, stepSize:10, callback:v=>[0,30,50,70,100].includes(v)?v:''},
        },
      },
      plugins: {
        ...CHART_DEFAULTS.plugins,
        annotation: undefined,
        legend: { display:false },
      },
    },
  });

  // Draw 70/30 reference lines via afterDraw
  Chart.register({
    id:'rsiLines',
    afterDraw(chart) {
      const {ctx:c, chartArea:{left,right,top}, scales:{y}} = chart;
      if (!y) return;
      for (const [val,color] of [[70,'rgba(239,68,68,.5)'],[30,'rgba(34,197,94,.5)']]) {
        const yPx = y.getPixelForValue(val);
        c.save(); c.beginPath(); c.setLineDash([4,3]);
        c.moveTo(left, yPx); c.lineTo(right, yPx);
        c.strokeStyle = color; c.lineWidth = 1; c.stroke(); c.restore();
      }
    }
  });
}

function renderMacdChart(macdData) {
  const ctx = document.getElementById('macd-chart');
  if (!ctx || !macdData || !macdData.length) return;
  _macdChart = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: macdData.map(m => fmtDate(m.time)),
      datasets: [
        { type:'bar',  label:'Histogram', data:macdData.map(m=>m.histogram), backgroundColor:macdData.map(m=>m.histogram>=0?'rgba(99,102,241,.6)':'rgba(239,68,68,.5)'), barPercentage:0.8 },
        { type:'line', label:'MACD',      data:macdData.map(m=>m.macd),      borderColor:'#22c55e', borderWidth:1.5, pointRadius:0, tension:0.2 },
        { type:'line', label:'Signal',    data:macdData.map(m=>m.signal),    borderColor:'#f87171', borderWidth:1.5, pointRadius:0, tension:0.2, borderDash:[4,2] },
      ],
    },
    options: { ...CHART_DEFAULTS, responsive:true, maintainAspectRatio:true },
  });
}

// ── Tables ─────────────────────────────────────────────────────────────────

function renderTable() {
  if (!_historyData || !_historyData.daily.length) return '<div style="padding:20px;text-align:center;color:var(--muted);">No data</div>';
  const rsiMap  = Object.fromEntries((_historyData.rsi  || []).map(r => [r.time,  r.value]));
  const macdMap = Object.fromEntries((_historyData.macd || []).map(m => [m.time, m]));
  const daily   = [..._historyData.daily].reverse();

  const rows = daily.map((d, i) => {
    const prev  = daily[i+1];
    const chg   = prev ? ((d.close - prev.close)/prev.close*100) : NaN;
    const rsi   = rsiMap[d.time];
    const macd  = macdMap[d.time];
    const up    = chg >= 0;
    return `<tr>
      <td>${fmtDate(d.time)}</td>
      <td>$${fmtNum(d.open)}</td>
      <td style="color:#4ade80;">$${fmtNum(d.high)}</td>
      <td style="color:#f87171;">$${fmtNum(d.low)}</td>
      <td style="font-weight:600;">$${fmtNum(d.close)}</td>
      <td style="color:${isNaN(chg)?'var(--muted)':up?'#4ade80':'#f87171'};">${isNaN(chg)?'—':(up?'+':'')+fmtNum(chg)}%</td>
      <td style="color:var(--muted);">${fmtVol(d.volume)}</td>
      <td style="color:${!rsi?'var(--muted)':rsi>=70?'#f87171':rsi<=30?'#4ade80':'#f59e0b'};font-weight:600;">${rsi?fmtNum(rsi,1):'—'}</td>
      <td style="color:${!macd?'var(--muted)':macd.macd>=0?'#4ade80':'#f87171'};">${macd?fmtNum(macd.macd,3):'—'}</td>
      <td style="color:var(--muted);">${macd?fmtNum(macd.signal,3):'—'}</td>
      <td style="color:${!macd?'var(--muted)':macd.histogram>=0?'#4ade80':'#f87171'};">${macd?fmtNum(macd.histogram,3):'—'}</td>
    </tr>`;
  }).join('');

  return `<div style="overflow:auto;border:1px solid var(--border);border-radius:8px;">
  <table><thead><tr>
    <th>Date</th><th>Open</th><th>High</th><th>Low</th><th>Close</th>
    <th>Chg%</th><th>Volume</th><th>RSI</th><th>MACD</th><th>Signal</th><th>Hist</th>
  </tr></thead><tbody>${rows}</tbody></table></div>`;
}

function renderIntradayTable(data) {
  const rows = [...data].reverse().map(c =>
    `<tr>
      <td>${fmtTime(c.time)}</td>
      <td>$${fmtNum(c.open)}</td>
      <td style="color:#4ade80;">$${fmtNum(c.high)}</td>
      <td style="color:#f87171;">$${fmtNum(c.low)}</td>
      <td style="font-weight:600;">$${fmtNum(c.close)}</td>
      <td style="color:var(--muted);">${fmtVol(c.volume)}</td>
    </tr>`
  ).join('');
  return `<table><thead><tr><th>Time</th><th>Open</th><th>High</th><th>Low</th><th>Close</th><th>Volume</th></tr></thead><tbody>${rows}</tbody></table>`;
}

// ── Sound alerts ───────────────────────────────────────────────────────────

let _audioEnabled = true;
let _audioCtx     = null;
let _prevStates   = {};   // symbol → status, seeded after first fetch

function getAudioCtx() {
  if (!_audioCtx) {
    _audioCtx = new (window.AudioContext || window.webkitAudioContext)();
    // Unlock on first user gesture (browser autoplay policy)
    document.addEventListener('click', () => {
      if (_audioCtx && _audioCtx.state === 'suspended') _audioCtx.resume();
    }, { once: true });
  }
  return _audioCtx;
}

function playAlertSound(type) {
  if (!_audioEnabled) return;
  try {
    const ctx = getAudioCtx();
    if (ctx.state === 'suspended') ctx.resume();
    const now   = ctx.currentTime;
    // TOP: descending two-tone | BOTTOM: ascending two-tone | PRICE: three-note chime
    const tones = type === 'top'
      ? [[880, 0.00, 0.35], [660, 0.20, 0.40]]
      : type === 'bottom'
      ? [[440, 0.00, 0.35], [660, 0.20, 0.40]]
      : [[523, 0.00, 0.22], [659, 0.18, 0.22], [784, 0.36, 0.35]]; // price alert: C-E-G chime
    for (const [freq, delay, dur] of tones) {
      const osc  = ctx.createOscillator();
      const gain = ctx.createGain();
      osc.connect(gain);
      gain.connect(ctx.destination);
      osc.type = 'sine';
      osc.frequency.setValueAtTime(freq, now + delay);
      gain.gain.setValueAtTime(0,    now + delay);
      gain.gain.linearRampToValueAtTime(0.15,  now + delay + 0.025);
      gain.gain.exponentialRampToValueAtTime(0.001, now + delay + dur);
      osc.start(now + delay);
      osc.stop(now  + delay + dur + 0.05);
    }
  } catch(e) { console.warn('Audio error:', e); }
}

function toggleSound() {
  _audioEnabled = !_audioEnabled;
  const btn   = document.getElementById('sound-btn');
  const icon  = document.getElementById('sound-icon');
  const label = document.getElementById('sound-label');
  if (_audioEnabled) {
    btn.classList.add('on');
    icon.textContent  = '🔔';
    label.textContent = 'Sound ON';
    // Tiny test ping on enable so user hears it works
    playAlertSound('bottom');
  } else {
    btn.classList.remove('on');
    icon.textContent  = '🔕';
    label.textContent = 'Sound OFF';
  }
}

// Toast pop-up notification
let _toastQueue = [];
function showToast(symbol, type, price, subtitle) {
  // Stack multiple toasts vertically
  const offset  = _toastQueue.length * 94;
  const isTop   = type === 'top';
  const isPrice = type === 'price';

  const color  = isPrice ? '#818cf8' : isTop ? '#f87171' : '#4ade80';
  const border = isPrice ? 'rgba(99,102,241,.5)' : isTop ? 'rgba(248,113,113,.5)' : 'rgba(74,222,128,.5)';
  const icon   = isPrice ? '🎯' : isTop ? '🔴' : '🟢';
  const title  = isPrice
    ? `${symbol} — PRICE TARGET HIT`
    : `${symbol} — ${isTop ? 'TOP' : 'BOTTOM'} CONFIRMED`;
  const sub = subtitle || `$${Number(price).toFixed(2)} · Click to view charts`;

  const el = document.createElement('div');
  el.className = 'toast';
  el.style.cssText += `border:1px solid ${border};bottom:${24 + offset}px;`;
  el.innerHTML = `
    <div style="font-size:22px;line-height:1;">${icon}</div>
    <div style="flex:1;">
      <div style="font-weight:900;font-size:13px;color:${color};">${title}</div>
      <div style="font-size:11px;color:var(--muted);margin-top:3px;">${sub}</div>
    </div>
    <div style="font-size:13px;color:var(--muted);cursor:pointer;" onclick="this.closest('.toast').remove()">✕</div>`;
  el.onclick = (e) => {
    if (e.target.textContent !== '✕') openHistory(symbol);
  };
  document.body.appendChild(el);
  _toastQueue.push(el);

  setTimeout(() => {
    el.classList.add('out');
    setTimeout(() => {
      el.remove();
      _toastQueue = _toastQueue.filter(t => t !== el);
    }, 320);
  }, 5500);
}

// Compare previous and current stock states to detect new confirmations
let _prevStatesSeeded = false;
function checkNewSignals(stocks) {
  for (const s of stocks) {
    const prev = _prevStates[s.symbol];
    if (!_prevStatesSeeded) {
      // Seed on first run — don't fire alerts for historical state
      _prevStates[s.symbol] = s.status;
      continue;
    }
    if (prev !== s.status) {
      if (s.status === 'confirmed_top') {
        playAlertSound('top');
        showToast(s.symbol, 'top', s.price);
      } else if (s.status === 'confirmed_bottom') {
        playAlertSound('bottom');
        showToast(s.symbol, 'bottom', s.price);
      }
      _prevStates[s.symbol] = s.status;
    }
  }
  _prevStatesSeeded = true;
}

// ── Price alerts (JS) ──────────────────────────────────────────────────────

let _priceAlerts = [];
let _paSeenIds   = new Set();   // IDs seen on first load — don't toast historical

function prefillPriceAlert(symbol, price) {
  document.getElementById('pa-symbol').value = symbol;
  if (price) document.getElementById('pa-target').value = price;
  document.getElementById('pa-symbol').focus();
  document.getElementById('pa-symbol').scrollIntoView({behavior:'smooth', block:'nearest'});
}

async function addPriceAlert() {
  const symbol = document.getElementById('pa-symbol').value.trim().toUpperCase();
  const target = parseFloat(document.getElementById('pa-target').value);
  const dir    = document.getElementById('pa-dir').value;
  const note   = document.getElementById('pa-note').value.trim();
  if (!symbol || isNaN(target) || target <= 0) {
    document.getElementById('pa-symbol').style.borderColor = '#f87171';
    setTimeout(() => document.getElementById('pa-symbol').style.borderColor = '', 1500);
    return;
  }
  try {
    const res = await fetch('/api/price-alerts', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({symbol, target, direction: dir, note}),
    });
    if (!res.ok) { const e = await res.json(); alert(e.error || 'Error'); return; }
    const pa = await res.json();
    // Immediately add to seen set so it doesn't trigger a false toast
    _paSeenIds.add(pa.id);
    document.getElementById('pa-symbol').value = '';
    document.getElementById('pa-target').value = '';
    document.getElementById('pa-note').value   = '';
    await loadPriceAlerts();
  } catch(e) { console.error('addPriceAlert', e); }
}

async function deletePriceAlert(id) {
  try {
    await fetch(`/api/price-alerts/${id}`, {method: 'DELETE'});
    await loadPriceAlerts();
  } catch(e) { console.error('deletePriceAlert', e); }
}

async function loadPriceAlerts() {
  try {
    const alerts = await fetch('/api/price-alerts').then(r => r.json());
    const isFirstLoad = _priceAlerts.length === 0 && _paSeenIds.size === 0;

    // Detect newly triggered alerts (not seen before) → play sound + toast
    for (const a of alerts) {
      if (a.triggered && !_paSeenIds.has(a.id)) {
        if (!isFirstLoad) {
          playAlertSound('price');
          showToast(
            a.symbol, 'price',
            a.target_price,
            `${a.direction === 'above' ? '↑ Above' : '↓ Below'} $${a.target_price.toFixed(2)}${a.note ? ' · ' + a.note : ''}`
          );
        }
        _paSeenIds.add(a.id);
      } else {
        _paSeenIds.add(a.id);
      }
    }

    _priceAlerts = alerts;

    // Render
    const list    = document.getElementById('pa-list');
    const counter = document.getElementById('pa-count');
    const active  = alerts.filter(a => !a.triggered);
    counter.textContent = `${active.length} active`;

    if (!alerts.length) {
      list.innerHTML = '<div style="padding:14px;text-align:center;color:var(--muted);font-size:11px;">No price alerts set</div>';
      return;
    }

    list.innerHTML = alerts.map(a => {
      const done  = a.triggered;
      const isAbove = a.direction === 'above';
      const arrowColor = isAbove ? '#4ade80' : '#f87171';
      const dimmed = done ? 'opacity:.45;' : '';
      const ts = a.triggered_at
        ? new Date(a.triggered_at + 'Z').toLocaleTimeString('en-US',{hour:'numeric',minute:'2-digit'})
        : new Date(a.created_at + 'Z').toLocaleTimeString('en-US',{hour:'numeric',minute:'2-digit'});
      return `<div style="padding:9px 12px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:8px;${dimmed}">
        <span style="font-size:14px;">${done ? '✅' : '🎯'}</span>
        <div style="flex:1;min-width:0;">
          <div style="display:flex;align-items:center;gap:6px;">
            <span style="font-weight:700;font-size:12px;">${a.symbol}</span>
            <span style="color:${arrowColor};font-size:10px;font-weight:700;">${isAbove ? '↑' : '↓'} $${Number(a.target_price).toFixed(2)}</span>
            ${done ? `<span style="font-size:9px;color:var(--muted);">HIT ${ts}</span>` : ''}
          </div>
          ${a.note ? `<div style="font-size:9px;color:var(--muted);margin-top:1px;">${a.note}</div>` : ''}
        </div>
        <button onclick="deletePriceAlert(${a.id})"
          style="background:none;border:none;color:var(--muted);cursor:pointer;font-size:14px;padding:0 2px;opacity:.6;" title="Remove">✕</button>
      </div>`;
    }).join('');
  } catch(e) { console.error('loadPriceAlerts', e); }
}

// ── Ticker tape ────────────────────────────────────────────────────────────

let _tickerStocks = [];

function buildTickerHTML(stocks) {
  if (!stocks.length) return '';
  // Duplicate list so the second copy fills the gap when first scrolls off
  const items = [...stocks, ...stocks].map(s => {
    const up      = s.changePct >= 0;
    const hasData = s.price > 0;
    const dotColor = s.status === 'confirmed_top'    ? '#f87171'
                   : s.status === 'confirmed_bottom' ? '#4ade80'
                   : s.status === 'pending_top'      ? '#fb923c'
                   : s.status === 'pending_bottom'   ? '#fb923c'
                   : '#334155';
    return `<span class="ticker-item" onclick="openHistory('${s.symbol}')">
      <span class="ticker-dot" style="background:${dotColor};"></span>
      <span class="ticker-sym">${s.symbol}</span>
      <span class="ticker-price">${hasData ? '$'+s.price.toFixed(2) : '$--'}</span>
      ${hasData
        ? `<span class="${up ? 'ticker-up' : 'ticker-dn'}">${up ? '▲' : '▼'} ${Math.abs(s.changePct).toFixed(2)}%</span>`
        : `<span style="color:var(--muted);">--%</span>`}
    </span>`;
  }).join('');
  return items;
}

async function refreshTicker() {
  try {
    const stocks = await fetch('/api/stocks').then(r => r.json());
    // Sort: confirmed signals first, then by changePct magnitude
    stocks.sort((a, b) => {
      const sigA = a.status.startsWith('confirmed') ? 2 : a.status.startsWith('pending') ? 1 : 0;
      const sigB = b.status.startsWith('confirmed') ? 2 : b.status.startsWith('pending') ? 1 : 0;
      if (sigB !== sigA) return sigB - sigA;
      return Math.abs(b.changePct) - Math.abs(a.changePct);
    });
    _tickerStocks = stocks;

    const track = document.getElementById('ticker-track');
    if (!track) return;

    // Preserve animation timing by only updating innerHTML when data changes
    const newHTML = buildTickerHTML(stocks);
    if (track.innerHTML !== newHTML) {
      // Restart animation smoothly by removing & re-adding class
      track.style.animation = 'none';
      track.innerHTML = newHTML;
      requestAnimationFrame(() => {
        requestAnimationFrame(() => { track.style.animation = ''; });
      });
    }
  } catch(e) { console.error('refreshTicker', e); }
}

// ── Initialization ─────────────────────────────────────────────────────────

async function init() {
  await Promise.all([refreshStocks(), refreshAlerts(), refreshStatus(), refreshTicker(), loadPriceAlerts(), loadScannerHistory()]);
}

init();
setInterval(refreshStocks,    15000);
setInterval(refreshAlerts,    30000);
setInterval(refreshStatus,     3000);
setInterval(refreshCustom,    15000);
setInterval(refreshTicker,    15000);
setInterval(loadPriceAlerts,    20000);  // check for triggered price alerts every 20s
setInterval(loadScannerHistory, 60000);  // refresh scanner history every minute

// ── Scanner History ──────────────────────────────────────────────────────────

async function loadScannerHistory() {
  const symbol = (document.getElementById('sh-symbol').value || '').trim().toUpperCase();
  const type   = document.getElementById('sh-type').value;
  const from   = document.getElementById('sh-from').value;
  const to     = document.getElementById('sh-to').value;

  const params = new URLSearchParams();
  if (symbol) params.set('symbol', symbol);
  if (type !== 'all') params.set('type', type);
  if (from)   params.set('from', from);
  if (to)     params.set('to', to);

  try {
    const res  = await fetch('/api/scanner-history?' + params.toString());
    const rows = await res.json();
    renderHistory(rows);
  } catch(e) {
    document.getElementById('sh-body').innerHTML =
      '<div style="padding:20px;text-align:center;color:#f87171;font-size:12px;">Error loading history</div>';
  }
}

function renderHistory(rows) {
  const countEl = document.getElementById('sh-count');
  countEl.textContent = rows.length + ' record' + (rows.length !== 1 ? 's' : '');

  if (!rows.length) {
    document.getElementById('sh-body').innerHTML =
      '<div style="padding:24px;text-align:center;color:var(--muted);font-size:12px;">No records match the current filters.</div>';
    return;
  }

  const sigMap = {
    top:          ['▼ Confirmed Top',    '#f87171', 'rgba(248,113,113,.12)'],
    bottom:       ['▲ Confirmed Bottom', '#4ade80', 'rgba(74,222,128,.12)'],
    price_target: ['🎯 Price Target',    '#818cf8', 'rgba(129,140,248,.12)'],
  };

  const thead = `<tr>
    <th style="text-align:left;">Time (Local)</th>
    <th style="text-align:left;">Symbol</th>
    <th style="text-align:left;">Type</th>
    <th style="text-align:right;">Price</th>
    <th style="text-align:right;">Chg %</th>
    <th style="text-align:left;">Note</th>
  </tr>`;

  const tbody = rows.map(r => {
    const dt      = new Date(r.fired_at.endsWith('Z') ? r.fired_at : r.fired_at + 'Z');
    const timeStr = isNaN(dt) ? r.fired_at : dt.toLocaleString(undefined, {
      month:'2-digit', day:'2-digit', year:'2-digit',
      hour:'2-digit',  minute:'2-digit', hour12:true
    });
    const [sigl, sigc, sigbg] = sigMap[r.signal_type] || ['?', '#64748b', 'transparent'];
    const up       = r.change_pct >= 0;
    const chgColor = up ? '#4ade80' : '#f87171';

    return `<tr style="cursor:pointer;"
        onclick="openHistory('${r.symbol}')"
        onmouseenter="this.style.background='rgba(255,255,255,.035)'"
        onmouseleave="this.style.background=''">
      <td style="text-align:left;color:var(--muted);font-size:10px;white-space:nowrap;">${timeStr}</td>
      <td style="text-align:left;">
        <div style="font-weight:700;font-size:12px;">${r.symbol}</div>
        <div style="font-size:9px;color:var(--muted);max-width:90px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${r.name}</div>
      </td>
      <td>
        <span style="display:inline-flex;align-items:center;gap:3px;font-size:9px;font-weight:700;padding:2px 7px;border-radius:4px;color:${sigc};background:${sigbg};white-space:nowrap;">
          ${sigl}
        </span>
      </td>
      <td style="text-align:right;font-weight:600;font-size:12px;">$${r.price.toFixed(2)}</td>
      <td style="text-align:right;font-weight:700;color:${chgColor};">${up?'+':''}${r.change_pct.toFixed(2)}%</td>
      <td style="text-align:left;font-size:10px;color:var(--muted);max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${r.note || '—'}</td>
    </tr>`;
  }).join('');

  document.getElementById('sh-body').innerHTML = `
    <table style="width:100%;border-collapse:collapse;font-size:11px;">
      <thead>${thead}</thead>
      <tbody>${tbody}</tbody>
    </table>`;
}

function exportHistoryCSV() {
  const symbol = (document.getElementById('sh-symbol').value || '').trim().toUpperCase();
  const type   = document.getElementById('sh-type').value;
  const from   = document.getElementById('sh-from').value;
  const to     = document.getElementById('sh-to').value;
  const params = new URLSearchParams();
  if (symbol) params.set('symbol', symbol);
  if (type !== 'all') params.set('type', type);
  if (from)   params.set('from', from);
  if (to)     params.set('to', to);
  window.open('/api/scanner-history/export?' + params.toString(), '_blank');
}

async function clearHistory() {
  if (!confirm('Clear ALL scanner history records? This cannot be undone.')) return;
  await fetch('/api/scanner-history/clear', { method: 'DELETE' });
  loadScannerHistory();
}

// ── Leaderboard ─────────────────────────────────────────────────────────────

let _allStocks = [];
let _lbSortKey = 'changePct';
let _lbSortAsc = false;

const LB_SIGNAL_RANK = {
  confirmed_top: 5, confirmed_bottom: 5,
  pending_top:   3, pending_bottom:   3,
  idle:          0,
};

function setLbSort(key) {
  if (_lbSortKey === key) {
    _lbSortAsc = !_lbSortAsc;
  } else {
    _lbSortKey = key;
    _lbSortAsc = false;
  }
  document.querySelectorAll('.lb-sort-btn').forEach(b => b.classList.remove('active'));
  const btn = document.getElementById('lbsort-' + key);
  if (btn) btn.classList.add('active');
  renderLeaderboard(_allStocks);
}

function renderLeaderboard(stocks) {
  if (!stocks || !stocks.length) return;
  _allStocks = stocks;

  const maxAbs = Math.max(...stocks.map(s => Math.abs(s.changePct || 0)), 0.01);

  const sorted = [...stocks].sort((a, b) => {
    const va =
      _lbSortKey === 'changePct' ? Math.abs(a.changePct || 0) :
      _lbSortKey === 'price'     ? (a.price || 0) :
      _lbSortKey === 'score'     ? scoreNumber(a.status) :
      _lbSortKey === 'signal'    ? (LB_SIGNAL_RANK[a.status] || 0) :
      _lbSortKey === 'change'    ? Math.abs(a.change || 0) : 0;
    const vb =
      _lbSortKey === 'changePct' ? Math.abs(b.changePct || 0) :
      _lbSortKey === 'price'     ? (b.price || 0) :
      _lbSortKey === 'score'     ? scoreNumber(b.status) :
      _lbSortKey === 'signal'    ? (LB_SIGNAL_RANK[b.status] || 0) :
      _lbSortKey === 'change'    ? Math.abs(b.change || 0) : 0;
    return _lbSortAsc ? va - vb : vb - va;
  });

  const arrow = _lbSortAsc ? ' ▲' : ' ▼';
  const hdr = (label, key, align='right') => {
    const active = _lbSortKey === key;
    return `<th onclick="${key ? `setLbSort('${key}')` : ''}"
      style="text-align:${align};${key ? 'cursor:pointer;user-select:none;' : ''}${active ? 'color:#818cf8;' : ''}">
      ${label}${active ? arrow : ''}</th>`;
  };

  const thead = `<tr>
    ${hdr('#', '', 'left')}
    ${hdr('Symbol', '', 'left')}
    ${hdr('Price', 'price')}
    ${hdr('Chg $', 'change')}
    ${hdr('Chg %', 'changePct')}
    ${hdr('Signal', 'signal', 'left')}
    ${hdr('Score', 'score', 'center')}
  </tr>`;

  const sigStyle = {
    confirmed_top:    ['▼ Conf Top',  '#f87171','rgba(248,113,113,.12)'],
    confirmed_bottom: ['▲ Conf Bot',  '#4ade80','rgba(74,222,128,.12)'],
    pending_top:      ['⏳ Pend Top', '#fb923c','rgba(251,146,60,.10)'],
    pending_bottom:   ['⏳ Pend Bot', '#fb923c','rgba(251,146,60,.10)'],
    idle:             ['· Idle',      '#475569','rgba(71,85,105,.10)'],
  };

  const medalColor = ['#f59e0b','#94a3b8','#b87333'];

  const rows = sorted.map((s, i) => {
    const rank     = i + 1;
    const hasData  = s.price > 0;
    const up       = s.changePct >= 0;
    const chgColor = hasData ? (up ? '#4ade80' : '#f87171') : '#475569';
    const barW     = hasData ? Math.round(Math.abs(s.changePct) / maxAbs * 100) : 0;
    const barBg    = up ? 'rgba(74,222,128,.22)' : 'rgba(248,113,113,.22)';
    const [sigl, sigc, sigbg] = sigStyle[s.status] || sigStyle.idle;
    const [sc, sbg, sbrd]     = scoreColor(s.status);
    const scn                 = scoreNumber(s.status);
    const rankCol = rank <= 3 ? medalColor[rank-1] : '#475569';

    return `<tr style="cursor:pointer;"
        onclick="openHistory('${s.symbol}')"
        onmouseenter="this.style.background='rgba(255,255,255,.035)'"
        onmouseleave="this.style.background=''">
      <td style="text-align:left;font-weight:900;font-size:13px;color:${rankCol};padding-left:14px;">${rank <= 3 ? ['🥇','🥈','🥉'][rank-1] : rank}</td>
      <td style="text-align:left;">
        <div style="font-weight:700;font-size:12px;letter-spacing:.02em;">${s.symbol}</div>
        <div style="font-size:9px;color:var(--muted);max-width:80px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${s.name}</div>
      </td>
      <td style="text-align:right;font-weight:600;font-size:12px;">${hasData ? '$'+s.price.toFixed(2) : '$--'}</td>
      <td style="text-align:right;color:${chgColor};font-weight:600;">${hasData ? (up?'+':'')+s.change.toFixed(2) : '--'}</td>
      <td style="text-align:right;padding-right:12px;">
        <div style="display:flex;align-items:center;gap:6px;justify-content:flex-end;">
          <span style="color:${chgColor};font-weight:700;font-size:12px;min-width:52px;text-align:right;">
            ${hasData ? (up?'+':'')+s.changePct.toFixed(2)+'%' : '--%'}
          </span>
          <div style="width:72px;height:5px;background:rgba(255,255,255,.06);border-radius:3px;overflow:hidden;flex-shrink:0;">
            <div style="height:100%;width:${barW}%;background:${barBg};border-radius:3px;transition:width .6s ease;"></div>
          </div>
        </div>
      </td>
      <td>
        <span style="display:inline-flex;align-items:center;gap:3px;font-size:9px;font-weight:700;padding:2px 6px;border-radius:4px;color:${sigc};background:${sigbg};white-space:nowrap;">
          ${sigl}
        </span>
      </td>
      <td style="text-align:center;">
        <div style="width:26px;height:26px;border-radius:50%;display:inline-flex;align-items:center;justify-content:center;font-weight:900;font-size:11px;color:${sc};background:${sbg};border:1px solid ${sbrd};">
          ${scn}
        </div>
      </td>
    </tr>`;
  }).join('');

  document.getElementById('leaderboard-body').innerHTML = `
    <table style="width:100%;border-collapse:collapse;font-size:11px;">
      <thead>${thead}</thead>
      <tbody>${rows}</tbody>
    </table>`;
}
</script>
</body>
</html>
"""

# ── Main ──────────────────────────────────────────────────────────────────────

def run_once():
    print("\n[GitHub Mode] Running single scan...\n")

    init_db()

    all_stocks = DOW_30.copy()
    combined = all_stocks + [{"symbol": s, "name": s} for s in _custom_symbols if s not in DOW_30_SET]

    for stock in combined:
        try:
            scan_symbol(stock["symbol"], stock["name"])
            print(f"{stock['symbol']} scanned")
        except Exception as exc:
            print(f"Error scanning {stock['symbol']}: {exc}")

    print("\n[GitHub Mode] Scan complete. Exiting.\n")


if __name__ == "__main__":
    # 👉 Detect GitHub Actions environment
    if os.environ.get("GITHUB_ACTIONS") == "true":
        run_once()

    else:
        # ===== LOCAL MODE (your original behavior) =====
        PORT = int(os.environ.get("PORT", 5000))

        print("\n" + "=" * 60)
        print("  DOW 30 TOP/BOTTOM ALERT TRACKER — Python Edition")
        print("=" * 60)
        print(f"  Dashboard → http://localhost:{PORT}")
        print(f"  Database  → {DB_PATH}")
        print(f"  Email     → {'Configured ✓' if os.environ.get('RESEND_API_KEY') else 'Not configured (optional)'}")
        print("=" * 60 + "\n")

        init_db()

        # Start background scanner
        scanner_thread = threading.Thread(target=_run_scanner, daemon=True, name="scanner")
        scanner_thread.start()
        log.info("Scanner thread started — first scan beginning…")

        # Run dashboard
        socketio.run(
            app,
            host="0.0.0.0",
            port=PORT,
            debug=False,
            use_reloader=False,
            allow_unsafe_werkzeug=True
        )
