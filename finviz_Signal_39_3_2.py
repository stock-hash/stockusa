# ==============================================================================
# HYBRID ULTIMATE MARKET SCANNER - FINAL EXPANDED PRO VERSION (v50.2)
# ==============================================================================
# FIXED: KeyError 'BB_Upper' on Intraday Dataframe.
#
# v50.0 FEATURES PRESERVED:
# 1. PRICE TREND MATRIX: 2D to 360D returns.
# 2. BREAKOUTS: 20-Day High/Low detection.
# 3. SMA MATRIX: Price vs SMA 20/50/200.
# 4. VOLATILITY: High/Low IV filters.
# 5. UI: History Toggle, Trend Bars, Forecast Links.
# ==============================================================================

import os
import sys
import time
import json
import math
import re
import logging
import shutil
import threading
import queue
import sqlite3
import imaplib
import email
import io
import webbrowser
import urllib.request
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer
from datetime import datetime, timedelta, timezone

# --- DEPENDENCIES CHECK ---
try:
    import pandas as pd
    import numpy as np
    from bs4 import BeautifulSoup
    from dateutil import parser
except ImportError:
    print("CRITICAL ERROR: Missing libraries.")
    print("Run: pip install pandas numpy beautifulsoup4 lxml openpyxl python-dateutil")
    sys.exit(1)
import pandas_market_calendars as mcal
# ------------------------------------------------------------------------------
# 1. CONFIGURATION & DIRECTORY SETUP
# ------------------------------------------------------------------------------
BASE_DIR = os.getcwd()
OUTPUT_ROOT = os.path.join(BASE_DIR, "docs")
CACHE_DIR = os.path.join(OUTPUT_ROOT, "metadata_cache")
LOG_DIR = os.path.join(OUTPUT_ROOT, "system_logs")
WATCHLIST_FILE = os.path.join(BASE_DIR, "Waatchlista.xlsx")
DB_FILE = os.path.join(BASE_DIR, "market_master_v5.db")

# GMAIL CREDENTIALS
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS", "your_email@gmail.com")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "your_app_password")
SENDER_EMAIL = "stockusals@gmail.com"

# SCANNING PARAMETERS
THREADS = 30
REQUEST_TIMEOUT = 20
INBOX_LOOKBACK_DAYS =200
TRASH_LOOKBACK_DAYS = 14
TRASH_SCAN_LIMIT = 2000
DAILY_LOOKBACK = 400
INTRA_DAYS = 5
INTRA_INTERVAL = "5m"

# *** GLOBAL PROCESSING LIMIT ***
MAX_STOCK_LIMIT = 3000

# TECHNICAL INDICATOR SETTINGS
RSI_PERIOD = 14
BB_PERIOD = 20
BB_STD = 2.0
ATR_PERIOD = 14
PRICE_TREND_DAYS = [2, 3, 5, 7, 9, 11, 15, 30, 60, 90, 180, 360]

# SPECIAL ASSET CATEGORIES
LEVERAGED_ETFS = ["TQQQ", "SQQQ", "SPXL", "SPXU", "UPRO", "SOXL", "SOXS", "TMF", "TMV", "UCO", "SCO"]
COMMODITY_ETFS = ["GLD", "SLV", "USO", "UNG", "DBA", "WEAT", "CORN", "SOYB"]
CRYPTO_ETFS = ["IBIT", "FBTC", "BITB", "ARKB", "BTCO", "GBTC", "BITO", "ETHE"]

SECTOR_MAP = {
    "Technology": "Tech", "Financial Services": "Financial", "Financial": "Financial",
    "Healthcare": "Healthcare", "Industrials": "Industrials", "Energy": "Energy",
    "Utilities": "Utilities", "Communication Services": "Communication",
    "Real Estate": "RealEstate", "Consumer Cyclical": "ConsumerCyclical",
    "Consumer Defensive": "ConsumerDefensive", "Basic Materials": "Materials"
}

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("UltimateScanner")

# ------------------------------------------------------------------------------
# 2. DATABASE LAYER
# ------------------------------------------------------------------------------
def setup_database():
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS stocks (
            ticker TEXT PRIMARY KEY, 
            entry_price REAL, 
            entry_date TEXT, 
            source TEXT, 
            note TEXT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        conn.commit()

def db_insert_ticker(ticker, price, date_str, source, note=None):
    ticker = ticker.upper()
    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()
        existing = cur.execute("SELECT entry_price FROM stocks WHERE ticker=?", (ticker,)).fetchone()
        if existing:
            if price and price > 0:
                 cur.execute("UPDATE stocks SET entry_price=?, entry_date=?, source=?, note=? WHERE ticker=?",
                            (float(price), date_str, source, note, ticker))
        else:
            cur.execute("INSERT INTO stocks (ticker, entry_price, entry_date, source, note) VALUES (?,?,?,?,?)",
                        (ticker, float(price or 0.0), date_str, source, note))
        conn.commit()

def db_get_entry(ticker):
    with sqlite3.connect(DB_FILE) as conn:
        r = conn.execute("SELECT entry_price, entry_date FROM stocks WHERE ticker=?", (ticker.upper(),)).fetchone()
        if r: return (float(r[0] or 0.0), r[1] or "N/A")
    return (0.0, "N/A")

# ------------------------------------------------------------------------------
# 3. INPUT PARSING
# ------------------------------------------------------------------------------
def check_limit(current_count):
    if current_count >= MAX_STOCK_LIMIT:
        logger.warning(f"MAX STOCK LIMIT REACHED ({MAX_STOCK_LIMIT}). Stopping new additions.")
        return True
    return False

def parse_watchlist_excel():
    data_map = {}
    if not os.path.exists(WATCHLIST_FILE): return data_map
    try:
        xls = pd.ExcelFile(WATCHLIST_FILE)
        for sheet in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=sheet)
            col = next((c for c in df.columns if 'ticker' in c.lower() or 'symbol' in c.lower()), None)
            if col:
                ticks = [str(x).strip().upper() for x in df[col].dropna().tolist() if str(x).strip()]
                if ticks: data_map[sheet] = list(set(ticks))
    except Exception as e: logger.error(f"Excel error: {e}")
    return data_map

def extract_tickers_regex(text):
    if not text: return []
    candidates = re.findall(r'\b[A-Z]{1,5}\b', text)
    blacklist = {"THE","FOR","AND","NEW","BUY","SELL","ALERT","INFO","STOCK","ETF","MARKET","SIGNAL","READY","HIGH","LOW"}
    return sorted(list({t.upper() for t in candidates if t.upper() not in blacklist}))

def scan_gmail_inbox_attachments():
    inbox_results = {}
    if not EMAIL_ADDRESS or "@" not in EMAIL_ADDRESS: 
        return inbox_results

    max_retries = 2
    retry_delay = 300  # 5 minutes in seconds

    for attempt in range(max_retries + 1):
        try:
            mail = imaplib.IMAP4_SSL("imap.gmail.com")
            mail.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            mail.select("inbox")
            
            since = (datetime.now() - timedelta(days=INBOX_LOOKBACK_DAYS)).strftime("%d-%b-%Y")
            _, ids = mail.search(None, f'(SINCE "{since}" FROM "{SENDER_EMAIL}")')
            
            if ids[0]:
                for uid in ids[0].split():
                    if check_limit(len(inbox_results)): 
                        break
                    
                    # Small delay between fetches to prevent triggering OVERQUOTA
                    time.sleep(0.5) 
                    
                    _, data = mail.fetch(uid, "(RFC822)")
                    msg = email.message_from_bytes(data[0][1])
                    
                    try: 
                        d_str = parser.parse(msg.get("date")).strftime("%Y-%m-%d")
                    except: 
                        d_str = datetime.now().strftime("%Y-%m-%d")
                    
                    for part in msg.walk():
                        fname = part.get_filename()
                        if fname and any(ext in fname.lower() for ext in [".csv", ".xlsx", ".xls"]):
                            try:
                                content = part.get_payload(decode=True)
                                df = pd.read_csv(io.BytesIO(content)) if ".csv" in fname.lower() else pd.read_excel(io.BytesIO(content))
                                t_col = next((c for c in df.columns if "ticker" in c.lower() or "symbol" in c.lower()), df.columns[0])
                                p_col = next((c for c in df.columns if "price" in c.lower() or "current" in c.lower()), None)
                                
                                for _, row in df.iterrows():
                                    sym = str(row[t_col]).strip().upper()
                                    if not sym: continue
                                    price = float(row[p_col]) if p_col else 0.0
                                    db_insert_ticker(sym, price, d_str, "InboxFile", fname)
                                    inbox_results[sym] = {'p': price, 'd': d_str}
                            except: 
                                pass
            mail.logout()
            return inbox_results  # Success! Exit and return results

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Inbox scan failed (Attempt {attempt + 1}): {error_msg}")
            
            if "OVERQUOTA" in error_msg.upper() and attempt < max_retries:
                logger.info(f"Quota exceeded. Sleeping for 5 minutes before retry...")
                time.sleep(retry_delay)
            else:
                # If it's a different error or we've run out of retries, exit
                break
                
    return inbox_results

def scan_gmail_trash_subjects():
    trash_alerts = {}
    if not EMAIL_ADDRESS or "@" not in EMAIL_ADDRESS: 
        return trash_alerts

    max_retries = 2
    retry_delay = 300  # 5 minutes

    for attempt in range(max_retries + 1):
        try:
            mail = imaplib.IMAP4_SSL("imap.gmail.com")
            mail.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            
            # Gmail Trash folder naming can vary, but [Gmail]/Trash is standard
            mail.select("[Gmail]/Trash")
            
            since = (datetime.now() - timedelta(days=TRASH_LOOKBACK_DAYS)).strftime("%d-%b-%Y")
            _, ids = mail.search(None, f'(SINCE "{since}")')
            
            if ids[0]:
                uids = ids[0].split()[-TRASH_SCAN_LIMIT:]
                for uid in uids:
                    if check_limit(len(trash_alerts)): 
                        break
                    
                    # --- THROTTLING: Prevents OVERQUOTA ---
                    time.sleep(0.5) 
                    
                    _, data = mail.fetch(uid, "(RFC822)")
                    msg = email.message_from_bytes(data[0][1])
                    subject = msg.get("subject", "")
                    
                    keywords = ["PPS", "MACD", "BOLLINGER", "AMPS", "SIGNAL", "ALERT", "CROSS", "SPIKE","SAR"]
                    if not any(k in subject.upper() for k in keywords): 
                        continue
                        
                    try: 
                        msg_date = parser.parse(msg.get("date")).strftime("%Y-%m-%d")
                    except: 
                        msg_date = datetime.now().strftime("%Y-%m-%d")
                    
                    match = re.search(r'([A-Z]+)\s*\[(.*?)\]', subject, re.IGNORECASE)
                    if match:
                        ticker = match.group(1).upper()
                        signal_name = match.group(2).strip()
                        if ticker not in trash_alerts: 
                            trash_alerts[ticker] = []
                        trash_alerts[ticker].append({'tag': signal_name, 'date': msg_date})
                    else:
                        found_signals = []
                        if "PPS" in subject.upper(): found_signals.append("PPS Signal")
                        if "MACD" in subject.upper(): found_signals.append("MACD Cross")
                        if "BOLLINGER" in subject.upper(): found_signals.append("Bollinger Alert")
                        if "AMPS" in subject.upper(): found_signals.append("AMPS Alert")
                        
                        target_list = extract_tickers_regex(subject)
                        for t in target_list:
                            if t not in trash_alerts: 
                                trash_alerts[t] = []
                            if found_signals:
                                for fs in found_signals: 
                                    trash_alerts[t].append({'tag': fs, 'date': msg_date})
                            else:
                                trash_alerts[t].append({'tag': "Generic Alert", 'date': msg_date})
            
            mail.logout()
            return trash_alerts # Success!

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Trash scan failed (Attempt {attempt + 1}): {error_msg}")
            
            if "OVERQUOTA" in error_msg.upper() and attempt < max_retries:
                logger.info("Quota exceeded in Trash scan. Waiting 5 minutes...")
                time.sleep(retry_delay)
            else:
                break

    return trash_alerts
# ------------------------------------------------------------------------------
# 4. DATA ACQUISITION
# ------------------------------------------------------------------------------
def _fetch_url(url):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    for _ in range(3):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                return resp.read()
        except: time.sleep(0.5)
    return None

def fetch_chart_history(ticker, interval, days):
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range={days}d&interval={interval}"
        raw = _fetch_url(url)
        if not raw: return pd.DataFrame()
        js = json.loads(raw)
        res = js['chart']['result'][0]
        q = res['indicators']['quote'][0]
        times = pd.to_datetime(res['timestamp'], unit='s', utc=True)
        return pd.DataFrame({
            'Date': times, 'Open': q.get('open',[]), 'High': q.get('high',[]),
            'Low': q.get('low',[]), 'Close': q.get('close',[]), 'Volume': q.get('volume',[])
        }).dropna()
    except: return pd.DataFrame()

def fetch_stock_meta(ticker):
    cpath = os.path.join(CACHE_DIR, f"{ticker}_meta_v4.json")
    if os.path.exists(cpath):
        if time.time() - os.path.getmtime(cpath) < 86400: return json.load(open(cpath))
    
    meta = {'sector': 'Unknown', 'market_cap': 0, 'pe': None, 'peg': None}
    try:
        url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?modules=assetProfile,summaryDetail,defaultKeyStatistics"
        raw = _fetch_url(url)
        if raw:
            d = json.loads(raw)['quoteSummary']['result'][0]
            meta['sector'] = d.get('assetProfile', {}).get('sector', 'Unknown')
            sd = d.get('summaryDetail', {})
            ks = d.get('defaultKeyStatistics', {})
            meta['market_cap'] = sd.get('marketCap', {}).get('raw', 0)
            meta['pe'] = sd.get('trailingPE', {}).get('raw', None)
            meta['peg'] = ks.get('pegRatio', {}).get('raw', None)
            with open(cpath, 'w') as f: json.dump(meta, f)
    except: pass
    return meta

def fetch_earnings_date(ticker):
    try:
        url = f"https://finviz.com/quote.ashx?t={ticker}"
        raw = _fetch_url(url)
        if raw:
            soup = BeautifulSoup(raw, 'lxml')
            label = soup.find('td', string=re.compile("Earnings"))
            if label:
                val = label.find_next_sibling('td').text.strip()
                if val and val != '-':
                    return re.sub(r'\s+(AMC|BMO)', '', val) 
    except: pass
    return "N/A"

def fetch_price_at_date(ticker, date_str):
    if not date_str or date_str == "N/A": return 0.0
    try:
        dt = parser.parse(date_str)
        t1 = int(dt.timestamp())
        t2 = int((dt + timedelta(days=5)).timestamp())
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?period1={t1}&period2={t2}&interval=1d"
        raw = _fetch_url(url)
        if raw:
            d = json.loads(raw)
            closes = d['chart']['result'][0]['indicators']['quote'][0]['close']
            return closes[0] if closes else 0.0
    except: pass
    return 0.0

# ------------------------------------------------------------------------------
# 5. TECHNICAL ANALYSIS & FILTERING ENGINE
# ------------------------------------------------------------------------------
def calculate_sar(df, af=0.02, max_af=0.2):
    high, low = df['High'], df['Low']
    sar = [low.iloc[0]]; bull = True; ep = high.iloc[0]; acc = af
    for i in range(1, len(df)):
        prev_sar = sar[-1]
        if bull:
            curr_sar = prev_sar + acc * (ep - prev_sar)
            curr_sar = min(curr_sar, low.iloc[max(0, i-1)], low.iloc[i])
            if low.iloc[i] < curr_sar:
                bull = False; curr_sar = ep; ep = low.iloc[i]; acc = af
            else:
                if high.iloc[i] > ep: ep = high.iloc[i]; acc = min(acc + af, max_af)
        else:
            curr_sar = prev_sar + acc * (ep - prev_sar)
            curr_sar = max(curr_sar, high.iloc[max(0, i-1)], high.iloc[i])
            if high.iloc[i] > curr_sar:
                bull = True; curr_sar = ep; ep = high.iloc[i]; acc = af
            else:
                if low.iloc[i] < ep: ep = low.iloc[i]; acc = min(acc + af, max_af)
        sar.append(curr_sar)
    return sar

def calculate_volatility(df, window=30):
    log_return = np.log(df['Close'] / df['Close'].shift(1))
    return log_return.rolling(window=window).std() * np.sqrt(252)

def run_technical_calculations(df):
    d = df.copy()
    if len(d) < 5: return d
    
    # RSI
    delta = d['Close'].diff()
    up = delta.clip(lower=0); down = -1 * delta.clip(upper=0)
    avg_gain = up.rolling(RSI_PERIOD).mean()
    avg_loss = down.rolling(RSI_PERIOD).mean()
    rs = avg_gain / avg_loss
    d['RSI'] = 100 - (100 / (1 + rs)).fillna(50)
    
    # Bollinger Bands
    mid = d['Close'].rolling(BB_PERIOD).mean()
    std = d['Close'].rolling(BB_PERIOD).std()
    d['BB_Upper'] = mid + (BB_STD * std)
    d['BB_Lower'] = mid - (BB_STD * std)
    
    # MACD
    d['E12'] = d['Close'].ewm(span=12, adjust=False).mean()
    d['E26'] = d['Close'].ewm(span=26, adjust=False).mean()
    d['MACD'] = d['E12'] - d['E26']
    d['MACD_Signal'] = d['MACD'].ewm(span=9, adjust=False).mean()
    
    # SMAs (Expanded)
    d['MA5'] = d['Close'].rolling(5).mean()
    d['MA10'] = d['Close'].rolling(10).mean()
    d['SMA20'] = d['Close'].rolling(20).mean()
    d['MA30'] = d['Close'].rolling(30).mean()
    d['SMA50'] = d['Close'].rolling(50).mean()
    d['SMA200'] = d['Close'].rolling(200).mean()
    
    # SAR & VWAP
    d['SAR'] = calculate_sar(d)
    d['TypPrice'] = (d['High'] + d['Low'] + d['Close']) / 3
    d['VWAP'] = d['TypPrice'] # Simplification for daily rows
    d['VWAP_Status'] = np.where(d['Close'] > d['VWAP'], "Above", "Below")
    d['HV'] = calculate_volatility(d)
    
    # Breakout Levels
    d['High20'] = d['High'].rolling(20).max().shift(1)
    d['Low20'] = d['Low'].rolling(20).min().shift(1)
    
    # Crossovers
    d['MA5_10'] = np.where(d['MA5'] > d['MA10'], "Bull", "Bear")
    d['MA10_20'] = np.where(d['MA10'] > d['SMA20'], "Bull", "Bear")

    d['VolAvg'] = d['Volume'].rolling(50).mean()
    
    return d

def generate_ticker_tags(ticker, df_daily, df_intra, meta, earnings_raw):
    tags = []
    last = df_daily.iloc[-1]
    prev = df_daily.iloc[-2]
    price = last['Close']
    
    vol_avg = last.get('VolAvg', 0)
    sma20 = last.get('SMA20', 0)
    sma50 = last.get('SMA50', 0)
    sma200 = last.get('SMA200', 0)
    rsi = last.get('RSI', 50)
    
    # 1. Performance Filters
    if price > prev['Close']: tags.append("Positive Today")
    else: tags.append("Negative Today")
    
    if (last['Close'] > last['Open']) and (prev['Close'] > prev['Open']) and last['Volume'] > vol_avg: tags.append("2-Day Positive & Vol")

    if last['Volume'] > vol_avg * 1.5: tags.append("Volume High Today")
    if last['Volume'] < vol_avg * 0.5: tags.append("Volume Low Today")

    # 2. SMA Relationships
    if sma20 > 0:
        if price > sma20: tags.append("Price > SMA20")
        else: tags.append("Price < SMA20")
    if sma50 > 0:
        if price > sma50: tags.append("Price > SMA50")
        else: tags.append("Price < SMA50")
    if sma200 > 0:
        if price > sma200: tags.append("Price > SMA200")
        else: tags.append("Price < SMA200")

    # NEW: Crossover Tags
    if last.get('MA5_10') == "Bull": tags.append("MA 5 Cross 10")
    if last.get('MA10_20') == "Bull": tags.append("MA 10 Cross 20")
    if last.get('VWAP_Status') == "Above": tags.append("VWAP Positive Today")

    # NEW: Breakouts
    if price > last.get('High20', 999999): tags.append("Breakout Up")
    if price < last.get('Low20', 0): tags.append("Breakout Down")

    # NEW: Volatility (IV Proxy)
    hv = last.get('HV', 0)
    if hv > 0.50: tags.append("High IV")
    elif hv < 0.20: tags.append("Low IV")

    # 3. Trend Classifications
    prev_sma20 = prev.get('SMA20', 0)
    if prev['Close'] < prev_sma20 and price > sma20: tags.append("Emerging Trend")
    if sma20 > sma50: tags.append("Building Trend")
    if sma20 > sma50 and sma50 > sma200: tags.append("Strong Trend")
    
    prev_sma50 = prev.get('SMA50', 0); prev_sma200 = prev.get('SMA200', 0)
    if sma50 > sma200 and prev_sma50 <= prev_sma200: tags.append("Golden Cross")
    if sma50 < sma200 and prev_sma50 >= prev_sma200: tags.append("Death Cross")

    # 4. Timeframe Extrema
    if len(df_daily) >= 9:
        if price >= df_daily.tail(9)['High'].max() * 0.99: tags.append("9 Day High")
        if price <= df_daily.tail(9)['Low'].min() * 1.01: tags.append("9 Day Low")
    if len(df_daily) >= 5:
        if price >= df_daily.tail(5)['High'].max() * 0.99: tags.append("Weekly High")
        if price <= df_daily.tail(5)['Low'].min() * 1.01: tags.append("Weekly Low")
        if price > df_daily.iloc[-6]['Close']: tags.append("Weekly Up")
        else: tags.append("Weekly Down")
    if len(df_daily) >= 21:
        if price >= df_daily.tail(21)['High'].max() * 0.99: tags.append("Monthly High")
        if price <= df_daily.tail(21)['Low'].min() * 1.01: tags.append("Monthly Low")
        if price > df_daily.iloc[-22]['Close']: tags.append("Monthly Up")
        else: tags.append("Monthly Down")
    if len(df_daily) >= 63:
        if price >= df_daily.tail(63)['High'].max() * 0.99: tags.append("Quarterly High")
    if len(df_daily) >= 252:
        if price >= df_daily.tail(252)['High'].max() * 0.97: tags.append("Near 52W High")
        if price >= df_daily.tail(252)['High'].max(): tags.append("New 52 High")
        if price > df_daily.iloc[-253]['Close']: tags.append("Yearly Up")
        else: tags.append("Yearly Down")

    # 5. YTD
    current_year = datetime.now().year
    ytd_df = df_daily[df_daily['Date'].dt.year == current_year]
    if not ytd_df.empty:
        if price > ytd_df.iloc[0]['Close']: tags.append("YearToDate Up")
        else: tags.append("YearToDate Down")

    # 6. Technical Alerts
    if rsi < 30: tags.append("RSI Oversold")
    elif rsi > 70: tags.append("RSI Overbought")
    
    last_macd = last.get('MACD', 0); last_sig = last.get('MACD_Signal', 0)
    if price > sma20 and rsi > 50 and last_macd > last_sig:
        tags.append("PPS Buy Signal")
        
    bb_u = last.get('BB_Upper', 999999); bb_l = last.get('BB_Lower', 0)
    if price > bb_u: tags.append("Bollinger Breakout")
    if sma20 > 0 and (bb_u - bb_l) / sma20 < 0.10: tags.append("Bollinger Squeeze")
    
    # 7. Fundamentals
    cap = meta.get('market_cap', 0)
    if cap > 200e9: tags.append("Mega Cap")
    elif cap > 10e9: tags.append("Large Cap")
    elif cap > 2e9: tags.append("Mid Cap")
    elif cap > 300e6: tags.append("Small Cap")
    elif cap > 0: tags.append("Micro Cap")

    # Earnings (Strict 7-Day Logic)
    if earnings_raw != "N/A":
        try:
            e_dt = parser.parse(f"{earnings_raw} {datetime.now().year}")
            delta = (e_dt - datetime.now()).days
            if 0 <= delta <= 7: tags.append("Upcoming Earnings")
            elif -7 <= delta < 0: tags.append("Post-Earnings")
        except: pass

    # Asset Classes
    if ticker in LEVERAGED_ETFS: tags.append("Leveraged ETF")
    if ticker in CRYPTO_ETFS: tags.append("Crypto")
    if ticker in COMMODITY_ETFS: tags.append("Commodities")

    return sorted(list(set(tags)))

# ------------------------------------------------------------------------------
# 6. CORE SCANNER PROCESSING
# ------------------------------------------------------------------------------
def process_ticker(ticker, db_info, trash_signals):
    df_daily = fetch_chart_history(ticker, "1d", DAILY_LOOKBACK)
    df_intra = fetch_chart_history(ticker, INTRA_INTERVAL, INTRA_DAYS)
    
    if df_daily.empty or len(df_daily) < 50: return None
    
    df_daily = run_technical_calculations(df_daily)
    # FIX: Ensure Intraday Dataframe also has techs for Charting
    if not df_intra.empty:
        df_intra = run_technical_calculations(df_intra)

    last = df_daily.iloc[-1]
    price = last['Close']
    prev = df_daily.iloc[-2]
    meta = fetch_stock_meta(ticker)
    earn = fetch_earnings_date(ticker)
    tags = generate_ticker_tags(ticker, df_daily, df_intra, meta, earn)
    
    entry_p, entry_d = db_info
    for alert in trash_signals:
        tags.append(alert['tag'])
        if not entry_p or entry_p == 0:
            entry_d = alert['date']
            hist = fetch_price_at_date(ticker, entry_d)
            if hist > 0: entry_p = hist

    perf = (price - entry_p) / entry_p if entry_p > 0 else 0.0
    if perf > 0.25: tags.append("Winners >25%")

    # Range Bars Data
    yr_h = df_daily.tail(252)['High'].max()
    yr_l = df_daily.tail(252)['Low'].min()
    d_h = last['High']; d_l = last['Low']

    # Signal Matrix
    last_macd = last.get('MACD', 0); last_sig = last.get('MACD_Signal', 0)
    bb_l = last.get('BB_Lower', 0); bb_u = last.get('BB_Upper', 999999)
    sma20 = last.get('SMA20', 0)

    signals = {
        'MACD': "Positive" if last_macd > last_sig else "Negative",
        'Bollinger': "Oversold" if price < bb_l else ("Overbought" if price > bb_u else "Neutral"),
        'PPS': "Gap Up" if (last['Open'] - prev['Close']) / prev['Close'] > 0.015 else "Neutral",
        'Trend': "Bullish" if price > sma20 else "Bearish"
    }
    
    # AMPS Score
    amps_score = 0
    if price > sma20: amps_score += 1
    if last.get('RSI', 50) > 50: amps_score += 1
    if last_macd > last_sig: amps_score += 1
    if last['Volume'] > last.get('VolAvg', 0): amps_score += 1
    signals['AMPS'] = f"{amps_score}/4"
    if amps_score >= 3: tags.append("High AMPS Score")

    # NEW: Price Trend Matrix (2D to 360D)
    trend_data = {}
    for d in PRICE_TREND_DAYS:
        if len(df_daily) > d:
            pct = (price - df_daily['Close'].iloc[-(d+1)]) / df_daily['Close'].iloc[-(d+1)]
            trend_data[f"{d}D"] = pct
        else:
            trend_data[f"{d}D"] = 0.0

    # History Data Build
    history_data = []
    subset = df_daily.tail(15).sort_values(by='Date', ascending=False)
    for index, row in subset.iterrows():
        history_data.append({
            'date': row['Date'].strftime("%Y-%m-%d"),
            'open': f"{row['Open']:.2f}",
            'high': f"{row['High']:.2f}",
            'low': f"{row['Low']:.2f}",
            'close': f"{row['Close']:.2f}",
            'volume': f"{int(row['Volume']):,}",
            'rsi': f"{row['RSI']:.1f}",
            'sar': f"{row['SAR']:.2f}",
            'vwap': f"{row['VWAP']:.2f}",
            'ma5': f"{row['MA5']:.2f}",
            'ma10': f"{row['MA10']:.2f}",
            'ma20': f"{row['SMA20']:.2f}",
            'ma30': f"{row['MA30']:.2f}",
            'ma5_10': row['MA5_10'],
            'ma10_20': row['MA10_20'],
            'vwap_status': row['VWAP_Status']
        })

    return {
        'ticker': ticker, 'price': round(price, 2), 
        'chg': (price - prev['Close']) / prev['Close'],
        'entry_price': entry_p, 'entry_date': entry_d, 'perf': perf,
        'yr_h': yr_h, 'yr_l': yr_l, 'd_h': d_h, 'd_l': d_l,
        'sector': meta['sector'], 'pe': meta['pe'], 'peg': meta['peg'],
        'tags': sorted(list(set(tags))), 
        'trends': trend_data, 'signals': signals, 'earnings': earn,
        'history': history_data,
        'daily_df': df_daily, 'intra_df': df_intra
    }

# ------------------------------------------------------------------------------
# 7. DASHBOARD GENERATION
# ------------------------------------------------------------------------------
def df_to_plotly_json(df):
    if df is None or df.empty: return {}
    d = df.tail(150)
    # Safe checks for columns
    return {
        'dates': [t.strftime("%Y-%m-%d %H:%M") if ' ' in str(t) else t.strftime("%Y-%m-%d") for t in d['Date']],
        'o': d['Open'].tolist(), 'h': d['High'].tolist(), 
        'l': d['Low'].tolist(), 'c': d['Close'].tolist(),
        'bbu': d['BB_Upper'].tolist() if 'BB_Upper' in d.columns else [],
        'bbl': d['BB_Lower'].tolist() if 'BB_Lower' in d.columns else [],
        'sma20': d['SMA20'].tolist() if 'SMA20' in d.columns else []
    }

def build_history_table_rows(history):
    rows = []
    for r in history:
        rows.append(f"""
        <tr class="hover:bg-slate-700 border-b border-slate-700">
            <td class="p-2 whitespace-nowrap">{r['date']}</td>
            <td class="p-2">{r['open']}</td><td class="p-2">{r['high']}</td><td class="p-2">{r['low']}</td>
            <td class="p-2 font-bold">{r['close']}</td><td class="p-2">{r['volume']}</td>
            <td class="p-2 { 'text-red-400' if float(r['rsi'])>70 else 'text-green-400' if float(r['rsi'])<30 else '' }">{r['rsi']}</td>
            <td class="p-2">{r['sar']}</td><td class="p-2">{r['vwap']}</td>
            <td class="p-2">{r['ma5']}</td><td class="p-2">{r['ma10']}</td><td class="p-2">{r['ma20']}</td><td class="p-2">{r['ma30']}</td>
            <td class="p-2 { 'text-green-400' if r['ma5_10']=='Bull' else 'text-red-400' }">{r['ma5_10']}</td>
            <td class="p-2 { 'text-green-400' if r['ma10_20']=='Bull' else 'text-red-400' }">{r['ma10_20']}</td>
            <td class="p-2 { 'text-green-400' if r['vwap_status']=='Above' else 'text-red-400' }">{r['vwap_status']}</td>
        </tr>""")
    return "\n".join(rows)

def build_trend_badges(trends):
    html = '<div class="flex flex-wrap gap-1 mb-3">'
    for d in PRICE_TREND_DAYS:
        key = f"{d}D"
        val = trends.get(key, 0.0)
        color = "bg-green-600" if val >= 0 else "bg-red-600"
        html += f'<div class="{color} text-white text-[9px] px-1 rounded border border-white/10 text-center min-w-[35px]"><div class="font-bold">{d}D</div><div>{val*100:.1f}%</div></div>'
    html += '</div>'
    return html

def build_dashboard(results):
    clean_data = []; charts_daily = {}; charts_intra = {}; tag_set = set()
    for r in results:
        t = r['ticker']
        charts_daily[t] = df_to_plotly_json(r['daily_df'])
        charts_intra[t] = df_to_plotly_json(r['intra_df'])
        r['history_html'] = build_history_table_rows(r['history'])
        r['trend_html'] = build_trend_badges(r['trends'])
        clean_data.append({k:v for k,v in r.items() if k not in ['daily_df', 'intra_df']})
        for tg in r['tags']: tag_set.add(tg)

    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Hybrid Ultimate Scanner v50.2</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        body {{ background: #0f172a; color: #e2e8f0; font-family: 'Inter', sans-serif; }}
        .badge {{ padding: 2px 6px; border-radius: 4px; font-size: 10px; font-weight: 800; }}
        .range-bar-bg {{ height: 6px; background: #334155; border-radius: 3px; position: relative; overflow: hidden; }}
        .range-dot {{ width: 8px; height: 8px; background: #38bdf8; border-radius: 50%; position: absolute; top: -1px; transform: translateX(-50%); box-shadow: 0 0 5px #38bdf8; }}
        .history-table-container {{ max-height: 250px; overflow-y: auto; background: #1e293b; border-radius: 8px; font-size: 10px; margin-top:10px; }}
        .history-table th {{ position: sticky; top: 0; background: #0f172a; padding: 5px; font-weight: 700; text-align: left; z-index: 10; border-bottom: 1px solid #475569; }}
        .history-table td {{ border-bottom: 1px solid #334155; }}
        .active-btn {{ background: #2563eb !important; color: white !important; border-color: #1e40af; }}
        ::-webkit-scrollbar {{ width: 8px; height: 8px; }}
        ::-webkit-scrollbar-track {{ background: #0f172a; }}
        ::-webkit-scrollbar-thumb {{ background: #334155; border-radius: 4px; }}
    </style>
</head>
<body class="p-6">
    <div class="max-w-[2200px] mx-auto">
        <div class="flex flex-col lg:flex-row justify-between items-end mb-8 gap-6 border-b border-slate-700 pb-6">
            <div>
                <h1 class="text-4xl font-black text-transparent bg-clip-text bg-gradient-to-r from-blue-400 to-emerald-400">ULTIMATE SCANNER v50.2 <span id="count" class="text-sm text-slate-500 ml-2"></span></h1>
                <p class="text-xs font-bold text-slate-500 uppercase tracking-widest mt-1">Price Trends • History Table • Breakouts • SMA Matrix • Volatility</p>
            </div>
            
            <div class="bg-slate-800 p-5 rounded-2xl shadow-sm border border-slate-700 flex flex-wrap gap-4 items-center">
                <input type="text" id="search" oninput="filter()" placeholder="Search..." class="bg-slate-900 border border-slate-600 rounded-xl px-4 py-2 text-sm font-bold w-40 text-white">
                <input type="number" id="maxP" oninput="filter()" placeholder="Max $" class="bg-slate-900 border border-slate-600 rounded-xl px-4 py-2 text-sm font-bold w-24 text-white">
                <button onclick="setFilter('Breakout Up')" class="bg-emerald-900/30 text-emerald-400 border border-emerald-800 px-3 py-2 rounded-lg text-xs font-black">BREAKOUT UP</button>
                <button onclick="setFilter('Upcoming Earnings')" class="bg-orange-900/30 text-orange-400 border border-orange-800 px-3 py-2 rounded-lg text-xs font-black">EARNINGS</button>
                <button onclick="reset()" class="bg-red-900/30 text-red-400 border border-red-800 px-6 py-2 rounded-xl text-xs font-black">RESET</button>
            </div>
        </div>

        <div class="bg-slate-800 p-6 rounded-3xl shadow-sm border border-slate-700 mb-8">
            <h3 class="text-[10px] font-black text-slate-400 uppercase mb-4 tracking-tighter">Signal Filters</h3>
            <div id="filters" class="flex flex-wrap gap-2"></div>
        </div>

        <div id="grid" class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-3 2xl:grid-cols-4 gap-6"></div>
    </div>

    <div id="modal" class="fixed inset-0 bg-black/90 hidden z-50 p-6">
        <div class="bg-slate-900 w-full h-full rounded-3xl flex flex-col overflow-hidden border border-slate-700">
            <div class="flex justify-between items-center p-6 border-b border-slate-700 bg-slate-800">
                <h2 id="mtick" class="text-3xl font-black text-white"></h2>
                <button onclick="closeModal()" class="bg-red-600 text-white px-10 py-3 rounded-2xl font-black shadow-lg hover:bg-red-700">CLOSE [ESC]</button>
            </div>
            <div class="flex-1 flex flex-col lg:flex-row gap-4 p-4 bg-slate-900">
                <div id="cd" class="w-full lg:w-1/2 border border-slate-700 rounded-2xl"></div>
                <div id="ci" class="w-full lg:w-1/2 border border-slate-700 rounded-2xl"></div>
            </div>
        </div>
    </div>

<script>
const data = {json.dumps(clean_data)};
const cDaily = {json.dumps(charts_daily)};
const cIntra = {json.dumps(charts_intra)};
const tags = {json.dumps(sorted(list(tag_set)))};
let activeTags = new Set();

function toggleId(id) {{
    const el = document.getElementById(id);
    el.classList.toggle('hidden');
}}

function getPos(curr, l, h) {{
    return Math.min(Math.max(((curr-l)/(h-l))*100, 0), 100);
}}

function filter() {{
    const s = document.getElementById('search').value.toUpperCase();
    const p = parseFloat(document.getElementById('maxP').value) || 999999;
    const filt = data.filter(i => {{
        return i.ticker.includes(s) && i.price <= p && Array.from(activeTags).every(t => i.tags.includes(t));
    }});
    document.getElementById('count').innerText = `(${{filt.length}}/${{data.length}})`;
    renderFilters(filt);
    renderGrid(filt);
}}

function setFilter(tagName) {{
    if(!activeTags.has(tagName)) {{
        activeTags.add(tagName);
        filter();
    }}
}}

function renderFilters(currentData) {{
    const div = document.getElementById('filters');
    div.innerHTML = '';
    tags.forEach(t => {{
        const count = currentData.filter(i => i.tags.includes(t)).length;
        if(count > 0 || activeTags.has(t)) {{
            const btn = document.createElement('button');
            btn.className = `px-3 py-1.5 rounded-xl text-[10px] font-black border transition-all ${{activeTags.has(t)?'active-btn':'bg-slate-700 text-slate-300 border-slate-600 hover:bg-slate-600'}}`;
            btn.innerHTML = `${{t}} <span class="opacity-40 ml-1 font-bold">${{count}}</span>`;
            btn.onclick = () => {{
                if(activeTags.has(t)) activeTags.delete(t);
                else activeTags.add(t);
                filter();
            }};
            div.appendChild(btn);
        }}
    }});
}}

function renderGrid(items) {{
    const g = document.getElementById('grid');
    g.innerHTML = items.map(i => `
        <div class="bg-slate-800 p-5 rounded-3xl border border-slate-700 shadow-lg hover:border-blue-500 transition-all duration-200">
            <div class="flex justify-between mb-3 cursor-pointer" onclick="openChart('${{i.ticker}}')">
                <div>
                    <div class="text-3xl font-black text-white tracking-tight">${{i.ticker}}</div>
                    <div class="text-[10px] text-slate-400 font-bold uppercase tracking-widest">${{i.sector}}</div>
                </div>
                <div class="text-right">
                    <div class="text-2xl font-black ${{i.chg>=0?'text-green-400':'text-red-400'}}">${{(i.chg*100).toFixed(2)}}%</div>
                    <div class="text-sm font-bold text-slate-500">$${{i.price.toFixed(2)}}</div>
                </div>
            </div>

            ${{i.trend_html}}

            <div class="space-y-3 mb-4 bg-slate-900/50 p-3 rounded-xl border border-slate-700/50">
                <div>
                    <div class="flex justify-between text-[9px] font-black text-slate-400 uppercase mb-1">
                        <span>Day L: ${{i.d_l.toFixed(2)}}</span><span>Day H: ${{i.d_h.toFixed(2)}}</span>
                    </div>
                    <div class="range-bar-bg"><div class="range-dot" style="left: ${{getPos(i.price, i.d_l, i.d_h)}}%"></div></div>
                </div>
                <div>
                    <div class="flex justify-between text-[9px] font-black text-slate-400 uppercase mb-1">
                        <span>52W L: ${{i.yr_l.toFixed(2)}}</span><span>52W H: ${{i.yr_h.toFixed(2)}}</span>
                    </div>
                    <div class="range-bar-bg"><div class="range-dot" style="left: ${{getPos(i.price, i.yr_l, i.yr_h)}}%"></div></div>
                </div>
            </div>

            <div class="grid grid-cols-2 gap-2 text-[10px] mb-4 bg-slate-700/30 p-2 rounded-xl">
               <div class="font-bold text-slate-400">Entry: <span class="text-white">$${{i.entry_price.toFixed(2)}}</span></div>
               <div class="font-bold text-slate-400 text-right">Perf: <span class="${{i.perf>=0?'text-green-400':'text-red-400'}}">${{(i.perf*100).toFixed(1)}}%</span></div>
               <div class="font-bold text-slate-400">P/E: <span class="text-white">${{i.pe}}</span></div>
               <div class="font-bold text-slate-400 text-right">PEG: <span class="text-white">${{i.peg}}</span></div>
            </div>

            <div class="flex gap-2 mb-4">
                <a href="https://finviz.com/quote.ashx?t=${{i.ticker}}" target="_blank" class="flex-1 text-center text-[10px] bg-blue-900/20 text-blue-400 hover:bg-blue-600 hover:text-white border border-blue-900/50 px-2 py-2 rounded-lg font-black transition-colors">FINVIZ</a>
                <a href="https://stockanalysis.com/stocks/${{i.ticker}}/forecast/" target="_blank" class="flex-1 text-center text-[10px] bg-purple-900/20 text-purple-400 hover:bg-purple-600 hover:text-white border border-purple-900/50 px-2 py-2 rounded-lg font-black transition-colors">FORECAST</a>
                <button onclick="toggleId('hist-${{i.ticker}}')" class="flex-1 text-[10px] bg-emerald-900/20 text-emerald-400 hover:bg-emerald-600 hover:text-white border border-emerald-900/50 px-2 py-2 rounded-lg font-black uppercase">HISTORY</button>
            </div>

            <div id="hist-${{i.ticker}}" class="hidden history-table-container mb-4 border border-slate-700 shadow-inner">
                <table class="history-table w-full text-slate-300">
                    <thead>
                        <tr>
                            <th>Date</th><th>Open</th><th>High</th><th>Low</th><th>Close</th><th>Volume</th>
                            <th>RSI</th><th>SAR</th><th>VWAP</th><th>MA5</th><th>MA10</th><th>MA20</th><th>MA30</th>
                            <th>5/10</th><th>10/20</th><th>VWAP St</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${{i.history_html}}
                    </tbody>
                </table>
            </div>

            <div class="flex flex-wrap gap-1 mt-auto">
                ${{i.tags.map(t => `<span class="badge ${{t.includes('2-Day')?'badge-green':'badge-gray bg-slate-700 text-slate-300 border-slate-600'}}">${{t}}</span>`).join('')}}
                ${{i.earnings !== 'N/A' ? `<span class="badge bg-orange-900/30 text-orange-400 border-orange-800">EARN: ${{i.earnings}}</span>` : ''}}
            </div>
        </div>
    `).join('');
}}

function openChart(t) {{
    document.getElementById('modal').style.display = 'block';
    document.getElementById('mtick').innerText = t + " Multi-Timeframe Analysis";
    plot('cd', cDaily[t], 'Daily History + Indicators');
    plot('ci', cIntra[t], 'Intraday 5m (5 Days)');
}}

function plot(id, d, title) {{
    if(!d.dates) return;
    const tr = {{ x: d.dates, close: d.c, high: d.h, low: d.l, open: d.o, type: 'candlestick', name:'Price', increasing:{{line:{{color:'#10b981', width:1}}}}, decreasing:{{line:{{color:'#ef4444', width:1}}}} }};
    const layout = {{ 
        margin: {{l:40, r:10, t:50, b:40}}, 
        xaxis: {{rangeslider: {{visible: false}}, gridcolor:'#334155'}}, 
        yaxis: {{gridcolor:'#334155'}},
        title: {{ text: title, font:{{size:14, weight:800, color:'#94a3b8'}} }},
        plot_bgcolor:'#0f172a', paper_bgcolor:'#0f172a',
        font: {{color: '#94a3b8'}}
    }};
    
    let series = [tr];
    if(id === 'cd') {{
        if(d.sma20.length > 0) series.push({{ x: d.dates, y: d.sma20, type:'scatter', mode:'lines', line:{{color:'rgba(56, 189, 248, 0.5)', width:2}}, name:'SMA20' }});
        if(d.bbu.length > 0) series.push({{ x: d.dates, y: d.bbu, type:'scatter', mode:'lines', line:{{color:'rgba(148, 163, 184, 0.2)', dash:'dash'}}, name:'Upper BB' }});
        if(d.bbl.length > 0) series.push({{ x: d.dates, y: d.bbl, type:'scatter', mode:'lines', line:{{color:'rgba(148, 163, 184, 0.2)', dash:'dash'}}, name:'Lower BB' }});
    }}
    Plotly.newPlot(id, series, layout);
}}

function closeModal() {{ document.getElementById('modal').style.display = 'none'; }}
function reset() {{ activeTags.clear(); document.getElementById('search').value=''; document.getElementById('maxP').value=''; filter(); }}
window.onload = filter;
document.addEventListener('keydown', (e) => {{ if(e.key === 'Escape') closeModal(); }});
</script>
</body>
</html>"""
    with open(os.path.join(OUTPUT_ROOT, "dashboard.html"), "w", encoding="utf-8") as f:
        f.write(html)

# ------------------------------------------------------------------------------
# 8. MAIN EXECUTION (Multithreaded with Limit)
# ------------------------------------------------------------------------------
def main():
    print("=== HYBRID SCANNER PRO v50.2: STARTING ENGINE ===")
    print(f"MAX_STOCK_LIMIT = {MAX_STOCK_LIMIT}")
    setup_database()
    
    # 1. DATA GATHERING
    print("...Scanning Inputs (Inbox, Trash, Excel)")
    inbox = scan_gmail_inbox_attachments()
    trash = scan_gmail_trash_subjects()
    excel = parse_watchlist_excel()
    
    all_tickers = set(inbox.keys())
    all_tickers.update(trash.keys())
    for sheet_tickers in excel.values(): all_tickers.update(sheet_tickers)
    all_tickers.update(LEVERAGED_ETFS + COMMODITY_ETFS + CRYPTO_ETFS)
    
    # APPLY LIMIT
    ticker_list = list(all_tickers)
    if len(ticker_list) > MAX_STOCK_LIMIT:
        print(f"*** LIMIT REACHED: Trimming {len(ticker_list)} tickers down to {MAX_STOCK_LIMIT} ***")
        ticker_list = ticker_list[:MAX_STOCK_LIMIT]
    
    print(f"Total Tickers Queued: {len(ticker_list)}")
    
    # 2. CONCURRENT PROCESSING
    results = []
    q = queue.Queue()
    for t in ticker_list: q.put(t)
    
    def worker():
        while True:
            try:
                ticker = q.get_nowait()
                db_data = db_get_entry(ticker)
                tsig = trash.get(ticker, [])
                res = process_ticker(ticker, db_data, tsig)
                if res: results.append(res)
                q.task_done()
            except queue.Empty: break
            except Exception as e:
                logger.error(f"Error {ticker}: {e}")
                q.task_done()

    print(f"...Running analysis with {THREADS} threads")
    threads = []
    for _ in range(THREADS):
        th = threading.Thread(target=worker, daemon=True)
        th.start()
        threads.append(th)
    
    q.join()
    
    # 3. FINALIZATION
    if results:
        build_dashboard(results)
        path = os.path.join(OUTPUT_ROOT, "dashboard.html")
        print(f"=== SCAN COMPLETE: {len(results)} STOCKS READY ===")
        webbrowser.open(path)
    else:
        print("CRITICAL: No results generated. Check internet and credentials.")
def market_is_open():
    nyse = mcal.get_calendar("NYSE")
    now = pd.Timestamp.now(tz="America/New_York")
    sched = nyse.schedule(start_date=now.date(), end_date=now.date())
    if sched.empty: return False
    return sched.iloc[0]["market_open"] <= now <= sched.iloc[0]["market_close"]
if __name__ == "__main__":
    if not market_is_open(): logger.info("Market is currently CLOSED. Running in offline/review mode.")
    else: logger.info("Market is OPEN.")
    main()








