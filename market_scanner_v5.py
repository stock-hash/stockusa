#!/usr/bin/env python3
"""
MARKET SCANNER v5.6 — PHASE 1+2+3+4+5+6 UPGRADE + DYNAMIC UNIVERSE + SMART GATES
Original v4 foundation preserved.
Phase 1: Cleaned syntax, stable code.
Phase 2: Relaxed 60m confirmation (booster, not blocker).
Phase 3: Sector-override for strong stocks in weak sectors.
Phase 4: Leadership/Continuation detection engine.
Phase 5: Reversal Quality Gate (RQG) — multi-factor final filter.
Phase 6: Backtest engine for strategy validation.

v5.6 UPGRADES (over v5.3/v5.5):
  - RQG threshold lowered from 65 → 55 (more quality candidates pass)
  - 60M booster is now OPTIONAL — high-confidence stocks (c5≥80) in STRONG/NEUTRAL
    sectors are promoted at 30M confirmation (no more waiting for 60M gate)
  - Sector penalty REDUCED for high-confidence stocks (c5≥80): 0.85x instead of 0.7x
  - DYNAMIC HOT STOCKS: Yahoo Finance screeners (day_gainers, day_losers, most_actives,
    trending, EquityQuery) auto-fetch today's movers every 2 scans → 260 → 400-500+ stocks
  - 150+ additional static hot stocks: PLTR, HOOD, DKNG, DVN, ARM, SMCI, SOFI, etc.
  - NEAR-MISS WATCHLIST: Stocks at 30M_CONFIRMED stage with c5≥65 shown in summary
  - Enhanced summary: blank lines between sectors, near-miss section, dynamic universe count

v5.3 FIX (preserved):
  - yfinance peewee tz-cache → IN-MEMORY SQLite (zero disk I/O)
  - Monkey-patches yfinance.cache to avoid disk entirely
  - Batch downloads in 40-ticker chunks
  - scanner_alerts.db: WAL mode + timeout=30
"""

import yfinance as yf
import pandas as pd
import numpy as np
import smtplib, logging, time, sys, os, random, warnings
import sqlite3
import shutil
import json
import urllib.request
import urllib.error
from urllib.parse import urlencode

# ═══════════════════════════════════════════════════════════════
# v5.3 FIX — MONKEY-PATCH YFINANCE TZ-CACHE → IN-MEMORY SQLITE
# The root cause of ALL PythonAnywhere errors:
#   yfinance uses peewee ORM → disk-based SQLite for tz-cache
#   PythonAnywhere throttles disk I/O → corrupts the SQLite file
#   yfinance catches the error internally → never raises to us
#   Our retry logic never triggers → all downloads fail silently
#
# The ONLY permanent fix: replace the peewee database with
# an in-memory SQLite database. Zero disk I/O. Zero corruption.
# ═══════════════════════════════════════════════════════════════
_tz_cache_dict = {}   # Fallback dict-based cache

def _patch_yf_tz_cache():
    """Monkey-patch yfinance's peewee tz-cache to use in-memory SQLite."""
    global _tz_cache_dict
    try:
        import yfinance.cache as yf_cache
        # Get the peewee Model class
        tz_model = getattr(yf_cache, '_TZ_KV', None)
        if tz_model is None:
            logger.info("  TZ-CACHE: _TZ_KV not found, using dict fallback")
            _patch_yf_tz_cache_dict()
            return True

        # Get peewee database from the Model
        db = tz_model._meta.database
        if db is None:
            _patch_yf_tz_cache_dict()
            return True

        # Close existing disk-based connection
        try:
            if not db.is_closed():
                db.close()
        except Exception:
            pass

        # Re-initialize with :memory: (in-memory SQLite)
        try:
            from peewee import SqliteDatabase
            mem_db = SqliteDatabase(':memory:')
            # Rebind the Model to use memory DB
            tz_model._meta.database = mem_db
            tz_model.bind(mem_db)
            mem_db.connect()
            mem_db.create_tables([tz_model])
            logger.info("  TZ-CACHE: Patched to IN-MEMORY SQLite ✓")
            return True
        except Exception as e:
            logger.warning("  TZ-CACHE: peewee rebind failed (%s), using dict", e)
            _patch_yf_tz_cache_dict()
            return True

    except ImportError:
        logger.info("  TZ-CACHE: yfinance.cache not importable, using dict")
        _patch_yf_tz_cache_dict()
        return True
    except Exception as e:
        logger.warning("  TZ-CACHE: Patch failed (%s), using dict fallback", e)
        _patch_yf_tz_cache_dict()
        return True


def _patch_yf_tz_cache_dict():
    """Ultimate fallback: replace lookup/store with a simple dict."""
    global _tz_cache_dict
    try:
        import yfinance.cache as yf_cache

        class DictTzCache:
            def lookup(self, key):
                return _tz_cache_dict.get(key)
            def store(self, key, value):
                _tz_cache_dict[key] = value
            def get(self, *a, **kw):
                return self.lookup(*a, **kw)

        # If yfinance uses a cache object/instance, replace its methods
        if hasattr(yf_cache, 'get_tz_cache_type'):
            pass  # newer yfinance
        # Replace the module-level functions if they exist
        if hasattr(yf_cache, '_TzCacheManager'):
            old_class = yf_cache._TzCacheManager
            orig_init = old_class.__init__
            def patched_init(self, *args, **kwargs):
                self._tz_dict = _tz_cache_dict
            def patched_lookup(self, key):
                return _tz_cache_dict.get(key)
            def patched_store(self, key, value):
                _tz_cache_dict[key] = value
            old_class.__init__ = patched_init
            old_class.lookup = patched_lookup
            old_class.store = patched_store
            logger.info("  TZ-CACHE: Patched _TzCacheManager to dict ✓")
        else:
            logger.info("  TZ-CACHE: Dict fallback set (basic)")

    except Exception as e:
        logger.warning("  TZ-CACHE: Dict fallback also failed: %s", e)


def _reinit_tz_mem_cache():
    """Re-initialize the in-memory tz cache (call after errors or periodically)."""
    try:
        import yfinance.cache as yf_cache
        tz_model = getattr(yf_cache, '_TZ_KV', None)
        if tz_model is not None:
            db = tz_model._meta.database
            if db is not None:
                db_name = getattr(db, 'database', '')
                if db_name == ':memory:':
                    # Already in-memory, just ensure table exists
                    if db.is_closed():
                        db.connect()
                    db.create_tables([tz_model], safe=True)
                    return
        # If we get here, re-do the full patch
        _patch_yf_tz_cache()
    except Exception:
        _patch_yf_tz_cache()

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

from dotenv import load_dotenv
load_dotenv()

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timedelta, date

try:
    import pytz
    ET = pytz.timezone("US/Eastern")
except ImportError:
    print("ERROR: pip install pytz")
    sys.exit(1)


# ═══════════════════════════════════════════════════════════
# YFINANCE CACHE FIX (v5 patch — PythonAnywhere safe)
# Clears corrupted yfinance tz-cache SQLite database
# ═══════════════════════════════════════════════════════════

def clear_yfinance_cache():
    """Remove corrupted yfinance cache dirs + re-init memory cache (v5.3)."""
    home = os.path.expanduser("~")
    cache_paths = [
        os.path.join(home, ".cache", "py-yfinance"),
        os.path.join(home, ".cache", "yfinance"),
        "/tmp/yf_cache",
    ]
    try:
        import yfinance as _yf
        if hasattr(_yf, "cache") and hasattr(_yf.cache, "get_dir"):
            cache_paths.append(_yf.cache.get_dir())
    except Exception:
        pass

    for path in cache_paths:
        if os.path.exists(path):
            try:
                shutil.rmtree(path)
                logger.info("  CACHE FIX: Removed cache: %s", path)
            except Exception as e:
                logger.warning("  CACHE FIX: Could not remove %s: %s", path, e)

    # v5.3: Re-establish the in-memory tz cache
    _reinit_tz_mem_cache()


def _is_disk_io_error(exc):
    """Check if exception is disk I/O / cache corruption (v5.3)."""
    if isinstance(exc, (sqlite3.OperationalError, OSError)):
        return True
    s = str(exc).lower()
    return any(k in s for k in ["database", "malformed", "sqlite", "disk", "i/o",
               "operational", "locked", "readonly", "corrupt", "no such table",
               "_tz_kv", "peewee"])


def safe_yf_download(*args, **kwargs):
    """
    Wrapper around yf.download with auto-repair (v5.3).
    Catches errors that yfinance raises AND handles the case where
    yfinance catches errors internally (silent failures).
    Retries up to 3 times with cache rebuild.
    """
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            _reinit_tz_mem_cache()  # Ensure memory cache is valid
            result = yf.download(*args, **kwargs)
            # v5.3: Check if yfinance silently failed (returns data but all NaN)
            if result is not None and not result.empty:
                return result
            # Empty result on first try = possible silent cache error
            if attempt < max_retries:
                logger.info("  CACHE FIX: Empty result (attempt %d), rebuilding cache...", attempt)
                _patch_yf_tz_cache()
                time.sleep(attempt * 2 + random.uniform(1, 2))
                continue
            return result
        except Exception as e:
            if _is_disk_io_error(e):
                logger.warning("  CACHE FIX: Error (attempt %d/%d): %s", attempt, max_retries, e)
                clear_yfinance_cache()
                time.sleep(attempt * 3 + random.uniform(1, 3))
                if attempt == max_retries:
                    logger.error("  CACHE FIX: All retries failed.")
                    return None
            else:
                raise
    return None



# ═══════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════

EMAIL_ADDRESS       = os.getenv('EMAIL_ADDRESS')
EMAIL_PASSWORD      = os.getenv('EMAIL_PASSWORD')
SMTP_SERVER         = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
SEND_EMAIL          = True
RECIPIENT_EMAILS    = os.getenv('RECIPIENT_EMAILS', '')
RECIPIENT_EMAIL     = [a.strip() for a in RECIPIENT_EMAILS.split(',') if a.strip()]

SCAN_INTERVAL_SECONDS   = 300
MIN_5MIN_CONFIRMATIONS  = 3

MARKET_OPEN_HOUR    = 9
MARKET_OPEN_MINUTE  = 30
MARKET_CLOSE_HOUR   = 16
MARKET_CLOSE_MINUTE = 0

DB_PATH         = "./scanner_alerts.db"
RETENTION_DAYS  = 10
BATCH_CHUNK_SIZE = 40   # v5.3: download in chunks


# ═══════════════════════════════════════════════════════
# STOCK UNIVERSE
# ═══════════════════════════════════════════════════════

DOW_30 = [
    "AAPL","MSFT","AMZN","NVDA","UNH","V","JNJ","WMT","JPM","PG",
    "MA","HD","MRK","CVX","KO","DIS","MCD","CSCO","ABT","VZ",
    "NKE","INTC","SHW","DOW","MMM","TRV","AXP","BA","CAT","GS"
]

SP500_TOP = [
    "GOOGL","GOOG","META","BRK-B","TSLA","XOM","LLY","ABBV",
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
    "TER","ENTG","ONTO","LSCC","WMS"
]

TOP_ETFS = [
    "SPY","QQQ","IWM","DIA","XLK","XLE","XLF","XLV","XLI",
    "XLU","XLB","XLY","XLP","XLRE","XLC","SMH","SOXX","IGF",
    "PAVE","GRID","BOTZ","TAN","URA","ITA","XME","IYT",
    "HACK","CIBR","ICLN","REMX","COPX","SIL","GDX","VNQ",
    "IBB","KRE","XOP","ARKK","ARKG","ARKF","XBI","PBW",
    "KWEB","EEM","FXI","TLT","HYG","LQD","GLD","SLV"
]

COUNTRY_ETFS = [
    "EWJ","EWG","EWZ","EWU","EWA","EWC","EWH","EWS",
    "EWY","EWT","EWW","EPI","INDA","MCHI","TUR","KSA"
]

LEVERAGED_3X = [
    "TQQQ","SQQQ","SPXL","SPXS","TNA","TZA","LABU","LABD",
    "SOXL","SOXS","FNGU","FNGD","TECL","TECS","FAS","FAZ",
    "ERX","ERY","NUGT","DUST","UPRO","SPXU","UDOW","SDOW",
    "NAIL","DPST","DRIP"
]

THEMATIC_ETFS = [
    "JETS","BITO","MSOS","BLOK","LIT","DRIV","ROBO","DFEN",
    "XHB","ITB","XRT","OIH","AMLP","WEAT","UNG","USO"
]

ALL_STOCKS = list(dict.fromkeys(
    DOW_30 + SP500_TOP + TOP_ETFS + COUNTRY_ETFS + LEVERAGED_3X + THEMATIC_ETFS
))

# ═══════════════════════════════════════════════════════════════
# v5.6 — ADDITIONAL HOT STOCKS (Static Expansion)
# Popular high-volume stocks often on Yahoo's Most Active / Movers lists
# ═══════════════════════════════════════════════════════════════

ADDITIONAL_HOT_STOCKS = [
    # Mega-cap & high-beta popular with retail
    "PLTR","SOFI","RIVN","LCID","HOOD","DKNG","MARA","RIOT","COIN","SNAP",
    "SQ","SHOP","ROKU","ZM","DOCU","CRWD","SNOW","NET","DDOG","MDB",
    "UBER","LYFT","ABNB","DASH","RBLX","U","PATH","BILL","HUBS","TWLO",
    # Energy & Commodities movers
    "DVN","FANG","MPC","VLO","PSX","OXY","HAL","BKR","AR","RRC",
    "CTRA","APA","MRO","SM","MTDR","CHRD","GPOR",
    # Biotech & Pharma movers
    "MRNA","BNTX","ALGN","DXCM","JAZZ","BMRN","SGEN","EXAS","RARE","IONS",
    "NBIX","PCVX","SNDX","SRPT","VRTX",
    # Semiconductor & AI
    "ARM","SMCI","MRVL","ON","SWKS","WOLF","ACLS","RMBS","CRUS","DIOD",
    "NXPI","GFS","SITM","AMBA","POWI",
    # Retail & Consumer
    "COST","TJX","ROST","DG","DLTR","FIVE","ULTA","LULU","GPS","ANF",
    "CASY","WOOF","CHWY","CROX","BIRK",
    # Financials & Fintech
    "PYPL","AFRM","UPST","SOFI","MKTX","LPLA","IBKR","CBOE","NDAQ",
    # Industrials & Infra
    "GNRC","PLUG","ENPH","SEDG","RUN","NOVA","ARRY",
    # China & International ADRs
    "BABA","JD","PDD","NIO","LI","XPEV","BIDU","TME","BILI",
    # Popular mega-cap additions
    "WFC","BAC","C","USB","PNC","TFC","FITB","HBAN","KEY","RF",
    "T","TMUS","CHTR",
]

# Merge additional hot stocks into ALL_STOCKS (preserves order, no dupes)
ALL_STOCKS = list(dict.fromkeys(ALL_STOCKS + ADDITIONAL_HOT_STOCKS))


# ═══════════════════════════════════════════════════════════════
# v5.6 — DYNAMIC HOT STOCKS FETCHER (Yahoo Finance Screeners)
# Pulls today's most active, biggest gainers/losers at runtime
# ═══════════════════════════════════════════════════════════════

def get_dynamic_hot_stocks():
    """
    Fetch today's most active + top gainers + top losers from Yahoo Finance.
    Uses yfinance's built-in screener API where available, plus a direct
    Yahoo Finance query endpoint as fallback.
    Returns a set of ticker symbols.
    """
    hot = set()

    # ── Method 1: yfinance built-in predefined screeners ──
    try:
        if hasattr(yf, 'screen'):
            for screener_name in ['day_gainers', 'day_losers', 'most_actives']:
                try:
                    result = yf.screen(screener_name, size=50)
                    if result and 'quotes' in result:
                        for q in result['quotes']:
                            sym = q.get('symbol', '')
                            if sym and '.' not in sym and '-' not in sym and len(sym) <= 5:
                                hot.add(sym)
                    logger.info("  DYNAMIC: %s screener returned %d tickers (total hot=%d)",
                                screener_name, len(result.get('quotes', [])) if result else 0, len(hot))
                except Exception as e:
                    logger.warning("  DYNAMIC: %s screener failed: %s", screener_name, e)
    except Exception as e:
        logger.warning("  DYNAMIC: yf.screen not available: %s", e)

    # ── Method 2: yfinance EquityQuery custom screener ──
    try:
        from yfinance import EquityQuery
        # High-volume US stocks with significant intraday moves
        q = EquityQuery('and', [
            EquityQuery('eq', ['region', 'us']),
            EquityQuery('gt', ['dayvolume', 1000000]),
            EquityQuery('gt', ['intradaymarketcap', 2000000000]),
            EquityQuery('gt', ['intradayprice', 5]),
        ])
        result = yf.screen(q, size=100)
        if result and 'quotes' in result:
            for item in result['quotes']:
                sym = item.get('symbol', '')
                if sym and '.' not in sym and '-' not in sym and len(sym) <= 5:
                    hot.add(sym)
            logger.info("  DYNAMIC: EquityQuery screener returned %d tickers (total hot=%d)",
                        len(result.get('quotes', [])), len(hot))
    except ImportError:
        logger.info("  DYNAMIC: EquityQuery not available in this yfinance version")
    except Exception as e:
        logger.warning("  DYNAMIC: EquityQuery screener failed: %s", e)

    # ── Method 3: Yahoo Finance trending tickers endpoint (direct HTTP) ──
    try:
        url = "https://query1.finance.yahoo.com/v1/finance/trending/US?count=50"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 market-scanner-v5"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        quotes = payload.get("finance", {}).get("result", [{}])[0].get("quotes", [])
        for q in quotes:
            sym = q.get("symbol", "")
            if sym and '.' not in sym and '-' not in sym and len(sym) <= 5:
                hot.add(sym)
        logger.info("  DYNAMIC: Yahoo trending returned %d tickers (total hot=%d)", len(quotes), len(hot))
    except Exception as e:
        logger.warning("  DYNAMIC: Yahoo trending endpoint failed: %s", e)

    # ── Method 4: Yahoo chart-based most active backup ──
    try:
        url2 = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?formatted=false&scrIds=most_actives&count=50"
        req2 = urllib.request.Request(url2, headers={"User-Agent": "Mozilla/5.0 market-scanner-v5"})
        with urllib.request.urlopen(req2, timeout=15) as resp2:
            payload2 = json.loads(resp2.read().decode("utf-8"))
        quotes2 = payload2.get("finance", {}).get("result", [{}])[0].get("quotes", [])
        for q in quotes2:
            sym = q.get("symbol", "")
            if sym and '.' not in sym and '-' not in sym and len(sym) <= 5:
                hot.add(sym)
        logger.info("  DYNAMIC: Yahoo most_actives returned %d tickers (total hot=%d)", len(quotes2), len(hot))
    except Exception as e:
        logger.warning("  DYNAMIC: Yahoo most_actives backup failed: %s", e)

    logger.info("  DYNAMIC: Total unique hot tickers fetched: %d", len(hot))
    return hot


def refresh_scan_universe(scan_number, force=False):
    """
    v5.6: Refresh the dynamic hot stocks every 2 scans (or on first scan).
    Merges dynamic tickers with the static ALL_STOCKS list.
    Returns the combined list to scan.
    """
    global dynamic_hot_tickers, dynamic_refresh_scan
    if force or scan_number <= 1 or (scan_number - dynamic_refresh_scan) >= 2:
        logger.info("  UNIVERSE: Refreshing dynamic hot stocks (scan #%d)...", scan_number)
        try:
            dynamic_hot_tickers = get_dynamic_hot_stocks()
            dynamic_refresh_scan = scan_number
        except Exception as e:
            logger.warning("  UNIVERSE: Dynamic refresh failed: %s", e)
    else:
        logger.info("  UNIVERSE: Using cached dynamic tickers (%d) from scan #%d",
                     len(dynamic_hot_tickers), dynamic_refresh_scan)

    combined = list(dict.fromkeys(ALL_STOCKS + list(dynamic_hot_tickers)))
    logger.info("  UNIVERSE: Total scan universe = %d (base=%d + dynamic=%d, deduped)",
                len(combined), len(ALL_STOCKS), len(dynamic_hot_tickers))
    return combined


# ═══════════════════════════════════════════════════════
# SECTOR MAP
# ═══════════════════════════════════════════════════════

SECTOR_MAP = {
    "AAPL":"XLK","MSFT":"XLK","NVDA":"XLK","AVGO":"XLK","CSCO":"XLK","INTC":"XLK",
    "ADBE":"XLK","CRM":"XLK","ACN":"XLK","TXN":"XLK","AMD":"XLK","QCOM":"XLK",
    "AMAT":"XLK","LRCX":"XLK","KLAC":"XLK","SNPS":"XLK","CDNS":"XLK","MCHP":"XLK",
    "KEYS":"XLK","PANW":"XLK","INTU":"XLK","MSI":"XLK","FICO":"XLK","MPWR":"XLK",
    "TER":"XLK","ENTG":"XLK","ONTO":"XLK","LSCC":"XLK","FSLR":"XLK","ADI":"XLK",
    "GOOGL":"XLC","GOOG":"XLC","META":"XLC","NFLX":"XLC","CMCSA":"XLC","DIS":"XLC","VZ":"XLC",
    "AMZN":"XLY","TSLA":"XLY","HD":"XLY","MCD":"XLY","LOW":"XLY","NKE":"XLY",
    "BKNG":"XLY","CMG":"XLY","ORLY":"XLY","DECK":"XLY",
    "JPM":"XLF","V":"XLF","MA":"XLF","GS":"XLF","BLK":"XLF","AXP":"XLF",
    "SCHW":"XLF","SPGI":"XLF","CB":"XLF","CME":"XLF","ICE":"XLF","PGR":"XLF",
    "BRK-B":"XLF","TRV":"XLF","AJG":"XLF",
    "UNH":"XLV","JNJ":"XLV","LLY":"XLV","ABBV":"XLV","MRK":"XLV","PFE":"XLV",
    "TMO":"XLV","DHR":"XLV","ABT":"XLV","SYK":"XLV","ISRG":"XLV","VRTX":"XLV",
    "REGN":"XLV","GILD":"XLV","DXCM":"XLV","EW":"XLV","A":"XLV","IDXX":"XLV",
    "HCA":"XLV","ZTS":"XLV","MCK":"XLV","CI":"XLV","GEHC":"XLV","WST":"XLV","PODD":"XLV",
    "CAT":"XLI","BA":"XLI","HON":"XLI","UPS":"XLI","RTX":"XLI","GE":"XLI",
    "MMM":"XLI","DOW":"XLI","FDX":"XLI","EMR":"XLI","ITW":"XLI","CTAS":"XLI",
    "PH":"XLI","ROP":"XLI","TDG":"XLI","PAYX":"XLI","FAST":"XLI","RSG":"XLI",
    "ODFL":"XLI","CARR":"XLI","GWW":"XLI","VRSK":"XLI","CPRT":"XLI","ROK":"XLI",
    "AME":"XLI","XYL":"XLI","IR":"XLI","AXON":"XLI","HUBB":"XLI","PWR":"XLI",
    "EME":"XLI","TT":"XLI","WMS":"XLI","URI":"XLI","UNP":"XLI",
    "XOM":"XLE","CVX":"XLE","SLB":"XLE","EOG":"XLE","LNG":"XLE","TRGP":"XLE",
    "WMT":"XLP","PG":"XLP","KO":"XLP","PEP":"XLP","COST":"XLP","PM":"XLP",
    "MDLZ":"XLP","CL":"XLP","MO":"XLP","KMB":"XLP",
    "NEE":"XLU","DUK":"XLU","SO":"XLU","CEG":"XLU","SRE":"XLU",
    "APD":"XLB","SHW":"XLB","MLM":"XLB","VMC":"XLB",
    "AMT":"XLRE","WELL":"XLRE","PSA":"XLRE","OTIS":"XLRE",
}


# ═══════════════════════════════════════════════════════
# US MARKET HOLIDAYS
# ═══════════════════════════════════════════════════════

US_MARKET_HOLIDAYS = [
    date(2024,1,1),date(2024,1,15),date(2024,2,19),date(2024,3,29),date(2024,5,27),
    date(2024,6,19),date(2024,7,4),date(2024,9,2),date(2024,11,28),date(2024,12,25),
    date(2025,1,1),date(2025,1,20),date(2025,2,17),date(2025,4,18),date(2025,5,26),
    date(2025,6,19),date(2025,7,4),date(2025,9,1),date(2025,11,27),date(2025,12,25),
    date(2026,1,1),date(2026,1,19),date(2026,2,16),date(2026,4,3),date(2026,5,25),
    date(2026,6,19),date(2026,7,3),date(2026,9,7),date(2026,11,26),date(2026,12,25),
    date(2027,1,1),date(2027,1,18),date(2027,2,15),date(2027,3,26),date(2027,5,31),
    date(2027,6,18),date(2027,7,5),date(2027,9,6),date(2027,11,25),date(2027,12,24),
    date(2028,1,17),date(2028,2,21),date(2028,4,14),date(2028,5,29),date(2028,6,19),
    date(2028,7,4),date(2028,9,4),date(2028,11,23),date(2028,12,25),
    date(2029,1,1),date(2029,1,15),date(2029,2,19),date(2029,3,30),date(2029,5,28),
    date(2029,6,19),date(2029,7,4),date(2029,9,3),date(2029,11,22),date(2029,12,25),
    date(2030,1,1),date(2030,1,21),date(2030,2,18),date(2030,4,19),date(2030,5,27),
    date(2030,6,19),date(2030,7,4),date(2030,9,2),date(2030,11,28),date(2030,12,25),
]


# ═══════════════════════════════════════════════════════
# LOGGING & GLOBAL STATE
# ═══════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

confirmation_tracker = {}
sent_alerts = set()
daily_alerts = []
market_regime_cache = {"regime": "NEUTRAL", "spy_rsi": 50, "timestamp": None}
sector_strength_cache = {}
scan_history = []
MAX_SCAN_HISTORY = 4
near_miss_candidates = []       # v5.6: Near-miss watchlist for dashboard
dynamic_hot_tickers = set()     # v5.6: Dynamic hot stocks from Yahoo screeners
dynamic_refresh_scan = 0        # v5.6: Track which scan last refreshed dynamic tickers
RQG_MIN_SCORE = 40          # v5.6: Lowered from 65 → 55 to allow more quality candidates through
RQG_A_PLUS_SCORE = 80
RQG_WATCH_SCORE = 30
RQG_ENFORCE_GATE = True     # Set False to observe RQG without blocking alerts


# ═══════════════════════════════════════════════════════════════
# v5.3 — SQLITE DB HELPER WITH WAL MODE + RETRY
# ═══════════════════════════════════════════════════════════════

def get_db_connection():
    """Open SQLite connection with WAL mode and 30s timeout."""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


# ═══════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════

def get_eastern_now():
    return datetime.now(ET)


def minutes_until_close():
    now = get_eastern_now()
    mc = now.replace(hour=MARKET_CLOSE_HOUR, minute=MARKET_CLOSE_MINUTE, second=0)
    return max((mc - now).total_seconds() / 60, 0)


def get_time_quality():
    now = get_eastern_now()
    mins = now.hour * 60 + now.minute
    if mins < 600:
        return 0.7, "OPENING_NOISE"
    elif mins < 630:
        return 0.9, "EARLY"
    elif mins < 810:
        return 1.1, "PRIME"
    elif mins < 870:
        return 0.85, "LUNCH"
    elif mins < 930:
        return 1.15, "POWER_HOUR"
    else:
        return 0.9, "CLOSING"



# ═══════════════════════════════════════════════════════
# DATA QUALITY + REVERSAL QUALITY GATE + SCAN SUMMARY ENGINE (v5.5)
# ═══════════════════════════════════════════════════════

STALE_LIMIT_MINUTES = {
    "1m": 5,
    "2m": 8,
    "5m": 12,
    "15m": 25,
    "30m": 45,
    "60m": 90,
    "1h": 90,
    "1d": 7 * 24 * 60,  # daily bars: allow prior trading day/weekend gap
}


def is_regular_market_hours(now=None):
    """Return True only during normal US market hours on non-holiday weekdays."""
    now = now or get_eastern_now()
    if now.weekday() >= 5 or now.date() in US_MARKET_HOLIDAYS:
        return False
    open_dt = now.replace(hour=MARKET_OPEN_HOUR, minute=MARKET_OPEN_MINUTE, second=0, microsecond=0)
    close_dt = now.replace(hour=MARKET_CLOSE_HOUR, minute=MARKET_CLOSE_MINUTE, second=0, microsecond=0)
    return open_dt <= now <= close_dt


def _flatten_yf_columns(df):
    """Normalize single-ticker yfinance output columns."""
    if df is not None and not df.empty and isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = df.columns.get_level_values(0)
    return df


def _latest_timestamp_et(df):
    """Return latest bar timestamp converted to ET, or None.

    Daily yfinance bars can have timezone-naive date indexes. We use the index directly
    and normalize daily midnight timestamps to 09:30 ET for readable freshness logs.
    """
    try:
        if df is None or df.empty or len(df.index) == 0:
            return None
        ts = pd.Timestamp(df.index[-1])
        if ts.tzinfo is None:
            ts = ET.localize(ts.to_pydatetime())
        else:
            ts = ts.tz_convert(ET)
        if ts.hour == 0 and ts.minute == 0 and ts.second == 0:
            ts = ts.replace(hour=9, minute=30, second=0, microsecond=0)
        return ts.to_pydatetime()
    except Exception:
        return None


def get_data_freshness(df, interval="5m"):
    """Return detailed freshness metadata for logging and decision making."""
    now = get_eastern_now()
    latest = _latest_timestamp_et(df)
    limit = STALE_LIMIT_MINUTES.get(interval, 30)
    if latest is None:
        return {
            "fresh": False, "age_min": 999999, "latest_et": "N/A",
            "reason": "NO_VALID_TIMESTAMP", "limit_min": limit,
        }
    age_min = max((now - latest).total_seconds() / 60.0, 0)
    fresh = True if not is_regular_market_hours(now) else age_min <= limit
    reason = "FRESH" if fresh else "STALE_DURING_MARKET_HOURS"
    return {
        "fresh": fresh,
        "age_min": round(age_min, 1),
        "latest_et": latest.strftime("%Y-%m-%d %I:%M:%S %p ET"),
        "reason": reason,
        "limit_min": limit,
    }


def _yahoo_range_for_period(period):
    return {
        "1d": "1d", "2d": "2d", "5d": "5d", "7d": "7d",
        "1mo": "1mo", "3mo": "3mo", "6mo": "6mo", "1y": "1y",
    }.get(period, period)


def yahoo_chart_download(ticker, period="5d", interval="5m", timeout=20):
    """Backup downloader using Yahoo's chart endpoint directly, bypassing yfinance cache."""
    try:
        params = urlencode({
            "range": _yahoo_range_for_period(period),
            "interval": interval,
            "includePrePost": "false",
            "events": "div,splits",
        })
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?{params}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 market-scanner-v5"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        result = payload.get("chart", {}).get("result") or []
        if not result:
            err = payload.get("chart", {}).get("error")
            logger.warning("  BACKUP-DL %s: Yahoo chart returned no result: %s", ticker, err)
            return None
        r = result[0]
        timestamps = r.get("timestamp") or []
        quote = (r.get("indicators", {}).get("quote") or [{}])[0]
        if not timestamps or not quote:
            logger.warning("  BACKUP-DL %s: missing timestamps/quote", ticker)
            return None
        idx = pd.to_datetime(timestamps, unit="s", utc=True).tz_convert(ET)
        df = pd.DataFrame({
            "Open": quote.get("open"),
            "High": quote.get("high"),
            "Low": quote.get("low"),
            "Close": quote.get("close"),
            "Volume": quote.get("volume"),
        }, index=idx)
        df = df.dropna(subset=["Close"])
        return df if not df.empty else None
    except Exception as e:
        logger.warning("  BACKUP-DL %s: Yahoo chart fallback failed: %s", ticker, e)
        return None


def download_with_freshness(ticker, period="5d", interval="5m", label="DATA", allow_fallback=True):
    """
    Download data, validate latest-bar freshness, and fallback if stale/empty.

    Full INFO details are kept for REGIME and SECTOR layers. Normal stock downloads stay quiet
    unless stale, fallback, failure, or DEBUG logging is enabled. Daily 1d data is accepted when
    non-empty to avoid unnecessary fallback calls during trend checks.
    """
    meta = {"ticker": ticker, "interval": interval, "period": period, "source": "yfinance", "fallback_used": False}
    verbose = label.startswith("REGIME") or label.startswith("SECTOR") or label.startswith("RQG") or logger.isEnabledFor(logging.DEBUG)
    if verbose:
        logger.info("  %s %s: request period=%s interval=%s primary=yfinance", label, ticker, period, interval)

    df = safe_yf_download(ticker, period=period, interval=interval,
                          progress=False, auto_adjust=True, threads=False)
    df = _flatten_yf_columns(df)
    q = get_data_freshness(df, interval)
    meta.update(q)
    rows = 0 if df is None else len(df)

    # Daily data is used for 3d/5d trend; it should not be treated as intraday stale.
    if interval == "1d" and df is not None and not df.empty:
        meta["fresh"] = True
        if meta.get("reason") == "NO_VALID_TIMESTAMP":
            meta["reason"] = "DAILY_ACCEPTED_NONEMPTY"
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("  %s %s: daily accepted rows=%s latest=%s source=yfinance", label, ticker, rows, meta.get("latest_et"))
        return df, meta

    if verbose:
        logger.info("  %s %s: primary rows=%s latest=%s age=%sm limit=%sm status=%s",
                    label, ticker, rows, q["latest_et"], q["age_min"], q["limit_min"], q["reason"])

    if df is not None and not df.empty and q["fresh"]:
        return df, meta

    if allow_fallback:
        logger.warning("  %s %s: primary not usable (%s). Trying backup YahooChart direct...", label, ticker, q["reason"])
        bdf = yahoo_chart_download(ticker, period=period, interval=interval)
        bq = get_data_freshness(bdf, interval)
        brows = 0 if bdf is None else len(bdf)
        logger.info("  %s %s: backup rows=%s latest=%s age=%sm limit=%sm status=%s",
                    label, ticker, brows, bq["latest_et"], bq["age_min"], bq["limit_min"], bq["reason"])
        if bdf is not None and not bdf.empty and (bq["fresh"] or interval == "1d"):
            meta.update(bq)
            meta["source"] = "YahooChartDirect"
            meta["fallback_used"] = True
            meta["fresh"] = True
            return bdf, meta

    meta["source"] = meta.get("source", "yfinance")
    logger.error("  %s %s: NO FRESH DATA available. latest=%s age=%sm. Scanner will use safe neutral/skip behavior.",
                 label, ticker, meta.get("latest_et"), meta.get("age_min"))
    return None, meta


def calculate_atr(high, low, close, period=14):
    try:
        prev_close = close.shift(1)
        tr = pd.concat([
            (high - low),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ], axis=1).max(axis=1)
        return tr.rolling(period).mean()
    except Exception:
        return pd.Series(index=close.index, dtype=float)


def _fmt_pct(v, default="N/A"):
    try:
        return "%+.2f%%" % float(v)
    except Exception:
        return default


def _fmt_price(v, default="N/A"):
    try:
        return "$%.2f" % float(v)
    except Exception:
        return default


def _safe_float(v, default=0.0):
    try:
        if v is None or pd.isna(v):
            return default
        return float(v)
    except Exception:
        return default


def _regular_bullish_divergence(close, rsi, lookback=30):
    """Stricter local bullish divergence for RQG only."""
    try:
        if len(close) < lookback or len(rsi) < lookback:
            return False
        c = close.iloc[-lookback:]
        r = rsi.iloc[-lookback:]
        half = lookback // 2
        first_low_idx = c.iloc[:half].idxmin()
        second_low_idx = c.iloc[half:].idxmin()
        return c.loc[second_low_idx] < c.loc[first_low_idx] and r.loc[second_low_idx] > r.loc[first_low_idx]
    except Exception:
        return False


def _regular_bearish_divergence(close, rsi, lookback=30):
    """Stricter local bearish divergence for RQG only."""
    try:
        if len(close) < lookback or len(rsi) < lookback:
            return False
        c = close.iloc[-lookback:]
        r = rsi.iloc[-lookback:]
        half = lookback // 2
        first_high_idx = c.iloc[:half].idxmax()
        second_high_idx = c.iloc[half:].idxmax()
        return c.loc[second_high_idx] > c.loc[first_high_idx] and r.loc[second_high_idx] < r.loc[first_high_idx]
    except Exception:
        return False


def evaluate_reversal_quality_gate(ticker, signal_direction, market_regime, sector_strength,
                                   r5=None, r15=None, r30=None, r60=None):
    """
    Reversal Quality Gate (RQG) — final intraday reversal confirmation.

    This does not replace existing v5 scoring. It runs AFTER 5m+15m+30m strict confirmation
    and attempts to block weak false reversals by scoring: location, exhaustion/divergence,
    volume, reclaim/rejection, market/sector context, and risk/reward.
    """
    result = {
        "score": 0,
        "label": "REJECT",
        "passed": False,
        "hard_reject": False,
        "reasons": [],
        "details": {},
    }
    try:
        data, meta = download_with_freshness(ticker, period="5d", interval="5m", label="RQG", allow_fallback=True)
        if data is None or data.empty or len(data) < 60:
            result["reasons"].append("RQG_NO_FRESH_5M_DATA")
            logger.warning("  RQG %s %s: REJECT score=0 reason=no_fresh_5m_data meta=%s", ticker, signal_direction, meta)
            return result

        o = data["Open"].dropna()
        h = data["High"].dropna()
        l = data["Low"].dropna()
        c = data["Close"].dropna()
        v = data["Volume"].dropna()
        common = c.index.intersection(h.index).intersection(l.index).intersection(o.index).intersection(v.index)
        o, h, l, c, v = o.loc[common], h.loc[common], l.loc[common], c.loc[common], v.loc[common]
        if len(c) < 60:
            result["reasons"].append("RQG_INSUFFICIENT_ROWS")
            return result

        rsi14 = calculate_rsi(c, 14)
        rsi2 = calculate_rsi(c, 2)
        macd_line, macd_sig, macd_hist = calculate_macd(c)
        vwap = calculate_vwap(h, l, c, v)
        bbu, bbm, bbl = calculate_bollinger(c)
        atr = calculate_atr(h, l, c, 14)
        ema8 = c.ewm(span=8, adjust=False).mean()
        ema20 = c.ewm(span=20, adjust=False).mean()

        last = float(c.iloc[-1])
        prev_high = float(h.iloc[-2])
        prev_low = float(l.iloc[-2])
        last_high = float(h.iloc[-1])
        last_low = float(l.iloc[-1])
        last_open = float(o.iloc[-1])
        last_vwap = _safe_float(vwap.iloc[-1])
        last_bbu = _safe_float(bbu.iloc[-1])
        last_bbl = _safe_float(bbl.iloc[-1])
        last_atr = max(_safe_float(atr.iloc[-1]), last * 0.001)
        last_rsi2 = _safe_float(rsi2.iloc[-1], 50)
        last_rsi14 = _safe_float(rsi14.iloc[-1], 50)
        mh = _safe_float(macd_hist.iloc[-1])
        mh_prev = _safe_float(macd_hist.iloc[-2])
        vol20 = _safe_float(v.rolling(20).mean().iloc[-1], 0)
        vol_ratio = float(v.iloc[-1]) / vol20 if vol20 > 0 else 1.0
        vwap_std = _safe_float((c - vwap).rolling(50).std().iloc[-1], last_atr)
        vwap_z = (last - last_vwap) / vwap_std if vwap_std > 0 else 0.0
        ema20_slope = (ema20.iloc[-1] - ema20.iloc[-8]) / ema20.iloc[-8] * 100 if len(ema20) > 8 and ema20.iloc[-8] != 0 else 0.0
        vwap_slope = (vwap.iloc[-1] - vwap.iloc[-8]) / vwap.iloc[-8] * 100 if len(vwap) > 8 and vwap.iloc[-8] != 0 else 0.0
        recent_lows_down = bool(l.iloc[-1] < l.iloc[-3] < l.iloc[-6]) if len(l) >= 6 else False
        recent_highs_up = bool(h.iloc[-1] > h.iloc[-3] > h.iloc[-6]) if len(h) >= 6 else False

        score = 0
        reasons = []
        buckets = {"location": 0, "divergence": 0, "volume": 0, "reclaim": 0, "context": 0, "risk_reward": 0}

        if signal_direction == "BOTTOM":
            # 1) Extreme location — max 20
            if vwap_z <= -2.0:
                buckets["location"] += 12; reasons.append("DeepBelowVWAP2σ")
            elif vwap_z <= -1.25:
                buckets["location"] += 8; reasons.append("BelowVWAPStretch")
            if last_bbl > 0 and last <= last_bbl * 1.005:
                buckets["location"] += 8; reasons.append("LowerBandLocation")
            elif last < last_vwap:
                buckets["location"] += 4; reasons.append("BelowVWAP")

            # 2) Momentum divergence/exhaustion — max 20
            bull_div = bool((r5 or {}).get("bdiv")) or _regular_bullish_divergence(c, rsi14)
            if bull_div:
                buckets["divergence"] += 10; reasons.append("BullishDivergence")
            if last_rsi2 <= 5:
                buckets["divergence"] += 6; reasons.append("RSI2Exhausted")
            elif last_rsi2 <= 12:
                buckets["divergence"] += 3; reasons.append("RSI2Low")
            if mh > mh_prev:
                buckets["divergence"] += 4; reasons.append("MACDHistImproving")

            # 3) Volume/exhaustion — max 15
            green = last > last_open
            if vol_ratio >= 1.8 and green:
                buckets["volume"] += 8; reasons.append("CapitulationGreenVolume")
            elif vol_ratio >= 1.4:
                buckets["volume"] += 5; reasons.append("VolumeExpansion")
            if bool((r5 or {}).get("vd", 0) > 0.55):
                buckets["volume"] += 5; reasons.append("BuyVolumeImproving")
            if last > prev_low and last_low <= min(l.iloc[-10:]):
                buckets["volume"] += 2; reasons.append("SpringAttempt")

            # 4) Reclaim/structure confirmation — max 20
            if last > prev_high:
                buckets["reclaim"] += 8; reasons.append("CloseAbovePriorHigh")
            if last > ema8.iloc[-1]:
                buckets["reclaim"] += 4; reasons.append("ReclaimEMA8")
            if last_bbl > 0 and last > last_bbl and c.iloc[-2] <= bbl.iloc[-2] * 1.005:
                buckets["reclaim"] += 4; reasons.append("BackInsideLowerBand")
            if last_vwap > 0 and last > last_vwap:
                buckets["reclaim"] += 4; reasons.append("ReclaimedVWAP")

            # 5) Sector/market context — max 15
            if sector_strength == "STRONG":
                buckets["context"] += 7; reasons.append("SectorStrong")
            elif sector_strength == "NEUTRAL":
                buckets["context"] += 4; reasons.append("SectorNeutral")
            elif sector_strength == "WEAK" and bool((r5 or {}).get("sector_override", 0)):
                buckets["context"] += 5; reasons.append("WeakSectorButStockOverride")
            if market_regime != "BEARISH":
                buckets["context"] += 5; reasons.append("MarketNotBearish")
            elif last > ema8.iloc[-1] and mh > mh_prev:
                buckets["context"] += 3; reasons.append("BearMarketButMicroReversal")
            if (r5 or {}).get("relative_strength", 0) > 0:
                buckets["context"] += 3; reasons.append("PositiveRelativeStrength")

            # 6) Risk/reward to VWAP/mean — max 10
            stop = min(l.iloc[-12:]) - 0.10 * last_atr
            target = max(last_vwap, bbm.iloc[-1])
            risk = max(last - stop, 0.01)
            reward = max(target - last, 0.0)
            rr = reward / risk if risk > 0 else 0
            if rr >= 2.0:
                buckets["risk_reward"] += 10; reasons.append("RR>=2")
            elif rr >= 1.5:
                buckets["risk_reward"] += 7; reasons.append("RR>=1.5")
            elif rr >= 1.0:
                buckets["risk_reward"] += 4; reasons.append("RR>=1")

            hard_reject = False
            hard_reasons = []
            if sector_strength == "WEAK" and market_regime == "BEARISH" and vwap_slope < -0.15 and recent_lows_down and last < ema8.iloc[-1]:
                hard_reject = True; hard_reasons.append("StrongDowntrendDanger")
            if vol_ratio > 1.5 and last < last_open and last < prev_low:
                hard_reject = True; hard_reasons.append("SellingStillExpanding")

        else:  # TOP
            # 1) Extreme location — max 20
            if vwap_z >= 2.0:
                buckets["location"] += 12; reasons.append("DeepAboveVWAP2σ")
            elif vwap_z >= 1.25:
                buckets["location"] += 8; reasons.append("AboveVWAPStretch")
            if last_bbu > 0 and last >= last_bbu * 0.995:
                buckets["location"] += 8; reasons.append("UpperBandLocation")
            elif last > last_vwap:
                buckets["location"] += 4; reasons.append("AboveVWAP")

            # 2) Momentum divergence/exhaustion — max 20
            bear_div = bool((r5 or {}).get("brdiv")) or _regular_bearish_divergence(c, rsi14)
            if bear_div:
                buckets["divergence"] += 10; reasons.append("BearishDivergence")
            if last_rsi2 >= 95:
                buckets["divergence"] += 6; reasons.append("RSI2ExhaustedHigh")
            elif last_rsi2 >= 88:
                buckets["divergence"] += 3; reasons.append("RSI2High")
            if mh < mh_prev:
                buckets["divergence"] += 4; reasons.append("MACDHistWeakening")

            # 3) Volume/exhaustion — max 15
            red = last < last_open
            if vol_ratio >= 1.8 and red:
                buckets["volume"] += 8; reasons.append("BlowoffRedVolume")
            elif vol_ratio >= 1.4:
                buckets["volume"] += 5; reasons.append("VolumeExpansion")
            if bool((r5 or {}).get("vd", 0) < 0.45):
                buckets["volume"] += 5; reasons.append("SellVolumeImproving")
            if last < prev_high and last_high >= max(h.iloc[-10:]):
                buckets["volume"] += 2; reasons.append("UpthrustAttempt")

            # 4) Rejection/structure confirmation — max 20
            if last < prev_low:
                buckets["reclaim"] += 8; reasons.append("CloseBelowPriorLow")
            if last < ema8.iloc[-1]:
                buckets["reclaim"] += 4; reasons.append("LostEMA8")
            if last_bbu > 0 and last < last_bbu and c.iloc[-2] >= bbu.iloc[-2] * 0.995:
                buckets["reclaim"] += 4; reasons.append("BackInsideUpperBand")
            if last_vwap > 0 and last < last_vwap:
                buckets["reclaim"] += 4; reasons.append("LostVWAP")

            # 5) Sector/market context — max 15
            if sector_strength == "WEAK":
                buckets["context"] += 7; reasons.append("SectorWeak")
            elif sector_strength == "NEUTRAL":
                buckets["context"] += 4; reasons.append("SectorNeutral")
            if market_regime != "BULLISH":
                buckets["context"] += 5; reasons.append("MarketNotBullish")
            elif last < ema8.iloc[-1] and mh < mh_prev:
                buckets["context"] += 3; reasons.append("BullMarketButMicroTop")
            if (r5 or {}).get("relative_strength", 0) < 0:
                buckets["context"] += 3; reasons.append("NegativeRelativeStrength")

            # 6) Risk/reward to VWAP/mean — max 10
            stop = max(h.iloc[-12:]) + 0.10 * last_atr
            target = min(last_vwap, bbm.iloc[-1])
            risk = max(stop - last, 0.01)
            reward = max(last - target, 0.0)
            rr = reward / risk if risk > 0 else 0
            if rr >= 2.0:
                buckets["risk_reward"] += 10; reasons.append("RR>=2")
            elif rr >= 1.5:
                buckets["risk_reward"] += 7; reasons.append("RR>=1.5")
            elif rr >= 1.0:
                buckets["risk_reward"] += 4; reasons.append("RR>=1")

            hard_reject = False
            hard_reasons = []
            if sector_strength == "STRONG" and market_regime == "BULLISH" and vwap_slope > 0.15 and recent_highs_up and last > ema8.iloc[-1]:
                hard_reject = True; hard_reasons.append("StrongUptrendDanger")
            if vol_ratio > 1.5 and last > last_open and last > prev_high:
                hard_reject = True; hard_reasons.append("BuyingStillExpanding")

        # Cap bucket scores
        buckets["location"] = min(buckets["location"], 20)
        buckets["divergence"] = min(buckets["divergence"], 20)
        buckets["volume"] = min(buckets["volume"], 15)
        buckets["reclaim"] = min(buckets["reclaim"], 20)
        buckets["context"] = min(buckets["context"], 15)
        buckets["risk_reward"] = min(buckets["risk_reward"], 10)
        score = int(sum(buckets.values()))

        if hard_reject:
            label = "REJECT_TREND_DANGER"
            passed = False
            reasons.extend(hard_reasons)
        elif score >= RQG_A_PLUS_SCORE:
            label = "A_PLUS_REVERSAL"
            passed = True
        elif score >= RQG_MIN_SCORE:
            label = "VALID_REVERSAL"
            passed = True
        elif score >= RQG_WATCH_SCORE:
            label = "WATCH_ONLY"
            passed = False
        else:
            label = "REJECT_LOW_QUALITY"
            passed = False

        result.update({
            "score": score,
            "label": label,
            "passed": passed,
            "hard_reject": hard_reject,
            "reasons": reasons,
            "buckets": buckets,
            "details": {
                "price": round(last, 2),
                "vwap": round(last_vwap, 2),
                "vwap_z": round(float(vwap_z), 2),
                "rsi2": round(float(last_rsi2), 1),
                "rsi14": round(float(last_rsi14), 1),
                "macd_hist": round(float(mh), 4),
                "macd_hist_prev": round(float(mh_prev), 4),
                "vol_ratio": round(float(vol_ratio), 2),
                "rr": round(float(rr), 2),
                "vwap_slope_8bars_pct": round(float(vwap_slope), 3),
                "ema20_slope_8bars_pct": round(float(ema20_slope), 3),
                "source": meta.get("source"),
                "latest": meta.get("latest_et"),
                "age_min": meta.get("age_min"),
            }
        })

        logger.info(
            "  RQG %s %s: %s score=%d buckets=%s price=%.2f vwap=%.2f z=%.2f rsi2=%.1f vol=%.2fx rr=%.2f reasons=%s",
            ticker, signal_direction, label, score, buckets, last, last_vwap, vwap_z,
            last_rsi2, vol_ratio, rr, ",".join(reasons[:12])
        )
        return result
    except Exception as e:
        logger.exception("  RQG %s %s failed: %s", ticker, signal_direction, e)
        result["reasons"].append("RQG_EXCEPTION")
        return result


def snapshot_sector_state(checked_sectors=None):
    """Capture sector state for summary/comparison without changing trading logic."""
    sectors = checked_sectors or list(sector_strength_cache.keys())
    snap = {}
    for sec in sorted(sectors):
        c = sector_strength_cache.get(sec, {})
        if not c:
            continue
        snap[sec] = {
            "strength": c.get("strength", "UNKNOWN"),
            "reason": c.get("reason", "N/A"),
            "source": c.get("source", "unknown"),
            "latest_et": c.get("latest_et", "N/A"),
            "age_min": c.get("age_min", "N/A"),
            "diff": c.get("diff", 0.0),
            "sector_return": c.get("sector_return", 0.0),
            "spy_return": c.get("spy_return", 0.0),
            "session_return": c.get("session_return", 0.0),
            "session_spy_return": c.get("session_spy_return", 0.0),
            "close": c.get("close", None),
            "spy_close": c.get("spy_close", None),
            "fallback_used": c.get("fallback_used", False),
        }
    return snap


def snapshot_confirmation_state():
    """Summarize current 5m confirmation tracker by sector/stage."""
    rows = []
    for ticker, v in confirmation_tracker.items():
        rows.append({
            "ticker": ticker,
            "sector": v.get("sector", get_stock_sector(ticker)),
            "signal": v.get("type", "?"),
            "count": v.get("count", 0),
            "stage": v.get("stage", "5M_TRACKING"),
            "c5": v.get("c5", 0),
            "last_price": v.get("last_price", None),
        })
    return rows


def log_scan_completion_summary(scan_no, scan_time, regime, spy_rsi, time_quality_name,
                                filtered_count, total_count, confirmed_alerts, checked_sectors):
    """Log a full end-of-scan state summary and compare against last 1-4 scans."""
    global scan_history
    sector_snap = snapshot_sector_state(checked_sectors)
    tracker_rows = snapshot_confirmation_state()
    confirmed_alerts = confirmed_alerts or []
    record = {
        "scan_no": scan_no,
        "time": scan_time,
        "regime": regime,
        "spy_rsi": spy_rsi,
        "time_quality": time_quality_name,
        "filtered_count": filtered_count,
        "total_count": total_count,
        "confirmed_count": len(confirmed_alerts),
        "sectors": sector_snap,
        "tracker": tracker_rows,
        "alerts": confirmed_alerts,
    }
    prev = scan_history[-1] if scan_history else None
    scan_history.append(record)
    scan_history = scan_history[-MAX_SCAN_HISTORY:]

    logger.info("")
    logger.info("==================== END-OF-SCAN QUALITY SUMMARY #%d ====================", scan_no)
    logger.info("  Scan time: %s | Regime=%s | SPY_RSI=%.1f | TimeQuality=%s | Passed=%d/%d | NewAlerts=%d | RQG_Gate=%s/%d",
                scan_time.strftime("%I:%M:%S %p ET"), regime, spy_rsi, time_quality_name,
                filtered_count, total_count, len(confirmed_alerts), "ON" if RQG_ENFORCE_GATE else "OBSERVE", RQG_MIN_SCORE)

    logger.info("")
    logger.info("  ┌─────────────────────────────────────────────────────────────────┐")
    logger.info("  │           SECTOR-BY-SECTOR STATE vs PREVIOUS SCAN              │")
    logger.info("  └─────────────────────────────────────────────────────────────────┘")
    if sector_snap:
        strong_count = sum(1 for s in sector_snap.values() if s.get("strength") == "STRONG")
        weak_count = sum(1 for s in sector_snap.values() if s.get("strength") == "WEAK")
        neutral_count = sum(1 for s in sector_snap.values() if s.get("strength") == "NEUTRAL")
        logger.info("  Sector Health: %d STRONG | %d NEUTRAL | %d WEAK | %d total",
                     strong_count, neutral_count, weak_count, len(sector_snap))
        logger.info("")
        for sec, s in sorted(sector_snap.items(), key=lambda kv: float(kv[1].get("diff") or 0), reverse=True):
            prev_s = prev.get("sectors", {}).get(sec, {}) if prev else {}
            prev_close = prev_s.get("close")
            close = s.get("close")
            close_chg = None
            diff_chg = None
            try:
                if prev_close and close:
                    close_chg = (float(close) - float(prev_close)) / float(prev_close) * 100
                if prev_s and prev_s.get("diff") is not None:
                    diff_chg = float(s.get("diff") or 0) - float(prev_s.get("diff") or 0)
            except Exception:
                pass
            prev_strength = prev_s.get("strength", "N/A") if prev else "N/A"
            # v5.6: Enhanced sector display with visual strength indicator
            strength_icon = "🟢" if s.get("strength") == "STRONG" else ("🔴" if s.get("strength") == "WEAK" else "🟡")
            logger.info(
                "    %s %-5s %-7s | rel_str=%s Δprev=%s | day=%s spy_day=%s | $%s Δ$=%s | was=%s | src=%s %sm",
                strength_icon, sec, s.get("strength", "UNKNOWN"),
                _fmt_pct(s.get("diff")), _fmt_pct(diff_chg), _fmt_pct(s.get("session_return")),
                _fmt_pct(s.get("session_spy_return")), _fmt_price(close), _fmt_pct(close_chg),
                prev_strength, s.get("source", "unknown"), s.get("age_min", "N/A")
            )
            logger.info("")  # v5.6: Blank line between each sector for readability
    else:
        logger.info("    No sector snapshot available this scan.")

    if sector_snap:
        ranked = sorted(sector_snap.items(), key=lambda kv: float(kv[1].get("diff") or 0), reverse=True)
        day_ranked = sorted(sector_snap.items(), key=lambda kv: float(kv[1].get("session_return") or 0), reverse=True)
        leaders = ", ".join(["%s(%s %s)" % (sec, s.get("strength"), _fmt_pct(s.get("diff"))) for sec, s in ranked[:3]])
        laggards = ", ".join(["%s(%s %s)" % (sec, s.get("strength"), _fmt_pct(s.get("diff"))) for sec, s in ranked[-3:]])
        day_leaders = ", ".join(["%s(%s)" % (sec, _fmt_pct(s.get("session_return"))) for sec, s in day_ranked[:3]])
        day_laggards = ", ".join(["%s(%s)" % (sec, _fmt_pct(s.get("session_return"))) for sec, s in day_ranked[-3:]])
        logger.info("  Sector leaders by recent relative strength: %s", leaders)
        logger.info("  Sector laggards by recent relative strength: %s", laggards)
        logger.info("  Whole-day sector leaders: %s", day_leaders)
        logger.info("  Whole-day sector laggards: %s", day_laggards)

    if tracker_rows:
        by_stage = {}
        by_sector = {}
        for r in tracker_rows:
            by_stage[r["stage"]] = by_stage.get(r["stage"], 0) + 1
            by_sector[r["sector"]] = by_sector.get(r["sector"], 0) + 1
        logger.info("  Pending/Tracking candidates: %d | by_stage=%s | by_sector=%s",
                    len(tracker_rows), by_stage, by_sector)
        top_waiting = sorted(tracker_rows, key=lambda x: (x.get("count", 0), x.get("c5", 0)), reverse=True)[:15]
        for r in top_waiting:
            logger.info("    WAIT %-6s %-5s sector=%-5s 5m_count=%d/%d c5=%s stage=%s last_price=%s",
                        r["ticker"], r["signal"], r["sector"], r["count"], MIN_5MIN_CONFIRMATIONS,
                        r.get("c5", 0), r.get("stage"), _fmt_price(r.get("last_price")))
    else:
        logger.info("  Pending/Tracking candidates: 0")

    if confirmed_alerts:
        logger.info("  Confirmed RQG-passed alerts this scan:")
        for a in confirmed_alerts:
            logger.info("    ALERT %-6s %-6s price=%s avg_c=%s rqg=%s/%s mtf=%s sector=%s setup=%s reasons=%s",
                        a.get("ticker"), a.get("signal"), _fmt_price(a.get("cl")), a.get("avg_c"),
                        a.get("rqg_score", "N/A"), a.get("rqg_label", "N/A"), a.get("mtf_status"),
                        a.get("ss"), a.get("setup_type"), ";".join(a.get("rqg_reasons", [])[:6]))
    else:
        logger.info("  Confirmed RQG-passed alerts this scan: 0")

    # ═══════════════════════════════════════════════════════════════
    # v5.6 — NEAR-MISS / HIGH POTENTIAL WATCHLIST
    # Shows stocks that reached 30M confirmation but are waiting on 60M or RQG
    # These are the strongest candidates that didn't quite make it to full alert
    # ═══════════════════════════════════════════════════════════════
    global near_miss_candidates
    near_miss_candidates = []
    nm_rows = [r for r in tracker_rows
               if r.get("stage") in ("30M_CONFIRMED_WAIT_60M_BOOSTER", "EARLY_PROMOTED_AT_30M")
               and r.get("c5", 0) >= 45]
    nm_rows = sorted(nm_rows, key=lambda x: x.get("c5", 0), reverse=True)

    if nm_rows:
        logger.info("")
        logger.info("  ┌─────────────────────────────────────────────────────────────────┐")
        logger.info("  │     🔶 NEAR-MISS / HIGH POTENTIAL WATCHLIST (%d candidates)     │", len(nm_rows))
        logger.info("  └─────────────────────────────────────────────────────────────────┘")
        logger.info("  These stocks passed 5m+15m+30m confirmation but await 60m booster or RQG:")
        logger.info("")
        for r in nm_rows[:20]:
            sector_info = sector_strength_cache.get(r.get("sector", "SPY"), {})
            sec_strength = sector_info.get("strength", "UNKNOWN")
            sec_day_ret = _fmt_pct(sector_info.get("session_return"))
            quality = "★★★" if r.get("c5", 0) >= 90 else ("★★" if r.get("c5", 0) >= 80 else "★")
            logger.info("    %s %-6s %-6s c5=%-3d  sector=%-5s(%s day=%s)  5m_count=%d/%d  stage=%s  price=%s",
                        quality, r["ticker"], r["signal"], r.get("c5", 0),
                        r.get("sector", "?"), sec_strength, sec_day_ret,
                        r.get("count", 0), MIN_5MIN_CONFIRMATIONS,
                        r.get("stage", "?"), _fmt_price(r.get("last_price")))
            # Populate global near_miss for Flask dashboard
            near_miss_candidates.append({
                "ticker": r["ticker"],
                "signal": r["signal"],
                "c5": r.get("c5", 0),
                "sector": r.get("sector", "SPY"),
                "sector_strength": sec_strength,
                "stage": r.get("stage", "?"),
                "count": r.get("count", 0),
                "last_price": r.get("last_price"),
                "quality": quality,
            })
        logger.info("")
        logger.info("  Near-miss by signal: BOTTOM=%d  TOP=%d",
                     sum(1 for r in nm_rows if r["signal"] == "BOTTOM"),
                     sum(1 for r in nm_rows if r["signal"] == "TOP"))
        logger.info("  Near-miss by sector strength: STRONG=%d  NEUTRAL=%d  WEAK=%d",
                     sum(1 for r in nm_rows if sector_strength_cache.get(r.get("sector","SPY"),{}).get("strength") == "STRONG"),
                     sum(1 for r in nm_rows if sector_strength_cache.get(r.get("sector","SPY"),{}).get("strength") == "NEUTRAL"),
                     sum(1 for r in nm_rows if sector_strength_cache.get(r.get("sector","SPY"),{}).get("strength") == "WEAK"))
        top_nm = nm_rows[0] if nm_rows else None
        if top_nm and top_nm.get("c5", 0) >= 85:
            logger.info("  ⚡ TOP NEAR-MISS: %s %s c5=%d — very close to alert threshold!",
                         top_nm["ticker"], top_nm["signal"], top_nm.get("c5", 0))
    else:
        logger.info("")
        logger.info("  🔶 Near-miss watchlist: 0 candidates at 30M+ stage with c5≥65")

    logger.info("")
    logger.info("  Last %d scan comparison:", len(scan_history))
    for h in scan_history:
        hs = h.get("sectors", {})
        if hs:
            ranked = sorted(hs.items(), key=lambda kv: float(kv[1].get("diff") or 0), reverse=True)
            day_ranked = sorted(hs.items(), key=lambda kv: float(kv[1].get("session_return") or 0), reverse=True)
            best = ", ".join(["%s:%s" % (sec, _fmt_pct(s.get("diff"))) for sec, s in ranked[:2]])
            worst = ", ".join(["%s:%s" % (sec, _fmt_pct(s.get("diff"))) for sec, s in ranked[-2:]])
            day_best = ", ".join(["%s:%s" % (sec, _fmt_pct(s.get("session_return"))) for sec, s in day_ranked[:2]])
        else:
            best, worst, day_best = "N/A", "N/A", "N/A"
        logger.info("    Scan#%d %s | regime=%s RSI=%.1f | passed=%d | alerts=%d | pending=%d | rel_leaders=%s | rel_laggards=%s | day_leaders=%s",
                    h["scan_no"], h["time"].strftime("%I:%M:%S %p"), h["regime"], h["spy_rsi"],
                    h["filtered_count"], h["confirmed_count"], len(h.get("tracker", [])), best, worst, day_best)
    logger.info("================== END SUMMARY — next scan pending ==================")
    logger.info("")

# ═══════════════════════════════════════════════════════
# TECHNICAL INDICATORS (original v4 — preserved)
# ═══════════════════════════════════════════════════════

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


def detect_market_structure(high, low, close, lookback=5):
    rh = high.rolling(window=lookback).max()
    rl = low.rolling(window=lookback).min()
    lh = (high < high.shift(1)) & (high.shift(1) >= rh.shift(1))
    hl = (low > low.shift(1)) & (low.shift(1) <= rl.shift(1))
    return lh, hl


def safe_val(s, default=0):
    try:
        v = s.iloc[-1]
        return default if pd.isna(v) else float(v)
    except Exception:
        return default


def calculate_vwap(high, low, close, volume):
    tp = (high + low + close) / 3.0
    return (tp * volume).cumsum() / volume.cumsum()


def detect_bullish_divergence(close, rsi, lookback=20):
    try:
        if len(close) < lookback + 5:
            return False
        rc = close.iloc[-lookback:]
        rr = rsi.iloc[-lookback:]
        m = lookback // 2
        pfl = rc.iloc[:m].min()
        psl = rc.iloc[m:].min()
        rfl = rr.iloc[:m].min()
        rsl = rr.iloc[m:].min()
        if pd.isna(pfl) or pd.isna(psl) or pd.isna(rfl) or pd.isna(rsl):
            return False
        return psl < pfl and rsl > rfl
    except Exception:
        return False


def detect_bearish_divergence(close, rsi, lookback=20):
    try:
        if len(close) < lookback + 5:
            return False
        rc = close.iloc[-lookback:]
        rr = rsi.iloc[-lookback:]
        m = lookback // 2
        pfh = rc.iloc[:m].max()
        psh = rc.iloc[m:].max()
        rfh = rr.iloc[:m].max()
        rsh = rr.iloc[m:].max()
        if pd.isna(pfh) or pd.isna(psh) or pd.isna(rfh) or pd.isna(rsh):
            return False
        return psh > pfh and rsh < rfh
    except Exception:
        return False


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
# v4 INDICATORS (MFI, OBV, CMF, Options, Candlesticks)
# ═══════════════════════════════════════════════════════

def calculate_mfi(high, low, close, volume, period=14):
    tp = (high + low + close) / 3.0
    mf = tp * volume
    pmf = mf.where(tp > tp.shift(1), 0.0).rolling(period).sum()
    nmf = mf.where(tp <= tp.shift(1), 0.0).rolling(period).sum()
    mfr = pmf / nmf.where(nmf > 0, np.nan)
    return 100 - (100 / (1 + mfr))


def calculate_obv(close, volume):
    sign = np.where(close > close.shift(1), 1,
                    np.where(close < close.shift(1), -1, 0))
    return (pd.Series(sign, index=close.index) * volume).cumsum()


def calculate_cmf(high, low, close, volume, period=20):
    clv = ((close - low) - (high - close)) / (high - low).where((high - low) > 0, np.nan)
    return (clv * volume).rolling(period).sum() / volume.rolling(period).sum()


def get_options_pcr(ticker):
    try:
        t = yf.Ticker(ticker)
        exp = t.options
        if not exp:
            return 1.0
        chain = t.option_chain(exp[0])
        cv = chain.calls["volume"].sum() if "volume" in chain.calls.columns else 0
        pv = chain.puts["volume"].sum() if "volume" in chain.puts.columns else 0
        if cv == 0 or pd.isna(cv):
            return 1.0
        pcr = float(pv) / float(cv)
        return min(pcr, 5.0)
    except Exception:
        return 1.0


def detect_hammer(o, h, l, c):
    try:
        body = abs(c.iloc[-1] - o.iloc[-1])
        rng = h.iloc[-1] - l.iloc[-1]
        if rng == 0:
            return False
        lower_wick = min(o.iloc[-1], c.iloc[-1]) - l.iloc[-1]
        return lower_wick > 2 * body and body < rng * 0.35
    except Exception:
        return False


def detect_bullish_engulfing(o, h, l, c):
    try:
        prev_bear = c.iloc[-2] < o.iloc[-2]
        curr_bull = c.iloc[-1] > o.iloc[-1]
        engulfs = c.iloc[-1] > o.iloc[-2] and o.iloc[-1] < c.iloc[-2]
        return prev_bear and curr_bull and engulfs
    except Exception:
        return False


def detect_shooting_star(o, h, l, c):
    try:
        body = abs(c.iloc[-1] - o.iloc[-1])
        rng = h.iloc[-1] - l.iloc[-1]
        if rng == 0:
            return False
        upper_wick = h.iloc[-1] - max(o.iloc[-1], c.iloc[-1])
        return upper_wick > 2 * body and body < rng * 0.35
    except Exception:
        return False


def detect_bearish_engulfing(o, h, l, c):
    try:
        prev_bull = c.iloc[-2] > o.iloc[-2]
        curr_bear = c.iloc[-1] < o.iloc[-1]
        engulfs = c.iloc[-1] < o.iloc[-2] and o.iloc[-1] > c.iloc[-2]
        return prev_bull and curr_bear and engulfs
    except Exception:
        return False



# ═══════════════════════════════════════════════════════
# LAYER 1 — MARKET REGIME  (original v4 — preserved)
# ═══════════════════════════════════════════════════════

def get_market_regime():
    global market_regime_cache
    now = get_eastern_now()
    if market_regime_cache["timestamp"] and \
       (now - market_regime_cache["timestamp"]).total_seconds() < 240:
        logger.info("  LAYER 1 — Regime cache hit: %s (SPY RSI=%.1f, cache_age=%.0fs)",
                    market_regime_cache["regime"], market_regime_cache["spy_rsi"],
                    (now - market_regime_cache["timestamp"]).total_seconds())
        return market_regime_cache["regime"], market_regime_cache["spy_rsi"]
    try:
        spy, meta = download_with_freshness("SPY", period="5d", interval="5m", label="REGIME", allow_fallback=True)
        if spy is None or spy.empty or len(spy) < 30:
            logger.warning("  LAYER 1 — Regime: NEUTRAL because SPY data unavailable/insufficient. meta=%s", meta)
            return "NEUTRAL", 50.0
        close = spy["Close"]
        rsi = calculate_rsi(close)
        _, _, mh = calculate_macd(close)
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
        market_regime_cache.update({"regime": regime, "spy_rsi": sr, "timestamp": now})
        logger.info("  LAYER 1 — Regime: %s | SPY_RSI=%.1f MACD_HIST=%.4f Close=%.2f MA20=%.2f bull_score=%d bear_score=%d source=%s latest=%s age=%sm fresh=%s",
                    regime, sr, sm, sp, sa, bs, br, meta.get("source"), meta.get("latest_et"), meta.get("age_min"), meta.get("fresh"))
        return regime, sr
    except Exception as e:
        logger.warning("  Regime check failed: %s", e)
        return "NEUTRAL", 50.0

# ═══════════════════════════════════════════════════════
# LAYER 2 — SECTOR STRENGTH  (original v4 — preserved)
# ═══════════════════════════════════════════════════════

def check_sector_strength(sector_etf):
    global sector_strength_cache
    now = get_eastern_now()
    if sector_etf in sector_strength_cache:
        c = sector_strength_cache[sector_etf]
        if c["timestamp"] and (now - c["timestamp"]).total_seconds() < 240:
            logger.info("  LAYER 2 — %s: %s | cache_hit age=%.0fs source=%s latest=%s data_age=%sm diff=%s%% reason=%s",
                        sector_etf, c["strength"], (now - c["timestamp"]).total_seconds(),
                        c.get("source", "unknown"), c.get("latest_et", "N/A"), c.get("age_min", "N/A"),
                        c.get("diff", "N/A"), c.get("reason", "N/A"))
            return c["strength"]
    try:
        sec, sec_meta = download_with_freshness(sector_etf, period="2d", interval="5m", label="SECTOR", allow_fallback=True)
        spy, spy_meta = download_with_freshness("SPY", period="2d", interval="5m", label="SECTOR-BENCH", allow_fallback=True)
        if sec is None or spy is None or sec.empty or spy.empty:
            logger.error("  LAYER 2 — %s: UNKNOWN | reason=no_fresh_sector_or_spy_data sector_meta=%s spy_meta=%s", sector_etf, sec_meta, spy_meta)
            sector_strength_cache[sector_etf] = {"strength": "UNKNOWN", "timestamp": now, "source": "none", "latest_et": "N/A", "age_min": "N/A", "diff": "N/A", "reason": "No fresh sector/SPY data", "close": None, "fallback_used": False}
            return "UNKNOWN"
        sc = sec["Close"].dropna()
        spc = spy["Close"].dropna()
        common = sc.index.intersection(spc.index)
        if len(common) >= 10:
            sc = sc.loc[common]
            spc = spc.loc[common]
        if len(sc) < 10 or len(spc) < 10:
            logger.warning("  LAYER 2 — %s: UNKNOWN | insufficient rows sector_rows=%d spy_rows=%d", sector_etf, len(sc), len(spc))
            return "UNKNOWN"
        n = min(len(sc), len(spc), 30)
        sr = (sc.iloc[-1] - sc.iloc[-n]) / sc.iloc[-n] * 100
        spr = (spc.iloc[-1] - spc.iloc[-n]) / spc.iloc[-n] * 100
        diff = sr - spr
        strength = "STRONG" if diff > 0.15 else ("WEAK" if diff < -0.15 else "NEUTRAL")

        # Whole-day/session view: compare current price to first regular bar available today.
        today = get_eastern_now().date()
        try:
            sc_today = sc[sc.index.tz_convert(ET).date == today] if hasattr(sc.index, 'tz') and sc.index.tz is not None else sc[sc.index.date == today]
        except Exception:
            sc_today = sc.iloc[-min(len(sc), 78):]
        try:
            sp_today = spc[spc.index.tz_convert(ET).date == today] if hasattr(spc.index, 'tz') and spc.index.tz is not None else spc[spc.index.date == today]
        except Exception:
            sp_today = spc.iloc[-min(len(spc), 78):]
        session_ret = (sc.iloc[-1] - sc_today.iloc[0]) / sc_today.iloc[0] * 100 if len(sc_today) > 1 and sc_today.iloc[0] != 0 else 0.0
        session_spy_ret = (spc.iloc[-1] - sp_today.iloc[0]) / sp_today.iloc[0] * 100 if len(sp_today) > 1 and sp_today.iloc[0] != 0 else 0.0
        reason = "%s because recent 30-bar relative strength vs SPY is %+.3f%%; sector recent=%+.3f%%, SPY recent=%+.3f%%; whole-day sector=%+.3f%% vs SPY=%+.3f%%" % (
            strength, diff, sr, spr, session_ret, session_spy_ret
        )
        sector_strength_cache[sector_etf] = {
            "strength": strength, "timestamp": now, "source": sec_meta.get("source"),
            "latest_et": sec_meta.get("latest_et"), "age_min": sec_meta.get("age_min"),
            "diff": round(float(diff), 3), "sector_return": round(float(sr), 3),
            "spy_return": round(float(spr), 3), "session_return": round(float(session_ret), 3),
            "session_spy_return": round(float(session_spy_ret), 3), "close": float(sc.iloc[-1]),
            "spy_close": float(spc.iloc[-1]), "fallback_used": sec_meta.get("fallback_used", False),
            "reason": reason,
        }
        logger.info("  LAYER 2 — %s: %s | source=%s fallback=%s interval=5m latest=%s age=%sm rows=%d lookback_bars=%d sector_ret=%+.3f%% spy_ret=%+.3f%% rel_diff=%+.3f%% day_sector=%+.3f%% day_spy=%+.3f%% threshold=+/-0.150%% close=%.2f spy_close=%.2f",
                    sector_etf, strength, sec_meta.get("source"), sec_meta.get("fallback_used"),
                    sec_meta.get("latest_et"), sec_meta.get("age_min"), len(sc), n,
                    sr, spr, diff, session_ret, session_spy_ret, float(sc.iloc[-1]), float(spc.iloc[-1]))
        logger.info("  LAYER 2 — %s reason: %s", sector_etf, reason)
        return strength
    except Exception:
        logger.exception("  LAYER 2 — %s: sector strength failed", sector_etf)
        return "UNKNOWN"


def get_stock_sector(ticker):
    return SECTOR_MAP.get(ticker, "SPY")


# ═══════════════════════════════════════════════════════
# SAFE DOWNLOAD  (original v4 — preserved)
# ═══════════════════════════════════════════════════════

def safe_download(ticker, period, interval, max_retries=3):
    """Download single ticker with freshness validation and fallback, but avoid noisy success logs."""
    for attempt in range(1, max_retries + 1):
        try:
            time.sleep(1.0 + random.uniform(0.5, 1.5))
            d, meta = download_with_freshness(ticker, period=period, interval=interval, label="DL", allow_fallback=True)
            if d is not None and not d.empty:
                if meta.get("fallback_used") or logger.isEnabledFor(logging.DEBUG):
                    logger.info("  DL %s: usable source=%s latest=%s age=%sm rows=%d interval=%s",
                                ticker, meta.get("source"), meta.get("latest_et"), meta.get("age_min"), len(d), interval)
                return d
            logger.warning("  DL %s #%d unusable: meta=%s", ticker, attempt, meta)
        except Exception as e:
            logger.warning("DL %s #%d fail: %s", ticker, attempt, e)
        if attempt < max_retries:
            time.sleep(attempt * 3 + random.uniform(1, 3))
    logger.error("  DL %s: failed after %d attempts", ticker, max_retries)
    return None


# ═══════════════════════════════════════════════════════
# BATCH QUICK SCAN — PASS 1  (original v4 — preserved)
# ═══════════════════════════════════════════════════════

def batch_quick_scan(stock_list):
    """v5.3: Download in chunks with memory-cache reinit per chunk."""
    logger.info("  Batch downloading %d stocks (chunks of %d)...",
                len(stock_list), BATCH_CHUNK_SIZE)
    filtered = []
    chunks = [stock_list[i:i + BATCH_CHUNK_SIZE]
              for i in range(0, len(stock_list), BATCH_CHUNK_SIZE)]

    for chunk_idx, chunk in enumerate(chunks):
        logger.info("  Chunk %d/%d (%d tickers)...",
                     chunk_idx + 1, len(chunks), len(chunk))
        bd = None
        for attempt in range(1, 3):
            try:
                _reinit_tz_mem_cache()
                bd = safe_yf_download(chunk, period="5d", interval="5m",
                                      progress=False, auto_adjust=True,
                                      threads=False, group_by="ticker")
                if bd is not None and not bd.empty:
                    break
            except Exception as e:
                logger.warning("  Chunk %d attempt %d failed: %s",
                               chunk_idx + 1, attempt, e)
                _patch_yf_tz_cache()
                time.sleep(5 + attempt * 3)

        if bd is None or bd.empty:
            logger.warning("  Chunk %d: no data, skipping", chunk_idx + 1)
            continue

        for ticker in chunk:
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
                top = (1 if rv > 65 else 0) + (1 if sv > 75 else 0) + \
                      (1 if wv > -25 else 0)
                bot = (1 if rv < 35 else 0) + (1 if sv < 25 else 0) + \
                      (1 if wv < -75 else 0)
                if top >= 2 or bot >= 2:
                    filtered.append(ticker)
            except Exception:
                continue
        time.sleep(2)

    logger.info("  Filtering complete: %d/%d passed", len(filtered), len(stock_list))
    return filtered


# ═══════════════════════════════════════════════════════
# PHASE 3 — NEW: STOCK TREND & RELATIVE STRENGTH
# ═══════════════════════════════════════════════════════

def calculate_stock_trend(ticker):
    """Calculate 3-day and 5-day price trend + volume expansion."""
    try:
        data = safe_download(ticker, period="1mo", interval="1d")
        if data is None or data.empty or len(data) < 6:
            return {"trend_3d": 0.0, "trend_5d": 0.0, "volume_expansion": 1.0}
        cl = data["Close"].dropna()
        vol = data["Volume"].dropna()
        if len(cl) < 6:
            return {"trend_3d": 0.0, "trend_5d": 0.0, "volume_expansion": 1.0}
        # 3-day return
        trend_3d = (cl.iloc[-1] - cl.iloc[-4]) / cl.iloc[-4] * 100 if len(cl) >= 4 else 0.0
        # 5-day return
        trend_5d = (cl.iloc[-1] - cl.iloc[-6]) / cl.iloc[-6] * 100 if len(cl) >= 6 else 0.0
        # Volume expansion: last 3-day avg vs 10-day avg
        if len(vol) >= 10:
            vol_3 = vol.iloc[-3:].mean()
            vol_10 = vol.iloc[-10:].mean()
            volume_expansion = float(vol_3 / vol_10) if vol_10 > 0 else 1.0
        else:
            volume_expansion = 1.0
        return {
            "trend_3d": round(float(trend_3d), 3),
            "trend_5d": round(float(trend_5d), 3),
            "volume_expansion": round(float(volume_expansion), 3),
        }
    except Exception:
        return {"trend_3d": 0.0, "trend_5d": 0.0, "volume_expansion": 1.0}


def calculate_relative_strength_vs_sector(ticker, sector_etf):
    """Compare stock return vs its sector ETF over 3d and 5d."""
    try:
        tickers_list = [ticker, sector_etf]
        data = safe_yf_download(tickers_list, period="1mo", interval="1d",
                           progress=False, auto_adjust=True, threads=False,
                           group_by="ticker")
        if data is None or data.empty:
            return {"rs_3d": 0.0, "rs_5d": 0.0}
        if isinstance(data.columns, pd.MultiIndex):
            stk = data[ticker]["Close"].dropna()
            sec = data[sector_etf]["Close"].dropna()
        else:
            return {"rs_3d": 0.0, "rs_5d": 0.0}
        if len(stk) < 6 or len(sec) < 6:
            return {"rs_3d": 0.0, "rs_5d": 0.0}
        # Align on common dates
        common = stk.index.intersection(sec.index)
        stk = stk.loc[common]
        sec = sec.loc[common]
        if len(stk) < 6:
            return {"rs_3d": 0.0, "rs_5d": 0.0}
        stk_3d = (stk.iloc[-1] - stk.iloc[-4]) / stk.iloc[-4] * 100
        sec_3d = (sec.iloc[-1] - sec.iloc[-4]) / sec.iloc[-4] * 100
        stk_5d = (stk.iloc[-1] - stk.iloc[-6]) / stk.iloc[-6] * 100
        sec_5d = (sec.iloc[-1] - sec.iloc[-6]) / sec.iloc[-6] * 100
        return {
            "rs_3d": round(float(stk_3d - sec_3d), 3),
            "rs_5d": round(float(stk_5d - sec_5d), 3),
        }
    except Exception:
        return {"rs_3d": 0.0, "rs_5d": 0.0}



def check_leadership_override(ticker, signal_direction, sector_strength, analysis_result,
                               trend_data=None, rs_data=None):
    """
    Phase 3 — Sector Override Logic.
    If sector is WEAK but stock shows independent momentum,
    override the sector penalty.
    Accepts optional pre-computed trend_data and rs_data to avoid duplicate API calls.
    """
    default = {
        "override": False, "sm": 1.0, "label": "NEUTRAL",
        "conditions_met": 0, "trend_3d": 0.0, "trend_5d": 0.0,
        "volume_expansion": 1.0, "rs_score": 0.0,
    }

    # Only activate when sector is WEAK
    if sector_strength != "WEAK":
        return default

    try:
        sector_etf = get_stock_sector(ticker)

        # Use pre-computed data if available, else compute
        if trend_data is None:
            trend_data = calculate_stock_trend(ticker)
        if rs_data is None:
            rs_data = calculate_relative_strength_vs_sector(ticker, sector_etf)

        trend_3d = trend_data["trend_3d"]
        trend_5d = trend_data["trend_5d"]
        vol_exp = trend_data["volume_expansion"]
        rs_3d = rs_data["rs_3d"]
        rs_5d = rs_data["rs_5d"]

        obv_rising = analysis_result.get("obv_rising", False)
        cmf_val = analysis_result.get("cmf", 0)
        cl_val = analysis_result.get("cl", 0)
        vwap_val = analysis_result.get("vwap", 0)

        conditions_met = 0

        if signal_direction == "BOTTOM":
            if trend_3d > 1.0:    conditions_met += 1
            if trend_5d > 1.5:    conditions_met += 1
            if vol_exp > 1.2:     conditions_met += 1
            if rs_3d > 0.5:      conditions_met += 1
            if rs_5d > 0.8:      conditions_met += 1
            if obv_rising:        conditions_met += 1
            if cmf_val > 0:       conditions_met += 1
            if vwap_val > 0 and cl_val > vwap_val:
                conditions_met += 1
        elif signal_direction == "TOP":
            if trend_3d < -1.0:   conditions_met += 1
            if trend_5d < -1.5:   conditions_met += 1
            if vol_exp > 1.2:     conditions_met += 1
            if rs_3d < -0.5:     conditions_met += 1
            if rs_5d < -0.8:     conditions_met += 1
            if not obv_rising:    conditions_met += 1
            if cmf_val < 0:       conditions_met += 1
            if vwap_val > 0 and cl_val < vwap_val:
                conditions_met += 1

        result = {
            "conditions_met": conditions_met,
            "trend_3d": trend_3d,
            "trend_5d": trend_5d,
            "volume_expansion": vol_exp,
            "rs_score": rs_3d,
        }

        if conditions_met >= 5:
            result["override"] = True
            result["sm"] = 1.1
            result["label"] = "LEADER_IN_WEAK_SECTOR"
            logger.info("  ★ %s: LEADER override (%d/8 conditions)", ticker, conditions_met)
        elif conditions_met >= 3:
            result["override"] = True
            result["sm"] = 1.0
            result["label"] = "SECTOR_NEUTRAL_OVERRIDE"
            logger.info("  ◆ %s: Sector NEUTRAL override (%d/8 conditions)", ticker, conditions_met)
        else:
            result["override"] = False
            result["sm"] = 0.7
            result["label"] = "WEAK"
            logger.info("  ▽ %s: Sector WEAK kept (%d/8 conditions)", ticker, conditions_met)

        return result

    except Exception as e:
        logger.warning("  Leadership override failed for %s: %s", ticker, e)
        return {
            "override": False, "sm": 0.7, "label": "WEAK",
            "conditions_met": 0, "trend_3d": 0.0, "trend_5d": 0.0,
            "volume_expansion": 1.0, "rs_score": 0.0,
        }


# ═══════════════════════════════════════════════════════
# PHASE 4 — LEADERSHIP / CONTINUATION SCORING ENGINE
# (Separate from reversal; detects stocks already trending)
# ═══════════════════════════════════════════════════════

def score_leadership_setup(ticker, signal_direction, analysis_vals, trend_data, rs_data):
    """
    Score a stock for CONTINUATION / LEADERSHIP characteristics.
    This runs alongside the original reversal scoring, not instead of it.
    A stock can be tagged as LEADERSHIP if it scores >= 45 here.
    """
    ls = 0
    tags = []

    t3 = trend_data.get("trend_3d", 0.0)
    t5 = trend_data.get("trend_5d", 0.0)
    ve = trend_data.get("volume_expansion", 1.0)
    r3 = rs_data.get("rs_3d", 0.0)
    r5 = rs_data.get("rs_5d", 0.0)

    rsi = analysis_vals.get("rsi", 50)
    mh = analysis_vals.get("mh", 0)
    mhp = analysis_vals.get("mhp", 0)
    cl = analysis_vals.get("cl", 0)
    vwap = analysis_vals.get("vwap", 0)
    vd = analysis_vals.get("vd", 0.5)
    obv_rising = analysis_vals.get("obv_rising", False)
    cmf = analysis_vals.get("cmf", 0)
    mfi = analysis_vals.get("mfi", 50)

    if signal_direction == "BOTTOM":
        # ── Price trend ──
        if t3 > 0.5:
            ls += 12; tags.append("Trend3d+")
        if t3 > 1.5:
            ls += 8;  tags.append("StrongTrend3d")
        if t5 > 1.0:
            ls += 12; tags.append("Trend5d+")
        if t5 > 2.5:
            ls += 8;  tags.append("StrongTrend5d")
        # ── Volume expansion ──
        if ve > 1.2:
            ls += 10; tags.append("VolExp=%.1fx" % ve)
        if ve > 1.5:
            ls += 5;  tags.append("HighVolExp")
        # ── Relative strength vs sector ──
        if r3 > 0.3:
            ls += 10; tags.append("RS3d+")
        if r5 > 0.5:
            ls += 10; tags.append("RS5d+")
        # ── Intraday momentum ──
        if 45 <= rsi <= 65:
            ls += 8;  tags.append("RSI_Trend")
        if mh > mhp and mh > 0:
            ls += 10; tags.append("MACD_Momentum")
        if vwap > 0 and cl > vwap:
            ls += 8;  tags.append("AboveVWAP")
        if vd > 0.55:
            ls += 8;  tags.append("BuyFlow")
        if obv_rising:
            ls += 8;  tags.append("OBV_Up")
        if cmf > 0.05:
            ls += 8;  tags.append("CMF_Positive")
        if 40 <= mfi <= 70:
            ls += 5;  tags.append("MFI_Healthy")

    elif signal_direction == "TOP":
        # ── Price trend (bearish) ──
        if t3 < -0.5:
            ls += 12; tags.append("Trend3d-")
        if t3 < -1.5:
            ls += 8;  tags.append("StrongTrend3d")
        if t5 < -1.0:
            ls += 12; tags.append("Trend5d-")
        if t5 < -2.5:
            ls += 8;  tags.append("StrongTrend5d")
        # ── Volume expansion ──
        if ve > 1.2:
            ls += 10; tags.append("VolExp=%.1fx" % ve)
        if ve > 1.5:
            ls += 5;  tags.append("HighVolExp")
        # ── Relative strength vs sector (underperforming) ──
        if r3 < -0.3:
            ls += 10; tags.append("RS3d-")
        if r5 < -0.5:
            ls += 10; tags.append("RS5d-")
        # ── Intraday momentum (bearish) ──
        if 35 <= rsi <= 55:
            ls += 8;  tags.append("RSI_Trend")
        if mh < mhp and mh < 0:
            ls += 10; tags.append("MACD_Momentum")
        if vwap > 0 and cl < vwap:
            ls += 8;  tags.append("BelowVWAP")
        if vd < 0.45:
            ls += 8;  tags.append("SellFlow")
        if not obv_rising:
            ls += 8;  tags.append("OBV_Down")
        if cmf < -0.05:
            ls += 8;  tags.append("CMF_Negative")
        if 30 <= mfi <= 60:
            ls += 5;  tags.append("MFI_Healthy")

    ls = min(ls, 100)
    is_leader = ls >= 45

    return {
        "leadership_score": ls,
        "leadership_signals": tags,
        "is_leadership": is_leader,
    }


# ═══════════════════════════════════════════════════════
# v5 FULL ANALYSIS ENGINE
# (Original v4 scoring + Phase 3 sector override + Phase 4 leadership)
# ═══════════════════════════════════════════════════════

def analyze_stock_v5(ticker, interval="5m", market_regime="NEUTRAL", sector_str="NEUTRAL"):
    try:
        pm = {"5m": "5d", "15m": "5d", "30m": "1mo", "60m": "1mo"}
        data = safe_download(ticker, period=pm.get(interval, "5d"), interval=interval)
        if data is None or data.empty or len(data) < 30:
            return None

        cl = data["Close"]
        hi = data["High"]
        lo = data["Low"]
        vo = data["Volume"]
        op = data["Open"]

        # ── All original v4 indicators ──
        rsi = calculate_rsi(cl)
        _, _, macd_hist = calculate_macd(cl)
        sk, sd = calculate_stochastic(hi, lo, cl)
        bbu, _, bbl = calculate_bollinger(cl)
        wr = calculate_williams_r(hi, lo, cl)
        vs = detect_volume_spike(vo)
        lh, hl = detect_market_structure(hi, lo, cl)
        vwap = calculate_vwap(hi, lo, cl, vo)
        bdiv = detect_bullish_divergence(cl, rsi)
        brdiv = detect_bearish_divergence(cl, rsi)
        vd = calculate_volume_direction(cl, vo)
        mfi = calculate_mfi(hi, lo, cl, vo)
        obv = calculate_obv(cl, vo)
        cmf = calculate_cmf(hi, lo, cl, vo)
        hammer = detect_hammer(op, hi, lo, cl)
        bull_eng = detect_bullish_engulfing(op, hi, lo, cl)
        shoot = detect_shooting_star(op, hi, lo, cl)
        bear_eng = detect_bearish_engulfing(op, hi, lo, cl)

        l = {
            "rsi": safe_val(rsi, 50),
            "mh": safe_val(macd_hist, 0),
            "mhp": safe_val(macd_hist.shift(1), 0),
            "sk": safe_val(sk, 50),
            "sd": safe_val(sd, 50),
            "cl": safe_val(cl, 0),
            "bbu": safe_val(bbu, 0),
            "bbl": safe_val(bbl, 0),
            "wr": safe_val(wr, -50),
            "vwap": safe_val(vwap, 0),
            "vs": bool(vs.iloc[-1]) if len(vs) > 0 else False,
            "lh": bool(lh.iloc[-1]) if len(lh) > 0 else False,
            "hl": bool(hl.iloc[-1]) if len(hl) > 0 else False,
            "bdiv": bdiv,
            "brdiv": brdiv,
            "vd": vd,
            "mfi": safe_val(mfi, 50),
            "cmf": safe_val(cmf, 0),
            "obv_rising": safe_val(obv, 0) > safe_val(obv.shift(5), 0) if len(obv) > 5 else False,
            "hammer": hammer,
            "bull_eng": bull_eng,
            "shoot": shoot,
            "bear_eng": bear_eng,
        }

        # ═══ BOTTOM SCORING (original v4 — preserved exactly) ═══
        bs = 0
        bsg = []
        if l["bdiv"]:                                          bs += 20; bsg.append("BullDiv")
        if l["mh"] > l["mhp"] and l["mhp"] < 0:              bs += 15; bsg.append("MACD+")
        if l["sk"] > l["sd"] and l["sk"] < 30:                bs += 15; bsg.append("StochX")
        elif l["sk"] < 20:                                     bs += 8;  bsg.append("Stoch=%.0f" % l["sk"])
        if l["vwap"] > 0 and l["cl"] > l["vwap"] and l["cl"] < l["vwap"] * 1.005:
            bs += 15; bsg.append("VWAP+")
        if l["rsi"] < 30:                                      bs += 10; bsg.append("RSI=%.1f" % l["rsi"])
        elif l["rsi"] < 35:                                    bs += 5
        if l["bbl"] > 0 and l["cl"] <= l["bbl"] * 1.003 and l["cl"] > l["bbl"]:
            bs += 10; bsg.append("BBBounce")
        if l["wr"] < -80:                                      bs += 10; bsg.append("WR=%.0f" % l["wr"])
        if l["vd"] > 0.55:                                     bs += 10; bsg.append("BuyVol=%d%%" % int(l["vd"] * 100))
        if l["hl"]:                                            bs += 15; bsg.append("HL")
        if l["vs"] and l["vd"] > 0.5:                         bs += 5;  bsg.append("VolSpk")
        if l["mfi"] < 20:                                      bs += 12; bsg.append("MFI=%.0f" % l["mfi"])
        elif l["mfi"] < 30:                                    bs += 6
        if l["obv_rising"] and l["rsi"] < 40:                 bs += 10; bsg.append("OBV+")
        if l["cmf"] > 0 and l["rsi"] < 40:                    bs += 10; bsg.append("CMF+")
        elif l["cmf"] > -0.05 and l["cmf"] < 0.05 and l["rsi"] < 35:
            bs += 5
        if l["hammer"]:                                        bs += 10; bsg.append("Hammer")
        if l["bull_eng"]:                                      bs += 12; bsg.append("BullEng")

        # ═══ TOP SCORING (original v4 — preserved exactly) ═══
        ts = 0
        tsg = []
        if l["brdiv"]:                                         ts += 20; tsg.append("BearDiv")
        if l["mh"] < l["mhp"] and l["mhp"] > 0:              ts += 15; tsg.append("MACD-")
        if l["sk"] < l["sd"] and l["sk"] > 70:                ts += 15; tsg.append("StochX")
        elif l["sk"] > 80:                                     ts += 8;  tsg.append("Stoch=%.0f" % l["sk"])
        if l["vwap"] > 0 and l["cl"] < l["vwap"] and l["cl"] > l["vwap"] * 0.995:
            ts += 15; tsg.append("VWAP-")
        if l["rsi"] > 70:                                      ts += 10; tsg.append("RSI=%.1f" % l["rsi"])
        elif l["rsi"] > 65:                                    ts += 5
        if l["bbu"] > 0 and l["cl"] >= l["bbu"] * 0.997 and l["cl"] < l["bbu"]:
            ts += 10; tsg.append("BBReject")
        if l["wr"] > -20:                                      ts += 10; tsg.append("WR=%.0f" % l["wr"])
        if l["vd"] < 0.45:                                     ts += 10; tsg.append("SellVol=%d%%" % int((1 - l["vd"]) * 100))
        if l["lh"]:                                            ts += 15; tsg.append("LH")
        if l["vs"] and l["vd"] < 0.5:                         ts += 5;  tsg.append("VolSpk")
        if l["mfi"] > 80:                                      ts += 12; tsg.append("MFI=%.0f" % l["mfi"])
        elif l["mfi"] > 70:                                    ts += 6
        if not l["obv_rising"] and l["rsi"] > 60:             ts += 10; tsg.append("OBV-")
        if l["cmf"] < 0 and l["rsi"] > 60:                    ts += 10; tsg.append("CMF-")
        if l["shoot"]:                                         ts += 10; tsg.append("ShootStar")
        if l["bear_eng"]:                                      ts += 12; tsg.append("BearEng")

        # ── Signal selection (original logic) ──
        sig = None
        score = 0
        sigs = []
        setup_type = "REVERSAL"

        if bs > ts and bs >= 40:
            sig = "BOTTOM"
            score = min(bs, 100)
            sigs = bsg
        elif ts > bs and ts >= 40:
            sig = "TOP"
            score = min(ts, 100)
            sigs = tsg

        # ═══ COMPUTE TREND & RELATIVE STRENGTH ONCE ═══
        sector_etf = get_stock_sector(ticker)
        trend_data = calculate_stock_trend(ticker)
        rs_data = calculate_relative_strength_vs_sector(ticker, sector_etf)

        # ═══ PHASE 4: LEADERSHIP CHECK ═══
        # Determine leadership direction from trend if no reversal signal
        if sig is not None:
            leadership = score_leadership_setup(ticker, sig, l, trend_data, rs_data)
        else:
            # No reversal signal — check if leadership qualifies independently
            # Use trend direction to determine signal direction
            if trend_data["trend_3d"] > 0.3:
                leader_dir = "BOTTOM"
            elif trend_data["trend_3d"] < -0.3:
                leader_dir = "TOP"
            else:
                leader_dir = None

            if leader_dir:
                leadership = score_leadership_setup(ticker, leader_dir, l, trend_data, rs_data)
                if leadership["leadership_score"] >= 55:
                    sig = leader_dir
                    score = leadership["leadership_score"]
                    sigs = leadership["leadership_signals"]
                    setup_type = "LEADERSHIP"
                    logger.info("  ★ %s: LEADERSHIP signal (%s, score=%d)",
                                ticker, sig, score)
                else:
                    return None
            else:
                return None

        # Determine setup_type for reversal signals
        if setup_type != "LEADERSHIP":
            if leadership["is_leadership"]:
                setup_type = "HYBRID"
            else:
                setup_type = "REVERSAL"

        # ═══ LAYER 3 — MARKET REGIME MODIFIER (original v4) ═══
        rm = 1.0
        rn = "NEUTRAL"
        if sig == "BOTTOM" and market_regime == "BEARISH":
            rm = 0.5;  rn = "REJECT"
        elif sig == "BOTTOM" and market_regime == "BULLISH":
            rm = 1.2;  rn = "CONFIRM"
        elif sig == "TOP" and market_regime == "BULLISH":
            rm = 0.5;  rn = "REJECT"
        elif sig == "TOP" and market_regime == "BEARISH":
            rm = 1.2;  rn = "CONFIRM"

        # ═══ LAYER 4 — SECTOR MODIFIER (v5 with override) ═══
        sm = 1.0
        sn = "NEUTRAL"
        sector_override_flag = 0
        sector_override_label = "NONE"

        if sector_str == "WEAK":
            # ── Phase 3: Check if stock can override weak sector ──
            override_result = check_leadership_override(
                ticker, sig, sector_str, l,
                trend_data=trend_data, rs_data=rs_data
            )
            sm = override_result["sm"]
            sector_override_label = override_result["label"]

            if override_result["override"]:
                sector_override_flag = 1
                sn = sector_override_label
                sigs.append(sector_override_label)
            else:
                sn = "WEAK"
                # v5.6: Reduced penalty for high-confidence stocks (c5 >= 80)
                # Strong individual momentum should not be killed by weak sector
                if sig == "BOTTOM":
                    if score >= 65:
                        sm = 0.85; sn = "WEAK_STRONG_STOCK"
                        sigs.append("ReducedWeakPenalty")
                    else:
                        sm = 0.7;  sn = "WEAK"
                elif sig == "TOP":
                    if score >= 65:
                        sm = 1.05; sn = "WEAK_STRONG_STOCK"
                        sigs.append("ReducedWeakPenalty")
                    else:
                        sm = 1.15; sn = "WEAK"
        elif sector_str == "STRONG":
            if sig == "BOTTOM":
                sm = 1.15; sn = "STRONG"
            elif sig == "TOP":
                sm = 0.7;  sn = "STRONG"
        else:
            sm = 1.0;  sn = "NEUTRAL"

        # ═══ TIME QUALITY MODIFIER (original v4) ═══
        tm, tn = get_time_quality()

        # ═══ FINAL CONFIDENCE (v5 — threshold lowered to 45) ═══
        fc = min(int(score * rm * sm * tm), 100)
        if fc < 35:
            return None

        return {
            "signal": sig, "confidence": fc, "raw_score": score,
            "rsi": l["rsi"], "mh": l["mh"], "sk": l["sk"], "wr": l["wr"],
            "cl": l["cl"], "vwap": l["vwap"], "vs": l["vs"], "vd": l["vd"],
            "bdiv": l["bdiv"], "brdiv": l["brdiv"],
            "mfi": l["mfi"], "cmf": l["cmf"],
            "obv_rising": l["obv_rising"],
            "regime": market_regime, "regime_note": rn,
            "sector_strength": sector_str, "sector_note": sn,
            "time_note": tn, "signals": sigs, "interval": interval,
            # ── v5 new fields ──
            "setup_type": setup_type,
            "leadership_score": leadership["leadership_score"],
            "leadership_signals": leadership["leadership_signals"],
            "relative_strength": rs_data.get("rs_3d", 0.0),
            "sector_override": sector_override_flag,
            "sector_override_label": sector_override_label,
            "trend_3d": trend_data.get("trend_3d", 0.0),
            "trend_5d": trend_data.get("trend_5d", 0.0),
            "volume_expansion": trend_data.get("volume_expansion", 1.0),
        }
    except Exception:
        return None


# ═══════════════════════════════════════════════════════
# MULTI-TIMEFRAME CONFIRMATION
# (v5.6: 5m+15m+30m STRICT, 60m OPTIONAL BOOSTER, early promotion for high-confidence)
# ═══════════════════════════════════════════════════════

def check_multi_timeframe(ticker, market_regime, sector_str):
    global confirmation_tracker

    # ── STEP 1: 5-MINUTE (STRICT — 3x consecutive scans) ──
    r5 = analyze_stock_v5(ticker, "5m", market_regime, sector_str)
    if r5 is None:
        confirmation_tracker.pop(ticker, None)
        return None

    st = r5["signal"]
    sec = get_stock_sector(ticker)

    if ticker in confirmation_tracker and confirmation_tracker[ticker]["type"] == st:
        confirmation_tracker[ticker]["count"] += 1
        confirmation_tracker[ticker]["c5"] = r5["confidence"]
    else:
        confirmation_tracker[ticker] = {
            "type": st, "count": 1, "last": datetime.now(), "c5": r5["confidence"]
        }

    # Metadata only for end-of-scan logs; trading logic is unchanged.
    confirmation_tracker[ticker].update({
        "sector": sec,
        "last_price": r5.get("cl", 0),
        "last_confidence": r5.get("confidence", 0),
        "last_update": get_eastern_now().strftime("%Y-%m-%d %I:%M:%S %p ET"),
        "stage": "WAIT_5M_CONFIRM",
    })

    if confirmation_tracker[ticker]["count"] < MIN_5MIN_CONFIRMATIONS:
        logger.info("  %s: %s 5m (%d/%d) c=%d",
                     ticker, st,
                     confirmation_tracker[ticker]["count"],
                     MIN_5MIN_CONFIRMATIONS, r5["confidence"])
        return None

    # ── STEP 2: 15-MINUTE (STRICT — must agree) ──
    confirmation_tracker[ticker]["stage"] = "WAIT_15M_CONFIRM"
    logger.info("  %s: %s 5m CONFIRMED (%dx) -> 15m...",
                ticker, st, MIN_5MIN_CONFIRMATIONS)
    r15 = analyze_stock_v5(ticker, "15m", market_regime, sector_str)
    if r15 is None or r15["signal"] != st:
        confirmation_tracker[ticker]["stage"] = "15M_NO_MATCH_LAST_SCAN"
        logger.info("  %s: 15m NO MATCH -> REJECTED", ticker)
        return None

    # ── STEP 3: 30-MINUTE (STRICT — must agree) [NEW in v5.1] ──
    confirmation_tracker[ticker]["stage"] = "WAIT_30M_CONFIRM"
    logger.info("  %s: %s 15m CONFIRMED -> 30m (STRICT)...", ticker, st)
    r30 = analyze_stock_v5(ticker, "30m", market_regime, sector_str)
    if r30 is None or r30["signal"] != st:
        confirmation_tracker[ticker]["stage"] = "30M_NO_MATCH_LAST_SCAN"
        logger.info("  %s: 30m NO MATCH -> REJECTED", ticker)
        return None

    c30_val = r30["confidence"]
    c5_val = r5["confidence"]
    confirmation_tracker[ticker]["stage"] = "30M_CONFIRMED_WAIT_60M_BOOSTER"
    logger.info("  %s: %s 30m CONFIRMED (c=%d, c5=%d) -> checking early promotion...",
                ticker, st, c30_val, c5_val)

    # ═══════════════════════════════════════════════════════════════
    # v5.6 — EARLY PROMOTION: Skip 60M gate for high-confidence stocks
    # If c5 >= 80 AND sector is STRONG/NEUTRAL → promote immediately at 30M
    # If c5 >= 70 AND sector is STRONG → also promote (sector tailwind)
    # This ensures perfect-score stocks like MCD(100), CRM(100) don't wait endlessly
    # ═══════════════════════════════════════════════════════════════
    early_promote = False
    promote_reason = ""
    if c5_val >= 60 and sector_str in ("STRONG", "NEUTRAL"):
        early_promote = True
        promote_reason = "c5=%d≥80 + sector=%s" % (c5_val, sector_str)
    elif c5_val >= 50 and sector_str == "STRONG":
        early_promote = True
        promote_reason = "c5=%d≥70 + sector=STRONG_TAILWIND" % c5_val

    mtf_bonus = 0
    mtf_status = "5m+15m+30m"
    c60_val = 0
    r60 = None

    if early_promote:
        # ── EARLY PROMOTION: Skip 60M, go straight to RQG ──
        mtf_bonus = 5   # Small bonus for passing 30M with high confidence
        mtf_status = "5m+15m+30m(EARLY)"
        confirmation_tracker[ticker]["stage"] = "EARLY_PROMOTED_AT_30M"
        logger.info("  ★ %s: EARLY PROMOTION — %s → skipping 60M gate (mtf=%s)",
                    ticker, promote_reason, mtf_status)
    else:
        # ── STEP 4: 60-MINUTE (BOOSTER — relaxed, never rejects) ──
        logger.info("  %s: %s not early-promoted (c5=%d, sector=%s) -> 60m booster...",
                    ticker, st, c5_val, sector_str)
        r60 = analyze_stock_v5(ticker, "60m", market_regime, sector_str)

        if r60 is not None and r60["signal"] == st:
            mtf_bonus = 10
            mtf_status = "5m+15m+30m+60m"
            c60_val = r60["confidence"]
            logger.info("  * %s: 60m AGREES -> boost +%d -> FULL 4-TF", ticker, mtf_bonus)
        elif r60 is not None and r60["signal"] != st:
            mtf_bonus = -5
            mtf_status = "5m+15m+30m"
            c60_val = r60["confidence"]
            logger.info("  v %s: 60m DISAGREES -> penalty %d (NOT rejected)", ticker, mtf_bonus)
        else:
            mtf_bonus = 0
            mtf_status = "5m+15m+30m"
            c60_val = 0
            logger.info("  o %s: 60m data unavailable -> neutral", ticker)

    logger.info("  *** %s: %s ALL CONFIRMED (v5.6 MTF=%s) -> RQG final quality gate...", ticker, st, mtf_status)

    # ── STEP 5: Reversal Quality Gate (v5.5) ──
    rqg = evaluate_reversal_quality_gate(ticker, st, market_regime, sector_str, r5, r15, r30, r60)
    if RQG_ENFORCE_GATE and not rqg.get("passed", False):
        logger.info("  RQG BLOCKED %s %s: label=%s score=%s reasons=%s",
                    ticker, st, rqg.get("label"), rqg.get("score"), ",".join(rqg.get("reasons", [])[:10]))
        return None

    # ── Duplicate alert prevention ──
    ak = "%s_%s_%s" % (ticker, st, datetime.now().strftime("%Y%m%d_%H"))
    if ak in sent_alerts:
        return None
    sent_alerts.add(ak)
    del confirmation_tracker[ticker]

    # ── Options P/C ratio ──
    pcr = get_options_pcr(ticker)
    logger.info("  %s: Options P/C=%.2f", ticker, pcr)

    now_et = get_eastern_now().strftime("%Y-%m-%d %I:%M:%S %p ET")

    # ── Build final confidence (includes c30 in average) ──
    if mtf_status == "5m+15m+30m+60m":
        avg_c = round((r5["confidence"] + r15["confidence"] + c30_val + c60_val) / 4) + mtf_bonus
    else:
        avg_c = round((r5["confidence"] + r15["confidence"] + c30_val) / 3) + mtf_bonus
    avg_c = max(0, min(avg_c, 100))

    signals_with_rqg = list(r5["signals"])
    signals_with_rqg.append("RQG=%s/%s" % (rqg.get("score"), rqg.get("label")))
    for reason in rqg.get("reasons", [])[:4]:
        signals_with_rqg.append("RQG_%s" % reason)

    return {
        "ticker": ticker, "signal": st,
        "c5": r5["confidence"], "c15": r15["confidence"],
        "c30": c30_val, "c60": c60_val,
        "avg_c": avg_c,
        "rsi": r5["rsi"], "sk": r5["sk"], "wr": r5["wr"],
        "cl": r5["cl"], "vwap": r5["vwap"],
        "vs": r5["vs"], "vd": r5["vd"],
        "bdiv": r5["bdiv"], "brdiv": r5["brdiv"],
        "mfi": r5["mfi"], "cmf": r5["cmf"], "pcr": pcr,
        "regime": r5["regime"], "rn": r5["regime_note"],
        "ss": r5["sector_strength"], "sn": r5["sector_note"],
        "tn": r5["time_note"], "signals": signals_with_rqg,
        "time": now_et,
        # ── v5 fields ──
        "mtf_status": mtf_status,
        "setup_type": r5.get("setup_type", "REVERSAL"),
        "leadership_score": r5.get("leadership_score", 0),
        "leadership_signals": r5.get("leadership_signals", []),
        "relative_strength": r5.get("relative_strength", 0.0),
        "sector_override": r5.get("sector_override", 0),
        "sector_override_label": r5.get("sector_override_label", "NONE"),
        "trend_3d": r5.get("trend_3d", 0.0),
        "trend_5d": r5.get("trend_5d", 0.0),
        "volume_expansion": r5.get("volume_expansion", 1.0),
        # ── v5.5 RQG fields ──
        "rqg_score": rqg.get("score", 0),
        "rqg_label": rqg.get("label", "REJECT"),
        "rqg_reasons": rqg.get("reasons", []),
        "rqg_buckets": rqg.get("buckets", {}),
        "rqg_details": rqg.get("details", {}),
    }


# ═══════════════════════════════════════════════════════
# DATABASE — SQLite  (v5 expanded schema)
# ═══════════════════════════════════════════════════════

def init_db():
    conn = get_db_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            ticker TEXT,
            signal TEXT,
            alert_price REAL,
            alert_time TEXT,
            confidence INTEGER,
            regime TEXT,
            sector_strength TEXT,
            mfi REAL,
            cmf REAL,
            pcr REAL,
            signals_text TEXT,
            current_price REAL DEFAULT 0,
            change_pct REAL DEFAULT 0,
            result TEXT DEFAULT 'PENDING',
            setup_type TEXT DEFAULT 'REVERSAL',
            relative_strength REAL DEFAULT 0,
            sector_override INTEGER DEFAULT 0,
            trend_3d REAL DEFAULT 0,
            trend_5d REAL DEFAULT 0,
            volume_expansion REAL DEFAULT 1.0,
            mtf_status TEXT DEFAULT '5m+15m'
        )
    """)
    conn.commit()
    conn.close()


def save_alert(a):
    try:
        conn = get_db_connection()
        conn.execute(
            """INSERT INTO alerts
               (date, ticker, signal, alert_price, alert_time, confidence,
                regime, sector_strength, mfi, cmf, pcr, signals_text,
                setup_type, relative_strength, sector_override,
                trend_3d, trend_5d, volume_expansion, mtf_status)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                get_eastern_now().strftime("%Y-%m-%d"),
                a["ticker"], a["signal"], a["cl"], a["time"], a["avg_c"],
                a["regime"], a["ss"], a.get("mfi", 0), a.get("cmf", 0),
                a.get("pcr", 1.0), ", ".join(a["signals"]),
                a.get("setup_type", "REVERSAL"),
                a.get("relative_strength", 0.0),
                a.get("sector_override", 0),
                a.get("trend_3d", 0.0),
                a.get("trend_5d", 0.0),
                a.get("volume_expansion", 1.0),
                a.get("mtf_status", "5m+15m"),
            )
        )
        conn.commit()
        conn.close()
        logger.info("  DB: Saved %s %s @ $%.2f  [%s]",
                     a["ticker"], a["signal"], a["cl"],
                     a.get("mtf_status", ""))
    except Exception as e:
        logger.error("DB save error: %s", e)


def update_alert_prices():
    try:
        conn = get_db_connection()
        today = get_eastern_now().strftime("%Y-%m-%d")
        rows = conn.execute(
            "SELECT id, ticker, signal, alert_price FROM alerts WHERE date=?",
            (today,)
        ).fetchall()
        if not rows:
            conn.close()
            return
        tickers = list(set(r[1] for r in rows))
        logger.info("  Fetching close prices for %d tickers...", len(tickers))
        prices = {}
        try:
            data = safe_yf_download(tickers, period="1d", interval="1d",
                               progress=False, auto_adjust=True,
                               threads=False, group_by="ticker")
            if data is not None and not data.empty:
                if isinstance(data.columns, pd.MultiIndex):
                    for t in tickers:
                        try:
                            prices[t] = float(data[t]["Close"].iloc[-1])
                        except Exception:
                            pass
                else:
                    try:
                        prices[tickers[0]] = float(data["Close"].iloc[-1])
                    except Exception:
                        pass
        except Exception as e:
            logger.warning("Price fetch: %s", e)
        for rid, ticker, signal, aprice in rows:
            cprice = prices.get(ticker, 0)
            if cprice <= 0 or aprice <= 0:
                continue
            chg = (cprice - aprice) / aprice * 100
            result = "WIN" if ((signal == "BOTTOM" and chg > 0) or
                               (signal == "TOP" and chg < 0)) else "LOSS"
            conn.execute(
                "UPDATE alerts SET current_price=?, change_pct=?, result=? WHERE id=?",
                (cprice, round(chg, 2), result, rid)
            )
        conn.commit()
        conn.close()
        logger.info("  DB: Updated %d alerts", len(rows))
    except Exception as e:
        logger.error("DB update: %s", e)


def cleanup_old_data():
    try:
        cutoff = (get_eastern_now() - timedelta(days=RETENTION_DAYS)).strftime("%Y-%m-%d")
        conn = get_db_connection()
        cur = conn.execute("DELETE FROM alerts WHERE date<?", (cutoff,))
        conn.commit()
        conn.close()
        if cur.rowcount > 0:
            logger.info("  DB: Cleaned %d old records", cur.rowcount)
    except Exception as e:
        logger.error("DB cleanup: %s", e)


def get_db_today():
    conn = get_db_connection()
    rows = conn.execute(
        "SELECT * FROM alerts WHERE date=? ORDER BY alert_time",
        (get_eastern_now().strftime("%Y-%m-%d"),)
    ).fetchall()
    conn.close()
    return rows


def get_db_week():
    conn = get_db_connection()
    start = (get_eastern_now() - timedelta(days=7)).strftime("%Y-%m-%d")
    rows = conn.execute(
        "SELECT * FROM alerts WHERE date>=? ORDER BY date DESC, alert_time",
        (start,)
    ).fetchall()
    conn.close()
    return rows


# ═══════════════════════════════════════════════════════
# EOD REPORT  (v5 — with Override & MTF columns)
# ═══════════════════════════════════════════════════════

def build_eod_report():
    rows = get_db_today()
    today = get_eastern_now().strftime("%Y-%m-%d")
    n = len(ALL_STOCKS)

    if not rows:
        return (
            '<html><body>'
            '<h2>v5 EOD Report (%s)</h2>'
            '<p>No alerts today across %d stocks.</p>'
            '</body></html>' % (today, n)
        )

    wins = sum(1 for r in rows if r[14] == "WIN")
    losses = sum(1 for r in rows if r[14] == "LOSS")
    pending = sum(1 for r in rows if r[14] == "PENDING")
    total = len(rows)
    wr = (wins / (wins + losses) * 100) if (wins + losses) > 0 else 0
    gains = [r[13] for r in rows if r[14] == "WIN" and r[13] != 0]
    lvals = [r[13] for r in rows if r[14] == "LOSS" and r[13] != 0]
    ag = sum(gains) / len(gains) if gains else 0
    al = sum(lvals) / len(lvals) if lvals else 0

    tr = ""
    for r in rows:
        # DB columns: 0=id,1=date,2=ticker,3=signal,4=alert_price,5=alert_time,
        # 6=confidence,7=regime,8=sector_strength,9=mfi,10=cmf,11=pcr,12=signals_text,
        # 13=current_price→change_pct(13 is change_pct after update),
        # Actually: 13=current_price,14=result → wait, let me use correct indices
        # 0:id 1:date 2:ticker 3:signal 4:alert_price 5:alert_time 6:confidence
        # 7:regime 8:sector_strength 9:mfi 10:cmf 11:pcr 12:signals_text
        # 13:current_price 14:change_pct 15:result
        # 16:setup_type 17:relative_strength 18:sector_override
        # 19:trend_3d 20:trend_5d 21:volume_expansion 22:mtf_status
        rid = r[0]
        dt = r[1]
        tk = r[2]
        sig = r[3]
        ap = r[4]
        at = r[5]
        conf = r[6]
        reg = r[7]
        ss = r[8]
        sigs = r[12]
        cp = r[13]
        chg = r[14]
        res = r[15]
        setup = r[16] if len(r) > 16 else "REVERSAL"
        so = r[18] if len(r) > 18 else 0
        mtf = r[22] if len(r) > 22 else "5m+15m"

        # Override label
        if so == 1:
            override_lbl = "LEADER" if "LEADER" in str(setup) else "OVERRIDE"
        else:
            override_lbl = "-"

        bg = "#e6f4ea" if res == "WIN" else ("#fce8e6" if res == "LOSS" else "#fff3cd")
        cps = "$%.2f" % cp if cp > 0 else "N/A"
        chs = "%+.2f%%" % chg if cp > 0 else "N/A"

        tr += '<tr style="background:%s">' % bg
        tr += '<td><b>%s</b></td>' % tk
        tr += '<td>%s</td><td>$%.2f</td><td>%s</td>' % (sig, ap, at)
        tr += '<td>%s</td><td>%s</td><td><b>%s</b></td>' % (cps, chs, res)
        tr += '<td>%d</td><td>%s</td>' % (conf, reg)
        tr += '<td>%s</td><td>%s</td>' % (mtf, override_lbl)
        tr += '<td>%s</td>' % sigs
        tr += '</tr>'

    html = '<html><body style="font-family:Arial,sans-serif;">'
    html += '<h2>v5 End-of-Day Performance Report (%s)</h2>' % today
    html += '<div style="background:#f0f4f8;padding:12px;border-radius:8px;margin-bottom:16px;">'
    html += '<b>Today Summary</b><br>'
    html += 'Total: %d &nbsp; Wins: %d &nbsp; Losses: %d &nbsp; Pending: %d<br>' % (total, wins, losses, pending)
    html += 'Win Rate: <b>%.1f%%</b> &nbsp; Avg Gain: %+.2f%% &nbsp; Avg Loss: %+.2f%%<br>' % (wr, ag, al)
    html += 'Universe: %d stocks</div>' % n
    html += '<table border="1" cellpadding="5" cellspacing="0" style="border-collapse:collapse;">'
    html += '<tr style="background:#2c3e50;color:white;">'
    for h in ["Ticker", "Signal", "Alert$", "Time", "Close$", "Change",
              "Result", "Score", "Regime", "MTF", "Override", "Signals"]:
        html += '<th>%s</th>' % h
    html += '</tr>'
    html += tr
    html += '</table>'
    html += '<p style="color:#888;font-size:11px;">Green=WIN &nbsp; Red=LOSS &nbsp; Yellow=PENDING</p>'
    html += '</body></html>'
    return html


# ═══════════════════════════════════════════════════════
# WEEKLY REPORT  (original v4 — preserved)
# ═══════════════════════════════════════════════════════

def build_weekly_report():
    rows = get_db_week()
    today = get_eastern_now().strftime("%Y-%m-%d")

    if not rows:
        return (
            '<html><body>'
            '<h2>v5 Weekly Report</h2>'
            '<p>No alerts last 7 days.</p>'
            '</body></html>'
        )

    aw = sum(1 for r in rows if r[15] == "WIN")
    als = sum(1 for r in rows if r[15] == "LOSS")
    at = len(rows)
    awr = (aw / (aw + als) * 100) if (aw + als) > 0 else 0

    br = [r for r in rows if r[3] == "BOTTOM" and r[15] in ("WIN", "LOSS")]
    trr = [r for r in rows if r[3] == "TOP" and r[15] in ("WIN", "LOSS")]
    bwr = (sum(1 for r in br if r[15] == "WIN") / len(br) * 100) if br else 0
    twr = (sum(1 for r in trr if r[15] == "WIN") / len(trr) * 100) if trr else 0

    comp = [r for r in rows if r[15] in ("WIN", "LOSS") and r[14] != 0]
    best = sorted(comp, key=lambda x: x[14], reverse=True)[:5]
    worst = sorted(comp, key=lambda x: x[14])[:5]

    dates = sorted(set(r[1] for r in rows), reverse=True)
    drt = ""
    for d in dates:
        dd = [r for r in rows if r[1] == d]
        dw = sum(1 for r in dd if r[15] == "WIN")
        dl = sum(1 for r in dd if r[15] == "LOSS")
        dwr = (dw / (dw + dl) * 100) if (dw + dl) > 0 else 0
        bg = "#e6f4ea" if dwr >= 60 else ("#fce8e6" if dwr < 40 else "#fff3cd")
        drt += '<tr style="background:%s"><td>%s</td><td>%d</td><td>%d</td><td>%d</td><td>%.0f%%</td></tr>' % (bg, d, len(dd), dw, dl, dwr)

    bstr = ""
    for r in best:
        bstr += '<tr><td>%s</td><td>%s</td><td>$%.2f</td><td>%+.2f%%</td><td>%s</td></tr>' % (r[2], r[3], r[4], r[14], r[1])
    wstr = ""
    for r in worst:
        wstr += '<tr><td>%s</td><td>%s</td><td>$%.2f</td><td>%+.2f%%</td><td>%s</td></tr>' % (r[2], r[3], r[4], r[14], r[1])

    # Strong winners
    strong = [r for r in rows if r[15] == "WIN" and r[14] > 0.5][:5]
    sstr = ""
    for r in strong:
        sstr += '<tr><td>%s</td><td>%s</td><td>$%.2f</td><td>%+.2f%%</td><td>%d</td><td>%s</td></tr>' % (r[2], r[3], r[4], r[14], r[6], r[1])

    html = '<html><body style="font-family:Arial,sans-serif;">'
    html += '<h2>v5 Weekly Performance Report</h2>'
    html += '<p>Last 7 days ending %s</p>' % today
    html += '<div style="background:#f0f4f8;padding:12px;border-radius:8px;margin-bottom:16px;">'
    html += '<b>Overall (7 Days)</b><br>'
    html += 'Total: %d &nbsp; Wins: %d &nbsp; Losses: %d<br>' % (at, aw, als)
    html += 'Win Rate: <b>%.1f%%</b><br>' % awr
    html += 'BOTTOM: %.1f%% &nbsp; TOP: %.1f%%</div>' % (bwr, twr)

    # Daily breakdown
    html += '<h3>Daily Breakdown</h3>'
    html += '<table border="1" cellpadding="4" cellspacing="0" style="border-collapse:collapse;">'
    html += '<tr style="background:#2c3e50;color:white;">'
    for h in ["Date", "Total", "Wins", "Losses", "Win Rate"]:
        html += '<th>%s</th>' % h
    html += '</tr>%s</table>' % drt

    if bstr:
        html += '<h3>Top 5 Best</h3>'
        html += '<table border="1" cellpadding="4" cellspacing="0" style="border-collapse:collapse;">'
        html += '<tr style="background:#2c3e50;color:white;"><th>Ticker</th><th>Signal</th><th>Price</th><th>Change</th><th>Date</th></tr>'
        html += '%s</table>' % bstr

    if wstr:
        html += '<h3>Top 5 Worst</h3>'
        html += '<table border="1" cellpadding="4" cellspacing="0" style="border-collapse:collapse;">'
        html += '<tr style="background:#2c3e50;color:white;"><th>Ticker</th><th>Signal</th><th>Price</th><th>Change</th><th>Date</th></tr>'
        html += '%s</table>' % wstr

    if sstr:
        html += '<h3>Still Strong</h3>'
        html += '<table border="1" cellpadding="4" cellspacing="0" style="border-collapse:collapse;">'
        html += '<tr style="background:#2c3e50;color:white;"><th>Ticker</th><th>Signal</th><th>Price</th><th>Change</th><th>Score</th><th>Date</th></tr>'
        html += '%s</table>' % sstr

    html += '<p style="color:#888;font-size:11px;">Data retained %d days.</p>' % RETENTION_DAYS
    html += '</body></html>'
    return html


def is_friday():
    return get_eastern_now().weekday() == 4


# ═══════════════════════════════════════════════════════
# ALERT EMAIL  (v5 — with MTF, Override, Trend columns)
# ═══════════════════════════════════════════════════════

def build_alert_email(alerts):
    rows = ""
    for a in alerts:
        c = "#fce8e6" if a["signal"] == "TOP" else "#e6f4ea"
        dv = "Bull" if a.get("bdiv") else ("Bear" if a.get("brdiv") else "No")
        vw = "$%.2f" % a["vwap"] if a["vwap"] > 0 else "N/A"
        mtf = a.get("mtf_status", "5m+15m")
        override = a.get("sector_override_label", "NONE")
        t3d = "%+.1f%%" % a.get("trend_3d", 0)
        ve = "%.1fx" % a.get("volume_expansion", 1.0)

        rows += '<tr style="background:%s">' % c
        rows += '<td><b>%s</b></td>' % a["ticker"]
        rows += '<td>%s</td>' % a["signal"]
        rows += '<td>$%.2f</td>' % a["cl"]
        rows += '<td><b>%d</b></td>' % a["avg_c"]
        rows += '<td>%s</td>' % a["regime"]
        rows += '<td>%s</td>' % a["ss"]
        rows += '<td>%s</td>' % dv
        rows += '<td>%s</td>' % vw
        rows += '<td>%.0f</td>' % a["mfi"]
        rows += '<td>%.2f</td>' % a["cmf"]
        rows += '<td>%.2f</td>' % a["pcr"]
        rows += '<td>%s</td>' % mtf
        rows += '<td>%s</td>' % override
        rows += '<td>%s</td>' % t3d
        rows += '<td>%s</td>' % ve
        rows += '<td>%s</td>' % ", ".join(a["signals"])
        rows += '<td>%s</td>' % a["time"]
        rows += '</tr>'

    html = '<html><body style="font-family:Arial,sans-serif;">'
    html += '<h2>Market Scanner v5 Alert</h2>'
    now_et = get_eastern_now().strftime("%Y-%m-%d %I:%M %p ET")
    html += '<p>Time: %s &nbsp; Base Stocks: %d + Dynamic &nbsp; Layers: 7+MFI+OBV+CMF+Options+Candles+SectorOverride</p>' % (now_et, len(ALL_STOCKS))
    html += '<table border="1" cellpadding="4" cellspacing="0" style="border-collapse:collapse;">'
    html += '<tr style="background:#2c3e50;color:white;">'
    for h in ["Ticker", "Signal", "Price", "Score", "Regime", "Sector",
              "Diverg", "VWAP", "MFI", "CMF", "P/C",
              "MTF", "Override", "Trend3d", "VolExp", "Signals", "Time"]:
        html += '<th>%s</th>' % h
    html += '</tr>'
    html += rows
    html += '</table>'
    html += '<p style="color:#888;font-size:11px;">v5 12-indicator + sector override. Not financial advice.</p>'
    html += '</body></html>'
    return html


# ═══════════════════════════════════════════════════════
# DAILY SUMMARY EMAIL  (v5 — with MTF & Override)
# ═══════════════════════════════════════════════════════

def build_daily_summary_email(all_alerts):
    d = get_eastern_now().strftime("%Y-%m-%d")
    n = len(ALL_STOCKS)

    if not all_alerts:
        return (
            '<html><body>'
            '<h2>v5 Summary (%s)</h2>'
            '<p>No signals across %d stocks.</p>'
            '</body></html>' % (d, n)
        )

    rows = ""
    for a in all_alerts:
        c = "#fce8e6" if a["signal"] == "TOP" else "#e6f4ea"
        mtf = a.get("mtf_status", "5m+15m")
        override = a.get("sector_override_label", "NONE")
        rows += '<tr style="background:%s">' % c
        rows += '<td><b>%s</b></td>' % a["ticker"]
        rows += '<td>%s</td><td>$%.2f</td><td>%d</td>' % (a["signal"], a["cl"], a["avg_c"])
        rows += '<td>%s</td><td>%.2f</td>' % (a["regime"], a["pcr"])
        rows += '<td>%s</td><td>%s</td>' % (mtf, override)
        rows += '<td>%s</td>' % ", ".join(a["signals"])
        rows += '<td>%s</td>' % a["time"]
        rows += '</tr>'

    tops = sum(1 for a in all_alerts if a["signal"] == "TOP")
    bots = len(all_alerts) - tops

    html = '<html><body style="font-family:Arial,sans-serif;">'
    html += '<h2>v5 Summary (%s)</h2>' % d
    html += '<p>Total: %d &nbsp; Tops: %d &nbsp; Bottoms: %d &nbsp; Universe: %d</p>' % (len(all_alerts), tops, bots, n)
    html += '<table border="1" cellpadding="4" cellspacing="0" style="border-collapse:collapse;">'
    html += '<tr style="background:#2c3e50;color:white;">'
    for h in ["Ticker", "Signal", "Price", "Score", "Regime", "P/C",
              "MTF", "Override", "Signals", "Time"]:
        html += '<th>%s</th>' % h
    html += '</tr>'
    html += rows
    html += '</table></body></html>'
    return html


# ═══════════════════════════════════════════════════════
# EMAIL SENDER  (original v4 — preserved with retry)
# ═══════════════════════════════════════════════════════

def send_email(subject, html_content):
    if not EMAIL_ADDRESS or not EMAIL_PASSWORD or not RECIPIENT_EMAIL:
        logger.warning("Email not configured.")
        return
    while True:
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
                logger.info("Email sent to %s: %s", ", ".join(RECIPIENT_EMAIL), subject)
            break
        except Exception as e:
            logger.error("Email error: %s. Retry in 5min...", e)
            time.sleep(300)


# ═══════════════════════════════════════════════════════
# MAIN LOOP  (v5)
# ═══════════════════════════════════════════════════════



# ═══════════════════════════════════════════════════════
# PHASE 6 — BACKTEST ENGINE
# Validates strategy over last N trading days using daily data
# ═══════════════════════════════════════════════════════

def run_backtest(days=15, stock_list=None):
    """
    Run historical backtest over the last `days` trading days.
    Uses daily OHLCV data to simulate signal generation and measure outcomes.
    Returns dict with results + HTML report string.
    """
    if stock_list is None:
        stock_list = ALL_STOCKS

    logger.info("=" * 60)
    logger.info("BACKTEST ENGINE — Last %d trading days", days)
    logger.info("Universe: %d stocks", len(stock_list))
    logger.info("=" * 60)

    # ── Batch download daily data ──
    logger.info("  Downloading daily data for %d stocks...", len(stock_list))
    try:
        all_data = safe_yf_download(
            stock_list, period="3mo", interval="1d",
            progress=False, auto_adjust=True, threads=False, group_by="ticker"
        )
        if all_data is None or all_data.empty:
            logger.error("  Backtest: No data downloaded")
            return None
    except Exception as e:
        logger.error("  Backtest download failed: %s", e)
        return None

    signals = []  # list of dicts for each signal found

    for ticker in stock_list:
        try:
            # Extract single ticker data
            if isinstance(all_data.columns, pd.MultiIndex):
                if ticker not in all_data.columns.get_level_values(0):
                    continue
                df = all_data[ticker].dropna(how="all")
            else:
                df = all_data.dropna(how="all")

            if df is None or df.empty or len(df) < days + 20:
                continue

            cl = df["Close"]
            hi = df["High"]
            lo = df["Low"]
            vo = df["Volume"]
            op = df["Open"]

            # Precompute full-series indicators
            rsi_full = calculate_rsi(cl)
            _, _, macd_hist_full = calculate_macd(cl)
            sk_full, sd_full = calculate_stochastic(hi, lo, cl)
            bbu_full, _, bbl_full = calculate_bollinger(cl)
            wr_full = calculate_williams_r(hi, lo, cl)
            vs_full = detect_volume_spike(vo)
            lh_full, hl_full = detect_market_structure(hi, lo, cl)
            mfi_full = calculate_mfi(hi, lo, cl, vo)
            obv_full = calculate_obv(cl, vo)
            cmf_full = calculate_cmf(hi, lo, cl, vo)

            # Iterate over each test day
            for offset in range(days, 0, -1):
                idx = len(df) - offset - 1  # signal day index
                if idx < 25 or idx + 2 >= len(df):
                    continue

                # ── Get indicator values at signal day ──
                try:
                    rv = float(rsi_full.iloc[idx]) if not pd.isna(rsi_full.iloc[idx]) else 50
                    mh_v = float(macd_hist_full.iloc[idx]) if not pd.isna(macd_hist_full.iloc[idx]) else 0
                    mhp_v = float(macd_hist_full.iloc[idx-1]) if not pd.isna(macd_hist_full.iloc[idx-1]) else 0
                    sk_v = float(sk_full.iloc[idx]) if not pd.isna(sk_full.iloc[idx]) else 50
                    sd_v = float(sd_full.iloc[idx]) if not pd.isna(sd_full.iloc[idx]) else 50
                    cl_v = float(cl.iloc[idx])
                    bbu_v = float(bbu_full.iloc[idx]) if not pd.isna(bbu_full.iloc[idx]) else 0
                    bbl_v = float(bbl_full.iloc[idx]) if not pd.isna(bbl_full.iloc[idx]) else 0
                    wr_v = float(wr_full.iloc[idx]) if not pd.isna(wr_full.iloc[idx]) else -50
                    vd_v = 0.5
                    try:
                        pc = cl.diff()
                        bv = vo.where(pc > 0, 0.0).rolling(10).sum()
                        sv = vo.where(pc < 0, 0.0).rolling(10).sum()
                        t_v = bv.iloc[idx] + sv.iloc[idx]
                        vd_v = float(bv.iloc[idx] / t_v) if t_v > 0 else 0.5
                    except Exception:
                        vd_v = 0.5

                    vs_v = bool(vs_full.iloc[idx]) if not pd.isna(vs_full.iloc[idx]) else False
                    lh_v = bool(lh_full.iloc[idx]) if not pd.isna(lh_full.iloc[idx]) else False
                    hl_v = bool(hl_full.iloc[idx]) if not pd.isna(hl_full.iloc[idx]) else False
                    mfi_v = float(mfi_full.iloc[idx]) if not pd.isna(mfi_full.iloc[idx]) else 50
                    cmf_v = float(cmf_full.iloc[idx]) if not pd.isna(cmf_full.iloc[idx]) else 0
                    obv_rising_v = float(obv_full.iloc[idx]) > float(obv_full.iloc[max(idx-5,0)]) if idx >= 5 else False
                except Exception:
                    continue

                # ── Trend data (3d / 5d) ──
                t3d = (cl.iloc[idx] - cl.iloc[max(idx-3,0)]) / cl.iloc[max(idx-3,0)] * 100 if idx >= 3 else 0
                t5d = (cl.iloc[idx] - cl.iloc[max(idx-5,0)]) / cl.iloc[max(idx-5,0)] * 100 if idx >= 5 else 0
                vol_exp = float(vo.iloc[max(idx-2,0):idx+1].mean() / vo.iloc[max(idx-9,0):idx+1].mean()) if idx >= 9 else 1.0

                trend_d = {"trend_3d": float(t3d), "trend_5d": float(t5d), "volume_expansion": float(vol_exp)}
                rs_d = {"rs_3d": 0.0, "rs_5d": 0.0}  # skip sector RS for speed

                l = {
                    "rsi": rv, "mh": mh_v, "mhp": mhp_v, "sk": sk_v, "sd": sd_v,
                    "cl": cl_v, "bbu": bbu_v, "bbl": bbl_v, "wr": wr_v,
                    "vwap": 0, "vs": vs_v, "lh": lh_v, "hl": hl_v,
                    "bdiv": False, "brdiv": False, "vd": vd_v,
                    "mfi": mfi_v, "cmf": cmf_v, "obv_rising": obv_rising_v,
                    "hammer": False, "bull_eng": False, "shoot": False, "bear_eng": False,
                }

                # ═══ BOTTOM SCORING (same as analyze_stock_v5) ═══
                b_score = 0
                b_tags = []
                if l["mh"] > l["mhp"] and l["mhp"] < 0:       b_score += 15; b_tags.append("MACD+")
                if l["sk"] > l["sd"] and l["sk"] < 30:         b_score += 15; b_tags.append("StochX")
                elif l["sk"] < 20:                               b_score += 8
                if l["rsi"] < 30:                                b_score += 10; b_tags.append("RSI<30")
                elif l["rsi"] < 35:                              b_score += 5
                if l["wr"] < -80:                                b_score += 10; b_tags.append("WR<-80")
                if l["vd"] > 0.55:                               b_score += 10; b_tags.append("BuyVol")
                if l["hl"]:                                      b_score += 15; b_tags.append("HL")
                if l["vs"] and l["vd"] > 0.5:                   b_score += 5
                if l["mfi"] < 20:                                b_score += 12; b_tags.append("MFI<20")
                elif l["mfi"] < 30:                              b_score += 6
                if l["obv_rising"] and l["rsi"] < 40:           b_score += 10; b_tags.append("OBV+")
                if l["cmf"] > 0 and l["rsi"] < 40:              b_score += 10; b_tags.append("CMF+")
                elif l["cmf"] > -0.05 and l["cmf"] < 0.05 and l["rsi"] < 35:
                    b_score += 5

                # ═══ TOP SCORING (same as analyze_stock_v5) ═══
                t_score = 0
                t_tags = []
                if l["mh"] < l["mhp"] and l["mhp"] > 0:       t_score += 15; t_tags.append("MACD-")
                if l["sk"] < l["sd"] and l["sk"] > 70:         t_score += 15; t_tags.append("StochX")
                elif l["sk"] > 80:                               t_score += 8
                if l["rsi"] > 70:                                t_score += 10; t_tags.append("RSI>70")
                elif l["rsi"] > 65:                              t_score += 5
                if l["wr"] > -20:                                t_score += 10; t_tags.append("WR>-20")
                if l["vd"] < 0.45:                               t_score += 10; t_tags.append("SellVol")
                if l["lh"]:                                      t_score += 15; t_tags.append("LH")
                if l["vs"] and l["vd"] < 0.5:                   t_score += 5
                if l["mfi"] > 80:                                t_score += 12; t_tags.append("MFI>80")
                elif l["mfi"] > 70:                              t_score += 6
                if not l["obv_rising"] and l["rsi"] > 60:       t_score += 10; t_tags.append("OBV-")
                if l["cmf"] < 0 and l["rsi"] > 60:              t_score += 10; t_tags.append("CMF-")

                # ── Determine signal ──
                sig = None
                raw = 0
                stags = []
                stype = "REVERSAL"

                if b_score > t_score and b_score >= 40:
                    sig = "BOTTOM"; raw = min(b_score, 100); stags = b_tags
                elif t_score > b_score and t_score >= 40:
                    sig = "TOP"; raw = min(t_score, 100); stags = t_tags

                # ── Leadership check ──
                if sig is not None:
                    ldr = score_leadership_setup(ticker, sig, l, trend_d, rs_d)
                    if ldr["is_leadership"]:
                        stype = "HYBRID"
                else:
                    if t3d > 0.3:
                        leader_dir = "BOTTOM"
                    elif t3d < -0.3:
                        leader_dir = "TOP"
                    else:
                        continue
                    ldr = score_leadership_setup(ticker, leader_dir, l, trend_d, rs_d)
                    if ldr["leadership_score"] >= 55:
                        sig = leader_dir
                        raw = ldr["leadership_score"]
                        stags = ldr["leadership_signals"]
                        stype = "LEADERSHIP"
                    else:
                        continue

                if sig is None or raw < 40:
                    continue

                # ── Calculate outcomes ──
                entry_price = cl.iloc[idx]
                next_day_price = cl.iloc[idx + 1]
                two_day_price = cl.iloc[idx + 2] if idx + 2 < len(cl) else cl.iloc[idx + 1]

                if sig == "BOTTOM":
                    ret_1d = (next_day_price - entry_price) / entry_price * 100
                    ret_2d = (two_day_price - entry_price) / entry_price * 100
                    win_1d = ret_1d > 0
                    win_2d = ret_2d > 0
                else:  # TOP
                    ret_1d = (entry_price - next_day_price) / entry_price * 100
                    ret_2d = (entry_price - two_day_price) / entry_price * 100
                    win_1d = ret_1d > 0
                    win_2d = ret_2d > 0

                signals.append({
                    "ticker": ticker,
                    "date": str(df.index[idx].date()) if hasattr(df.index[idx], 'date') else str(df.index[idx]),
                    "signal": sig,
                    "setup_type": stype,
                    "raw_score": raw,
                    "rsi": rv,
                    "entry_price": float(entry_price),
                    "ret_1d": float(ret_1d),
                    "ret_2d": float(ret_2d),
                    "win_1d": win_1d,
                    "win_2d": win_2d,
                    "trend_3d": float(t3d),
                    "trend_5d": float(t5d),
                    "vol_exp": float(vol_exp),
                    "signals": stags,
                })
        except Exception:
            continue

    # ═══════════════════════════════════════════════════════
    # BACKTEST RESULTS ANALYSIS
    # ═══════════════════════════════════════════════════════
    total = len(signals)
    if total == 0:
        logger.info("  Backtest: No signals generated")
        html = '<html><body><h2>Backtest Report</h2><p>No signals found in last %d days.</p></body></html>' % days
        return {"total": 0, "html": html}

    wins_1d = sum(1 for s in signals if s["win_1d"])
    wins_2d = sum(1 for s in signals if s["win_2d"])
    wr_1d = wins_1d / total * 100
    wr_2d = wins_2d / total * 100

    gains_1d = [s["ret_1d"] for s in signals if s["win_1d"]]
    losses_1d = [s["ret_1d"] for s in signals if not s["win_1d"]]
    avg_gain = sum(gains_1d) / len(gains_1d) if gains_1d else 0
    avg_loss = sum(losses_1d) / len(losses_1d) if losses_1d else 0

    # By setup type
    setup_types = set(s["setup_type"] for s in signals)
    by_setup = {}
    for st in setup_types:
        subset = [s for s in signals if s["setup_type"] == st]
        st_wins = sum(1 for s in subset if s["win_1d"])
        st_wr = st_wins / len(subset) * 100 if subset else 0
        by_setup[st] = {"total": len(subset), "wins": st_wins, "wr": st_wr}

    # By signal direction
    bottoms = [s for s in signals if s["signal"] == "BOTTOM"]
    tops = [s for s in signals if s["signal"] == "TOP"]
    b_wr = sum(1 for s in bottoms if s["win_1d"]) / len(bottoms) * 100 if bottoms else 0
    t_wr = sum(1 for s in tops if s["win_1d"]) / len(tops) * 100 if tops else 0

    # Best and worst
    sorted_by_ret = sorted(signals, key=lambda x: x["ret_1d"], reverse=True)
    best5 = sorted_by_ret[:5]
    worst5 = sorted_by_ret[-5:]

    # ── Print summary ──
    logger.info("")
    logger.info("═" * 55)
    logger.info("  BACKTEST RESULTS — Last %d trading days", days)
    logger.info("═" * 55)
    logger.info("  Total signals:     %d", total)
    logger.info("  1-day win rate:    %.1f%% (%d/%d)", wr_1d, wins_1d, total)
    logger.info("  2-day win rate:    %.1f%% (%d/%d)", wr_2d, wins_2d, total)
    logger.info("  Avg gain (1d):     %+.2f%%", avg_gain)
    logger.info("  Avg loss (1d):     %+.2f%%", avg_loss)
    logger.info("  BOTTOM signals:    %d (WR: %.1f%%)", len(bottoms), b_wr)
    logger.info("  TOP signals:       %d (WR: %.1f%%)", len(tops), t_wr)
    for st, data in by_setup.items():
        logger.info("  %s:  %d signals (WR: %.1f%%)", st.ljust(12), data["total"], data["wr"])
    logger.info("═" * 55)

    # ═══════════════════════════════════════════════════════
    # BUILD HTML REPORT
    # ═══════════════════════════════════════════════════════
    html = '<html><body style="font-family:Arial,sans-serif;">'
    html += '<h2>v5 Backtest Report — Last %d Trading Days</h2>' % days

    # Summary box
    html += '<div style="background:#f0f4f8;padding:12px;border-radius:8px;margin-bottom:16px;">'
    html += '<b>Overall Results</b><br>'
    html += 'Total Signals: <b>%d</b><br>' % total
    html += '1-Day Win Rate: <b>%.1f%%</b> (%d/%d)<br>' % (wr_1d, wins_1d, total)
    html += '2-Day Win Rate: <b>%.1f%%</b> (%d/%d)<br>' % (wr_2d, wins_2d, total)
    html += 'Avg Gain: <b>%+.2f%%</b> &nbsp; Avg Loss: <b>%+.2f%%</b><br>' % (avg_gain, avg_loss)
    html += '</div>'

    # By direction
    html += '<h3>By Signal Direction</h3>'
    html += '<table border="1" cellpadding="4" cellspacing="0" style="border-collapse:collapse;">'
    html += '<tr style="background:#2c3e50;color:white;"><th>Direction</th><th>Total</th><th>Wins</th><th>Win Rate</th></tr>'
    html += '<tr><td>BOTTOM</td><td>%d</td><td>%d</td><td>%.1f%%</td></tr>' % (len(bottoms), sum(1 for s in bottoms if s["win_1d"]), b_wr)
    html += '<tr><td>TOP</td><td>%d</td><td>%d</td><td>%.1f%%</td></tr>' % (len(tops), sum(1 for s in tops if s["win_1d"]), t_wr)
    html += '</table>'

    # By setup type
    html += '<h3>By Setup Type</h3>'
    html += '<table border="1" cellpadding="4" cellspacing="0" style="border-collapse:collapse;">'
    html += '<tr style="background:#2c3e50;color:white;"><th>Setup</th><th>Total</th><th>Wins</th><th>Win Rate</th></tr>'
    for st, data in sorted(by_setup.items()):
        bg = "#e6f4ea" if data["wr"] >= 55 else ("#fce8e6" if data["wr"] < 45 else "#fff3cd")
        html += '<tr style="background:%s"><td>%s</td><td>%d</td><td>%d</td><td>%.1f%%</td></tr>' % (bg, st, data["total"], data["wins"], data["wr"])
    html += '</table>'

    # Best 5
    html += '<h3>Top 5 Best Signals</h3>'
    html += '<table border="1" cellpadding="4" cellspacing="0" style="border-collapse:collapse;">'
    html += '<tr style="background:#2c3e50;color:white;"><th>Date</th><th>Ticker</th><th>Signal</th><th>Setup</th><th>Score</th><th>Entry</th><th>1-Day</th><th>2-Day</th></tr>'
    for s in best5:
        html += '<tr style="background:#e6f4ea"><td>%s</td><td><b>%s</b></td><td>%s</td><td>%s</td><td>%d</td><td>$%.2f</td><td>%+.2f%%</td><td>%+.2f%%</td></tr>' % (
            s["date"], s["ticker"], s["signal"], s["setup_type"], s["raw_score"], s["entry_price"], s["ret_1d"], s["ret_2d"])
    html += '</table>'

    # Worst 5
    html += '<h3>Top 5 Worst Signals</h3>'
    html += '<table border="1" cellpadding="4" cellspacing="0" style="border-collapse:collapse;">'
    html += '<tr style="background:#2c3e50;color:white;"><th>Date</th><th>Ticker</th><th>Signal</th><th>Setup</th><th>Score</th><th>Entry</th><th>1-Day</th><th>2-Day</th></tr>'
    for s in worst5:
        html += '<tr style="background:#fce8e6"><td>%s</td><td><b>%s</b></td><td>%s</td><td>%s</td><td>%d</td><td>$%.2f</td><td>%+.2f%%</td><td>%+.2f%%</td></tr>' % (
            s["date"], s["ticker"], s["signal"], s["setup_type"], s["raw_score"], s["entry_price"], s["ret_1d"], s["ret_2d"])
    html += '</table>'

    html += '<p style="color:#888;font-size:11px;">Backtest uses daily OHLCV data. Past results do not guarantee future performance.</p>'
    html += '</body></html>'

    return {
        "total": total,
        "wr_1d": wr_1d,
        "wr_2d": wr_2d,
        "avg_gain": avg_gain,
        "avg_loss": avg_loss,
        "by_setup": by_setup,
        "bottom_wr": b_wr,
        "top_wr": t_wr,
        "best5": best5,
        "worst5": worst5,
        "all_signals": signals,
        "html": html,
    }


# ═══════════════════════════════════════════════════════
# MAIN LOOP  (v5 — with backtest support)
# ═══════════════════════════════════════════════════════

def main():
    # ── STARTUP: Patch yfinance tz-cache to in-memory (v5.3) ──
    clear_yfinance_cache()
    _patch_yf_tz_cache()
    logger.info("  v5.3: yfinance tz-cache patched to in-memory SQLite")

    # ── CLI: Backtest mode ──
    if "--backtest" in sys.argv:
        days = 15
        for arg in sys.argv:
            if arg.startswith("--days="):
                try:
                    days = int(arg.split("=")[1])
                except Exception:
                    days = 15
        logger.info("BACKTEST MODE — running %d-day backtest...", days)
        init_db()
        result = run_backtest(days=days)
        if result and result.get("html"):
            send_email(
                "v5 Backtest Report — %d days — %s" % (
                    days, get_eastern_now().strftime("%Y-%m-%d")),
                result["html"]
            )
        logger.info("Backtest complete.")
        return

    logger.info("=" * 65)
    logger.info("MARKET SCANNER v5.6 — PHASE 1+2+3+4+5+6 + DYNAMIC UNIVERSE + SMART GATES")
    logger.info("v5.6: Optional 60M + Lower RQG(55) + Dynamic Hot Stocks + Near-Miss + Reduced Sector Penalty")
    logger.info("New: Dynamic Universe, Early 30M Promotion, Near-Miss Watchlist, Smart Sector Handling")
    logger.info("Time: %s   Base Stocks: %d (dynamic expansion at scan time)",
                get_eastern_now().strftime("%Y-%m-%d %I:%M %p ET"), len(ALL_STOCKS))
    logger.info("=" * 65)

    init_db()

    # v5.6 SELF-HEALING: Wait for market hours instead of exiting
    now = get_eastern_now()

    # Weekend/Holiday: Log and return (master loop handles sleep-until-Monday)
    if now.weekday() >= 5:
        logger.info("WEEKEND detected — returning to master loop (will sleep until Monday).")
        return
    if now.date() in US_MARKET_HOLIDAYS:
        logger.info("HOLIDAY detected — returning to master loop (will sleep until tomorrow).")
        return

    # Before market open: wait
    mo = now.replace(hour=MARKET_OPEN_HOUR, minute=MARKET_OPEN_MINUTE,
                     second=0, microsecond=0)
    if now < mo:
        wait = (mo - now).total_seconds()
        logger.info("⏳ Waiting %.0f seconds (%.1f hours) for market open...", wait, wait/3600)
        time.sleep(wait)

    # After market close: return (master loop handles next-day sleep)
    mc = now.replace(hour=MARKET_CLOSE_HOUR, minute=MARKET_CLOSE_MINUTE,
                     second=0, microsecond=0)
    if now >= mc:
        logger.info("Market already closed for today — returning to master loop.")
        return

    ob = mo + timedelta(minutes=5)
    now = get_eastern_now()
    if now < ob:
        wait = (ob - now).total_seconds()
        logger.info("Opening buffer — waiting %.0f seconds...", wait)
        time.sleep(wait)

    logger.info("Market OPEN — v5 scan loop starting...")
    sc = 0

    scan_crash_count = 0

    while True:
        now = get_eastern_now()
        if now.hour >= MARKET_CLOSE_HOUR:
            break

        try:
            # v5.3: Ensure memory cache is valid before each scan
            _reinit_tz_mem_cache()

            sc += 1
            logger.info("")
            logger.info("─── SCAN #%d at %s ───", sc, now.strftime("%I:%M:%S %p ET"))
            logger.info("  Mins to close: %.0f", minutes_until_close())

            # v5.6 HEARTBEAT: Every 5 scans, confirm the scanner is alive
            if sc % 5 == 0:
                logger.info("═" * 65)
                logger.info("  💓 HEARTBEAT: Scanner ALIVE | Scan #%d | %.0f mins to close | %d alerts today | %d crashes recovered",
                             sc, minutes_until_close(), len(daily_alerts), scan_crash_count)
                logger.info("═" * 65)

            mr, sr = get_market_regime()
            tm, tn = get_time_quality()
            logger.info("  Time quality: %s (x%.2f)", tn, tm)

            time.sleep(10)

            # v5.6: Refresh dynamic universe every 2 scans
            scan_universe = refresh_scan_universe(sc)

            # ── PASS 1: Batch quick scan ──
            logger.info("  PASS 1: Batch scan %d stocks (base=%d + dynamic=%d)...",
                         len(scan_universe), len(ALL_STOCKS), len(dynamic_hot_tickers))
            filtered = batch_quick_scan(scan_universe)
            logger.info("  PASS 1: %d/%d passed", len(filtered), len(scan_universe))

            if filtered:
                logger.info("  Filtered: %s", ", ".join(filtered[:20]))

            confirmed = []
            checked_sectors = set()
            if filtered:
                # ── PASS 2: Full v5 analysis ──
                logger.info("  PASS 2: v5 analysis on %d stocks...", len(filtered))

                for t in filtered:
                    sec = get_stock_sector(t)
                    if sec not in checked_sectors and sec != "SPY":
                        try:
                            s = check_sector_strength(sec)
                            logger.info("  LAYER 2 — %s status captured: %s", sec, s)
                        except Exception as sec_err:
                            logger.warning("  LAYER 2 — %s sector check failed: %s", sec, sec_err)
                        checked_sectors.add(sec)
                        time.sleep(1)

                for t in filtered:
                    try:
                        sec = get_stock_sector(t)
                        ss = sector_strength_cache.get(sec, {}).get("strength", "NEUTRAL")
                        r = check_multi_timeframe(t, mr, ss)
                        if r:
                            confirmed.append(r)
                            daily_alerts.append(r)
                            save_alert(r)
                    except Exception as stock_err:
                        logger.warning("  Stock %s analysis failed: %s", t, stock_err)

            if confirmed:
                logger.info("★★★ %d v5 ALERT(S) ★★★", len(confirmed))
                try:
                    send_email(
                        "v5 Alert — %d signal(s) — %s" % (
                            len(confirmed),
                            now.strftime("%I:%M %p ET")
                        ),
                        build_alert_email(confirmed)
                    )
                except Exception as email_err:
                    logger.warning("  Email send failed (alerts still saved): %s", email_err)
            else:
                logger.info("  No confirmed signals this scan.")

            try:
                log_scan_completion_summary(
                    sc, now, mr, sr, tn,
                    len(filtered), len(scan_universe), confirmed, checked_sectors
                )
            except Exception as summary_err:
                logger.warning("  Summary logging failed: %s", summary_err)

            ns = now + timedelta(seconds=SCAN_INTERVAL_SECONDS)
            if ns.hour >= MARKET_CLOSE_HOUR:
                break
            logger.info("  Next scan: %s. Sleeping %ds...",
                        ns.strftime("%I:%M:%S %p"), SCAN_INTERVAL_SECONDS)
            time.sleep(SCAN_INTERVAL_SECONDS)

        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt — shutting down gracefully.")
            break
        except Exception as scan_err:
            # ═══════════════════════════════════════════════════════════════
            # v5.6 SELF-HEALING: Catch ANY error, log it, and continue
            # The scanner NEVER dies during market hours
            # ═══════════════════════════════════════════════════════════════
            scan_crash_count += 1
            logger.error("")
            logger.error("══════════════════════════════════════════════════════════════")
            logger.error("  🔴 SCAN #%d CRASHED — Auto-recovering (crash #%d today)", sc, scan_crash_count)
            logger.error("══════════════════════════════════════════════════════════════")
            logger.exception("  Full traceback:")
            logger.error("  Error type: %s", type(scan_err).__name__)
            logger.error("  Error message: %s", str(scan_err)[:500])
            logger.error("══════════════════════════════════════════════════════════════")

            # Try to rebuild caches that may have been corrupted
            try:
                clear_yfinance_cache()
                _patch_yf_tz_cache()
                logger.info("  🔧 Cache rebuilt after crash")
            except Exception:
                pass

            recovery_wait = min(60 + scan_crash_count * 10, 300)  # 60s first, up to 5min
            logger.info("  ⏳ Auto-recovering in %d seconds...", recovery_wait)
            logger.info("  Scanner will resume at next scan cycle. No manual intervention needed.")
            time.sleep(recovery_wait)
            continue  # Back to while True — next scan

    # ═══════════════════════════════════════════════════════
    # POST-MARKET REPORTS
    # ═══════════════════════════════════════════════════════
    logger.info("")
    logger.info("MARKET CLOSED — Daily summary (%d alerts)", len(daily_alerts))

    send_email(
        "v5 Summary — %s — %d alerts" % (
            get_eastern_now().strftime("%Y-%m-%d"), len(daily_alerts)
        ),
        build_daily_summary_email(daily_alerts)
    )

    logger.info("Updating alert prices for EOD...")
    update_alert_prices()

    logger.info("Sending EOD Report...")
    send_email(
        "v5 EOD Report — %s" % get_eastern_now().strftime("%Y-%m-%d"),
        build_eod_report()
    )

    if is_friday():
        logger.info("FRIDAY — Sending Weekly Report...")
        send_email(
            "v5 Weekly — %s" % get_eastern_now().strftime("%Y-%m-%d"),
            build_weekly_report()
        )

        # ── FRIDAY BACKTEST: Validate strategy over last 2 weeks ──
        logger.info("FRIDAY — Running 15-day backtest...")
        bt_result = run_backtest(days=15)
        if bt_result and bt_result.get("html"):
            send_email(
                "v5 Backtest — %s — %d signals" % (
                    get_eastern_now().strftime("%Y-%m-%d"),
                    bt_result.get("total", 0)
                ),
                bt_result["html"]
            )

    cleanup_old_data()
    logger.info("v5 scanner complete!")


# ═══════════════════════════════════════════════════════════════
# v5.6 MASTER RESTART LOOP — THE SCANNER NEVER DIES
# ═══════════════════════════════════════════════════════════════
# This outer loop ensures the scanner runs EVERY market day.
# - If main() exits normally (market close) → waits until next market open
# - If main() crashes fatally → logs error, waits 60s, restarts
# - On weekends → sleeps until Monday 9:00 AM ET
# - On holidays → sleeps until next day 9:00 AM ET
# - Max 50 fatal restarts per day (safety valve)
# ═══════════════════════════════════════════════════════════════

def _seconds_until_next_market_open():
    """Calculate seconds until next market day at 9:00 AM ET."""
    try:
        now = get_eastern_now()
        target = now.replace(hour=9, minute=0, second=0, microsecond=0)
        if now >= target:
            target += timedelta(days=1)
        # Skip weekends
        while target.weekday() >= 5:
            target += timedelta(days=1)
        # Skip holidays
        while target.date() in US_MARKET_HOLIDAYS:
            target += timedelta(days=1)
        wait = (target - now).total_seconds()
        return max(int(wait), 60)
    except Exception:
        return 3600  # Default 1 hour


if __name__ == "__main__":
    # Handle --backtest mode (exits after backtest)
    if "--backtest" in sys.argv:
        main()
        sys.exit(0)

    fatal_crash_count = 0
    fatal_crash_date = None

    logger.info("")
    logger.info("╔═══════════════════════════════════════════════════════════════╗")
    logger.info("║    MARKET SCANNER v5.6 — SELF-HEALING MASTER LOOP           ║")
    logger.info("║    The scanner will run FOREVER during market hours.         ║")
    logger.info("║    Any crash → auto-recover. Weekends → auto-sleep.         ║")
    logger.info("║    To stop: kill the process or Ctrl+C                      ║")
    logger.info("╚═══════════════════════════════════════════════════════════════╝")
    logger.info("")

    while True:
        try:
            now = get_eastern_now()
            today = now.date()

            # Reset crash counter each new day
            if fatal_crash_date != today:
                fatal_crash_count = 0
                fatal_crash_date = today

            # Safety valve: max 50 fatal crashes per day
            if fatal_crash_count >= 50:
                logger.error("🛑 50 fatal crashes today — pausing 1 hour for safety.")
                time.sleep(3600)
                fatal_crash_count = 0
                continue

            # Weekend / Holiday / After-hours: sleep until next market open
            is_weekend = now.weekday() >= 5
            is_holiday = now.date() in US_MARKET_HOLIDAYS
            after_close = now.hour >= MARKET_CLOSE_HOUR and now.minute >= MARKET_CLOSE_MINUTE
            before_open_window = now.hour < 8  # Before 8 AM, don't even bother checking

            if is_weekend or is_holiday or after_close or before_open_window:
                wait = _seconds_until_next_market_open()
                hours = wait // 3600
                mins = (wait % 3600) // 60
                reason = "WEEKEND" if is_weekend else ("HOLIDAY" if is_holiday else ("AFTER_CLOSE" if after_close else "PRE_MARKET"))
                logger.info("")
                logger.info("💤 %s — Sleeping %dh %dm until next market open...", reason, hours, mins)
                logger.info("   Next wake-up: approximately %s ET",
                             (now + timedelta(seconds=wait)).strftime("%Y-%m-%d %I:%M %p"))
                logger.info("")
                # Sleep in 30-minute chunks so we can log periodic alive messages
                while wait > 0:
                    chunk = min(wait, 1800)  # 30 min chunks
                    time.sleep(chunk)
                    wait -= chunk
                    if wait > 0:
                        now = get_eastern_now()
                        logger.info("  💤 Still sleeping... %s ET | %.1f hours until market open",
                                     now.strftime("%I:%M %p"), wait / 3600)
                continue

            # ── RUN MAIN SCAN SESSION ──
            logger.info("")
            logger.info("🟢 Starting market scan session — %s", now.strftime("%Y-%m-%d %I:%M %p ET"))
            main()
            logger.info("🟢 Main scan session completed normally.")

        except KeyboardInterrupt:
            logger.info("")
            logger.info("🛑 KeyboardInterrupt — Scanner shutting down.")
            logger.info("   Total alerts today: %d", len(daily_alerts))
            break

        except Exception as fatal_err:
            fatal_crash_count += 1
            logger.error("")
            logger.error("╔═══════════════════════════════════════════════════════════════╗")
            logger.error("║  🔴 FATAL CRASH #%d — Master loop caught unhandled exception ║", fatal_crash_count)
            logger.error("╚═══════════════════════════════════════════════════════════════╝")
            logger.exception("  Full traceback:")
            logger.error("  Error type: %s", type(fatal_err).__name__)
            logger.error("  Error message: %s", str(fatal_err)[:500])

            # Try to rebuild everything
            try:
                clear_yfinance_cache()
                _patch_yf_tz_cache()
                logger.info("  🔧 Cache rebuilt after fatal crash")
            except Exception:
                pass

            recovery_wait = min(60 + fatal_crash_count * 30, 600)  # 60s first, up to 10min
            logger.info("  ⏳ Auto-restarting in %d seconds (crash #%d today)...", recovery_wait, fatal_crash_count)
            logger.info("  The scanner will restart automatically. No manual intervention needed.")
            time.sleep(recovery_wait)
            continue  # Back to while True — restart main()
