#!/usr/bin/env python3
"""
================================================================
MARKET SCANNER v5.6 - GITHUB ACTIONS RUNNER + NEAR-MISS WATCHLIST + SMART GATES
================================================================
Generates a self-contained HTML dashboard for GitHub Pages with:
  - Live alerts with scores, setup types, MTF status
  - 30-day rolling history (accumulated across runs)
  - Interactive charts (Plotly) for each alerted ticker
  - Options chain + strategies for each alerted ticker
  - All data embedded as JSON - NO API calls needed
  - v5.6: Near-miss watchlist, RQG gate 65->55, expanded universe (400+ stocks)

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

# v5.5: use RQG from market_scanner_v5 if present; otherwise use fallback in this file.
_HAS_RQG = False
try:
    from market_scanner_v5 import evaluate_reversal_quality_gate as _scanner_rqg
    from market_scanner_v5 import RQG_MIN_SCORE as _SCANNER_RQG_MIN_SCORE
    _HAS_RQG = True
    logger.info("v5.5 RQG imported from market_scanner_v5")
except Exception as e:
    _scanner_rqg = None
    _SCANNER_RQG_MIN_SCORE = 55  # v5.6: Lowered from 65
    logger.info("v5.5 RQG not imported; using GitHub fallback: %s", e)

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
# v5.5 GITHUB RQG + SUMMARY HELPERS
# ================================================================
GITHUB_RQG_MIN_SCORE = int(os.getenv("RQG_MIN_SCORE", str(_SCANNER_RQG_MIN_SCORE)))
GITHUB_RQG_A_PLUS_SCORE = 80
GITHUB_RQG_ENFORCE_GATE = os.getenv("RQG_ENFORCE_GATE", "true").lower() not in ("0", "false", "no")
MAX_GITHUB_SUMMARY_HISTORY = 8

def _latest_et_from_df(df):
    try:
        if df is None or df.empty or len(df.index) == 0: return "N/A", 999999
        ts = pd.Timestamp(df.index[-1])
        try:
            import pytz
            et = pytz.timezone("US/Eastern")
            ts = et.localize(ts.to_pydatetime()) if ts.tzinfo is None else ts.tz_convert(et)
            now = get_eastern_now() if SCANNER_IMPORTED else datetime.now(et)
        except Exception:
            now = datetime.now()
        return ts.strftime("%Y-%m-%d %I:%M:%S %p ET"), round(max((now-ts.to_pydatetime()).total_seconds()/60,0),1)
    except Exception:
        return "N/A", 999999

def fetch_fresh_ohlcv(ticker, period="5d", interval="5m"):
    try:
        df = patched_yf_download(ticker, period=period, interval=interval, progress=False, auto_adjust=True, threads=False)
        if df is None or df.empty:
            return None, {"fresh":False,"latest":"N/A","age_min":999999,"rows":0,"source":"yfinance"}
        if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
        latest, age = _latest_et_from_df(df)
        limit = {"1m":5,"2m":8,"5m":12,"15m":25,"30m":45,"60m":90,"1h":90,"1d":10080}.get(interval,30)
        return df, {"fresh": (True if interval=="1d" else age<=limit), "latest":latest, "age_min":age, "limit_min":limit, "rows":len(df), "source":"yfinance"}
    except Exception as e:
        return None, {"fresh":False,"latest":"N/A","age_min":999999,"rows":0,"source":"yfinance","error":str(e)}

def calc_atr(high, low, close, period=14):
    try:
        pc = close.shift(1)
        tr = pd.concat([(high-low),(high-pc).abs(),(low-pc).abs()],axis=1).max(axis=1)
        return tr.rolling(period).mean()
    except Exception:
        return pd.Series(index=close.index, dtype=float)

def evaluate_github_rqg(ticker, signal, regime, sector_strength, r5=None, r15=None, r30=None, r60=None):
    if _HAS_RQG and _scanner_rqg is not None:
        try: return _scanner_rqg(ticker, signal, regime, sector_strength, r5, r15, r30, r60)
        except Exception as e: logger.warning("Imported RQG failed %s: %s", ticker, e)
    out={"score":0,"label":"REJECT","passed":False,"reasons":[],"buckets":{},"details":{}}
    df, meta = fetch_fresh_ohlcv(ticker,"5d","5m")
    if df is None or df.empty or len(df)<60 or not meta.get("fresh"):
        out.update({"label":"REJECT_NO_FRESH_DATA","reasons":["NO_FRESH_5M_DATA"],"details":{"data_meta":meta}}); return out
    try:
        o,h,l,c,v = df["Open"].dropna(),df["High"].dropna(),df["Low"].dropna(),df["Close"].dropna(),df["Volume"].dropna()
        common=c.index.intersection(h.index).intersection(l.index).intersection(o.index).intersection(v.index)
        o,h,l,c,v=o.loc[common],h.loc[common],l.loc[common],c.loc[common],v.loc[common]
        rsi14=calc_rsi(c,14); rsi2=calc_rsi(c,2); _,_,mh=calc_macd(c); bbu,bbm,bbl=calc_bollinger(c); vwap=calc_vwap(h,l,c,v); atr=calc_atr(h,l,c); ema8=calc_ema(c,8)
        last=float(c.iloc[-1]); op=float(o.iloc[-1]); vv=float(vwap.iloc[-1]) if not pd.isna(vwap.iloc[-1]) else last
        std=float((c-vwap).rolling(50).std().iloc[-1]) if len(c)>50 and not pd.isna((c-vwap).rolling(50).std().iloc[-1]) else max(last*.003,.01)
        z=(last-vv)/std; vr=float(v.iloc[-1])/max(float(v.rolling(20).mean().iloc[-1]) if not pd.isna(v.rolling(20).mean().iloc[-1]) else float(v.iloc[-1]),1)
        r2=float(rsi2.iloc[-1]) if not pd.isna(rsi2.iloc[-1]) else 50; r14=float(rsi14.iloc[-1]) if not pd.isna(rsi14.iloc[-1]) else 50
        mh0=float(mh.iloc[-1]) if not pd.isna(mh.iloc[-1]) else 0; mh1=float(mh.iloc[-2]) if not pd.isna(mh.iloc[-2]) else 0
        buckets={"location":0,"momentum":0,"volume":0,"structure":0,"context":0,"rr":0}; reasons=[]; hard=False
        if signal=="BOTTOM":
            if z<=-2: buckets["location"]+=12; reasons.append("DeepBelowVWAP2σ")
            elif z<=-1.25: buckets["location"]+=8; reasons.append("BelowVWAPStretch")
            if not pd.isna(bbl.iloc[-1]) and last<=float(bbl.iloc[-1])*1.005: buckets["location"]+=8; reasons.append("LowerBand")
            if r2<=5: buckets["momentum"]+=6; reasons.append("RSI2Exhausted")
            elif r2<=12: buckets["momentum"]+=3; reasons.append("RSI2Low")
            if mh0>mh1: buckets["momentum"]+=6; reasons.append("MACDImproving")
            if (r5 or {}).get("bdiv"): buckets["momentum"]+=8; reasons.append("BullDiv")
            if vr>=1.5 and last>op: buckets["volume"]+=8; reasons.append("GreenVolumeReversal")
            elif vr>=1.3: buckets["volume"]+=5; reasons.append("VolumeExpansion")
            if (r5 or {}).get("vd",0)>.55: buckets["volume"]+=5; reasons.append("BuyVol")
            if last>float(h.iloc[-2]): buckets["structure"]+=8; reasons.append("CloseAbovePriorHigh")
            if last>float(ema8.iloc[-1]): buckets["structure"]+=4; reasons.append("ReclaimEMA8")
            if last>vv: buckets["structure"]+=5; reasons.append("ReclaimVWAP")
            if sector_strength=="STRONG": buckets["context"]+=7; reasons.append("SectorStrong")
            elif sector_strength=="NEUTRAL": buckets["context"]+=4; reasons.append("SectorNeutral")
            elif (r5 or {}).get("sector_override",0): buckets["context"]+=5; reasons.append("SectorOverride")
            if regime!="BEARISH": buckets["context"]+=5; reasons.append("MarketNotBearish")
            stop=float(l.iloc[-12:].min())-.1*max(float(atr.iloc[-1]) if not pd.isna(atr.iloc[-1]) else last*.003,.01); target=max(vv,float(bbm.iloc[-1]) if not pd.isna(bbm.iloc[-1]) else vv); rr=max(target-last,0)/max(last-stop,.01)
            if sector_strength=="WEAK" and regime=="BEARISH" and last<float(ema8.iloc[-1]): hard=True; reasons.append("TrendDanger")
        else:
            if z>=2: buckets["location"]+=12; reasons.append("DeepAboveVWAP2σ")
            elif z>=1.25: buckets["location"]+=8; reasons.append("AboveVWAPStretch")
            if not pd.isna(bbu.iloc[-1]) and last>=float(bbu.iloc[-1])*.995: buckets["location"]+=8; reasons.append("UpperBand")
            if r2>=95: buckets["momentum"]+=6; reasons.append("RSI2ExhaustedHigh")
            elif r2>=88: buckets["momentum"]+=3; reasons.append("RSI2High")
            if mh0<mh1: buckets["momentum"]+=6; reasons.append("MACDWeakening")
            if (r5 or {}).get("brdiv"): buckets["momentum"]+=8; reasons.append("BearDiv")
            if vr>=1.5 and last<op: buckets["volume"]+=8; reasons.append("RedVolumeReversal")
            elif vr>=1.3: buckets["volume"]+=5; reasons.append("VolumeExpansion")
            if (r5 or {}).get("vd",.5)<.45: buckets["volume"]+=5; reasons.append("SellVol")
            if last<float(l.iloc[-2]): buckets["structure"]+=8; reasons.append("CloseBelowPriorLow")
            if last<float(ema8.iloc[-1]): buckets["structure"]+=4; reasons.append("LostEMA8")
            if last<vv: buckets["structure"]+=5; reasons.append("LostVWAP")
            if sector_strength=="WEAK": buckets["context"]+=7; reasons.append("SectorWeak")
            elif sector_strength=="NEUTRAL": buckets["context"]+=4; reasons.append("SectorNeutral")
            if regime!="BULLISH": buckets["context"]+=5; reasons.append("MarketNotBullish")
            stop=float(h.iloc[-12:].max())+.1*max(float(atr.iloc[-1]) if not pd.isna(atr.iloc[-1]) else last*.003,.01); target=min(vv,float(bbm.iloc[-1]) if not pd.isna(bbm.iloc[-1]) else vv); rr=max(last-target,0)/max(stop-last,.01)
            if sector_strength=="STRONG" and regime=="BULLISH" and last>float(ema8.iloc[-1]): hard=True; reasons.append("TrendDanger")
        if rr>=2: buckets["rr"]=10; reasons.append("RR>=2")
        elif rr>=1.5: buckets["rr"]=7; reasons.append("RR>=1.5")
        elif rr>=1: buckets["rr"]=4; reasons.append("RR>=1")
        caps={"location":20,"momentum":20,"volume":15,"structure":20,"context":15,"rr":10}; buckets={k:min(v,caps[k]) for k,v in buckets.items()}
        score=int(sum(buckets.values())); label="REJECT_TREND_DANGER" if hard else ("A_PLUS_REVERSAL" if score>=GITHUB_RQG_A_PLUS_SCORE else "VALID_REVERSAL" if score>=GITHUB_RQG_MIN_SCORE else "WATCH_ONLY" if score>=50 else "REJECT_LOW_QUALITY")
        out.update({"score":score,"label":label,"passed":(not hard and score>=GITHUB_RQG_MIN_SCORE),"reasons":reasons,"buckets":buckets,"details":{"price":round(last,2),"vwap":round(vv,2),"vwap_z":round(z,2),"rsi2":round(r2,1),"rsi14":round(r14,1),"vol_ratio":round(vr,2),"rr":round(rr,2),"latest":meta.get("latest"),"age_min":meta.get("age_min")}})
        return out
    except Exception as e:
        out.update({"label":"RQG_EXCEPTION","reasons":[str(e)[:100]]}); return out

def sector_snapshot(checked):
    rows=[]
    for sec in sorted(checked):
        c=sector_strength_cache.get(sec,{})
        if c: rows.append({"sector":sec,"strength":c.get("strength","UNKNOWN"),"reason":c.get("reason","Relative strength vs SPY"),"diff":safe_float(c.get("diff",0)),"sector_return":safe_float(c.get("sector_return",0)),"spy_return":safe_float(c.get("spy_return",0)),"session_return":safe_float(c.get("session_return",0)),"session_spy_return":safe_float(c.get("session_spy_return",0)),"close":safe_float(c.get("close",0)),"latest":c.get("latest_et","N/A"),"age_min":c.get("age_min","N/A"),"source":c.get("source","scanner")})
    return rows

def load_summary_history():
    p=os.path.join(script_dir,"docs","scan_summary_history.json")
    if os.path.exists(p):
        try:
            with open(p,"r",encoding="utf-8") as f: data=json.load(f)
            return data if isinstance(data,list) else []
        except Exception: return []
    return []

def save_summary_history(items):
    items=items[-MAX_GITHUB_SUMMARY_HISTORY:]
    p=os.path.join(script_dir,"docs","scan_summary_history.json")
    os.makedirs(os.path.dirname(p),exist_ok=True)
    with open(p,"w",encoding="utf-8") as f: json.dump(items,f,indent=1)
    return items

# ================================================================
# SCAN CYCLE
# ================================================================

def run_single_scan(previous_summaries=None):
    if not SCANNER_IMPORTED:
        return [], "UNKNOWN", 50.0, {"error":"Scanner not imported"}
    previous_summaries = previous_summaries or []
    now = get_eastern_now()
    logger.info("="*70); logger.info("GITHUB SCAN v5.6 RQG+NearMiss - %s", now.strftime("%Y-%m-%d %I:%M %p ET")); logger.info("="*70)
    regime, spy_rsi = get_market_regime(); tq_mult, tq_name = get_time_quality()
    filtered = batch_quick_scan(ALL_STOCKS)
    logger.info("Pass 1: %d/%d passed | Regime=%s RSI=%.1f | TimeQuality=%s", len(filtered), len(ALL_STOCKS), regime, spy_rsi, tq_name)
    checked=set()
    for t in filtered:
        sec=get_stock_sector(t)
        if sec not in checked and sec != "SPY":
            try: check_sector_strength(sec)
            except Exception as e: logger.warning("Sector check failed %s: %s", sec, e)
            checked.add(sec); time.sleep(0.2)
    alerts=[]; blocked=[]; candidates=[]
    for ticker in filtered:
        try:
            sec=get_stock_sector(ticker); ss=sector_strength_cache.get(sec,{}).get("strength","NEUTRAL")
            r5=analyze_stock_v5(ticker,"5m",regime,ss)
            if not r5: continue
            sig=r5["signal"]; c5=r5["confidence"]
            r15=analyze_stock_v5(ticker,"15m",regime,ss)
            if not r15 or r15.get("signal")!=sig:
                candidates.append({"ticker":ticker,"signal":sig,"sector":sec,"stage":"5m_ONLY_15m_NO_MATCH","c5":c5,"price":round(r5.get("cl",0),2)}); continue
            r30=analyze_stock_v5(ticker,"30m",regime,ss)
            if not r30 or r30.get("signal")!=sig:
                candidates.append({"ticker":ticker,"signal":sig,"sector":sec,"stage":"5m+15m_30m_NO_MATCH","c5":c5,"c15":r15.get("confidence",0),"price":round(r5.get("cl",0),2)}); continue
            r60=None; c60=0; mtf="5m+15m+30m"
            try:
                r60=analyze_stock_v5(ticker,"60m",regime,ss)
                if r60 and r60.get("signal")==sig: mtf="5m+15m+30m+60m"; c60=r60.get("confidence",0)
            except Exception: pass
            confs=[c5,r15.get("confidence",0),r30.get("confidence",0)] + ([c60] if c60 else [])
            avg_c=int(sum(confs)/len(confs))
            rqg=evaluate_github_rqg(ticker,sig,regime,ss,r5,r15,r30,r60)
            if GITHUB_RQG_ENFORCE_GATE and not rqg.get("passed",False):
                blocked.append({"ticker":ticker,"signal":sig,"sector":sec,"price":round(r5.get("cl",0),2),"confidence":avg_c,"mtf_status":mtf,"rqg_score":rqg.get("score",0),"rqg_label":rqg.get("label","REJECT"),"rqg_reasons":rqg.get("reasons",[]),"rqg_buckets":rqg.get("buckets",{}),"rqg_details":rqg.get("details",{})}); continue
            try: pcr=get_options_pcr(ticker)
            except Exception: pcr=1.0
            alert={"ticker":ticker,"signal":sig,"confidence":avg_c,"alert_price":round(r5["cl"],2),"rsi":round(r5["rsi"],1),"mfi":round(r5.get("mfi",0),1),"cmf":round(r5.get("cmf",0),3),"pcr":round(pcr,2),"regime":regime,"sector":sec,"sector_strength":ss,"setup_type":r5.get("setup_type","REVERSAL"),"trend_3d":round(r5.get("trend_3d",0),2),"trend_5d":round(r5.get("trend_5d",0),2),"volume_expansion":round(r5.get("volume_expansion",1),2),"signals":list(r5.get("signals",[]))+["RQG=%s/%s"%(rqg.get("score"),rqg.get("label"))]+["RQG_"+x for x in rqg.get("reasons",[])[:4]],"mtf_status":mtf,"c5":c5,"c15":r15.get("confidence",0),"c30":r30.get("confidence",0),"c60":c60,"rqg_score":rqg.get("score",0),"rqg_label":rqg.get("label",""),"rqg_reasons":rqg.get("reasons",[]),"rqg_buckets":rqg.get("buckets",{}),"rqg_details":rqg.get("details",{}),"time":get_eastern_now().strftime("%Y-%m-%d %I:%M:%S %p ET"),"date":now.strftime("%Y-%m-%d"),"result":"PENDING"}
            alerts.append(alert); logger.info("ALERT %s %s conf=%d mtf=%s rqg=%s/%s", ticker, sig, avg_c, mtf, alert["rqg_score"], alert["rqg_label"])
        except Exception as e: logger.warning("Error %s: %s", ticker, e)
    # v5.6: Build near-miss watchlist from blocked (passed MTF, failed RQG) + high-stage candidates
    near_miss = []
    for b in blocked:
        rqg_s = b.get("rqg_score", 0)
        conf = b.get("confidence", 0)
        if rqg_s >= 40:
            quality = "***" if conf >= 90 else ("**" if conf >= 80 else "*")
            sec_str = sector_strength_cache.get(b.get("sector", "SPY"), {}).get("strength", "?")
            near_miss.append({
                "ticker": b["ticker"], "signal": b["signal"], "c5": conf,
                "sector": b.get("sector", "?"), "sector_strength": sec_str,
                "stage": "RQG_BLOCKED_%s" % b.get("rqg_label", "?"),
                "price": b.get("price", 0), "rqg_score": rqg_s,
                "rqg_label": b.get("rqg_label", "?"), "quality": quality,
                "mtf_status": b.get("mtf_status", "?"),
            })
    for c in candidates:
        if c.get("c5", 0) >= 75 and "5m_ONLY" not in c.get("stage", ""):
            quality = "**" if c.get("c5", 0) >= 85 else "*"
            sec_str = sector_strength_cache.get(c.get("sector", "SPY"), {}).get("strength", "?")
            near_miss.append({
                "ticker": c["ticker"], "signal": c["signal"], "c5": c.get("c5", 0),
                "sector": c.get("sector", "?"), "sector_strength": sec_str,
                "stage": c.get("stage", "?"), "price": c.get("price", 0),
                "rqg_score": 0, "rqg_label": "NOT_REACHED", "quality": quality,
                "mtf_status": "partial",
            })
    near_miss.sort(key=lambda x: (x.get("c5", 0), x.get("rqg_score", 0)), reverse=True)
    near_miss = near_miss[:25]
    logger.info("Near-miss watchlist: %d (from %d blocked, %d candidates)", len(near_miss), len(blocked), len(candidates))

    sectors=sorted(sector_snapshot(checked), key=lambda x:x.get("diff",0), reverse=True)
    summary={"scan_time":now.strftime("%Y-%m-%d %I:%M:%S %p ET"),"date":now.strftime("%Y-%m-%d"),"regime":regime,"spy_rsi":round(float(spy_rsi),1),"time_quality":tq_name,"stocks_scanned":len(ALL_STOCKS),"filtered":len(filtered),"alerts":len(alerts),"blocked":len(blocked),"candidates":candidates[:80],"blocked_alerts":blocked[:100],"sectors":sectors,"rqg_gate":GITHUB_RQG_ENFORCE_GATE,"rqg_threshold":GITHUB_RQG_MIN_SCORE,"leaders_recent":sectors[:3],"laggards_recent":sectors[-3:] if sectors else [],"previous_scan_time":(previous_summaries[-1].get("scan_time") if previous_summaries else "N/A"),"near_miss":near_miss,"near_miss_count":len(near_miss)}
    return alerts, regime, spy_rsi, summary


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
    """Persist rolling 30-day alert history for GitHub Pages. De-dupes repeated workflow runs."""
    cutoff = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    dedup = {}
    for h in history or []:
        if h.get("date", "") < cutoff:
            continue
        key = (h.get("date",""), h.get("ticker",""), h.get("signal",""), h.get("time") or h.get("alert_time",""), h.get("alert_price", h.get("price","")))
        dedup[key] = h
    history = sorted(dedup.values(), key=lambda x: (x.get("date",""), x.get("time") or x.get("alert_time", "")))
    p = os.path.join(script_dir, "docs", "scan_history.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=1)
    return history


# ================================================================
# HTML GENERATOR
# ================================================================


def _html_escape(x):
    return escape(str(x))

def _pct(v):
    try:
        return f"{float(v):+.2f}%"
    except Exception:
        return "N/A"

def _group_counts(rows, key):
    out = {}
    for r in rows or []:
        k = r.get(key, "UNKNOWN") or "UNKNOWN"
        out[k] = out.get(k, 0) + 1
    return out

def _is_email_window(now=None):
    """Return MIDDAY/EOD/None. GitHub Actions can run frequently; this prevents noisy sends."""
    now = now or (get_eastern_now() if SCANNER_IMPORTED else datetime.now())
    mins = now.hour * 60 + now.minute
    if 13*60 <= mins <= 14*60 + 15:
        return "MIDDAY"
    if 16*60 + 25 <= mins <= 17*60 + 15:
        return "EOD"
    return None

def _load_email_state():
    p = os.path.join(script_dir, "docs", "email_state.json")
    try:
        return json.load(open(p, "r", encoding="utf-8")) if os.path.exists(p) else {}
    except Exception:
        return {}

def _save_email_state(state):
    p = os.path.join(script_dir, "docs", "email_state.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)

def should_send_report_email(force=False):
    if force or os.getenv("FORCE_EMAIL", "").lower() in ("1", "true", "yes"):
        return "FORCED"
    if os.getenv("SEND_EMAIL_REPORT", "true").lower() in ("0", "false", "no"):
        return None
    now = get_eastern_now() if SCANNER_IMPORTED else datetime.now()
    window = _is_email_window(now)
    if not window:
        return None
    key = now.strftime("%Y-%m-%d") + "_" + window
    state = _load_email_state()
    if state.get("last_sent_key") == key:
        logger.info("Email report already sent for %s", key)
        return None
    state["last_sent_key"] = key
    state["last_sent_time"] = now.strftime("%Y-%m-%d %I:%M:%S %p ET")
    _save_email_state(state)
    return window

def save_quality_summary_json(scan_summary, summary_history):
    docs_dir = os.path.join(script_dir, "docs")
    os.makedirs(docs_dir, exist_ok=True)
    payload = dict(scan_summary or {})
    payload["summary_history"] = summary_history[-30:] if summary_history else []
    with open(os.path.join(docs_dir, "scan_quality_summary.json"), "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

def build_report_email_html(alerts, history, scan_summary, summary_history):
    """Compact email version of the GitHub dashboard."""
    def rows(items, cols, limit=50):
        if not items:
            return "<tr><td colspan='%d' style='padding:14px;color:#666'>No rows</td></tr>" % len(cols)
        h = ""
        for r in items[:limit]:
            h += "<tr>" + "".join("<td style='padding:7px;border-top:1px solid #ddd'>%s</td>" % _html_escape(fn(r)) for _, fn in cols) + "</tr>"
        return h
    sectors = scan_summary.get("sectors", [])
    candidates = scan_summary.get("candidates", [])
    blocked = scan_summary.get("blocked_alerts", [])
    title = f"Market Scanner {scan_summary.get('scan_time','')} — {scan_summary.get('regime','')}"
    return f"""
    <html><body style='font-family:Arial,sans-serif;color:#111'>
    <h2>{_html_escape(title)}</h2>
    <p><b>Regime:</b> {_html_escape(scan_summary.get('regime',''))} | <b>SPY RSI:</b> {_html_escape(scan_summary.get('spy_rsi',''))} | <b>Filtered:</b> {_html_escape(scan_summary.get('filtered',''))} | <b>Alerts:</b> {_html_escape(scan_summary.get('alerts',''))} | <b>Blocked:</b> {_html_escape(scan_summary.get('blocked',''))} | <b>Pending:</b> {len(candidates)}</p>
    <h3>Sector Leaders / Laggards</h3>
    <p><b>Leaders:</b> {_html_escape(', '.join([x.get('sector','')+' '+_pct(x.get('diff')) for x in scan_summary.get('leaders_recent', [])]))}<br>
    <b>Laggards:</b> {_html_escape(', '.join([x.get('sector','')+' '+_pct(x.get('diff')) for x in scan_summary.get('laggards_recent', [])]))}</p>
    <h3>Confirmed Alerts</h3><table cellspacing='0' cellpadding='0' style='border-collapse:collapse;width:100%'><tr><th>Ticker</th><th>Signal</th><th>Price</th><th>RQG</th><th>Sector</th></tr>{rows(alerts, [('Ticker',lambda r:r.get('ticker','')),('Signal',lambda r:r.get('signal','')),('Price',lambda r:r.get('alert_price','')),('RQG',lambda r:str(r.get('rqg_score',''))+' '+r.get('rqg_label','')),('Sector',lambda r:r.get('sector',''))], 100)}</table>
    <h3>Sector State</h3><table cellspacing='0' cellpadding='0' style='border-collapse:collapse;width:100%'><tr><th>Sector</th><th>State</th><th>Recent Rel</th><th>Day</th><th>Reason</th></tr>{rows(sectors, [('Sector',lambda r:r.get('sector','')),('State',lambda r:r.get('strength','')),('Recent Rel',lambda r:_pct(r.get('diff'))),('Day',lambda r:_pct(r.get('session_return'))),('Reason',lambda r:r.get('reason',''))], 20)}</table>
    <h3>Waiting / Tracking Candidates</h3><p>By stage: {_html_escape(_group_counts(candidates,'stage'))}<br>By sector: {_html_escape(_group_counts(candidates,'sector'))}</p>
    <h3>Last Runs</h3><table cellspacing='0' cellpadding='0' style='border-collapse:collapse;width:100%'><tr><th>Time</th><th>Regime</th><th>RSI</th><th>Filtered</th><th>Alerts</th><th>Blocked</th></tr>{rows(list(reversed(summary_history[-8:])), [('Time',lambda r:r.get('scan_time','')),('Regime',lambda r:r.get('regime','')),('RSI',lambda r:r.get('spy_rsi','')),('Filtered',lambda r:r.get('filtered','')),('Alerts',lambda r:r.get('alerts','')),('Blocked',lambda r:r.get('blocked',''))], 8)}</table>
    <p style='color:#666;font-size:12px'>Generated by GitHub Actions. Not financial advice.</p>
    </body></html>"""

def generate_html(alerts, regime, spy_rsi, history, charts, options_data, scan_summary=None, summary_history=None):
    now = get_eastern_now() if SCANNER_IMPORTED else datetime.utcnow()
    scan_time = now.strftime("%Y-%m-%d %I:%M:%S %p ET")
    scan_date = now.strftime("%Y-%m-%d")
    scan_summary = scan_summary or {}; summary_history = summary_history or []
    alerts_sorted = sorted(alerts, key=lambda a:(a.get("rqg_score",0),a.get("confidence",0)), reverse=True)
    cutoff = (now - timedelta(days=30)).strftime("%Y-%m-%d")
    history_30 = [h for h in history if h.get("date", "") >= cutoff]
    hist_wins=sum(1 for h in history_30 if h.get("result")=="WIN"); hist_losses=sum(1 for h in history_30 if h.get("result")=="LOSS")
    hist_wr=round(hist_wins/max(hist_wins+hist_losses,1)*100,1)
    scan_summary.setdefault("pending_total", len(scan_summary.get("candidates", [])))
    scan_summary.setdefault("pending_by_stage", _group_counts(scan_summary.get("candidates", []), "stage"))
    scan_summary.setdefault("pending_by_sector", _group_counts(scan_summary.get("candidates", []), "sector"))
    data_json=json.dumps({"alerts":alerts_sorted,"history":history_30,"charts":charts,"options":options_data,"summary":scan_summary,"summaryHistory":summary_history[-30:]}).replace("</","<\\/")
    html = """<!DOCTYPE html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><meta http-equiv='refresh' content='900'><title>TopBottom v5.6 GitHub Quality Summary __DATE__</title><script src='https://cdn.plot.ly/plotly-2.27.0.min.js'></script><style>
:root{--bg:#070b12;--panel:#111827;--panel2:#0b1220;--line:#243044;--text:#e5edf7;--muted:#8ba0b8;--cyan:#06b6d4;--green:#22c55e;--red:#ef4444;--yellow:#eab308;--blue:#60a5fa}*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font-family:Segoe UI,Arial,sans-serif;font-variant-numeric:tabular-nums}.top{position:sticky;top:0;background:#09111f;border-bottom:1px solid var(--line);padding:14px 22px;z-index:10}h1{margin:0;color:var(--cyan);font-size:20px}.sub{color:var(--muted);font-size:12px}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:14px;padding:18px 22px}.card,.box{background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:15px}.label{color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.04em}.value{font-size:28px;font-weight:800}.tabs{display:flex;gap:2px;padding:0 22px;background:#09111f;border-bottom:1px solid var(--line);flex-wrap:wrap}.tab{padding:13px 18px;color:var(--muted);cursor:pointer;font-weight:700;border-bottom:3px solid transparent}.tab.active{color:var(--cyan);border-bottom-color:var(--cyan)}.page{display:none;padding:20px 22px}.page.active{display:block}table{width:100%;border-collapse:collapse;background:var(--panel);border:1px solid var(--line);font-size:13px}th{background:var(--panel2);color:var(--muted);text-align:left;padding:10px;text-transform:uppercase;font-size:11px;position:sticky;top:118px}td{padding:10px;border-top:1px solid #263247;vertical-align:top}.pill{padding:3px 9px;border-radius:99px;font-weight:800;font-size:11px;display:inline-block}.bull{background:#22c55e22;color:var(--green)}.bear{background:#ef444422;color:var(--red)}.neutral{background:#eab30822;color:var(--yellow)}.ticker{color:var(--cyan);font-weight:900}.pos{color:var(--green)}.neg{color:var(--red)}.chips span{display:inline-block;margin:2px;padding:3px 7px;border-radius:8px;background:#1e293b;color:#cbd5e1;font-size:11px}.chartbox{height:560px;background:var(--panel);border:1px solid var(--line);border-radius:12px;margin-top:12px}select,input{background:var(--panel2);color:var(--text);border:1px solid var(--line);border-radius:8px;padding:8px}.two{display:grid;grid-template-columns:repeat(auto-fit,minmax(360px,1fr));gap:14px}.reason{max-width:620px;white-space:normal}.small{font-size:12px;color:var(--muted)}
</style></head><body><div class='top'><h1>TopBottom Universal v5.6 — GitHub Quality Gate + Near-Miss Dashboard</h1><div class='sub'>Generated __SCAN_TIME__ • Regime __REGIME__ • SPY RSI __SPY_RSI__ • Output docs/TopBottom_Universal.html</div></div>
<div class='grid'><div class='card'><div class='label'>RQG Passed</div><div class='value'>__ALERT_COUNT__</div></div><div class='card'><div class='label'>RQG Blocked</div><div class='value'>__BLOCKED__</div></div><div class='card'><div class='label'>Filtered / Universe</div><div class='value'>__FILTERED__</div><div class='sub'>of __SCANNED__ scanned</div></div><div class='card'><div class='label'>Pending / Tracking</div><div class='value'>__PENDING__</div></div><div class='card'><div class='label'>30-Day Win Rate</div><div class='value'>__WR__%</div><div class='sub'>W __WINS__ / L __LOSSES__ • __HISTCOUNT__ records</div></div><div class='card'><div class='label'>RQG Gate</div><div class='value'>__GATE__</div><div class='sub'>Threshold __THRESH__</div></div></div>
<div class='tabs'><div class='tab active' onclick="show('summary',this)">Summary</div><div class='tab' onclick="show('alerts',this)">RQG Alerts</div><div class='tab' onclick="show('sectors',this)">Sectors</div><div class='tab' onclick="show('blocked',this)">Blocked/Waiting</div><div class='tab' onclick="show('charts',this)">Charts</div><div class='tab' onclick="show('options',this)">Options</div><div class='tab' onclick="show('history',this)">30-Day History</div><div class='tab' onclick="show('logs',this)">Readable Log</div></div>
<div id='summary' class='page active'></div><div id='alerts' class='page'></div><div id='sectors' class='page'></div><div id='blocked' class='page'></div><div id='charts' class='page'><select id='chartSel' onchange='drawChart()'></select><div id='chart' class='chartbox'></div></div><div id='options' class='page'><select id='optSel' onchange='renderOptions()'></select><div id='optBox'></div></div><div id='history' class='page'></div><div id='logs' class='page'></div>
<script>const DATA=__DATA__;function esc(x){return String(x??'').replace(/[&<>]/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[m]))}function show(id,el){document.querySelectorAll('.page').forEach(x=>x.classList.remove('active'));document.querySelectorAll('.tab').forEach(x=>x.classList.remove('active'));document.getElementById(id).classList.add('active');if(el)el.classList.add('active');if(id==='charts')drawChart();if(id==='options')renderOptions()}function pct(v){let n=Number(v);return isFinite(n)?(n>=0?'+':'')+n.toFixed(2)+'%':'N/A'}function money(v){let n=Number(v);return isFinite(n)&&n!==0?'$'+n.toFixed(2):'N/A'}function cls(v){return v==='STRONG'||v==='BOTTOM'||v==='WIN'?'bull':v==='WEAK'||v==='TOP'||v==='LOSS'?'bear':'neutral'}function pill(v){return '<span class="pill '+cls(v)+'">'+esc(v)+'</span>'}function tbl(cols,rows){if(!rows||!rows.length)return '<div class="box">No rows.</div>';let h='<table><thead><tr>'+cols.map(c=>'<th>'+c[0]+'</th>').join('')+'</tr></thead><tbody>';for(const r of rows)h+='<tr>'+cols.map(c=>'<td>'+c[1](r)+'</td>').join('')+'</tr>';return h+'</tbody></table>'}function chips(obj){if(!obj)return '';if(Array.isArray(obj))return '<div class=chips>'+obj.map(x=>'<span>'+esc(x)+'</span>').join('')+'</div>';return '<div class=chips>'+Object.entries(obj).map(([k,v])=>'<span>'+esc(k)+': <b>'+esc(v)+'</b></span>').join('')+'</div>'}
function renderSummary(){let s=DATA.summary,h=DATA.summaryHistory||[];let wholeLeaders=(s.sectors||[]).slice().sort((a,b)=>Number(b.session_return||0)-Number(a.session_return||0)).slice(0,3);let wholeLag=(s.sectors||[]).slice().sort((a,b)=>Number(a.session_return||0)-Number(b.session_return||0)).slice(0,3);document.getElementById('summary').innerHTML=`<div class='box'><b>Current scan:</b> ${esc(s.scan_time)} | Regime <b>${esc(s.regime)}</b> | RSI <b>${esc(s.spy_rsi)}</b> | TimeQuality <b>${esc(s.time_quality)}</b> | Passed <b>${esc(s.filtered)}/${esc(s.stocks_scanned)}</b> | Alerts <b>${esc(s.alerts)}</b> | Blocked <b>${esc(s.blocked)}</b> | RQG <b>${s.rqg_gate?'ON':'OBS'}/${esc(s.rqg_threshold)}</b></div><div class='two'><div class='box'><h3>Recent Relative Strength</h3><b>Leaders:</b> ${(s.leaders_recent||[]).map(x=>esc(x.sector)+' '+pill(x.strength)+' '+pct(x.diff)).join(', ')||'N/A'}<br><b>Laggards:</b> ${(s.laggards_recent||[]).map(x=>esc(x.sector)+' '+pill(x.strength)+' '+pct(x.diff)).join(', ')||'N/A'}</div><div class='box'><h3>Whole-Day View</h3><b>Leaders:</b> ${wholeLeaders.map(x=>esc(x.sector)+' '+pct(x.session_return)).join(', ')||'N/A'}<br><b>Laggards:</b> ${wholeLag.map(x=>esc(x.sector)+' '+pct(x.session_return)).join(', ')||'N/A'}</div></div><div class='two'><div class='box'><h3>Pending by Stage</h3>${chips(s.pending_by_stage)}</div><div class='box'><h3>Pending by Sector</h3>${chips(s.pending_by_sector)}</div></div><h2>Last GitHub Run Comparison</h2>`+tbl([['Run',r=>esc(r.scan_time)],['Regime',r=>pill(r.regime)],['RSI',r=>esc(r.spy_rsi)],['Filtered',r=>esc(r.filtered)],['Alerts',r=>esc(r.alerts)],['Blocked',r=>esc(r.blocked)],['Pending',r=>esc((r.candidates||[]).length||r.pending_total||0)],['Top sectors',r=>(r.leaders_recent||[]).slice(0,2).map(x=>esc(x.sector)+' '+pct(x.diff)).join(', ')],['Laggards',r=>(r.laggards_recent||[]).slice(-2).map(x=>esc(x.sector)+' '+pct(x.diff)).join(', ')]],h.slice().reverse())}
function renderAlerts(){document.getElementById('alerts').innerHTML=tbl([['Ticker',r=>'<span class=ticker>'+esc(r.ticker)+'</span>'],['Signal',r=>pill(r.signal)],['Price',r=>money(r.alert_price)],['Conf',r=>esc(r.confidence)],['RQG',r=>'<b>'+esc(r.rqg_score)+'</b> '+esc(r.rqg_label)],['MTF',r=>esc(r.mtf_status)],['Sector',r=>esc(r.sector)+' '+pill(r.sector_strength)],['Reason Tags',r=>chips((r.rqg_reasons||[]).slice(0,10))]],DATA.alerts)}
function renderSectors(){document.getElementById('sectors').innerHTML=tbl([['Sector',r=>'<span class=ticker>'+esc(r.sector)+'</span>'],['State',r=>pill(r.strength)],['Recent Rel',r=>pct(r.diff)],['Sector Recent',r=>pct(r.sector_return)],['SPY Recent',r=>pct(r.spy_return)],['Whole Day',r=>pct(r.session_return)],['SPY Day',r=>pct(r.session_spy_return)],['Close',r=>money(r.close)],['Data',r=>esc(r.source||'')+' age '+esc(r.age_min||'N/A')+'m<br><span class=small>'+esc(r.latest||'')+'</span>'],['Reason',r=>'<div class=reason>'+esc(r.reason||'')+'</div>']],DATA.summary.sectors)}
function renderBlocked(){document.getElementById('blocked').innerHTML='<h2>Blocked by RQG</h2>'+tbl([['Ticker',r=>'<span class=ticker>'+esc(r.ticker)+'</span>'],['Signal',r=>pill(r.signal)],['Sector',r=>esc(r.sector)],['Price',r=>money(r.price)],['RQG',r=>esc(r.rqg_score)+' '+esc(r.rqg_label)],['Reasons',r=>chips(r.rqg_reasons||[])]],DATA.summary.blocked_alerts)+'<h2>Waiting / Tracking Candidates</h2>'+tbl([['Ticker',r=>'<span class=ticker>'+esc(r.ticker)+'</span>'],['Signal',r=>pill(r.signal)],['Sector',r=>esc(r.sector)],['Stage',r=>esc(r.stage)],['C5',r=>esc(r.c5)],['C15',r=>esc(r.c15||'')],['Price',r=>money(r.price)]],DATA.summary.candidates)}
function renderHistory(){let h=(DATA.history||[]).slice().reverse();document.getElementById('history').innerHTML='<div class=box><b>30-day rolling history:</b> '+h.length+' records. This data is loaded from docs/scan_history.json and preserved across GitHub runs when the workflow commits docs updates.</div>'+tbl([['Date',r=>esc(r.date)],['Time',r=>esc(r.time||r.alert_time||'')],['Ticker',r=>'<span class=ticker>'+esc(r.ticker)+'</span>'],['Signal',r=>pill(r.signal)],['Alert $',r=>money(r.alert_price)],['Current $',r=>money(r.current_price)],['Result',r=>pill(r.result||'PENDING')],['P&L %',r=>pct(r.change_pct||r.pnl_pct)],['RQG',r=>esc(r.rqg_score||'')],['Setup',r=>esc(r.setup_type||'')]],h)}
function renderLogs(){let s=DATA.summary;document.getElementById('logs').innerHTML='<h2>Readable End-of-Scan Log</h2><div class=box><pre style="white-space:pre-wrap;line-height:1.45">'+esc(JSON.stringify({scan_time:s.scan_time,regime:s.regime,spy_rsi:s.spy_rsi,time_quality:s.time_quality,passed:String(s.filtered)+'/'+String(s.stocks_scanned),alerts:s.alerts,blocked:s.blocked,pending_total:s.pending_total,pending_by_stage:s.pending_by_stage,pending_by_sector:s.pending_by_sector,sector_state:s.sectors,last_runs:DATA.summaryHistory},null,2))+'</pre></div>'}
function initSelectors(){let c=Object.keys(DATA.charts||{}), cs=document.getElementById('chartSel');cs.innerHTML=c.map(x=>'<option>'+esc(x)+'</option>').join('');let o=Object.keys(DATA.options||{}), os=document.getElementById('optSel');os.innerHTML=o.map(x=>'<option>'+esc(x)+'</option>').join('')}function drawChart(){let t=document.getElementById('chartSel').value,d=DATA.charts[t];if(!d){document.getElementById('chart').innerHTML='No chart data';return}let traces=[{x:d.timestamps,open:d.open,high:d.high,low:d.low,close:d.close,type:'candlestick',name:t},{x:d.timestamps,y:d.vwap,type:'scatter',name:'VWAP'},{x:d.timestamps,y:d.ema9,type:'scatter',name:'EMA9'},{x:d.timestamps,y:d.ema21,type:'scatter',name:'EMA21'}];Plotly.newPlot('chart',traces,{paper_bgcolor:'#111827',plot_bgcolor:'#111827',font:{color:'#e5edf7'},title:t+' intraday chart',xaxis:{rangeslider:{visible:false}}},{responsive:true})}function renderOptions(){let t=document.getElementById('optSel').value,o=DATA.options[t];if(!o){document.getElementById('optBox').innerHTML='<div class=box>No options data</div>';return}document.getElementById('optBox').innerHTML='<div class=box><b>'+esc(t)+'</b> Spot '+money(o.spot)+' Exp '+esc(o.expiration)+' DTE '+esc(o.dte)+' IV '+esc(o.avg_iv)+'% PCR '+esc(o.pcr)+'</div><h3>Suggested Strategies</h3>'+tbl([['Name',r=>esc(r.name)],['Cost',r=>esc(r.cost)],['Max Profit',r=>esc(r.max_profit)],['Breakeven',r=>esc(r.breakeven)],['Legs',r=>(r.legs||[]).map(l=>esc(l.action+' '+l.type+' '+l.strike+' @ '+l.price)).join('<br>')]],o.strategies||[])+'<h3>Unusual Options</h3>'+tbl([['Type',r=>esc(r.type)],['Strike',r=>esc(r.strike)],['Vol',r=>esc(r.volume)],['OI',r=>esc(r.oi)],['Ratio',r=>esc(r.ratio)]],o.unusual||[])}
renderSummary();renderAlerts();renderSectors();renderBlocked();renderHistory();renderLogs();initSelectors();</script></body></html>"""
    repl = {
        "__DATE__": scan_date, "__SCAN_TIME__": scan_time, "__REGIME__": str(regime), "__SPY_RSI__": f"{spy_rsi:.1f}",
        "__ALERT_COUNT__": str(len(alerts_sorted)), "__BLOCKED__": str(scan_summary.get('blocked',0)), "__FILTERED__": str(scan_summary.get('filtered',0)),
        "__SCANNED__": str(scan_summary.get('stocks_scanned',0)), "__PENDING__": str(scan_summary.get('pending_total',0)), "__WR__": str(hist_wr),
        "__WINS__": str(hist_wins), "__LOSSES__": str(hist_losses), "__HISTCOUNT__": str(len(history_30)),
        "__GATE__": ('ON' if scan_summary.get('rqg_gate',True) else 'OBS'), "__THRESH__": str(scan_summary.get('rqg_threshold',65)), "__DATA__": data_json
    }
    for k,v in repl.items(): html = html.replace(k,v)
    return html

def main():
    logger.info("=" * 65)
    logger.info("MARKET SCANNER v5.6 - GITHUB ACTIONS RQG + NEAR-MISS DASHBOARD")
    logger.info("=" * 65)
    summary_history = load_summary_history()
    alerts, regime, spy_rsi, scan_summary = run_single_scan(summary_history)
    charts = {}
    tickers = [a["ticker"] for a in alerts[:20]] + [b["ticker"] for b in scan_summary.get("blocked_alerts", [])[:10]] + [nm["ticker"] for nm in scan_summary.get("near_miss", [])[:10]]
    for t in list(dict.fromkeys(tickers)):
        cd = fetch_chart_data(t)
        if cd: charts[t] = cd
        time.sleep(0.2)
    options = {}
    for a in alerts[:15]:
        t=a["ticker"]
        od = fetch_options_data(t, a.get("signal", "BOTTOM"))
        if od: options[t] = od
        time.sleep(0.2)
    history = load_history()
    for a in alerts: history.append(a)
    history = save_history(history)
    summary_history.append(scan_summary)
    summary_history = save_summary_history(summary_history)
    save_quality_summary_json(scan_summary, summary_history)
    html = generate_html(alerts, regime, spy_rsi, history, charts, options, scan_summary, summary_history)
    docs_dir = os.path.join(script_dir, "docs"); os.makedirs(docs_dir, exist_ok=True)
    output_path = os.path.join(docs_dir, "TopBottom_Universal.html")
    with open(output_path, "w", encoding="utf-8") as f: f.write(html)
    with open(os.path.join(docs_dir, "latest_scan.json"), "w", encoding="utf-8") as f:
        json.dump({"scan_time": scan_summary.get("scan_time"), "regime": regime, "spy_rsi": spy_rsi, "total_alerts": len(alerts), "blocked": scan_summary.get("blocked",0), "stocks_scanned": len(ALL_STOCKS) if ALL_STOCKS else 260, "near_miss": scan_summary.get("near_miss", []), "near_miss_count": scan_summary.get("near_miss_count", 0), "alerts": alerts, "summary": scan_summary}, f, indent=2)
    email_window = should_send_report_email(force=False)
    if email_window:
        try:
            email_html = build_report_email_html(alerts, history, scan_summary, summary_history)
            send_email(f"{email_window} Market Scanner GitHub Report - {scan_summary.get('scan_time','')} - {len(alerts)} alerts", email_html)
        except Exception as e:
            logger.error("Scheduled report email failed: %s", e)
    logger.info("Dashboard: %s (%d bytes)", output_path, len(html))
    logger.info("GitHub Actions RQG scan complete!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
