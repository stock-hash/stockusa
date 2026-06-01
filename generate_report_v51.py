#!/usr/bin/env python3
"""generate_report_v51.py - Full GitHub Actions scanner with persistent state & dashboard"""
import yfinance as yf, pandas as pd, numpy as np
import json, os, sys, time, random, warnings, smtplib
from datetime import datetime, timedelta, date
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
warnings.filterwarnings("ignore")
try:
    import pytz; ET = pytz.timezone("US/Eastern")
except: print("pip install pytz"); sys.exit(1)

DOCS = "docs"
ALERTS_TODAY = os.path.join(DOCS, "alerts_today.json")
ALERTS_HISTORY = os.path.join(DOCS, "alerts_history.json")
LATEST_SCAN = os.path.join(DOCS, "latest_scan.json")
HTML_OUT = os.path.join(DOCS, "TopBottom_Universal.html")
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS", "")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "")
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
RECIPIENT_EMAILS = [e.strip() for e in os.getenv("RECIPIENT_EMAILS", "").split(",") if e.strip()]

US_HOLIDAYS = [
    date(2025,1,1),date(2025,1,20),date(2025,2,17),date(2025,4,18),date(2025,5,26),
    date(2025,6,19),date(2025,7,4),date(2025,9,1),date(2025,11,27),date(2025,12,25),
    date(2026,1,1),date(2026,1,19),date(2026,2,16),date(2026,4,3),date(2026,5,25),
    date(2026,6,19),date(2026,7,3),date(2026,9,7),date(2026,11,26),date(2026,12,25),
    date(2027,1,1),date(2027,1,18),date(2027,2,15),date(2027,3,26),date(2027,5,31),
]

DOW_30 = [
    "AAPL","MSFT","AMZN","NVDA","UNH","V","JNJ","WMT","JPM","PG",
    "MA","HD","MRK","CVX","KO","DIS","MCD","CSCO","ABT","VZ",
    "NKE","INTC","SHW","DOW","MMM","TRV","AXP","BA","CAT","GS"
]
SP500_TOP = [
    "GOOGL","META","BRK-B","TSLA","XOM","LLY","ABBV","PEP","COST",
    "AVGO","TMO","ADBE","CRM","TXN","NFLX","AMD","NEE","QCOM",
    "ISRG","INTU","AMAT","BKNG","GE","SYK","BLK","GILD","VRTX",
    "REGN","PANW","KLAC","SNPS","CDNS","PGR","CME","CI","ICE",
    "FDX","EMR","ITW","CTAS","ORLY","FICO","CPRT","CEG","CARR",
    "GWW","AXON","DECK","GEHC","FSLR","URI"
]
TOP_ETFS = [
    "SPY","QQQ","IWM","DIA","XLK","XLE","XLF","XLV","XLI","XLU",
    "XLB","XLY","XLP","XLRE","XLC","SMH","SOXX","GDX","GLD","SLV",
    "TLT","HYG","ARKK","XBI","KWEB","EEM","VNQ","IBB","KRE","XOP"
]
LEVERAGED = [
    "TQQQ","SQQQ","SPXL","SPXS","TNA","TZA","SOXL","SOXS",
    "FNGU","FNGD","TECL","TECS","FAS","FAZ","UPRO","SPXU"
]
ALL_STOCKS = list(dict.fromkeys(DOW_30 + SP500_TOP + TOP_ETFS + LEVERAGED))

def now_et(): return datetime.now(ET)

def is_market_open():
    n = now_et()
    if n.weekday() >= 5: return False
    if n.date() in US_HOLIDAYS: return False
    t = n.hour * 60 + n.minute
    return 570 <= t <= 960

def safe_val(s, default=0):
    try:
        v = s.iloc[-1]
        return default if pd.isna(v) else float(v)
    except: return default

def safe_download(ticker, period, interval, retries=2):
    for i in range(retries):
        try:
            time.sleep(0.8 + random.uniform(0.3, 1.0))
            d = yf.download(ticker, period=period, interval=interval,
                            progress=False, auto_adjust=True, threads=False)
            if d is not None and not d.empty:
                if isinstance(d.columns, pd.MultiIndex):
                    d.columns = d.columns.get_level_values(0)
                return d
        except: time.sleep(2)
    return None

def calc_rsi(s, p=14):
    d = s.diff(); g = d.where(d > 0, 0.0); lo = -d.where(d < 0, 0.0)
    ag = g.rolling(p, min_periods=p).mean(); al = lo.rolling(p, min_periods=p).mean()
    return 100 - (100 / (1 + ag / al))

def calc_macd_hist(s, f=12, sl=26, sg=9):
    ef = s.ewm(span=f, adjust=False).mean(); es = s.ewm(span=sl, adjust=False).mean()
    ml = ef - es; return ml - ml.ewm(span=sg, adjust=False).mean()

def calc_stoch_k(h, l, c, p=14):
    ll = l.rolling(p).min(); hh = h.rolling(p).max()
    return 100 * (c - ll) / (hh - ll)

def calc_stoch_d(h, l, c, kp=14, dp=3):
    return calc_stoch_k(h, l, c, kp).rolling(dp).mean()

def calc_wr(h, l, c, p=14):
    hh = h.rolling(p).max(); ll = l.rolling(p).min()
    return -100 * (hh - c) / (hh - ll)

def calc_bb(s, p=20, sd=2):
    m = s.rolling(p).mean(); st = s.rolling(p).std()
    return m + sd * st, m, m - sd * st

def calc_vwap(h, l, c, v):
    tp = (h + l + c) / 3.0; return (tp * v).cumsum() / v.cumsum()

def calc_mfi(h, l, c, v, p=14):
    tp = (h + l + c) / 3.0; mf = tp * v
    pmf = mf.where(tp > tp.shift(1), 0.0).rolling(p).sum()
    nmf = mf.where(tp <= tp.shift(1), 0.0).rolling(p).sum()
    return 100 - (100 / (1 + pmf / nmf.where(nmf > 0, np.nan)))

def calc_cmf(h, l, c, v, p=20):
    clv = ((c - l) - (h - c)) / (h - l).where((h - l) > 0, np.nan)
    return (clv * v).rolling(p).sum() / v.rolling(p).sum()

def calc_obv(c, v):
    sign = np.where(c > c.shift(1), 1, np.where(c < c.shift(1), -1, 0))
    return (pd.Series(sign, index=c.index) * v).cumsum()

def vol_direction(c, v, p=10):
    pc = c.diff()
    bv = v.where(pc > 0, 0.0).rolling(p).sum()
    sv = v.where(pc < 0, 0.0).rolling(p).sum()
    tv = bv + sv
    return safe_val(bv / tv.where(tv > 0, np.nan), 0.5)

def get_regime():
    try:
        spy = safe_download("SPY", "5d", "5m")
        if spy is None or len(spy) < 30: return "NEUTRAL", 50.0
        c = spy["Close"]
        r = safe_val(calc_rsi(c), 50); m = safe_val(calc_macd_hist(c), 0)
        ma = safe_val(c.rolling(20).mean(), 0); p = safe_val(c, 0)
        bs = (1 if r > 55 else 0) + (1 if r > 65 else 0) + (1 if m > 0 else 0) + (1 if p > ma else 0)
        br = (1 if r < 45 else 0) + (1 if r < 35 else 0) + (1 if m < 0 else 0) + (1 if p < ma else 0)
        return ("BULLISH" if bs >= 3 else ("BEARISH" if br >= 3 else "NEUTRAL")), r
    except: return "NEUTRAL", 50.0

def quick_filter(stocks):
    print("  Quick filter: %d stocks..." % len(stocks))
    out = []
    try:
        bd = yf.download(stocks, period="5d", interval="5m", progress=False, auto_adjust=True, threads=False, group_by="ticker")
        if bd is None or bd.empty: return []
        for tk in stocks:
            try:
                if isinstance(bd.columns, pd.MultiIndex):
                    if tk not in bd.columns.get_level_values(0): continue
                    df = bd[tk].dropna(how="all")
                else: df = bd.dropna(how="all")
                if df is None or df.empty or len(df) < 20: continue
                c, h, lo = df["Close"], df["High"], df["Low"]
                rv = safe_val(calc_rsi(c), 50)
                sk = safe_val(calc_stoch_k(h, lo, c), 50)
                wv = safe_val(calc_wr(h, lo, c), -50)
                tc = (1 if rv > 65 else 0) + (1 if sk > 75 else 0) + (1 if wv > -25 else 0)
                bc = (1 if rv < 35 else 0) + (1 if sk < 25 else 0) + (1 if wv < -75 else 0)
                if tc >= 2 or bc >= 2: out.append(tk)
            except: continue
    except Exception as e: print("  Batch err: %s" % e)
    return out

def analyze(ticker, interval, period):
    data = safe_download(ticker, period, interval)
    if data is None or len(data) < 30: return None
    c, h, lo, v, o = data["Close"], data["High"], data["Low"], data["Volume"], data["Open"]
    rv = safe_val(calc_rsi(c), 50); mh = safe_val(calc_macd_hist(c), 0)
    mhp = safe_val(calc_macd_hist(c).shift(1), 0)
    sk = safe_val(calc_stoch_k(h, lo, c), 50); sd = safe_val(calc_stoch_d(h, lo, c), 50)
    wr = safe_val(calc_wr(h, lo, c), -50); mfi = safe_val(calc_mfi(h, lo, c, v), 50)
    cmf = safe_val(calc_cmf(h, lo, c, v), 0); vw = safe_val(calc_vwap(h, lo, c, v), 0)
    bbu, _, bbl = calc_bb(c); bu, bl = safe_val(bbu, 0), safe_val(bbl, 0)
    cl, op, hi, low = safe_val(c, 0), safe_val(o, 0), safe_val(h, 0), safe_val(lo, 0)
    va = safe_val(v.rolling(20).mean(), 1); cv = safe_val(v, 0)
    vs = cv > 1.5 * va; vd = vol_direction(c, v)
    obv = calc_obv(c, v); obv_up = safe_val(obv, 0) > safe_val(obv.shift(5), 0)
    higher_low = low > safe_val(lo.shift(1), 0); lower_high = hi < safe_val(h.shift(1), 0)
    body = abs(cl - op); rng = hi - low
    lwick = min(cl, op) - low; uwick = hi - max(cl, op)
    # BOTTOM (15)
    b = 0
    if mh > mhp and mhp < 0: b += 15
    if sk > sd and sk < 30: b += 15
    elif sk < 20: b += 8
    if vw > 0 and cl > vw and cl < vw * 1.005: b += 15
    if rv < 30: b += 10
    elif rv < 35: b += 5
    if bl > 0 and cl <= bl * 1.003 and cl > bl: b += 10
    if wr < -80: b += 10
    if vd > 0.55: b += 10
    if higher_low: b += 15
    if vs and vd > 0.5: b += 5
    if mfi < 20: b += 12
    elif mfi < 30: b += 6
    if obv_up and rv < 40: b += 10
    if cmf > 0 and rv < 40: b += 10
    if rng > 0 and lwick > 2 * body and body < rng * 0.35: b += 10
    try:
        if c.iloc[-2] < o.iloc[-2] and cl > op and cl > o.iloc[-2] and op < c.iloc[-2]: b += 12
    except: pass
    # TOP (15)
    t = 0
    if mh < mhp and mhp > 0: t += 15
    if sk < sd and sk > 70: t += 15
    elif sk > 80: t += 8
    if vw > 0 and cl < vw and cl > vw * 0.995: t += 15
    if rv > 70: t += 10
    elif rv > 65: t += 5
    if bu > 0 and cl >= bu * 0.997 and cl < bu: t += 10
    if wr > -20: t += 10
    if vd < 0.45: t += 10
    if lower_high: t += 15
    if vs and vd < 0.5: t += 5
    if mfi > 80: t += 12
    elif mfi > 70: t += 6
    if not obv_up and rv > 60: t += 10
    if cmf < 0 and rv > 60: t += 10
    if rng > 0 and uwick > 2 * body and body < rng * 0.35: t += 10
    try:
        if c.iloc[-2] > o.iloc[-2] and cl < op and cl < o.iloc[-2] and op > c.iloc[-2]: t += 12
    except: pass
    sig, sc = None, 0
    if b > t and b >= 40: sig, sc = "BOTTOM", min(b, 100)
    elif t > b and t >= 40: sig, sc = "TOP", min(t, 100)
    return {"signal": sig, "score": sc, "rsi": round(rv, 1), "stoch_k": round(sk, 1),
            "wr": round(wr, 1), "mfi": round(mfi, 1), "cmf": round(cmf, 3),
            "vwap": round(vw, 2), "price": round(cl, 2), "vol_spike": vs, "vol_dir": round(vd, 2)}

def mtf_check(ticker):
    r5 = analyze(ticker, "5m", "5d")
    if not r5 or not r5["signal"]: return None
    sig, c5 = r5["signal"], r5["score"]
    r15 = analyze(ticker, "15m", "5d")
    if not r15 or r15["signal"] != sig: return None
    c15 = r15["score"]
    r30 = analyze(ticker, "30m", "1mo")
    if not r30 or r30["signal"] != sig: return None
    c30 = r30["score"]
    r60 = analyze(ticker, "60m", "1mo")
    bonus, mtf, c60 = 0, "5m+15m+30m", 0
    if r60 and r60["signal"] == sig: bonus, mtf, c60 = 10, "5m+15m+30m+60m", r60["score"]
    elif r60 and r60["signal"]: bonus = -5
    scores = [c5, c15, c30] + ([c60] if "60m" in mtf else [])
    avg = max(0, min(round(sum(scores) / len(scores)) + bonus, 100))
    return {"ticker": ticker, "signal": sig, "c5": c5, "c15": c15, "c30": c30, "c60": c60,
            "avg_c": avg, "mtf": mtf, "bonus": bonus, "price": r5["price"], "rsi": r5["rsi"],
            "stoch_k": r5["stoch_k"], "wr": r5["wr"], "mfi": r5["mfi"], "cmf": r5["cmf"],
            "vwap": r5["vwap"], "vol_spike": r5["vol_spike"], "vol_dir": r5["vol_dir"],
            "time": now_et().strftime("%I:%M %p")}

def load_json(path, default):
    try:
        with open(path, "r") as f: return json.load(f)
    except: return default

def save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f: json.dump(data, f, indent=2)

def load_today_alerts():
    data = load_json(ALERTS_TODAY, {"date": "", "alerts": []})
    today = now_et().strftime("%Y-%m-%d")
    if data.get("date") != today: return {"date": today, "alerts": []}
    return data

def is_duplicate(alerts, new):
    hour = now_et().strftime("%I")
    for a in alerts:
        if a["ticker"] == new["ticker"] and a["signal"] == new["signal"]:
            if a.get("time", "")[:2] == hour: return True
    return False

def update_history(today_data):
    hist = load_json(ALERTS_HISTORY, [])
    today = now_et().strftime("%Y-%m-%d")
    hist = [d for d in hist if d.get("date") != today]
    hist.append(today_data)
    cutoff = (now_et() - timedelta(days=7)).strftime("%Y-%m-%d")
    hist = [d for d in hist if d.get("date", "") >= cutoff]
    save_json(ALERTS_HISTORY, hist)

def update_win_loss(alerts):
    tickers = list(set(a["ticker"] for a in alerts if a.get("result", "PENDING") == "PENDING"))
    if not tickers: return alerts
    try:
        d = yf.download(tickers, period="1d", interval="1d", progress=False, auto_adjust=True, threads=False, group_by="ticker")
        if d is None or d.empty: return alerts
        prices = {}
        if isinstance(d.columns, pd.MultiIndex):
            for t in tickers:
                try: prices[t] = float(d[t]["Close"].iloc[-1])
                except: pass
        else:
            try: prices[tickers[0]] = float(d["Close"].iloc[-1])
            except: pass
        for a in alerts:
            cp = prices.get(a["ticker"], 0)
            if cp <= 0 or a.get("price", 0) <= 0: continue
            chg = (cp - a["price"]) / a["price"] * 100
            a["current_price"] = round(cp, 2); a["change_pct"] = round(chg, 2)
            a["result"] = "WIN" if ((a["signal"] == "BOTTOM" and chg > 0) or (a["signal"] == "TOP" and chg < 0)) else "LOSS"
    except: pass
    return alerts

def send_email(subject, html):
    if not EMAIL_ADDRESS or not EMAIL_PASSWORD or not RECIPIENT_EMAILS: return
    try:
        with smtplib.SMTP(SMTP_SERVER, 587, timeout=60) as s:
            s.starttls(); s.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            for r in RECIPIENT_EMAILS:
                msg = MIMEMultipart("alternative")
                msg["Subject"], msg["From"], msg["To"] = subject, EMAIL_ADDRESS, r
                msg.attach(MIMEText(html, "html"))
                s.sendmail(EMAIL_ADDRESS, [r], msg.as_string())
        print("  Email sent: %s" % subject)
    except Exception as e: print("  Email err: %s" % e)

def build_html(alerts, regime, spy_rsi, scan_time, total, filtered_ct, scan_num):
    bots = [a for a in alerts if a["signal"] == "BOTTOM"]
    tops = [a for a in alerts if a["signal"] == "TOP"]
    wins = [a for a in alerts if a.get("result") == "WIN"]
    losses = [a for a in alerts if a.get("result") == "LOSS"]
    wr = round(len(wins) / (len(wins) + len(losses)) * 100) if (wins or losses) else 0
    regime_c = "#00e676" if regime == "BULLISH" else ("#ff1744" if regime == "BEARISH" else "#ffab00")
    wr_c = "#00e676" if wr >= 50 else "#ff9800"
    rows = ""
    for a in sorted(alerts, key=lambda x: x.get("avg_c", 0), reverse=True):
        bg = "#0d2818" if a["signal"] == "BOTTOM" else "#2d0a0a"
        badge_bg = "#00c853" if a["signal"] == "BOTTOM" else "#ff1744"
        bar_w = min(a.get("avg_c", 0), 100)
        bar_c = "#4caf50" if bar_w >= 60 else ("#ff9800" if bar_w >= 45 else "#f44336")
        mtf_c = "#00e676" if "60m" in a.get("mtf", "") else "#ffab00"
        res = a.get("result", "PENDING")
        res_c = "#00e676" if res == "WIN" else ("#ff1744" if res == "LOSS" else "#888")
        chg = a.get("change_pct", 0)
        chg_s = "%+.1f%%" % chg if chg else "--"
        price_s = "$%.2f" % a.get("price", 0)
        rows += '<tr style="background:%s;">\n' % bg
        rows += '<td style="font-weight:bold;font-size:15px;">%s</td>\n' % a["ticker"]
        rows += '<td><span style="background:%s;color:#fff;padding:3px 10px;border-radius:12px;font-size:11px;font-weight:bold;">%s</span></td>\n' % (badge_bg, a["signal"])
        rows += '<td>%s</td>\n' % price_s
        rows += '<td><div style="background:#333;border-radius:6px;height:18px;width:110px;"><div style="background:%s;height:18px;width:%d%%;border-radius:6px;text-align:center;color:#fff;font-size:11px;line-height:18px;">%d</div></div></td>\n' % (bar_c, bar_w, a.get("avg_c", 0))
        rows += '<td style="font-size:11px;">%d/%d/%d/%d</td>\n' % (a.get("c5",0), a.get("c15",0), a.get("c30",0), a.get("c60",0))
        rows += '<td><span style="background:%s;color:#000;padding:2px 8px;border-radius:8px;font-size:10px;font-weight:bold;">%s</span></td>\n' % (mtf_c, a.get("mtf",""))
        rows += '<td>%.1f</td><td>%.1f</td><td>%.1f</td><td>%.1f</td><td>%.3f</td>\n' % (a.get("rsi",0), a.get("stoch_k",0), a.get("wr",0), a.get("mfi",0), a.get("cmf",0))
        rows += '<td>%s</td>\n' % ("YES" if a.get("vol_spike") else "no")
        rows += '<td style="color:%s;font-weight:bold;">%s %s</td>\n' % (res_c, res, chg_s)
        rows += '<td style="font-size:11px;">%s</td></tr>\n' % a.get("time","")

    hist = load_json(ALERTS_HISTORY, [])
    week_rows = ""
    for day in sorted(hist, key=lambda x: x.get("date", ""), reverse=True):
        da = day.get("alerts", [])
        dw = len([a for a in da if a.get("result") == "WIN"])
        dt = len([a for a in da if a.get("result") in ("WIN", "LOSS")])
        dwr = round(dw / dt * 100) if dt else 0
        db = len([a for a in da if a["signal"] == "BOTTOM"])
        dtp = len([a for a in da if a["signal"] == "TOP"])
        week_rows += '<tr><td>%s</td><td>%d</td><td style="color:#00c853;">%d</td><td style="color:#ff1744;">%d</td><td>%d%%</td></tr>\n' % (day.get("date",""), len(da), db, dtp, dwr)

    no_alerts_msg = '<p style="color:#484f58;text-align:center;margin:30px;font-size:14px;">No confirmed signals this scan</p>' if not alerts else ""
    no_hist_msg = '<p style="color:#484f58;text-align:center;margin:20px;">No history yet</p>' if not week_rows else ""

    css = """
body{background:#0a0a0a;color:#e0e0e0;font-family:"Segoe UI",sans-serif;margin:0;padding:0;}
.hdr{background:linear-gradient(135deg,#0d1117,#161b22);padding:15px 25px;display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid #30363d;}
.hdr h1{color:#58a6ff;font-size:22px;margin:0;} .hdr .sub{color:#8b949e;font-size:12px;}
.badge{display:inline-block;padding:3px 10px;border-radius:12px;font-size:11px;font-weight:bold;margin-left:8px;}
.stats{display:flex;gap:15px;padding:15px 25px;flex-wrap:wrap;}
.card{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:12px 20px;min-width:120px;}
.card-label{color:#8b949e;font-size:10px;text-transform:uppercase;letter-spacing:1px;}
.card-val{font-size:26px;font-weight:bold;margin-top:2px;}
.main{padding:0 25px 25px;} table{width:100%%;border-collapse:collapse;font-size:12px;}
th{background:#161b22;color:#58a6ff;text-align:left;padding:10px 6px;border-bottom:2px solid #30363d;position:sticky;top:0;}
td{padding:7px 6px;border-bottom:1px solid #21262d;} tr:hover{background:#1a1a3e!important;}
.section{background:#161b22;border:1px solid #30363d;border-radius:10px;margin-top:20px;padding:15px 20px;}
.section h2{color:#58a6ff;font-size:16px;margin:0 0 10px;} .timer{color:#58a6ff;font-size:13px;font-weight:bold;}
.foot{color:#484f58;font-size:11px;text-align:center;padding:15px;}"""

    js = """<script>let s=300;function tick(){s--;if(s<0)s=300;let m=Math.floor(s/60),sec=s%60;document.getElementById("timer").textContent=m+":"+(sec<10?"0":"")+sec;}setInterval(tick,1000);</script>"""

    html = '<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta http-equiv="refresh" content="300"><title>Market Scanner v5.1</title><style>' + css + '</style></head><body>\n'
    html += '<div class="hdr"><div><h1>Market Scanner v5.1</h1><span class="sub">12-Indicator / Multi-Timeframe / Sector Override</span></div>\n'
    html += '<div style="text-align:right;"><span class="badge" style="background:#1f6feb;color:#fff;">YAHOO</span>'
    html += '<span class="badge" style="background:#238636;color:#fff;">LIVE</span>\n'
    html += '<div style="margin-top:5px;color:#8b949e;font-size:11px;">Last: %s</div>\n' % scan_time
    html += '<div class="timer" id="timer">5:00</div></div></div>\n'

    html += '<div class="stats">\n'
    html += '<div class="card"><div class="card-label">Alerts Today</div><div class="card-val" style="color:#58a6ff;">%d</div></div>\n' % len(alerts)
    html += '<div class="card"><div class="card-label">Win Rate</div><div class="card-val" style="color:%s;">%d%%</div><div style="color:#8b949e;font-size:10px;">Today</div></div>\n' % (wr_c, wr)
    html += '<div class="card"><div class="card-label">Market Regime</div><div class="card-val" style="color:%s;">%s</div><div style="color:#8b949e;font-size:10px;">SPY-based</div></div>\n' % (regime_c, regime)
    html += '<div class="card"><div class="card-label">SPY RSI</div><div class="card-val">%.1f</div></div>\n' % spy_rsi
    html += '<div class="card"><div class="card-label">Universe</div><div class="card-val">%d</div></div>\n' % total
    html += '<div class="card"><div class="card-label">Filtered</div><div class="card-val">%d</div></div>\n' % filtered_ct
    html += '<div class="card"><div class="card-label">Scans Today</div><div class="card-val">%d</div></div>\n' % scan_num
    html += '<div class="card"><div class="card-label">Data Source</div><div class="card-val" style="font-size:16px;">Yahoo</div></div>\n'
    html += '</div>\n'

    html += '<div class="main"><div class="section"><h2>Live Alerts - Today</h2>\n'
    html += '<table><tr><th>Ticker</th><th>Signal</th><th>Price</th><th>Score</th><th>5/15/30/60</th><th>MTF</th><th>RSI</th><th>Stoch</th><th>W%%R</th><th>MFI</th><th>CMF</th><th>Vol</th><th>Result</th><th>Time</th></tr>\n'
    html += rows
    html += '</table>\n' + no_alerts_msg + '</div>\n'

    html += '<div class="section"><h2>Weekly Performance - Last 7 Days</h2>\n'
    html += '<table><tr><th>Date</th><th>Alerts</th><th>Bottom</th><th>Top</th><th>Win Rate</th></tr>\n'
    html += week_rows
    html += '</table>\n' + no_hist_msg + '</div></div>\n'
    html += '<div class="foot">Scanner v5.1 | MTF: 5m+15m+30m (STRICT) + 60m (BOOSTER) | GitHub Actions | Every 5 min</div>\n'
    html += js + '</body></html>'
    return html

def main():
    scan_time = now_et().strftime("%Y-%m-%d %I:%M:%S %p ET")
    today = now_et().strftime("%Y-%m-%d")
    os.makedirs(DOCS, exist_ok=True)
    print("\n" + "=" * 60)
    print("  SCANNER v5.1 GitHub Actions Live")
    print("  %s" % scan_time)
    print("  Universe: %d stocks" % len(ALL_STOCKS))
    print("=" * 60 + "\n")

    if not is_market_open():
        print("  Market closed. Updating win/loss & dashboard...")
        td = load_today_alerts()
        td["alerts"] = update_win_loss(td["alerts"])
        save_json(ALERTS_TODAY, td); update_history(td)
        regime, spy_rsi = get_regime()
        meta = load_json(LATEST_SCAN, {"scan_count": 0})
        html = build_html(td["alerts"], regime, spy_rsi, scan_time, len(ALL_STOCKS), 0, meta.get("scan_count", 0))
        with open(HTML_OUT, "w", encoding="utf-8") as f: f.write(html)
        print("  Dashboard updated (closed market)"); return

    td = load_today_alerts()
    meta = load_json(LATEST_SCAN, {"scan_count": 0, "date": ""})
    if meta.get("date") != today: meta = {"scan_count": 0, "date": today}
    meta["scan_count"] = meta.get("scan_count", 0) + 1

    regime, spy_rsi = get_regime()
    print("  Regime: %s (SPY RSI=%.1f)" % (regime, spy_rsi))
    filtered = quick_filter(ALL_STOCKS)
    print("  Filtered: %d passed" % len(filtered))

    new_alerts = []
    for i, tk in enumerate(filtered):
        print("  [%d/%d] %s..." % (i + 1, len(filtered), tk))
        try:
            r = mtf_check(tk)
            if r and r["avg_c"] >= 45:
                if not is_duplicate(td["alerts"], r):
                    r["result"] = "PENDING"
                    td["alerts"].append(r); new_alerts.append(r)
                    print("    *** %s confirmed! c=%d MTF=%s" % (r["signal"], r["avg_c"], r["mtf"]))
        except Exception as e: print("    Error: %s" % e)

    td["alerts"] = update_win_loss(td["alerts"])
    save_json(ALERTS_TODAY, td); update_history(td)
    meta["last_scan"] = scan_time; meta["filtered"] = len(filtered); meta["new_alerts"] = len(new_alerts)
    save_json(LATEST_SCAN, meta)

    html = build_html(td["alerts"], regime, spy_rsi, scan_time, len(ALL_STOCKS), len(filtered), meta["scan_count"])
    with open(HTML_OUT, "w", encoding="utf-8") as f: f.write(html)
    print("  Dashboard: %s (%d bytes)" % (HTML_OUT, len(html)))

    if new_alerts:
        subj = "v5.1 Alert: %d signals - %s" % (len(new_alerts), scan_time)
        er = ""
        for a in new_alerts:
            c = "#e6f4ea" if a["signal"] == "BOTTOM" else "#fce8e6"
            er += '<tr style="background:%s;"><td>%s</td><td>%s</td><td>$%.2f</td><td>%d</td><td>%s</td><td>%s</td></tr>' % (c, a["ticker"], a["signal"], a["price"], a["avg_c"], a["mtf"], a["time"])
        ehtml = '<h2>v5.1 Scanner Alert</h2><p>%s | SPY RSI %.1f</p><table border="1" cellpadding="5"><tr><th>Ticker</th><th>Signal</th><th>Price</th><th>Score</th><th>MTF</th><th>Time</th></tr>%s</table>' % (regime, spy_rsi, er)
        send_email(subj, ehtml)

    print("\n  New: %d | Total today: %d | Scan #%d" % (len(new_alerts), len(td["alerts"]), meta["scan_count"]))
    print("=" * 60)

if __name__ == "__main__":
    main()