#!/usr/bin/env python3
"""
================================================================
MARKET SCANNER v5.3 - GITHUB ACTIONS RUNNER
================================================================
Runs a single scan cycle and outputs a self-contained HTML dashboard
to docs/TopBottom_Universal.html for GitHub Pages hosting.

Usage:
  python run_github.py

This script imports from market_scanner_v5.py (must be in same dir).
No persistent DB needed - all data embedded in the HTML output.
================================================================
"""

import os, sys, json, time, random, logging, warnings
from datetime import datetime, timedelta, date
from html import escape

warnings.filterwarnings("ignore")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("github_runner")

# ================================================================
# IMPORT SCANNER
# ================================================================

script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, script_dir)

try:
    import yfinance as yf
    import pandas as pd
    import numpy as np
except ImportError as e:
    logger.error("Missing package: %s", e)
    logger.error("Run: pip install yfinance pandas numpy pytz")
    sys.exit(1)

try:
    from market_scanner_v5 import (
        ALL_STOCKS, SECTOR_MAP,
        get_eastern_now, get_market_regime, check_sector_strength,
        get_stock_sector, batch_quick_scan, analyze_stock_v5,
        get_time_quality, minutes_until_close,
        get_options_pcr,
        sector_strength_cache, confirmation_tracker, sent_alerts,
        US_MARKET_HOLIDAYS,
        MARKET_OPEN_HOUR, MARKET_OPEN_MINUTE,
        MARKET_CLOSE_HOUR, MARKET_CLOSE_MINUTE,
    )
    SCANNER_IMPORTED = True
    logger.info("Scanner module imported successfully (%d stocks)", len(ALL_STOCKS))
except ImportError as e:
    logger.error("Could not import market_scanner_v5: %s", e)
    SCANNER_IMPORTED = False
    ALL_STOCKS = []


# ================================================================
# EMAIL (optional - uses GitHub Secrets)
# ================================================================

def send_email_notification(subject, html_body):
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    addr = os.getenv("EMAIL_ADDRESS", "")
    pwd = os.getenv("EMAIL_PASSWORD", "")
    recipients = os.getenv("RECIPIENT_EMAILS", "")
    smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")

    if not addr or not pwd or not recipients:
        logger.info("Email not configured (set EMAIL_ADDRESS, EMAIL_PASSWORD, RECIPIENT_EMAILS)")
        return

    try:
        with smtplib.SMTP(smtp_server, 587, timeout=60) as server:
            server.starttls()
            server.login(addr, pwd)
            for r in [x.strip() for x in recipients.split(",") if x.strip()]:
                msg = MIMEMultipart("alternative")
                msg["Subject"] = subject
                msg["From"] = addr
                msg["To"] = r
                msg.attach(MIMEText(html_body, "html"))
                server.sendmail(addr, [r], msg.as_string())
        logger.info("Email sent: %s", subject)
    except Exception as e:
        logger.error("Email failed: %s", e)


# ================================================================
# SINGLE SCAN CYCLE
# ================================================================

def run_single_scan():
    """Run one complete scan cycle. Returns list of alert dicts."""
    if not SCANNER_IMPORTED:
        logger.error("Scanner not imported - cannot run scan")
        return [], "UNKNOWN", 50.0

    now = get_eastern_now()
    logger.info("=" * 60)
    logger.info("GITHUB ACTIONS SCAN - %s", now.strftime("%Y-%m-%d %I:%M %p ET"))
    logger.info("Universe: %d stocks", len(ALL_STOCKS))
    logger.info("=" * 60)

    # Layer 1: Market Regime
    regime, spy_rsi = get_market_regime()
    logger.info("Market Regime: %s (SPY RSI=%.1f)", regime, spy_rsi)

    # Pass 1: Quick scan
    logger.info("PASS 1: Scanning %d stocks...", len(ALL_STOCKS))
    filtered = batch_quick_scan(ALL_STOCKS)
    logger.info("PASS 1: %d/%d passed filter", len(filtered), len(ALL_STOCKS))

    if not filtered:
        logger.info("No stocks passed quick scan filter")
        return [], regime, spy_rsi

    # Layer 2: Sector strength
    checked_sectors = set()
    for t in filtered:
        sec = get_stock_sector(t)
        if sec not in checked_sectors and sec != "SPY":
            s = check_sector_strength(sec)
            logger.info("  Sector %s: %s", sec, s)
            checked_sectors.add(sec)
            time.sleep(0.5)

    # Pass 2: Full analysis
    logger.info("PASS 2: Full v5 analysis on %d stocks...", len(filtered))
    alerts = []

    for ticker in filtered:
        try:
            sec = get_stock_sector(ticker)
            ss = sector_strength_cache.get(sec, {}).get("strength", "NEUTRAL")

            # 5-minute analysis
            r5 = analyze_stock_v5(ticker, "5m", regime, ss)
            if r5 is None:
                continue

            sig = r5["signal"]
            conf = r5["confidence"]

            # Multi-timeframe check (single run mode)
            mtf_status = "5m"
            c15 = 0
            c30 = 0
            c60 = 0

            try:
                r15 = analyze_stock_v5(ticker, "15m", regime, ss)
                if r15 and r15["signal"] == sig:
                    mtf_status = "5m+15m"
                    c15 = r15["confidence"]

                    r30 = analyze_stock_v5(ticker, "30m", regime, ss)
                    if r30 and r30["signal"] == sig:
                        mtf_status = "5m+15m+30m"
                        c30 = r30["confidence"]

                        r60 = analyze_stock_v5(ticker, "60m", regime, ss)
                        if r60 and r60["signal"] == sig:
                            mtf_status = "5m+15m+30m+60m"
                            c60 = r60["confidence"]
            except Exception:
                pass

            # Must have at least 5m+15m agreement
            if "15m" not in mtf_status:
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
                "ticker": ticker,
                "signal": sig,
                "confidence": avg_c,
                "c5": conf, "c15": c15, "c30": c30, "c60": c60,
                "alert_price": round(r5["cl"], 2),
                "rsi": round(r5["rsi"], 1),
                "mfi": round(r5.get("mfi", 0), 1),
                "cmf": round(r5.get("cmf", 0), 3),
                "pcr": round(pcr, 2),
                "regime": regime,
                "sector_strength": ss,
                "setup_type": r5.get("setup_type", "REVERSAL"),
                "leadership_score": r5.get("leadership_score", 0),
                "trend_3d": round(r5.get("trend_3d", 0), 2),
                "trend_5d": round(r5.get("trend_5d", 0), 2),
                "volume_expansion": round(r5.get("volume_expansion", 1.0), 2),
                "relative_strength": round(r5.get("relative_strength", 0), 2),
                "sector_override": r5.get("sector_override", 0),
                "signals": r5.get("signals", []),
                "mtf_status": mtf_status,
                "time": now_et,
            }
            alerts.append(alert)
            logger.info("  * %s: %s conf=%d mtf=%s", ticker, sig, avg_c, mtf_status)

        except Exception as e:
            logger.warning("  Error analyzing %s: %s", ticker, e)
            continue

    logger.info("=" * 60)
    logger.info("SCAN COMPLETE: %d signals found", len(alerts))
    logger.info("=" * 60)

    return alerts, regime, spy_rsi


# ================================================================
# HTML GENERATOR
# ================================================================

def generate_html(alerts, regime, spy_rsi):
    """Generate self-contained HTML dashboard."""
    now = get_eastern_now() if SCANNER_IMPORTED else datetime.utcnow()
    scan_time = now.strftime("%Y-%m-%d %I:%M:%S %p ET")
    scan_date = now.strftime("%Y-%m-%d")

    alerts_sorted = sorted(alerts, key=lambda a: a.get("confidence", 0), reverse=True)
    total = len(alerts_sorted)
    bottoms = sum(1 for a in alerts_sorted if a["signal"] == "BOTTOM")
    tops = total - bottoms
    num_stocks = len(ALL_STOCKS) if ALL_STOCKS else 260

    alerts_json = json.dumps(alerts_sorted)

    regime_badge_class = "badge-bull" if regime == "BULLISH" else "badge-bear" if regime == "BEARISH" else "badge-neutral"
    empty_display = "" if total == 0 else "none"
    empty_msg = "No signals detected. Market may be closed or all indicators neutral." if total == 0 else ""

    html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta http-equiv="refresh" content="900">
<title>Market Scanner v5.3 - """ + scan_date + """</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
:root{--bg:#0a0e17;--card:#111827;--border:#1e293b;--text:#e2e8f0;--dim:#64748b;--green:#22c55e;--red:#ef4444;--blue:#3b82f6;--cyan:#06b6d4;--purple:#a855f7;--orange:#f97316;--yellow:#eab308}
body{font-family:'Segoe UI',system-ui,sans-serif;background:var(--bg);color:var(--text);font-variant-numeric:tabular-nums;min-height:100vh}
a{color:var(--cyan);text-decoration:none}
.topbar{display:flex;align-items:center;justify-content:space-between;padding:12px 24px;background:#0d1320;border-bottom:1px solid var(--border);flex-wrap:wrap;gap:10px}
.topbar h1{font-size:18px;font-weight:700;color:var(--cyan)}
.topbar small{font-size:11px;color:var(--dim)}
.badge{padding:3px 10px;border-radius:20px;font-size:11px;font-weight:700}
.badge-bull{background:#22c55e22;color:var(--green)}
.badge-bear{background:#ef444422;color:var(--red)}
.badge-neutral{background:#eab30822;color:var(--yellow)}
.meta{display:flex;align-items:center;gap:14px;font-size:12px;color:var(--dim)}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:14px;padding:18px 24px}
.card{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:18px}
.card .label{font-size:11px;color:var(--dim);text-transform:uppercase;letter-spacing:1px;margin-bottom:6px}
.card .value{font-size:26px;font-weight:700}
.card .sub{font-size:12px;color:var(--dim);margin-top:4px}
.toolbar{display:flex;justify-content:space-between;align-items:center;padding:16px 24px;flex-wrap:wrap;gap:10px}
.filters{display:flex;gap:6px}
.fbtn{padding:6px 14px;border-radius:20px;border:1px solid var(--border);background:transparent;color:var(--dim);cursor:pointer;font-size:12px;font-weight:600;transition:all .2s}
.fbtn:hover,.fbtn.active{background:var(--cyan);color:#000;border-color:var(--cyan)}
table{width:100%;border-collapse:collapse;font-size:13px}
thead th{background:#0d1320;color:var(--dim);font-size:11px;text-transform:uppercase;letter-spacing:.5px;padding:10px 12px;position:sticky;top:0;cursor:pointer;user-select:none;text-align:left;border-bottom:1px solid var(--border)}
thead th:hover{color:var(--cyan)}
tbody tr{border-bottom:1px solid #1e293b44;transition:background .15s}
tbody tr:hover{background:#1e293b55}
td{padding:10px 12px;white-space:nowrap}
.ticker{font-weight:700;color:var(--cyan)}
.sig{padding:3px 10px;border-radius:4px;font-size:11px;font-weight:700}
.sig-bottom{background:#22c55e22;color:var(--green)}
.sig-top{background:#ef444422;color:var(--red)}
.setup{padding:3px 8px;border-radius:4px;font-size:10px;font-weight:700}
.setup-reversal{background:#3b82f622;color:var(--blue)}
.setup-leadership{background:#a855f722;color:var(--purple)}
.setup-hybrid{background:#f9731622;color:var(--orange)}
.score-bar{display:flex;align-items:center;gap:6px}
.score-bar .bar{width:50px;height:6px;background:#1e293b;border-radius:3px;overflow:hidden}
.score-bar .bar .fill{height:100%;border-radius:3px}
.empty-state{text-align:center;padding:60px;color:var(--dim)}
.empty-state .icon{font-size:48px;margin-bottom:12px}
.footer{text-align:center;padding:20px;color:var(--dim);font-size:11px;border-top:1px solid var(--border);margin-top:20px}
@media(max-width:768px){.cards{grid-template-columns:1fr 1fr}td,th{padding:6px 8px;font-size:12px}.topbar{flex-direction:column;text-align:center}}
</style>
</head>
<body>

<div class="topbar">
  <div>
    <h1>&#9889; Market Scanner v5.3</h1>
    <small>12-Indicator - Multi-Timeframe - Sector Override - GitHub Actions</small>
  </div>
  <div class="meta">
    <span class="badge """ + regime_badge_class + '">' + regime + """</span>
    <span>Scanned: <b>""" + scan_time + """</b></span>
    <span>""" + str(num_stocks) + """ stocks</span>
  </div>
</div>

<div class="cards">
  <div class="card"><div class="label">Total Alerts</div><div class="value">""" + str(total) + """</div><div class="sub">&#9650; """ + str(bottoms) + """ Bottom &nbsp; &#9660; """ + str(tops) + """ Top</div></div>
  <div class="card"><div class="label">Market Regime</div><div class="value">""" + regime + """</div><div class="sub">SPY RSI: """ + str(round(spy_rsi, 1)) + """</div></div>
  <div class="card"><div class="label">Scan Time</div><div class="value" style="font-size:18px">""" + scan_time + """</div><div class="sub">Auto-refreshes every 15 min</div></div>
  <div class="card"><div class="label">Platform</div><div class="value" style="font-size:18px">GitHub Actions</div><div class="sub">Zero disk I/O issues</div></div>
</div>

<div class="toolbar">
  <div class="filters">
    <button class="fbtn active" onclick="filterAlerts('ALL')">ALL <b>""" + str(total) + """</b></button>
    <button class="fbtn" onclick="filterAlerts('BOTTOM')">&#9650; BOTTOM <b>""" + str(bottoms) + """</b></button>
    <button class="fbtn" onclick="filterAlerts('TOP')">&#9660; TOP <b>""" + str(tops) + """</b></button>
  </div>
  <span style="color:var(--dim);font-size:12px">Click column headers to sort</span>
</div>

<div style="padding:0 24px;overflow-x:auto">
  <table>
    <thead><tr>
      <th onclick="sortTable('ticker')">Ticker</th>
      <th onclick="sortTable('signal')">Signal</th>
      <th onclick="sortTable('alert_price')">Alert $</th>
      <th onclick="sortTable('confidence')">Score</th>
      <th onclick="sortTable('setup_type')">Setup</th>
      <th onclick="sortTable('mtf_status')">MTF</th>
      <th>Regime</th>
      <th>Sector</th>
      <th onclick="sortTable('rsi')">RSI</th>
      <th onclick="sortTable('trend_3d')">Trend 3d</th>
      <th onclick="sortTable('volume_expansion')">Vol Exp</th>
      <th>PCR</th>
      <th>Signals</th>
      <th>Time</th>
    </tr></thead>
    <tbody id="alertBody"></tbody>
  </table>
  <div class="empty-state" id="emptyState" style="display:""" + empty_display + """">
    <div class="icon">&#128225;</div>
    <p>""" + empty_msg + """</p>
  </div>
</div>

<div class="footer">
  Generated by Market Scanner v5.3 on GitHub Actions &bull; """ + scan_time + """ &bull; """ + str(num_stocks) + """ stocks scanned<br>
  <a href="https://stock-hash.github.io/stockusa/">stock-hash.github.io/stockusa</a>
</div>

<script>
var alerts = """ + alerts_json + """;
var currentFilter = 'ALL';
var sortCol = 'confidence';
var sortDir = -1;

function sigBadge(s){return s==='BOTTOM'?'<span class="sig sig-bottom">&#9650; BOTTOM</span>':'<span class="sig sig-top">&#9660; TOP</span>';}
function setupBadge(s){if(!s)return'-';var c=s.indexOf('LEADER')>=0?'setup-leadership':s.indexOf('HYBRID')>=0?'setup-hybrid':'setup-reversal';return'<span class="setup '+c+'">'+s+'</span>';}
function scoreBar(v){v=parseInt(v)||0;var c=v>=80?'var(--green)':v>=60?'var(--cyan)':v>=45?'var(--yellow)':'var(--red)';return'<div class="score-bar">'+v+'<div class="bar"><div class="fill" style="width:'+v+'%;background:'+c+'"></div></div></div>';}
function fmtPct(v){if(!v||v===0)return'<span style="color:var(--dim)">0.00%</span>';return v>0?'<span style="color:var(--green)">+'+v.toFixed(2)+'%</span>':'<span style="color:var(--red)">'+v.toFixed(2)+'%</span>';}
function extractTime(at){if(!at)return'-';try{var m=at.match(/(\\d{1,2}:\\d{2}:\\d{2}\\s*[AP]M)/i);return m?m[1]+' ET':at;}catch(e){return at;}}

function renderAlerts(){
  var data=alerts.slice();
  if(currentFilter!=='ALL')data=data.filter(function(a){return a.signal===currentFilter;});
  if(sortCol)data.sort(function(a,b){var va=a[sortCol],vb=b[sortCol];if(typeof va==='string')return sortDir*va.localeCompare(vb);return sortDir*((va||0)-(vb||0));});
  var body=document.getElementById('alertBody');
  var empty=document.getElementById('emptyState');
  if(!data.length){body.innerHTML='';empty.style.display='';return;}
  empty.style.display='none';
  var html='';
  for(var i=0;i<data.length;i++){
    var a=data[i];
    var sigs=(a.signals||[]).join(', ');
    html+='<tr>'+
      '<td class="ticker">'+a.ticker+'</td>'+
      '<td>'+sigBadge(a.signal)+'</td>'+
      '<td>$'+(a.alert_price||0).toFixed(2)+'</td>'+
      '<td>'+scoreBar(a.confidence)+'</td>'+
      '<td>'+setupBadge(a.setup_type)+'</td>'+
      '<td style="color:var(--dim);font-size:12px">'+(a.mtf_status||'-')+'</td>'+
      '<td style="font-size:12px">'+(a.regime||'-')+'</td>'+
      '<td style="font-size:12px">'+(a.sector_strength||'-')+'</td>'+
      '<td>'+(a.rsi||0).toFixed(1)+'</td>'+
      '<td>'+fmtPct(a.trend_3d)+'</td>'+
      '<td>'+(a.volume_expansion||1).toFixed(1)+'x</td>'+
      '<td>'+(a.pcr||1).toFixed(2)+'</td>'+
      '<td style="color:var(--dim);font-size:11px;max-width:180px;overflow:hidden;text-overflow:ellipsis">'+sigs+'</td>'+
      '<td style="color:var(--dim);font-size:12px">'+extractTime(a.time)+'</td>'+
    '</tr>';
  }
  body.innerHTML=html;
}

function filterAlerts(type){
  currentFilter=type;
  var btns=document.querySelectorAll('.fbtn');
  for(var i=0;i<btns.length;i++)btns[i].classList.remove('active');
  event.target.closest('.fbtn').classList.add('active');
  renderAlerts();
}

function sortTable(col){
  if(sortCol===col)sortDir*=-1;else{sortCol=col;sortDir=-1;}
  renderAlerts();
}

renderAlerts();
</script>
</body>
</html>"""

    return html


# ================================================================
# MAIN
# ================================================================

def main():
    logger.info("=" * 65)
    logger.info("MARKET SCANNER v5.3 - GITHUB ACTIONS MODE")
    logger.info("=" * 65)

    # Run scan
    alerts, regime, spy_rsi = run_single_scan()

    # Generate HTML
    html = generate_html(alerts, regime, spy_rsi)

    # Ensure docs directory exists
    docs_dir = os.path.join(script_dir, "docs")
    os.makedirs(docs_dir, exist_ok=True)

    # Write dashboard
    output_path = os.path.join(docs_dir, "TopBottom_Universal.html")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    logger.info("Dashboard written to %s (%d bytes)", output_path, len(html))

    # Write JSON data file
    json_path = os.path.join(docs_dir, "latest_scan.json")
    scan_time = get_eastern_now().strftime("%Y-%m-%d %I:%M:%S %p ET") if SCANNER_IMPORTED else datetime.utcnow().isoformat()
    scan_data = {
        "scan_time": scan_time,
        "regime": regime,
        "spy_rsi": spy_rsi,
        "total_alerts": len(alerts),
        "stocks_scanned": len(ALL_STOCKS) if ALL_STOCKS else 260,
        "alerts": alerts,
    }
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(scan_data, f, indent=2)
    logger.info("JSON data written to %s", json_path)

    # Send email if configured
    if alerts:
        try:
            et_now = get_eastern_now().strftime("%Y-%m-%d") if SCANNER_IMPORTED else ""
            send_email_notification(
                "v5.3 GitHub Scan - %d signals - %s" % (len(alerts), et_now),
                html
            )
        except Exception as e:
            logger.warning("Email notification failed: %s", e)

    logger.info("GitHub Actions scan complete!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
