"""A1 Ultimate Outlook dashboard.
Presentation-only layer: reads validated A1 outputs, fetches optional 7-session history,
and replaces a1_tables.html while retaining the prior full report as an appendix.
No scanner signal, score, price, baseline, or trading threshold is modified.
"""
import json, math, os, re
from pathlib import Path
from datetime import datetime, timezone
from html import escape
C={
 'navy':'#0B2E4F','blue':'#1565C0','cyan':'#E8F4FD','green':'#DFF3E4','green2':'#0B8043',
 'yellow':'#FFF3CD','amber':'#F9AB00','red':'#FDE2E1','red2':'#C5221F','purple':'#F3E8FF',
 'gray':'#EEF2F6','dark':'#1F2937','muted':'#5F6B7A','white':'#FFFFFF','border':'#CBD5E1'}
SECTOR_NAMES={'XLK':'Technology','XLF':'Financials','XLV':'Health Care','XLI':'Industrials','XLY':'Consumer Discretionary','XLP':'Consumer Staples','XLE':'Energy','XLU':'Utilities','XLB':'Materials','XLRE':'Real Estate','XLC':'Communication Services'}
SECTOR_ORDER=list(SECTOR_NAMES)
MARKER='<!-- A1_ULTIMATE_DASHBOARD_V1_1 -->'
def load(path,default):
 try:return json.loads(Path(path).read_text(encoding='utf-8'))
 except Exception:return default
def num(v):
 try:
  x=float(v);return x if math.isfinite(x) else None
 except (TypeError,ValueError):return None
def e(v):return escape('Not supplied' if v is None or v=='' else str(v))
def money(v):return 'Not supplied' if num(v) is None else '${:,.2f}'.format(float(v))
def pct(v):return 'Not supplied' if num(v) is None else '{:+.2f}%'.format(float(v))
def pp(v):return 'Not supplied' if num(v) is None else '{:+.2f} pp'.format(float(v))
def score(v):return 'N/A' if num(v) is None else str(int(float(v)))
def atomic(path,text):
 p=Path(path);tmp=p.with_name(p.name+'.ultimate_tmp');tmp.write_text(text,encoding='utf-8');tmp.replace(p)
def atomic_json(path,obj):atomic(path,json.dumps(obj,default=str,allow_nan=False,separators=(',',':')))
def bg_for(value,kind='return'):
 x=num(value)
 if x is None:return C['gray']
 if kind=='score':return C['green'] if x>=80 else C['cyan'] if x>=60 else C['yellow'] if x>=40 else C['red']
 if kind=='change':return C['green'] if x>0 else C['red'] if x<0 else C['gray']
 return C['green'] if x>1 else C['cyan'] if x>=0 else C['red'] if x<-1 else C['yellow']
def freshness_color(text):
 t=str(text).upper()
 if 'RECENT' in t or 'LATEST SESSION' in t or 'CURRENT' in t:return C['green']
 if 'DELAYED' in t or 'EARLIER' in t or 'INTRADAY CHECK' in t:return C['yellow']
 if 'STALE' in t or 'OLDER' in t or 'FAILED' in t or 'FUTURE' in t:return C['red']
 return C['gray']
def cell(text,bg=None,bold=False,align='left'):
 return '<td bgcolor="{0}" style="background:{0};border:1px solid {1};padding:7px 8px;vertical-align:middle;text-align:{2};font-weight:{3};color:{4}">{5}</td>'.format(bg or C['white'],C['border'],align,'700' if bold else '400',C['dark'],e(text))
def raw_cell(trusted_html,bg=None,align='left'):
 # Only locally generated trend_strip HTML may use this function.
 return '<td bgcolor="{0}" style="background:{0};border:1px solid {1};padding:7px 8px;vertical-align:middle;text-align:{2};color:{3}">{4}</td>'.format(bg or C['white'],C['border'],align,C['dark'],trusted_html)
def th(text):return '<th bgcolor="{0}" style="background:{0};color:white;border:1px solid {1};padding:8px;text-align:left;font-weight:700">{2}</th>'.format(C['blue'],C['border'],e(text))
def heading(text,sub=''):
 return '<h2 style="margin:22px 0 5px;color:{0};font:700 18px Segoe UI,Arial">{1}</h2>{2}'.format(C['navy'],e(text),'<div style="font:12px Segoe UI,Arial;color:'+C['muted']+';margin-bottom:8px">'+e(sub)+'</div>' if sub else '')
def table(headers,rows):
 return '<table cellpadding="0" cellspacing="0" style="border-collapse:collapse;width:100%;font:12px Segoe UI,Arial;margin-bottom:14px"><tr>'+''.join(th(x) for x in headers)+'</tr>'+''.join('<tr>'+''.join(c for c in row)+'</tr>' for row in rows)+'</table>'
def cards(items):
 out='<table cellpadding="0" cellspacing="6" style="width:100%;font-family:Segoe UI,Arial;table-layout:fixed"><tr>'
 for label,value,bg,note in items:
  out+='<td bgcolor="{0}" style="background:{0};border:1px solid {1};padding:12px;vertical-align:top"><div style="font-size:10px;text-transform:uppercase;color:{2};font-weight:700">{3}</div><div style="font-size:23px;color:{4};font-weight:800;margin:3px 0">{5}</div><div style="font-size:10px;color:{2}">{6}</div></td>'.format(bg,C['border'],C['muted'],e(label),C['navy'],e(value),e(note))
 return out+'</tr></table>'
def history_fetch(tickers):
 """Optional 7-session adjusted daily closes. Failure never fabricates or blocks base report."""
 result={};errors={}
 try:
  import pandas as pd, yfinance as yf
  names=sorted(set(t for t in tickers if t))
  for i in range(0,len(names),50):
   chunk=names[i:i+50]
   try:raw=yf.download(chunk,period='15d',interval='1d',auto_adjust=True,progress=False,threads=False,timeout=15)
   except Exception as ex:
    for t in chunk:errors[t]=str(ex)[:100]
    continue
   for t in chunk:
    try:
     if isinstance(raw.columns,pd.MultiIndex):
      frame=None
      for level in range(raw.columns.nlevels):
       if t in raw.columns.get_level_values(level):frame=raw.xs(t,axis=1,level=level);break
      if frame is None:raise ValueError('symbol absent')
     elif len(chunk)==1:frame=raw
     else:raise ValueError('unexpected columns')
     closes=frame['Close'].dropna().tail(7)
     if closes.empty:raise ValueError('no closes')
     result[t]={'dates':[str(x)[:10] for x in closes.index],'closes':[round(float(x),4) for x in closes.tolist()]}
    except Exception as ex:errors[t]=str(ex)[:100]
 except Exception as ex:errors['_batch']=str(ex)[:120]
 return result,errors
def trend_strip(history):
 closes=(history or {}).get('closes',[])
 if len(closes)<2:return '<span style="color:{0}">History unavailable</span>'.format(C['muted'])
 lo=min(closes);hi=max(closes);span=hi-lo
 blocks=[]
 for i,x in enumerate(closes):
  level=2 if span==0 else int(round((x-lo)/span*4))
  color=C['green2'] if i and x>=closes[i-1] else C['red2']
  blocks.append('<span title="{0}" style="display:inline-block;width:10px;height:{1}px;background:{2};margin-right:2px;vertical-align:bottom"></span>'.format(e(x),6+level*3,color))
 change=(closes[-1]/closes[0]-1)*100 if closes[0] else None
 return ''.join(blocks)+' <b style="color:{0}">{1}</b>'.format(C['green2'] if change is not None and change>=0 else C['red2'],pct(change))
def section_bar(title,subtitle=''):
 return '<div style="background:{0};color:white;padding:10px 12px;margin-top:20px;font:700 17px Segoe UI,Arial">{1}</div>{2}'.format(C['navy'],e(title),'<div style="font:11px Segoe UI,Arial;color:'+C['muted']+';padding:5px 2px">'+e(subtitle)+'</div>' if subtitle else '')
def build(docs_dir):
 docs=Path(docs_dir);original=(docs/'a1_tables.html').read_text(encoding='utf-8')
 if MARKER in original:return {'status':'ALREADY_BUILT'}
 report=load(docs/'report_scan.json',{});snap=load(docs/'snapshot_full.json',{});audit=load(docs/'a1_audit.json',{});changes=load(docs/'a1_changes.json',{})
 ai=load(docs/'a1_ai.json',{});archive=load(docs/'a1_full_archive.json',{})
 if audit.get('publish_ok') is not True:raise RuntimeError('A1 audit failed; ultimate dashboard not published')
 features=snap.get('daily_features') or [];by={r.get('ticker'):r for r in features if r.get('ticker')}
 alerts=((archive.get('report') or {}).get('alerts') or report.get('alerts') or []);blocked=(report.get('summary') or {}).get('blocked_alerts') or [];sectors=(report.get('summary') or {}).get('sectors') or []
 cross=report.get('cross_assets') or {};options=report.get('options') or {}
 improving=[r for r in features if r.get('panel')=='stock_or_other' and r.get('improving') and r.get('higher_low')]
 improving.sort(key=lambda r:(num(r.get('technical_score')) or -1,num(r.get('relative_20d_vs_spy_pp')) or -999),reverse=True)
 selected=set(SECTOR_ORDER)|{r.get('ticker') for r in alerts}|{r.get('ticker') for r in improving[:15]}|set(cross)
 hist,hist_errors=history_fetch(selected)
 bullish=sum(1 for r in alerts if r.get('signal')=='BOTTOM');bearish=sum(1 for r in alerts if r.get('signal')=='TOP')
 failures=len(snap.get('failures') or [])
 strong=[];weak=[]
 generated=audit.get('generated_at');scan=report.get('scan_time')
 out=MARKER+'<html><body style="margin:0;background:#F3F6F9;font-family:Segoe UI,Arial;color:'+C['dark']+'"><div style="max-width:1180px;margin:auto;background:white">'
 out+='<div style="background:linear-gradient(135deg,'+C['navy']+','+C['blue']+');background-color:'+C['navy']+';padding:22px;color:white"><div style="font-size:27px;font-weight:800">A1 PROFESSIONAL MARKET DASHBOARD</div><div style="font-size:12px;margin-top:6px">Assessed '+e(generated)+' | Scanner '+e(scan)+'</div></div><div style="padding:16px">'
 out+=cards([('Regime',report.get('regime','N/A'),C['cyan'],'Scanner classification'),('SPY RSI','{:.1f}'.format(num(report.get('spy_rsi')) or 0),C['yellow'],'Indicator, not forecast'),('Bullish setups',bullish,C['green'],'BOTTOM scanner signals'),('Bearish setups',bearish,C['red'],'TOP scanner signals'),('Screen matches',len(improving),C['cyan'],'Early-improving daily screen; appendix may use stricter criteria'),('Data failures',failures,C['gray'],'Visible in full appendix')])
 out+='<div style="background:'+C['yellow']+';border-left:5px solid '+C['amber']+';padding:10px;margin:10px 0;font-size:12px"><b>Data integrity:</b> equities use instrument-specific session labels; crypto ages continuously. Near-close bars are not official closes. Daily observations and intraday bars are never silently mixed. Missing values remain visible. Broad coverage does not mean uniform freshness: index observations and option timestamps can remain limited.</div>'
 # Sector heatmap + 7 sessions
 out+=section_bar('1. Sector Rotation Heatmap','All 11 GICS sector ETFs, relative return vs SPY, scan-to-scan change, and optional seven-session adjusted close strip.')
 raw_sec_by={r.get('sector'):r for r in sectors}
 calc_sec_by={r.get('ticker'):r for r in (ai.get('sectors') or [])}
 sec_by={}
 for ticker in SECTOR_ORDER:
  merged=dict(raw_sec_by.get(ticker,{}) or {})
  calculated=calc_sec_by.get(ticker,{}) or {}
  if 'relative_pp' in calculated:merged['diff']=calculated.get('relative_pp')
  if 'change_pp' in calculated:merged['change_pp']=calculated.get('change_pp')
  if calculated.get('stage') is not None:merged['stage']=calculated.get('stage')
  if calculated.get('timestamp'):merged['latest']=calculated.get('timestamp')
  if calculated.get('strength'):merged['strength']=calculated.get('strength')
  sec_by[ticker]=merged
 strong=[t for t,r in sec_by.items() if r.get('strength')=='STRONG']
 weak=[t for t,r in sec_by.items() if r.get('strength')=='WEAK']
 rows=[]
 for t in SECTOR_ORDER:
  r=sec_by.get(t,{})
  rows.append([cell(t,C['blue'] if r else C['gray'],True),cell(SECTOR_NAMES[t]),cell(r.get('strength'),C['green'] if r.get('strength')=='STRONG' else C['red'] if r.get('strength')=='WEAK' else C['yellow'],True),cell(pp(r.get('diff')),bg_for(r.get('diff'),'change')),cell(pp(r.get('change_pp')),bg_for(r.get('change_pp'),'change')),cell(r.get('stage') if r.get('stage') is not None else 'No calculated stage'),cell(r.get('latest') or 'Bar time not supplied'),raw_cell(trend_strip(hist.get(t)))])
 out+=table(['ETF','Sector','Strength','RS vs SPY','Change','Stage','Bar time','7-session strip'],rows)
 out+='<div style="font-size:11px;color:'+C['muted']+'"><b>Leaders:</b> '+e(', '.join(strong) or 'none classified')+' &nbsp; <b>Laggards:</b> '+e(', '.join(weak) or 'none classified')+'</div>'
 # Change since prior scan
 out+=section_bar('2. What Changed Since the Prior Successful Scan','Persistent Section 11 baseline. Missing/non-repeated alerts are not trade instructions.')
 out+='<div style="padding:7px;background:'+C['cyan']+';font-size:12px"><b>Status:</b> '+e(changes.get('status'))+' | <b>Previous:</b> '+e(changes.get('previous_generated_at'))+' | <b>Current:</b> '+e(changes.get('current_generated_at'))+'</div>'
 change_rows=[]
 for r in (changes.get('events') or [])[:20]:
  tone=r.get('tone');bg=C['green'] if tone=='up' else C['red'] if tone=='down' else C['cyan'] if tone=='new' else C['yellow']
  change_rows.append([cell(r.get('ticker'),bg,True),cell(r.get('event'),bg),cell(r.get('field'),bg),cell(r.get('previous'),bg),cell(r.get('current'),bg),cell(r.get('change'),bg),cell(r.get('note'),bg)])
 if change_rows:out+=table(['Ticker','Event','Field','Previous','Current','Change','Meaning'],change_rows)
 else:out+='<div style="padding:10px;background:'+C['gray']+'">'+e(changes.get('message','No comparison events supplied.'))+'</div>'
 # Setups with technicals and 7-session chart
 out+=section_bar('3. Scanner Setups','Confirmed scanner signals separated from daily research. Green BOTTOM and red TOP indicate direction, not an order.')
 setup_rows=[]
 for r in alerts:
  bg=C['green'] if r.get('signal')=='BOTTOM' else C['red']
  d=r.get('rqg_details') or {}
  freshness=r.get('freshness') or ('Bar time supplied; freshness not calculated' if d.get('latest') else 'Signal bar timestamp not supplied')
  readiness=r.get('entry_readiness') or ('Last-session signal - reconfirm' if 'LATEST SESSION' in freshness else 'Intraday confirmation required')
  setup_rows.append([cell(r.get('ticker'),bg,True),cell(r.get('signal'),bg,True),cell(r.get('setup_type') or 'Setup type not supplied'),cell(money(r.get('alert_price'))),cell(score(r.get('rqg_score')),bg_for(r.get('rqg_score'),'score')),cell(score(r.get('confidence')),bg_for(r.get('confidence'),'score')),cell(r.get('rsi') if r.get('rsi') is not None else 'RSI not supplied'),cell(r.get('mfi') if r.get('mfi') is not None else 'MFI not supplied'),cell(r.get('cmf') if r.get('cmf') is not None else 'CMF not supplied'),cell(freshness,freshness_color(freshness)),cell(readiness,freshness_color(readiness)),raw_cell(trend_strip(hist.get(r.get('ticker'))))])
 out+=table(['Ticker','Signal','Setup','Alert price','RQG','Confidence*','RSI','MFI','CMF','Freshness','Readiness','7-session strip'],setup_rows)
 out+='<div style="font-size:10px;color:'+C['muted']+'">* Scanner confidence is a heuristic score, not a probability. Full reasons remain in the evidence appendix.</div>'
 # Early research
 out+=section_bar('4. Early-Improving Bottom-Up Research','Top 12 daily research candidates. Overextension and intraday-confirmation status remain visible.')
 research=[]
 for r in improving[:12]:
  status=r.get('entry_readiness') or r.get('status');sbg=C['red'] if 'OVEREXTENDED' in str(status) else C['yellow']
  research.append([cell(r.get('ticker'),C['cyan'],True),cell(money(r.get('price'))),cell(score(r.get('technical_score')),bg_for(r.get('technical_score'),'score')),cell(pct(r.get('return_3d')),bg_for(r.get('return_3d'))),cell(pct(r.get('return_5d')),bg_for(r.get('return_5d'))),cell(pct(r.get('return_20d')),bg_for(r.get('return_20d'))),cell(pp(r.get('relative_20d_vs_spy_pp')),bg_for(r.get('relative_20d_vs_spy_pp'),'change')),cell(r.get('daily_bar_volume_ratio')),cell(r.get('extension_atr')),cell(status,sbg),raw_cell(trend_strip(hist.get(r.get('ticker'))))])
 out+=table(['Ticker','Price','Score','3D','5D','20D','RS vs SPY','Daily vol','ATR ext.','Research status','7-session strip'],research)
 # Cross assets compact heatmap
 out+=section_bar('5. Cross-Asset Confirmation','Equities, crypto, metals, energy, rates, credit, dollar and volatility with source basis and freshness.')
 cross_rows=[]
 groups={'Equity benchmarks':['SPY','QQQ','DIA','IWM','RSP'],'Crypto':['BTC-USD','ETH-USD','SOL-USD','IBIT','BITO','ETHA','BLOK'],'Metals':['GLD','SLV','GDX','SIL'],'Energy/materials':['USO','UNG','URA','URNM','COPX','LIT'],'Rates/credit':['^TNX','TLT','HYG','LQD','SHY'],'Currency/volatility':['DX-Y.NYB','UUP','^VIX']}
 for group,names in groups.items():
  for t in names:
   q=cross.get(t,{}) or {};d=by.get(t,{}) or {};price=q.get('price',d.get('price'));stamp=q.get('market_timestamp') or str(d.get('bar_timestamp',''))[:10];status=q.get('status') or d.get('daily_status') or 'Status not supplied';basis=q.get('price_basis') or d.get('price_basis') or q.get('instrument_type') or 'Price basis not supplied'
   cross_rows.append([cell(group,C['gray']),cell(t,C['cyan'],True),cell('{:,.2f}'.format(price) if t in {'^TNX','^VIX','DX-Y.NYB'} and num(price) is not None else money(price)),cell(pct(d.get('return_3d')),bg_for(d.get('return_3d'))),cell(pct(d.get('return_20d')),bg_for(d.get('return_20d'))),cell(stamp),cell(status,freshness_color(status)),cell(basis)])
 out+=table(['Group','Instrument','Price / index','3D','20D','Time/date','Status','Basis'],cross_rows)
 # Options, tactical, country concise
 out+=section_bar('6. Options Coverage','Selected-expiration volume ratios. PCR does not establish buyer intent or institutional positioning.')
 opt=[]
 for t,v in options.items():opt.append([cell(t,C['purple'],True),cell(v.get('expiration')),cell(v.get('dte')),cell(v.get('pcr'),bg_for(-1 if num(v.get('pcr')) and num(v.get('pcr'))>1 else 1)),cell(v.get('avg_iv')),cell(v.get('pcr_status')),cell(v.get('quote_timestamp_status') or 'Option quote timestamp not supplied by source')])
 out+=table(['Ticker','Expiration','DTE','PCR','Avg IV','Calculation status','Quote-time status'],opt)
 out+=section_bar('7. Tactical, Country and Theme Monitor','Tactical products remain separate from long-term research. Complete tables are retained in the appendix.')
 country=[r for r in features if r.get('panel')=='country'][:12];tactical=[r for r in features if r.get('panel')=='tactical'][:12]
 combo=[]
 for kind,items,bg in [('Country ETF',country,C['cyan']),('Leveraged/inverse',tactical,C['purple'])]:
  for r in items:combo.append([cell(kind,bg),cell(r.get('ticker'),bg,True),cell(money(r.get('price'))),cell(score(r.get('technical_score')),bg_for(r.get('technical_score'),'score')),cell(pct(r.get('return_5d')),bg_for(r.get('return_5d'))),cell(pct(r.get('return_20d')),bg_for(r.get('return_20d'))),cell(r.get('daily_status') or r.get('bar_timestamp'))])
 out+=table(['Type','Ticker','Price','Score','5D','20D','Data status/date'],combo)
 # Data quality & full appendix
 out+=section_bar('8. Data Quality and Coverage','Nothing is silently converted from stale to current. Exact provider failures and missing values remain in the complete appendix.')
 out+=cards([('11 sectors',str(len({r.get('sector') for r in sectors})),C['green'],'Expected 11'),('Scanner alerts',str(len(alerts)),C['cyan'],'Complete source list'),('Blocked/watch',str(len(blocked)),C['yellow'],'Not outflows'),('Cross assets priced',str(sum(1 for q in cross.values() if q.get('price') is not None)),C['cyan'],'Provider observations'),('History strips',str(len(hist)),C['green'],'Optional daily history'),('History failures',str(len(hist_errors)),C['gray'],'Kept visible')])
 out+='<div style="font-size:11px;color:'+C['muted']+';margin-top:8px">History strips use up to seven adjusted daily closes fetched at dashboard-build time. Failure does not replace or relabel the validated A1 observations. Seven-session strip percentage is first-to-last close, not a forecast.</div>'
 out+=section_bar('9. Complete Evidence Appendix','Every prior A1 table is retained below for auditability. This appendix may be long by design.')
 # Strip document wrappers only; preserve the prior report itself.
 appendix=re.sub(r'^.*?<body[^>]*>','',original,count=1,flags=re.S|re.I);appendix=re.sub(r'</body>.*$','',appendix,count=1,flags=re.S|re.I)
 out+=appendix
 out+='<div style="background:'+C['navy']+';color:white;padding:12px;margin-top:20px;font-size:11px">Research only. No automatic trade execution. Colors communicate category/status, not certainty. Verify current market conditions before acting.</div></div></div></body></html>'
 # Reader-friendly machine-label presentation. Source JSON remains unchanged.
 label_map={
  'HISTORICAL_RESEARCH_NOT_ENTRY_READY':'Daily research - not entry-ready',
  'NOT_EVALUATED':'Not evaluated by a fundamental-data feed',
  'CHAIN_VOLUME_RATIO_SELECTED_EXPIRATION':'Selected-expiration put/call volume ratio',
  'VALID_REVERSAL':'Valid scanner reversal gate',
  'WATCH_ONLY':'Watch only',
  'REJECT_LOW_QUALITY':'Rejected by quality gate',
  'REJECT_TREND_DANGER':'Rejected: trend danger'
 }
 for machine_label,friendly_label in label_map.items():out=out.replace(machine_label,friendly_label)
 # FIX: apply the same labels to the saved appendix copy so the retention check matches.
 for machine_label,friendly_label in label_map.items():appendix=appendix.replace(machine_label,friendly_label)
 # Final integrity checks.
 if len(sec_by)!=11 or any(sec_by[t].get('stage') is None or 'change_pp' not in sec_by[t] for t in SECTOR_ORDER):raise RuntimeError('Ultimate dashboard sector join incomplete')
 if '&lt;span title=' in out or '\\\\<span title=' in out:raise RuntimeError('Seven-session strip was escaped instead of rendered')
 if appendix not in out:raise RuntimeError('Evidence appendix not retained; refusing publication')
 if len(out.encode())<len(original.encode()):raise RuntimeError('Ultimate report lost content; refusing publication')
 backup=docs/'a1_tables_pre_ultimate.html';backup.write_text(original,encoding='utf-8')
 atomic(docs/'a1_tables.html',out)
 # Browser companion: same validated content plus local search/filter controls. Email remains script-free.
 controls='''<div id="a1-controls" style="position:sticky;top:0;z-index:20;background:#0B2E4F;color:white;padding:10px;font:12px Segoe UI,Arial"><b>A1 Dashboard</b> &nbsp; <label for="a1-search">Filter rows:</label> <input id="a1-search" type="search" placeholder="ticker, sector, status..." style="padding:6px;width:260px"> <button type="button" id="a1-clear" style="padding:6px">Clear</button> <span id="a1-count" aria-live="polite" style="margin-left:12px"></span></div>'''
 script='''<script>(function(){var q=document.getElementById('a1-search'),c=document.getElementById('a1-clear'),o=document.getElementById('a1-count');function f(){var x=(q.value||'').toLowerCase(),n=0;document.querySelectorAll('table tr').forEach(function(r,i){if(r.querySelector('th'))return;var show=!x||r.textContent.toLowerCase().indexOf(x)>=0;r.style.display=show?'':'none';if(show)n++;});o.textContent=n+' visible data rows';}q.addEventListener('input',f);c.addEventListener('click',function(){q.value='';f();q.focus();});f();})();</script>'''
 browser=out.replace('<div style="max-width:1180px',controls+'<div style="max-width:1180px',1).replace('</body></html>',script+'</body></html>')
 atomic(docs/'a1_dashboard.html',browser)
 details={'version':'ultimate-v1.1-final','generated_at':datetime.now(timezone.utc).isoformat(),'prior_html_bytes':len(original.encode()),'ultimate_html_bytes':len(out.encode()),'sector_calculated_fields_complete':all(sec_by[t].get('stage') is not None and 'change_pp' in sec_by[t] for t in SECTOR_ORDER),'trend_strips_rendered_as_html':('&lt;span title=' not in out),'alert_rows_with_explicit_freshness':sum(1 for r in alerts if r.get('freshness') or (r.get('rqg_details') or {}).get('latest')),'history_symbols':len(hist),'history_failures':hist_errors,'all_prior_html_retained':appendix in out,'browser_dashboard_bytes':len(browser.encode()),'sections':['sector heatmap','changes','scanner setups','bottom-up research','cross-assets','options','country/tactical','quality','complete appendix']}
 atomic_json(docs/'a1_ultimate_audit.json',details)
 audit['ultimate_dashboard']=details;audit['html_bytes']=len(out.encode());atomic_json(docs/'a1_audit.json',audit)
 return details
# A1_ULTIMATE_DASHBOARD_V1_1
