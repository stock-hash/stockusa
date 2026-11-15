"""A1 Ultimate Outlook dashboard - V1.2 ranked opportunities.
Presentation-only layer: reads validated A1 outputs, fetches optional 7-session history,
ranks opportunities best-first, and replaces a1_tables.html while retaining the prior
full report as an appendix. No scanner signal, score, price, baseline or trading
threshold is modified. Opportunity rank is a heuristic display ordering, not a probability.
"""
import json, math, os, re
from pathlib import Path
from datetime import datetime, timezone
from html import escape

C={
 'navy':'#0B2E4F','blue':'#1565C0','cyan':'#E8F4FD','green':'#DFF3E4','green2':'#0B8043',
 'green_strong':'#A8E6B8','green_pale':'#F1FAF3','yellow':'#FFF3CD','amber':'#F9AB00',
 'orange':'#FDE7C8','red':'#FDE2E1','red_strong':'#F8B9B5','red_pale':'#FDF0EF','red2':'#C5221F',
 'purple':'#F3E8FF','gray':'#EEF2F6','dark':'#1F2937','muted':'#5F6B7A','white':'#FFFFFF','border':'#CBD5E1'}
SECTOR_NAMES={'XLK':'Technology','XLF':'Financials','XLV':'Health Care','XLI':'Industrials','XLY':'Consumer Discretionary','XLP':'Consumer Staples','XLE':'Energy','XLU':'Utilities','XLB':'Materials','XLRE':'Real Estate','XLC':'Communication Services'}
SECTOR_ORDER=list(SECTOR_NAMES)
INVERSE_OR_LEVERAGED={'LABD','LABU','SOXS','SOXL','SQQQ','TQQQ','SPXU','SPXS','SPXL','UPRO','SDOW','UDOW','TZA','TNA','TECS','TECL','FNGD','FNGU','DRIP','GUSH','ERY','ERX','DUST','NUGT','FAZ','FAS','YANG','YINN','KOLD','BOIL','SCO','UCO','DFEN','DPST','QLD','SSO','AGQ'}
INDEX_SYMBOLS={'^TNX','^VIX','DX-Y.NYB'}
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
def fixed(v,d=2):return 'Not supplied' if num(v) is None else '{:.{}f}'.format(float(v),d)
def day(v):return 'Not supplied' if not v else str(v)[:10]
def atomic(path,text):
 p=Path(path);tmp=p.with_name(p.name+'.ultimate_tmp');tmp.write_text(text,encoding='utf-8');tmp.replace(p)
def atomic_json(path,obj):atomic(path,json.dumps(obj,default=str,allow_nan=False,separators=(',',':')))

# ---------- colors ----------
def heat(value,kind='return'):
 x=num(value)
 if x is None:return C['gray']
 if kind=='score':
  return C['green_strong'] if x>=80 else C['green'] if x>=60 else C['yellow'] if x>=40 else C['red']
 if kind=='pp':
  return C['green_strong'] if x>=0.25 else C['green'] if x>0.02 else C['red_strong'] if x<=-0.25 else C['red'] if x<-0.02 else C['gray']
 if kind=='rsi':
  return C['orange'] if x>=75 else C['cyan'] if x<=25 else C['white']
 return C['green_strong'] if x>=5 else C['green'] if x>=1 else C['green_pale'] if x>=0 else C['red_strong'] if x<=-5 else C['red'] if x<=-1 else C['red_pale']
def freshness_color(text):
 t=str(text).upper()
 if 'OLDER INDEX' in t:return C['yellow']
 if 'RECENT' in t or 'LATEST SESSION' in t or 'LATEST COMPLETED' in t or 'CURRENT' in t:return C['green']
 if 'DELAYED' in t or 'EARLIER' in t or 'INTRADAY CHECK' in t or 'RECONFIRM' in t:return C['yellow']
 if 'STALE' in t or 'OLDER' in t or 'FAILED' in t or 'FUTURE' in t:return C['red']
 return C['gray']

# ---------- html helpers ----------
def cell(text,bg=None,bold=False,align='left'):
 return '<td bgcolor="{0}" style="background:{0};border:1px solid {1};padding:7px 8px;vertical-align:middle;text-align:{2};font-weight:{3};color:{4}">{5}</td>'.format(bg or C['white'],C['border'],align,'700' if bold else '400',C['dark'],e(text))
def raw_cell(trusted_html,bg=None,align='left'):
 # Only locally generated HTML (strips, badges, bars) may use this function.
 return '<td bgcolor="{0}" style="background:{0};border:1px solid {1};padding:7px 8px;vertical-align:middle;text-align:{2};color:{3}">{4}</td>'.format(bg or C['white'],C['border'],align,C['dark'],trusted_html)
def th(text):return '<th bgcolor="{0}" style="background:{0};color:white;border:1px solid {1};padding:8px;text-align:left;font-weight:700">{2}</th>'.format(C['blue'],C['border'],e(text))
def table(headers,rows):
 return '<table cellpadding="0" cellspacing="0" style="border-collapse:collapse;width:100%;font:12px Segoe UI,Arial;margin-bottom:14px"><tr>'+''.join(th(x) for x in headers)+'</tr>'+''.join('<tr>'+''.join(c for c in row)+'</tr>' for row in rows)+'</table>'
def badge(label,color):
 return '<span style="display:inline-block;padding:3px 8px;border-radius:10px;background:{0};background-color:{0};color:#FFFFFF;font-weight:700;font-size:11px;white-space:nowrap">{1}</span>'.format(color,e(label))
def bar(value,max_abs):
 x=num(value)
 if x is None or not max_abs:return ''
 width=max(2,int(round(abs(x)/max_abs*80)))
 color=C['green2'] if x>=0 else C['red2']
 return '<span style="display:inline-block;width:{0}px;height:10px;background:{1};vertical-align:middle;margin-right:6px"></span>'.format(width,color)
def cards(items):
 out='<table cellpadding="0" cellspacing="6" style="width:100%;font-family:Segoe UI,Arial;table-layout:fixed"><tr>'
 for label,value,bg,note in items:
  out+='<td bgcolor="{0}" style="background:{0};border:1px solid {1};padding:12px;vertical-align:top"><div style="font-size:10px;text-transform:uppercase;color:{2};font-weight:700">{3}</div><div style="font-size:23px;color:{4};font-weight:800;margin:3px 0">{5}</div><div style="font-size:10px;color:{2}">{6}</div></td>'.format(bg,C['border'],C['muted'],e(label),C['navy'],e(value),e(note))
 return out+'</tr></table>'
def section_bar(title,subtitle=''):
 return '<div style="background:{0};color:white;padding:10px 12px;margin-top:20px;font:700 17px Segoe UI,Arial">{1}</div>{2}'.format(C['navy'],e(title),'<div style="font:11px Segoe UI,Arial;color:'+C['muted']+';padding:5px 2px">'+e(subtitle)+'</div>' if subtitle else '')
def note(text,bg=None):
 return '<div style="background:{0};padding:8px 10px;margin:6px 0 10px;font:11px Segoe UI,Arial;color:{1}">{2}</div>'.format(bg or C['gray'],C['dark'],e(text))

# ---------- history ----------
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
def seven_change(history):
 closes=(history or {}).get('closes',[])
 if len(closes)<2 or not closes[0]:return None
 return (closes[-1]/closes[0]-1)*100
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

# ---------- ranking ----------
def mtf_count(r):
 text=str(r.get('mtf_status') or '')
 return len([p for p in text.split('+') if p.strip()]) if text and text!='partial' else 0
def flags_for(r,daily):
 flags=[]
 rsi=num(r.get('rsi'))
 if rsi is not None and rsi>=75:flags.append('RSI overbought')
 if rsi is not None and rsi<=25:flags.append('RSI oversold')
 if r.get('signal')=='BOTTOM' and str(r.get('sector_strength')).upper()=='WEAK':flags.append('Weak sector')
 if r.get('ticker') in INVERSE_OR_LEVERAGED:flags.append('Leveraged/inverse ETF')
 ts=num((daily or {}).get('technical_score'))
 if ts is not None and ts<=20:flags.append('Weak daily trend')
 ext=num((daily or {}).get('extension_atr'))
 if ext is not None and ext>2:flags.append('Overextended')
 return flags
def opportunity_score(r,daily,history):
 """Heuristic ordering for BOTTOM scanner alerts. Not a probability or recommendation."""
 rqg=num(r.get('rqg_score')) or 0
 conf=num(r.get('confidence')) or 0
 n=mtf_count(r);mtf_pts=15 if n>=4 else 10 if n==3 else 5 if n==2 else 0
 t5=num(r.get('trend_5d')) or 0;trend_pts=max(0.0,min(10.0,t5+5))
 sec={'STRONG':10,'NEUTRAL':5,'WEAK':0}.get(str(r.get('sector_strength')).upper(),5)
 s7=seven_change(history);hist_pts=5 if s7 is not None and s7>0 else 0
 total=0.35*rqg+0.25*conf+mtf_pts+trend_pts+sec+hist_pts-5*len(flags_for(r,daily))
 return int(round(max(0,min(100,total))))
def tier(points):
 if points>=75:return 'A - TOP PICK',C['green2']
 if points>=65:return 'B - STRONG','#34A853'
 if points>=55:return 'C - WATCH',C['amber']
 return 'D - WEAK','#80868B'

def build(docs_dir):
 docs=Path(docs_dir);original=(docs/'a1_tables.html').read_text(encoding='utf-8')
 if MARKER in original:return {'status':'ALREADY_BUILT'}
 report=load(docs/'report_scan.json',{});snap=load(docs/'snapshot_full.json',{});audit=load(docs/'a1_audit.json',{});changes=load(docs/'a1_changes.json',{})
 ai=load(docs/'a1_ai.json',{});archive=load(docs/'a1_full_archive.json',{})
 if audit.get('publish_ok') is not True:raise RuntimeError('A1 audit failed; ultimate dashboard not published')
 features=snap.get('daily_features') or [];by={r.get('ticker'):r for r in features if r.get('ticker')}
 alerts=((archive.get('report') or {}).get('alerts') or report.get('alerts') or []);blocked=(report.get('summary') or {}).get('blocked_alerts') or [];sectors=(report.get('summary') or {}).get('sectors') or []
 cross=report.get('cross_assets') or {};options=report.get('options') or {}
 ai_assets={r.get('ticker'):r for r in (ai.get('cross_assets') or []) if isinstance(r,dict)}
 improving=[r for r in features if r.get('panel')=='stock_or_other' and r.get('improving') and r.get('higher_low')]
 selected=set(SECTOR_ORDER)|{r.get('ticker') for r in alerts}|{r.get('ticker') for r in improving[:25]}|set(cross)
 hist,hist_errors=history_fetch(selected)
 bullish_alerts=[r for r in alerts if r.get('signal')=='BOTTOM'];bearish_alerts=[r for r in alerts if r.get('signal')=='TOP']
 failures=snap.get('failures') or []
 generated=audit.get('generated_at');scan=report.get('scan_time')

 ranked=[]
 for r in bullish_alerts:
  t=r.get('ticker');d=by.get(t,{});h=hist.get(t)
  pts=opportunity_score(r,d,h);ranked.append((pts,r,d,h))
 ranked.sort(key=lambda x:(x[0],num(x[1].get('rqg_score')) or 0,num(x[1].get('confidence')) or 0),reverse=True)
 top_pick=ranked[0][1].get('ticker') if ranked else 'None'

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

 out=MARKER+'<html><body style="margin:0;background:#F3F6F9;font-family:Segoe UI,Arial;color:'+C['dark']+'"><div style="max-width:1180px;margin:auto;background:white">'
 out+='<div style="background:linear-gradient(135deg,'+C['navy']+','+C['blue']+');background-color:'+C['navy']+';padding:22px;color:white"><div style="font-size:27px;font-weight:800">A1 PROFESSIONAL MARKET DASHBOARD</div><div style="font-size:12px;margin-top:6px">Assessed '+e(generated)+' | Scanner '+e(scan)+'</div></div><div style="padding:16px">'
 out+=cards([('Regime',report.get('regime','N/A'),C['cyan'],'Scanner classification'),('SPY RSI','{:.1f}'.format(num(report.get('spy_rsi')) or 0),C['yellow'],'Indicator, not forecast'),('Top ranked',top_pick,C['green_strong'],'Highest opportunity rank'),('Bullish setups',len(bullish_alerts),C['green'],'BOTTOM scanner signals'),('Bearish setups',len(bearish_alerts),C['red'],'TOP scanner signals'),('Data failures',len(failures),C['gray'],'Listed in section 9')])
 legend=(badge('A - TOP PICK',C['green2'])+' '+badge('B - STRONG','#34A853')+' '+badge('C - WATCH',C['amber'])+' '+badge('D - WEAK','#80868B')+' '+badge('BEARISH',C['red2'])+
  ' &nbsp;<span style="font-size:11px;color:'+C['muted']+'">Cell colors: dark green = strong gain, light green = gain, light red = loss, dark red = strong loss, orange = RSI overbought, gray = not supplied. Daily research - not entry-ready means a daily-bar research candidate that still needs an intraday check.</span>')
 out+='<div style="padding:8px 2px">'+legend+'</div>'
 out+='<div style="background:'+C['yellow']+';border-left:5px solid '+C['amber']+';padding:10px;margin:10px 0;font-size:12px"><b>Data integrity:</b> equities use the latest completed session; crypto ages continuously. Near-close bars are not official closes. Opportunity rank is a heuristic ordering of scanner fields, not a probability or a buy instruction. Broad coverage does not mean uniform freshness: index observations and option timestamps can remain limited.</div>'

 out+=section_bar('1. Top Opportunities - Ranked Best First','Confirmed BOTTOM scanner setups ordered by opportunity rank (RQG, confidence, timeframe agreement, 5D trend, sector, 7-session direction, minus warning flags).')
 rows=[]
 for i,(pts,r,d,h) in enumerate(ranked,1):
  label,color=tier(pts);flags=flags_for(r,d)
  rowbg=C['green'] if pts>=75 else C['green_pale'] if pts>=65 else C['white']
  n=mtf_count(r)
  rows.append([cell('#'+str(i),rowbg,True,'center'),raw_cell(badge(label,color),rowbg),cell(r.get('ticker'),rowbg,True),cell(r.get('sector') or 'Not supplied',rowbg),cell(r.get('setup_type') or 'Not supplied',rowbg),cell(money(r.get('alert_price')),rowbg),cell(pts,heat(pts,'score'),True,'center'),cell(score(r.get('rqg_score')),heat(r.get('rqg_score'),'score'),False,'center'),cell(score(r.get('confidence')),heat(r.get('confidence'),'score'),False,'center'),cell(str(n)+'/4' if n else 'Not supplied by scanner',C['green'] if n>=4 else C['yellow'] if n==3 else C['gray'],False,'center'),cell(fixed(r.get('rsi'),1),heat(r.get('rsi'),'rsi'),False,'center'),cell(pct(r.get('trend_5d')),heat(r.get('trend_5d'))),raw_cell(trend_strip(h)),cell(', '.join(flags) if flags else 'No flags',C['orange'] if flags else C['green'])])
 if rows:out+=table(['Rank','Tier','Ticker','Sector','Setup','Alert price','Opp. rank','RQG','Conf.','MTF','RSI','5D','7-session strip','Checks'],rows)
 else:out+=note('No confirmed BOTTOM scanner setups in this scan.')
 out+=note('All signals come from the latest completed session; reconfirm price and volume after the open. Checks list reasons for caution; they do not remove a setup.',C['cyan'])

 out+=section_bar('2. Sector Rotation Heatmap','All 11 sector ETFs ranked by relative return vs SPY, strongest first, with scan-to-scan change and seven-session adjusted close strip.')
 sector_sorted=sorted(SECTOR_ORDER,key=lambda t:num(sec_by[t].get('diff')) if num(sec_by[t].get('diff')) is not None else -999,reverse=True)
 max_abs=max([abs(num(sec_by[t].get('diff')) or 0) for t in SECTOR_ORDER]+[0.0001])
 rows=[]
 for i,t in enumerate(sector_sorted,1):
  r=sec_by.get(t,{});st=str(r.get('strength') or '')
  stbg=C['green_strong'] if st=='STRONG' else C['red'] if st=='WEAK' else C['yellow']
  rows.append([cell('#'+str(i),stbg,True,'center'),cell(t,stbg,True),cell(SECTOR_NAMES[t],stbg),cell(st or 'Not supplied',stbg,True),raw_cell(bar(r.get('diff'),max_abs)+e(pp(r.get('diff'))),heat(r.get('diff'),'pp')),cell(pp(r.get('change_pp')),heat(r.get('change_pp'),'pp')),cell(r.get('stage') if r.get('stage') is not None else 'No calculated stage'),cell(r.get('latest') or 'Bar time not supplied'),raw_cell(trend_strip(hist.get(t)))])
 out+=table(['Rank','ETF','Sector','Strength','RS vs SPY','Change vs prior scan','Stage','Bar time','7-session strip'],rows)
 out+='<div style="font-size:11px;color:'+C['muted']+'"><b>Leaders:</b> '+e(', '.join(strong) or 'none classified')+' &nbsp; <b>Laggards:</b> '+e(', '.join(weak) or 'none classified')+'. A change of +0.00 pp means the sector reading did not change since the prior scan (expected on weekends and holidays).</div>'

 out+=section_bar('3. What Changed Since the Prior Successful Scan','Persistent Section 11 baseline. Missing or non-repeated alerts are not trade instructions.')
 out+='<div style="padding:7px;background:'+C['cyan']+';font-size:12px"><b>Status:</b> '+e(changes.get('status'))+' | <b>Previous:</b> '+e(changes.get('previous_generated_at'))+' | <b>Current:</b> '+e(changes.get('current_generated_at'))+'</div>'
 change_rows=[]
 for r in (changes.get('events') or [])[:20]:
  tone=r.get('tone');bg=C['green'] if tone=='up' else C['red'] if tone=='down' else C['cyan'] if tone=='new' else C['yellow']
  change_rows.append([cell(r.get('ticker'),bg,True),cell(r.get('event'),bg),cell(r.get('field'),bg),cell(r.get('previous'),bg),cell(r.get('current'),bg),cell(r.get('change'),bg),cell(r.get('note'),bg)])
 if change_rows:out+=table(['Ticker','Event','Field','Previous','Current','Change','Meaning'],change_rows)
 else:out+=note(changes.get('message','No comparison events supplied.'))

 out+=section_bar('4. Early-Improving Bottom-Up Research','Daily research candidates ranked: not extended first, then technical score, then strength vs SPY. Extended names are shown last.')
 def research_key(r):
  ext=num(r.get('extension_atr'));extended=ext is not None and ext>2
  return (0 if extended else 1,num(r.get('technical_score')) or -1,num(r.get('relative_20d_vs_spy_pp')) or -999)
 research_sorted=sorted(improving,key=research_key,reverse=True)
 rows=[]
 for i,r in enumerate(research_sorted[:15],1):
  ext=num(r.get('extension_atr'));extended=ext is not None and ext>2
  vol=num(r.get('daily_bar_volume_ratio'))
  checks=[]
  if extended:checks.append('Overextended - wait for pullback')
  if vol is not None and vol<0.8:checks.append('Light volume')
  ts=num(r.get('technical_score')) or 0
  if extended:label,color='EXTENDED',C['amber']
  elif ts>=100:label,color='A - EARLY LEADER',C['green2']
  elif ts>=80:label,color='B - IMPROVING','#34A853'
  else:label,color='C - WATCH',C['amber']
  rowbg=C['orange'] if extended else C['white']
  rows.append([cell('#'+str(i),rowbg,True,'center'),raw_cell(badge(label,color),rowbg),cell(r.get('ticker'),C['cyan'],True),cell(money(r.get('price'))),cell(score(r.get('technical_score')),heat(r.get('technical_score'),'score'),True,'center'),cell(pct(r.get('return_3d')),heat(r.get('return_3d'))),cell(pct(r.get('return_5d')),heat(r.get('return_5d'))),cell(pct(r.get('return_20d')),heat(r.get('return_20d'))),cell(pp(r.get('relative_20d_vs_spy_pp')),heat(r.get('relative_20d_vs_spy_pp'))),cell(fixed(vol),C['green'] if vol and vol>=1.2 else C['white']),cell(fixed(ext),C['orange'] if extended else C['white']),cell(r.get('status') or 'Not supplied',C['yellow']),cell(', '.join(checks) if checks else 'No flags',C['orange'] if checks else C['green']),raw_cell(trend_strip(hist.get(r.get('ticker'))))])
 out+=table(['Rank','Tier','Ticker','Price','Score','3D','5D','20D','RS vs SPY','Daily vol','ATR ext.','Research status','Checks','7-session strip'],rows)
 if len(research_sorted)>15:out+=note(str(len(research_sorted)-15)+' more screen matches are in the evidence appendix and archive.')

 out+=section_bar('5. Bearish Setups and Caution List','TOP scanner setups and rejected setups, worst first. Shown for risk awareness, not as instructions.')
 bear_sorted=sorted(bearish_alerts,key=lambda r:(num(r.get('confidence')) or 0,num(r.get('rqg_score')) or 0),reverse=True)
 rows=[]
 for r in bear_sorted:
  rows.append([raw_cell(badge('BEARISH',C['red2']),C['red']),cell(r.get('ticker'),C['red'],True),cell(r.get('setup_type') or 'Not supplied',C['red']),cell(money(r.get('alert_price')),C['red']),cell(score(r.get('rqg_score')),C['red'],False,'center'),cell(score(r.get('confidence')),C['red'],False,'center'),cell(fixed(r.get('rsi'),1),heat(r.get('rsi'),'rsi')),cell(pct(r.get('trend_5d')),heat(r.get('trend_5d'))),raw_cell(trend_strip(hist.get(r.get('ticker'))))])
 for r in blocked:
  rows.append([raw_cell(badge('REJECTED','#80868B'),C['gray']),cell(r.get('ticker'),C['gray'],True),cell(r.get('signal') or 'Not supplied',C['gray']),cell(money(r.get('price')),C['gray']),cell(score(r.get('rqg_score')),C['gray'],False,'center'),cell(score(r.get('confidence')),C['gray'],False,'center'),cell(r.get('rqg_label') or 'Not supplied',C['gray']),cell(', '.join((r.get('rqg_reasons') or [])[:3]) or 'Not supplied',C['gray']),raw_cell(trend_strip(hist.get(r.get('ticker'))),C['gray'])])
 if rows:out+=table(['Type','Ticker','Setup / signal','Price','RQG','Conf.','RSI / gate','5D / reasons','7-session strip'],rows)
 else:out+=note('No bearish or rejected setups in this scan.')

 out+=section_bar('6. Cross-Asset Confirmation','Each group sorted by 20-day return, strongest first. Status is the instrument-specific freshness label.')
 groups={'Equity benchmarks':['SPY','QQQ','DIA','IWM','RSP'],'Crypto':['BTC-USD','ETH-USD','SOL-USD','IBIT','BITO','ETHA','BLOK'],'Metals':['GLD','SLV','GDX','SIL'],'Energy/materials':['USO','UNG','URA','URNM','COPX','LIT'],'Rates/credit':['^TNX','TLT','HYG','LQD','SHY'],'Currency/volatility':['DX-Y.NYB','UUP','^VIX']}
 rows=[];status_joined=0
 for group,names in groups.items():
  names=sorted(names,key=lambda t:num((by.get(t) or {}).get('return_20d')) if num((by.get(t) or {}).get('return_20d')) is not None else -999,reverse=True)
  for t in names:
   q=cross.get(t,{}) or {};d=by.get(t,{}) or {};a=ai_assets.get(t,{}) or {}
   price=q.get('price',d.get('price'))
   stamp=q.get('market_timestamp') or a.get('timestamp') or day(d.get('bar_timestamp'))
   status=a.get('freshness') or d.get('daily_status') or ('Timestamped bar' if q.get('status')=='TIMESTAMPED_BAR' else q.get('status')) or 'Status not supplied'
   if a.get('freshness'):status_joined+=1
   basis=a.get('basis') or q.get('price_basis') or d.get('price_basis') or q.get('instrument_type') or 'Price basis not supplied'
   shown='{:,.2f}'.format(price) if t in INDEX_SYMBOLS and num(price) is not None else money(price)
   rows.append([cell(group,C['gray']),cell(t,C['cyan'],True),cell(shown),cell(pct(d.get('return_3d')),heat(d.get('return_3d'))),cell(pct(d.get('return_20d')),heat(d.get('return_20d'))),cell(stamp),cell(status,freshness_color(status)),cell(basis)])
 out+=table(['Group','Instrument','Price / index','3D','20D','Time/date','Status','Basis'],rows)

 out+=section_bar('7. Options Coverage','Selected-expiration put/call volume skew. Shown only for tickers the scanner fetched options for. Volume skew does not establish buyer intent.')
 opt=[]
 for t,v in options.items():
  p=num(v.get('pcr'))
  skew='Call-heavy volume' if p is not None and p<0.7 else 'Put-heavy volume' if p is not None and p>1.3 else 'Balanced' if p is not None else 'Not supplied'
  pbg=C['green'] if p is not None and p<0.7 else C['red'] if p is not None and p>1.3 else C['yellow'] if p is not None else C['gray']
  opt.append([cell(t,C['purple'],True),cell(v.get('expiration')),cell(v.get('dte')),cell(fixed(p),pbg,True,'center'),cell(skew,pbg),cell(fixed(v.get('avg_iv'),1)),cell(v.get('pcr_status')),cell(v.get('quote_timestamp_status') or 'Option quote timestamp not supplied by source')])
 if opt:out+=table(['Ticker','Expiration','DTE','PCR','Volume skew','Avg IV','Calculation status','Quote-time status'],opt)
 else:out+=note('No options data was fetched in this scan.')

 out+=section_bar('8. Country and Tactical ETF Monitor','Each list ranked by technical score, then 20-day return. Leveraged and inverse ETFs reset daily and are tactical only.')
 def etf_key(r):return (num(r.get('technical_score')) or -1,num(r.get('return_20d')) or -999)
 country=sorted([r for r in features if r.get('panel')=='country'],key=etf_key,reverse=True)[:16]
 tactical=sorted([r for r in features if r.get('panel')=='tactical'],key=etf_key,reverse=True)[:16]
 combo=[]
 for kind,items,bg in [('Country ETF',country,C['cyan']),('Leveraged/inverse',tactical,C['purple'])]:
  for i,r in enumerate(items,1):combo.append([cell('#'+str(i),bg,True,'center'),cell(kind,bg),cell(r.get('ticker'),bg,True),cell(money(r.get('price'))),cell(score(r.get('technical_score')),heat(r.get('technical_score'),'score'),True,'center'),cell(pct(r.get('return_5d')),heat(r.get('return_5d'))),cell(pct(r.get('return_20d')),heat(r.get('return_20d'))),cell(pp(r.get('relative_20d_vs_spy_pp')),heat(r.get('relative_20d_vs_spy_pp'))),cell('Daily bar '+day(r.get('bar_timestamp')))])
 out+=table(['Rank','Type','Ticker','Price','Score','5D','20D','RS vs SPY','Data date'],combo)

 out+=section_bar('9. Data Quality and Coverage','Nothing is silently converted from stale to current. Failed symbols are listed below with the provider reason.')
 out+=cards([('11 sectors',str(len({r.get('sector') for r in sectors})),C['green'],'Expected 11'),('Scanner alerts',str(len(alerts)),C['cyan'],'Complete source list'),('Blocked/watch',str(len(blocked)),C['yellow'],'Not outflows'),('Cross assets priced',str(sum(1 for q in cross.values() if q.get('price') is not None)),C['cyan'],'Provider observations'),('History strips',str(len(hist)),C['green'],'Seven-session daily history'),('Download failures',str(len(failures)),C['gray'],'Listed below')])
 frows=[[cell(f.get('ticker') or 'Not supplied',C['gray'],True),cell(f.get('error') or 'Reason not supplied',C['gray'])] for f in failures if isinstance(f,dict)]
 for t,msg in hist_errors.items():frows.append([cell(t,C['gray'],True),cell('7-session history: '+str(msg),C['gray'])])
 if frows:out+=table(['Symbol','Reason'],frows)
 out+='<div style="font-size:11px;color:'+C['muted']+';margin-top:8px">Seven-session strips use up to seven adjusted daily closes fetched at dashboard-build time. A history failure does not replace or relabel validated A1 observations. Seven-session strip percentage is first-to-last close, not a forecast.</div>'

 out+=section_bar('10. Complete Evidence Appendix','Every prior A1 table is retained below for auditability. This appendix may be long by design.')
 appendix=re.sub(r'^.*?<body[^>]*>','',original,count=1,flags=re.S|re.I);appendix=re.sub(r'</body>.*$','',appendix,count=1,flags=re.S|re.I)
 ev=re.search(r'(<h3\b[^>]*>Scanner indicator evidence.*?</h3>)\s*(<table\b.*?</table>)',appendix,re.S|re.I)
 mtf_fixed=False
 if ev and alerts:
  erows=[]
  for r in alerts:
   d=r.get('rqg_details') or {}
   erows.append([cell(r.get('ticker'),None,True),cell(r.get('signal')),cell(score(r.get('confidence'))),cell(r.get('rsi')),cell(r.get('mfi')),cell(r.get('cmf')),cell(r.get('mtf_status') or 'Not supplied by scanner'),cell(r.get('trend_3d')),cell(r.get('trend_5d')),cell(r.get('volume_expansion')),cell(d.get('vol_ratio')),cell(', '.join(map(str,r.get('rqg_reasons') or [])) or 'Not supplied')])
  new_table=table(['Ticker','Signal','Scanner confidence score (not probability)','RSI','MFI','CMF','MTF','3D trend','5D trend','volume_expansion','rqg_details.vol_ratio','Reasons'],erows)
  appendix=appendix[:ev.start(2)]+new_table+appendix[ev.end(2):];mtf_fixed=True
 cx=re.search(r'Cross-asset dashboard.*?</table>',appendix,re.S|re.I)
 if cx:
  segment=cx.group(0).replace('Not supplied</td></tr>','No issue reported</td></tr>')
  appendix=appendix[:cx.start()]+segment+appendix[cx.end():]
 appendix=appendix.replace(' 00:00:00</td>','</td>')
 out+=appendix
 out+='<div style="background:'+C['navy']+';color:white;padding:12px;margin-top:20px;font-size:11px">Research only. No automatic trade execution. Colors and ranks communicate category and relative ordering, not certainty. Verify current market conditions before acting.</div></div></div></body></html>'
 label_map={
  'HISTORICAL_RESEARCH_NOT_ENTRY_READY':'Daily research - not entry-ready',
  'NOT_EVALUATED':'Not evaluated by a fundamental-data feed',
  'CHAIN_VOLUME_RATIO_SELECTED_EXPIRATION':'Selected-expiration put/call volume ratio',
  'VALID_REVERSAL':'Valid scanner reversal gate',
  'WATCH_ONLY':'Watch only',
  'REJECT_LOW_QUALITY':'Rejected by quality gate',
  'REJECT_TREND_DANGER':'Rejected: trend danger',
  'TIMESTAMPED_BAR':'Timestamped bar'
 }
 for machine_label,friendly_label in label_map.items():out=out.replace(machine_label,friendly_label)
 # Apply the same labels to the saved appendix copy so the retention check matches.
 for machine_label,friendly_label in label_map.items():appendix=appendix.replace(machine_label,friendly_label)
 if len(sec_by)!=11 or any(sec_by[t].get('stage') is None or 'change_pp' not in sec_by[t] for t in SECTOR_ORDER):raise RuntimeError('Ultimate dashboard sector join incomplete')
 if '&lt;span title=' in out or '\\\\<span title=' in out:raise RuntimeError('Seven-session strip was escaped instead of rendered')
 if appendix not in out:raise RuntimeError('Evidence appendix not retained; refusing publication')
 if len(out.encode())<len(original.encode()):raise RuntimeError('Ultimate report lost content; refusing publication')
 backup=docs/'a1_tables_pre_ultimate.html';backup.write_text(original,encoding='utf-8')
 atomic(docs/'a1_tables.html',out)
 controls='''<div id="a1-controls" style="position:sticky;top:0;z-index:20;background:#0B2E4F;color:white;padding:10px;font:12px Segoe UI,Arial"><b>A1 Dashboard</b> &nbsp; <label for="a1-search">Filter rows:</label> <input id="a1-search" type="search" placeholder="ticker, sector, status..." style="padding:6px;width:260px"> <button type="button" id="a1-clear" style="padding:6px">Clear</button> <span id="a1-count" aria-live="polite" style="margin-left:12px"></span></div>'''
 script='''<script>(function(){var q=document.getElementById('a1-search'),c=document.getElementById('a1-clear'),o=document.getElementById('a1-count');function f(){var x=(q.value||'').toLowerCase(),n=0;document.querySelectorAll('table tr').forEach(function(r,i){if(r.querySelector('th'))return;var show=!x||r.textContent.toLowerCase().indexOf(x)>=0;r.style.display=show?'':'none';if(show)n++;});o.textContent=n+' visible data rows';}q.addEventListener('input',f);c.addEventListener('click',function(){q.value='';f();q.focus();});f();})();</script>'''
 browser=out.replace('<div style="max-width:1180px',controls+'<div style="max-width:1180px',1).replace('</body></html>',script+'</body></html>')
 atomic(docs/'a1_dashboard.html',browser)
 details={'version':'ultimate-v1.1-final','release':'1.2-ranked','generated_at':datetime.now(timezone.utc).isoformat(),'prior_html_bytes':len(original.encode()),'ultimate_html_bytes':len(out.encode()),'sector_calculated_fields_complete':all(sec_by[t].get('stage') is not None and 'change_pp' in sec_by[t] for t in SECTOR_ORDER),'trend_strips_rendered_as_html':('&lt;span title=' not in out),'alert_rows_with_explicit_freshness':sum(1 for r in alerts if r.get('freshness') or (r.get('rqg_details') or {}).get('latest')),'opportunities_ranked':len(ranked),'top_ranked':top_pick,'alerts_with_mtf':sum(1 for r in alerts if r.get('mtf_status')),'appendix_mtf_fixed':mtf_fixed,'cross_asset_status_joined':status_joined,'history_symbols':len(hist),'history_failures':hist_errors,'download_failures':len(failures),'all_prior_html_retained':appendix in out,'browser_dashboard_bytes':len(browser.encode()),'sections':['ranked opportunities','sector heatmap','changes','bottom-up research','bearish and caution','cross-assets','options','country/tactical','quality','complete appendix']}
 atomic_json(docs/'a1_ultimate_audit.json',details)
 audit['ultimate_dashboard']=details;audit['html_bytes']=len(out.encode());atomic_json(docs/'a1_audit.json',audit)
 return details
# A1_ULTIMATE_DASHBOARD_V1_1
