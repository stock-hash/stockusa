#!/usr/bin/env python3
"""A1 additive reporting layer. Does not alter scanner signals or thresholds."""
import json, os, math
from pathlib import Path
from datetime import datetime, timezone
from html import escape
from a1_quality import (fmt, parse_ts, freshness, classify_entry, normalize_features, signal_timestamp, asset_row, INDEX_SYMBOLS, EXTRA_TACTICAL, EXTRA_ETFS, json_write)

SECTORS={'XLK':'Technology','XLF':'Financials','XLV':'Health Care','XLI':'Industrials','XLY':'Consumer Discretionary','XLP':'Consumer Staples','XLE':'Energy','XLU':'Utilities','XLB':'Materials','XLRE':'Real Estate','XLC':'Communication Services'}
CROSS_ASSET_GROUPS={
 'Equity benchmarks':['SPY','QQQ','DIA','IWM','RSP'],
 'Crypto':['BTC-USD','ETH-USD','SOL-USD','IBIT','BITO','ETHA','BLOK'],
 'Metals':['GLD','SLV','GDX','SIL'],
 'Energy and materials':['USO','UNG','URA','URNM','COPX','LIT'],
 'Rates and credit':['^TNX','TLT','HYG','LQD','SHY'],
 'Currency and volatility':['DX-Y.NYB','UUP','^VIX']}
TACTICAL={'TQQQ':('3x Long','Nasdaq-100'),'SQQQ':('3x Inverse','Nasdaq-100'),'SOXL':('3x Long','Semiconductors'),'SOXS':('3x Inverse','Semiconductors'),'UPRO':('3x Long','S&P 500'),'SPXU':('3x Inverse','S&P 500'),'UDOW':('3x Long','Dow 30'),'SDOW':('3x Inverse','Dow 30'),'TNA':('3x Long','Russell 2000'),'TZA':('3x Inverse','Russell 2000'),'TECL':('3x Long','Technology'),'TECS':('3x Inverse','Technology')}
PALETTE={'navy':'#123B66','blue':'#1769AA','green':'#DFF3E4','blue_row':'#E8F1FB','yellow':'#FFF3CD','red':'#FDE2E1','purple':'#F3E8FF','gray':'#ECEFF3','white':'#FFFFFF','text':'#1F2937'}

def load(path, default):
    try:return json.loads(Path(path).read_text(encoding='utf-8'))
    except Exception:return default

def finite(v):
    try:
        n=float(v);return n if math.isfinite(n) else None
    except Exception:return None




def style_for(row,kind='normal'):
    text=' '.join(str(v) for v in row.values()).upper()
    if kind=='tactical':return PALETTE['purple']
    if 'STALE' in text or 'UNVERIFIED' in text or 'TIMESTAMP CHECK' in text:return PALETTE['gray']
    if 'REJECT' in text or row.get('signal')=='TOP' or 'WEAKENING' in text:return PALETTE['red']
    if row.get('signal')=='BOTTOM' or 'STRONG' in text or 'ACCELERATING' in text:return PALETTE['green']
    if 'WAIT' in text or 'WATCH' in text:return PALETTE['yellow']
    if row.get('improving'):return PALETTE['blue_row']
    return PALETTE['white']

def table(title, rows, columns, kind='normal', note=''):
    h=f'<h2 style="color:{PALETTE["navy"]};font:700 18px Segoe UI,Arial;margin:24px 0 8px">{escape(title)}</h2>'
    if note:h+=f'<p style="font:12px Segoe UI,Arial;color:#5B6876;margin:0 0 7px">{escape(note)}</p>'
    h+=f'<table cellpadding="0" cellspacing="0" style="border-collapse:collapse;width:100%;font:12px Segoe UI,Arial;color:{PALETTE["text"]};margin-bottom:16px"><tr>'
    h+=''.join(f'<th bgcolor="{PALETTE["blue"]}" style="background:{PALETTE["blue"]};color:white;border:1px solid #B7C5D5;padding:8px;text-align:left">{escape(label)}</th>' for key,label in columns)+'</tr>'
    if not rows:h+=f'<tr><td colspan="{len(columns)}" bgcolor="{PALETTE["yellow"]}" style="background:{PALETTE["yellow"]};padding:9px;border:1px solid #D5DDE6">No verified rows available.</td></tr>'
    for r in rows:
        bg=style_for(r,kind)
        h+='<tr data-a1-row="1">'+''.join(f'<td bgcolor="{bg}" style="background:{bg};padding:7px;border:1px solid #D5DDE6;vertical-align:top;font-weight:{"700" if i==0 else "400"}">{escape(fmt(r.get(key),'index_value' if key=='price' and r.get('ticker') in INDEX_SYMBOLS else key))}</td>' for i,(key,label) in enumerate(columns))+'</tr>'
    return h+'</table>'

def cards(items):
    h='<table cellpadding="0" cellspacing="8" style="width:100%;font-family:Segoe UI,Arial"><tr>'
    for label,value,bg in items:h+=f'<td bgcolor="{bg}" style="background:{bg};border:1px solid #C7D2DF;padding:12px;width:{100//max(len(items),1)}%"><div style="font-size:10px;color:#5B6876;text-transform:uppercase">{escape(label)}</div><div style="font-size:22px;font-weight:800;color:{PALETTE["navy"]}">{escape(str(value))}</div></td>'
    return h+'</tr></table>'


def build(docs_dir, assessment_time=None):
    docs=Path(docs_dir); now=assessment_time or datetime.now(timezone.utc)
    report=load(docs/'report_scan.json',{}); snap=load(docs/'snapshot_full.json',{}); cfg=load(docs/'a1_universe_resolved.json',load(Path(__file__).with_name('a1_universes.json'),{}))
    features=snap.get('daily_features',[]); by={r.get('ticker'):r for r in features}; summary=report.get('summary',{})
    sectors=summary.get('sectors',[]); alerts=report.get('alerts',[]); blocked=summary.get('blocked_alerts',[]); cross=report.get('cross_assets',{}); options=report.get('options',{})
    # Freshness and entry readiness
    for r in alerts:
        ts=signal_timestamp(r); fr,age=freshness(ts,now); r['freshness']=fr;r['age_min']=age;r['entry_readiness']=classify_entry(r,fr)
    normalize_features(features,now)
    for r in features:r['entry_readiness']=classify_entry(r)
    # Sector transition uses previous snapshot when available.
    prev=load(docs/'a1_previous.json',{}); current_sector={s.get('sector'):finite(s.get('diff')) for s in sectors}
    sector_rows=[]
    for s in sectors:
        old=finite((prev.get('sectors') or {}).get(s.get('sector'))); cur=finite(s.get('diff')); delta=cur-old if cur is not None and old is not None else None
        stage='ACCELERATING' if delta is not None and delta>=0.25 else 'WEAKENING' if delta is not None and delta<=-0.25 else 'NEW LEADERSHIP' if s.get('strength')=='STRONG' and (old is not None and old<=0) else s.get('strength','UNVERIFIED')
        sector_rows.append({'ticker':s.get('sector'),'sector':SECTORS.get(s.get('sector'),'Unknown'),'strength':s.get('strength'),'relative_pp':cur,'change_pp':delta if delta is not None else 'No prior baseline - next successful scan','stage':stage,'timestamp':s.get('latest')})
    # Cross assets retain missing rows.
    asset_rows=[]
    for group,tickers in CROSS_ASSET_GROUPS.items():
        for t in tickers:
            asset_rows.append(asset_row(t,group,cross.get(t),by.get(t),now))
    # Bottom-up list excludes tactical ETFs and requires improving/higher low.
    improving=[r for r in features if r.get('panel')=='stock_or_other' and r.get('ticker') not in (set(TACTICAL)|EXTRA_TACTICAL|EXTRA_ETFS) and r.get('improving') and r.get('higher_low')]
    improving.sort(key=lambda r:(r.get('technical_score') or 0,r.get('relative_20d_vs_spy_pp') if r.get('relative_20d_vs_spy_pp') is not None else -999),reverse=True)
    tactical=[]
    for t,(direction,benchmark) in TACTICAL.items():
        r=dict(by.get(t,{}) or {});r.update({'ticker':t,'direction':direction,'benchmark':benchmark,'risk':'Daily-reset leverage; path-dependent'});tactical.append(r)
    # Universe coverage is not overstated.
    coverage=[]
    for name,tickers in (cfg.get('groups') or {}).items():
        valid=[t for t in tickers if t in by];coverage.append({'universe':name,'expected':len(tickers),'valid':len(valid),'missing':len(tickers)-len(valid),'verified_as_of':(cfg.get('group_metadata',{}).get(name,{}) or {}).get('as_of'),'status':(cfg.get('group_metadata',{}).get(name,{}) or {}).get('basis','UNIVERSE NOT CONFIGURED'),'missing_symbols':[t for t in tickers if t not in by]})
    confirmed_bull=[r for r in alerts if r.get('signal')=='BOTTOM'];confirmed_bear=[r for r in alerts if r.get('signal')=='TOP']
    all_ts=[parse_ts(x.get('timestamp')) for x in asset_rows if parse_ts(x.get('timestamp')) is not None]; latest=max(all_ts).isoformat() if all_ts else 'Unavailable'
    html=f'<html><body style="margin:0;background:#F4F7FA;color:{PALETTE["text"]};font-family:Segoe UI,Arial"><div style="max-width:1200px;margin:auto;background:white"><div style="background:{PALETTE["navy"]};padding:22px;color:white"><h1 style="margin:0;font-size:26px">A1 FULL MARKET INTELLIGENCE | 1.1 DATA QUALITY</h1><div style="font-size:12px;margin-top:6px">Generated {escape(now.isoformat())} | Latest instrument timestamp {escape(latest)}</div></div><div style="padding:16px">'
    html+=cards([('Regime',report.get('regime','Unavailable'),PALETTE['blue_row']),('SPY RSI',fmt(report.get('spy_rsi')),PALETTE['yellow']),('Bullish passed',len(confirmed_bull),PALETTE['green']),('Bearish passed',len(confirmed_bear),PALETTE['red']),('Improving watch',len(improving),PALETTE['blue_row']),('Data failures',len(snap.get('failures',[])),PALETTE['gray'])])
    html+=table('1. Universe coverage',coverage,[('universe','Universe'),('expected','Expected'),('valid','Valid'),('missing','Missing'),('verified_as_of','Reference as of'),('status','Status')],note='Dated reference universes are analyzed; ETF holdings proxies are not certified index membership. Prices and returns may use different observation times.')
    for group,tickers in cfg.get('groups',{}).items():
        meta=cfg.get('group_metadata',{}).get(group,{})
        universe_rows=[]
        for ticker in tickers:
            row=dict(by.get(ticker,{}));row['ticker']=ticker
            if ticker not in by:row['daily_status']='DOWNLOAD FAILED OR OMITTED BY SNAPSHOT CAP'
            universe_rows.append(row)
        universe_rows.sort(key=lambda r:r.get('technical_score') if r.get('technical_score') is not None else -1,reverse=True)
        html+=table(group+' | dated reference universe (not certified index membership)',universe_rows,[('ticker','Ticker'),('price','Daily price'),('technical_score','Tech score'),('return_20d','20-session return'),('bar_timestamp','Daily bar date'),('daily_status','Data status')],note='Source as of '+str(meta.get('as_of','not supplied'))+'; '+str(meta.get('basis','not configured')))
    html+=table('2. All 11 sectors and rotation stage',sector_rows,[('ticker','ETF'),('sector','Sector'),('strength','Strength'),('relative_pp','Relative return vs SPY (pp)'),('change_pp','Change vs previous scan (pp)'),('stage','Rotation stage'),('timestamp','Timestamp')])
    html+=table('3. Cross-asset dashboard',asset_rows,[('group','Group'),('ticker','Instrument'),('type','Type'),('price','Price'),('return_3d','3D'),('return_20d','20D'),('timestamp','Price time/date'),('freshness','Data status'),('basis','Price basis'),('daily_return_date','Daily return date'),('reason','Source note')])
    html+=table('4. Confirmed scanner alerts',alerts,[('ticker','Ticker'),('signal','Direction'),('setup_type','Setup'),('alert_price','Alert price'),('rqg_score','RQG'),('rqg_label','Gate'),('freshness','Freshness'),('entry_readiness','Entry readiness')])
    html+=table('5. Early-improving bottom-up research | top 12',improving[:12],[('ticker','Ticker'),('price','Price'),('technical_score','Score'),('return_3d','3D'),('return_5d','5D'),('return_20d','20D'),('relative_20d_vs_spy_pp','RS vs SPY'),('daily_bar_volume_ratio','Daily vol ratio'),('extension_atr','ATR extension'),('bar_timestamp','Daily bar date'),('entry_readiness','Status')])
    html+=table('6. Options and PCR verification',[{'ticker':k,**v,'quote_timestamp_status':v.get('quote_timestamp_status','Quote timestamp not supplied by provider'),'dte':v.get('dte','Expiration not returned')} for k,v in options.items()],[('ticker','Ticker'),('expiration','Expiration'),('dte','DTE'),('pcr','PCR'),('pcr_status','PCR status'),('avg_iv','Avg IV'),('quote_timestamp_status','Timestamp status')])
    html+=table('7. Country ETFs',[r for r in features if r.get('panel')=='country'][:12],[('ticker','Ticker'),('price','Price'),('technical_score','Score'),('return_5d','5D'),('return_20d','20D'),('relative_20d_vs_spy_pp','RS vs SPY'),('bar_timestamp','Timestamp')])
    html+=table('8. Tactical leveraged and inverse ETFs',tactical,[('ticker','Ticker'),('direction','Objective'),('benchmark','Benchmark'),('price','Price'),('return_5d','5D'),('return_20d','20D'),('technical_score','Score'),('extension_atr','ATR extension'),('risk','Risk')],kind='tactical')
    html+=table('9. Blocked and waiting setups',blocked,[('ticker','Ticker'),('signal','Direction'),('rqg_score','RQG'),('rqg_label','Gate'),('rqg_reasons','Reasons')])
    themes=(cfg.get('themes') or {})
    theme_rows=[{'theme':name,'instruments':vals,'status':'RESEARCH HYPOTHESIS; fundamentals not verified'} for name,vals in themes.items()]
    html+=table('10. Secular growth research hypotheses',theme_rows,[('theme','Theme'),('instruments','Tracked instruments'),('status','Data status')])
    html+=table('11. Changes since prior scan',snap.get('score_changes',[])[:20],[('ticker','Ticker'),('score_change','Technical score change')])
    html+=f'<p style="background:{PALETTE["yellow"]};padding:12px;font-size:11px">Green = bullish/strong; blue = improving research; yellow = watch; red = bearish/rejected; purple = leveraged/inverse; gray = stale/unverified. pp = percentage points. Daily prices are adjusted observations, not live quotes. Freshness is assessed at report build time, not email delivery. Technical research only; no automatic trades.</p></div></div></body></html>'
    (docs/'a1_tables.html').write_text(html,encoding='utf-8')
    # Compact AI payload includes every dimension but bounded complete records.
    json_write(docs/'a1_data_quality_details.json',{'generated_at':now.isoformat(),'asset_rows':asset_rows,'coverage':coverage,'signal_timestamp_missing':[r.get('ticker') for r in alerts if signal_timestamp(r) is None]})
    json_write(docs/'a1_full_archive.json',{'report':report,'snapshot':snap,'assets':asset_rows,'universes':cfg})
    compact={'metadata':{'generated_at':now.isoformat(),'latest_timestamp':latest,'source':'GitHub A1 1.1','freshness_assessed_at':now.isoformat(),'universe_basis':cfg.get('group_metadata',{}),'limitations':['No verified institutional transaction flow','No fundamental-quality feed','Instrument-specific freshness','Universe membership must be dated']},'regime':report.get('regime'),'spy_rsi':report.get('spy_rsi'),'coverage':coverage,'universe_shortlists':{g:[{k:by[t].get(k) for k in ['ticker','price','technical_score','return_20d','bar_timestamp']} for t in sorted(ts,key=lambda t:by.get(t,{}).get('technical_score',-1),reverse=True) if t in by][:5] for g,ts in cfg.get('groups',{}).items()},'sectors':sector_rows,'cross_assets':asset_rows,'confirmed_bullish':confirmed_bull[:10],'confirmed_bearish':confirmed_bear[:8],'improving':improving[:12],'options':[{'ticker':k,'pcr':v.get('pcr'),'pcr_status':v.get('pcr_status'),'avg_iv':v.get('avg_iv')} for k,v in options.items()][:10],'country':[r for r in features if r.get('panel')=='country'][:8],'tactical':tactical[:8],'themes':theme_rows,'changes':snap.get('score_changes',[])[:12]}
    budget=int(os.getenv('A1_AI_MAX_BYTES','26000'))
    compact['metadata']['omitted_records']={}
    shrink=['improving','confirmed_bullish','confirmed_bearish','changes','options','country','tactical']
    while len(json.dumps(compact,default=str).encode())>budget:
        candidates=[k for k in shrink if compact.get(k)]
        if not candidates:raise RuntimeError('A1_AI_MAX_BYTES too small')
        key=max(candidates,key=lambda k:len(json.dumps(compact[k],default=str)))
        compact[key].pop()
        compact['metadata']['omitted_records'][key]=compact['metadata']['omitted_records'].get(key,0)+1
    (docs/'a1_ai.json').write_text(json.dumps(compact,default=str,allow_nan=False,separators=(',',':')),encoding='utf-8')
    audit={'generated_at':now.isoformat(),'sector_count':len({s.get('ticker') for s in sector_rows}),'expected_sector_count':11,'alerts_source':len(alerts),'alerts_rendered':html.split('4. Confirmed scanner alerts',1)[1].split('</table>',1)[0].count('data-a1-row="1"'),'cross_assets_expected':sum(map(len,CROSS_ASSET_GROUPS.values())),'cross_assets_with_price':sum(1 for r in asset_rows if r.get('price') is not None),'universe_groups':coverage,'universe_missing_total':sum(x['missing'] for x in coverage),'ai_bytes':(docs/'a1_ai.json').stat().st_size,'html_bytes':(docs/'a1_tables.html').stat().st_size,'color_markers':all(x in html for x in [PALETTE['green'],PALETTE['red'],PALETTE['yellow'],PALETTE['purple']])}
    audit['future_timestamp_count']=sum(1 for x in asset_rows if x.get('freshness')=='FUTURE TIMESTAMP')+sum(1 for x in alerts if x.get('freshness')=='FUTURE TIMESTAMP')
    audit['cross_asset_coverage_ok']=audit['cross_assets_with_price']>=int(audit['cross_assets_expected']*0.7)
    audit['universe_coverage_ok']=len(coverage)==2 and all(x['expected']>0 and x['valid']/x['expected']>=0.9 for x in coverage)
    audit['publish_ok']=audit['cross_asset_coverage_ok'] and audit['universe_coverage_ok'] and audit['future_timestamp_count']==0 and audit['sector_count']==11 and audit['alerts_source']==audit['alerts_rendered'] and audit['ai_bytes']<=budget and audit['html_bytes']>=5000 and audit['color_markers']
    (docs/'a1_audit.json').write_text(json.dumps(audit,indent=2),encoding='utf-8')
    if not audit['publish_ok']:raise RuntimeError('A1 validation failed: '+json.dumps(audit))
    json_write(docs/'a1_previous.json',{'sectors':current_sector,'generated_at':now.isoformat()})
    print('A1 package generated',audit)
    return audit
