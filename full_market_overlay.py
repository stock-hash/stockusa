"""Full-coverage additive market report: preserves original scanner and snapshot outputs."""
import json, os, math
from pathlib import Path
from html import escape
from datetime import datetime, timezone

SECTORS={'XLK':'Technology','XLF':'Financials','XLV':'Healthcare','XLE':'Energy','XLI':'Industrials','XLY':'Consumer Discretionary','XLP':'Consumer Staples','XLU':'Utilities','XLB':'Materials','XLRE':'Real Estate','XLC':'Communication Services'}
ASSETS=[('Equity benchmarks',['SPY','QQQ','DIA','IWM','RSP']),('Crypto spot',['BTC-USD','ETH-USD','SOL-USD']),('Crypto ETFs',['IBIT','BITO','ETHA','BLOK']),('Precious metals',['GLD','SLV','GDX','SIL']),('Energy and materials',['USO','UNG','URA','URNM','COPX','LIT']),('Bonds and credit',['TLT','HYG','LQD','SHY']),('Currencies and volatility',['DX-Y.NYB','UUP','^VIX','^TNX'])]
ETF_EXTRA={'FNGD','FAS','DRIP','ERY','DFEN','DPST','LABU','LABD','BOIL','KOLD','NUGT','DUST','TQQQ','SQQQ','SOXL','SOXS','TECL','TECS','TNA','TZA','UPRO','SPXU','SDOW','UDOW'}

def val(v):
    if v is None:return 'Unavailable'
    if isinstance(v,float):return f'{v:,.2f}' if math.isfinite(v) else 'Unavailable'
    if isinstance(v,list):return ', '.join(map(str,v[:6]))+(' ...' if len(v)>6 else '')
    return str(v)

def cell(v,bg='#ffffff',bold=False):
    return '<td bgcolor="'+bg+'" style="border:1px solid #d4dce6;background-color:'+bg+';padding:7px;color:#1e293b;font-weight:'+('bold' if bold else 'normal')+'">'+escape(val(v))+'</td>'

def section(title,columns,rows,limit=None):
    out='<h2 style="font-family:Segoe UI,Arial;color:#1565ad;margin-top:22px">'+escape(title)+'</h2>'
    out+='<table cellspacing="0" cellpadding="0" style="border-collapse:collapse;width:100%;font-family:Segoe UI,Arial;font-size:12px"><tr>'
    out+=''.join('<th bgcolor="#1565ad" style="background-color:#1565ad;color:#ffffff;border:1px solid #b6c5d7;padding:8px;text-align:left">'+escape(x)+'</th>' for x in columns)+'</tr>'
    items=rows if limit is None else rows[:limit]
    if not items:out+='<tr>'+cell('No verified data', '#fff3cd')+'<td colspan="'+str(max(1,len(columns)-1))+'" style="background:#fff3cd">Unavailable or not configured</td></tr>'
    for row in items:
        status=' '.join(str(x) for x in row).upper()
        bg='#fce8e6' if ('REJECT' in status or 'TOP' in status or 'STALE' in status) else '#e6f4ea' if ('BOTTOM' in status or 'STRONG' in status or 'IMPROVING' in status) else '#ffffff'
        out+='<tr>'+''.join(cell(x,bg,i==0) for i,x in enumerate(row))+'</tr>'
    return out+'</table>'

def build(docs):
    docs=Path(docs)
    report=json.loads((docs/'report_scan.json').read_text())
    full=json.loads((docs/'snapshot_full.json').read_text())
    ai=json.loads((docs/'snapshot_ai.json').read_text())
    features=full.get('daily_features',[])
    by_ticker={r.get('ticker'):r for r in features}
    summary=report.get('summary',{})
    alerts=report.get('alerts',[])
    blocked=summary.get('blocked_alerts',[])
    assets=report.get('cross_assets',{})
    options=report.get('options',{})
    sectors=summary.get('sectors',[])
    # Original report dimensions are retained as dedicated sections, even if source unavailable.
    coverage={}
    for title,tickers in ASSETS:
        coverage[title]=[{'ticker':t,'quote':assets.get(t,{}),'daily':by_ticker.get(t)} for t in tickers]
    meta={'generated_at':datetime.now(timezone.utc).isoformat(),'scanner_scan_time':report.get('scan_time'),'scanner_completed_at':report.get('completed_at'),'daily_coverage':full.get('coverage',{}),'source':'report_scan.json + snapshot_full.json','limitations':['Data age is instrument-specific; no single LIVE flag for the entire report.','Technical signals and volume proxies do not prove institutional buying or selling.','No fundamental-quality or current policy data feed; do not claim verified secular 10x probability.','Unverified Dow/Nasdaq membership must not be called complete.','Options only for tickers fetched by existing scanner; PCR may be unavailable.']}
    package={'metadata':meta,'market':{'regime':report.get('regime'),'spy_rsi':report.get('spy_rsi'),'sectors':sectors,'cross_assets':coverage},'alerts':alerts,'blocked':blocked,'options':options,'daily_features':features,'changes':full.get('score_changes',[])}
    (docs/'market_full_archive.json').write_text(json.dumps(package,default=str,allow_nan=False,separators=(',',':')))
    # Compact AI input: retain small summaries of every original dimension, never truncate JSON mid-object.
    compact={'metadata':meta,'regime':report.get('regime'),'spy_rsi':report.get('spy_rsi'),'sectors':[{'ticker':s.get('sector'),'strength':s.get('strength'),'relative_pp':s.get('diff'),'timestamp':s.get('latest')} for s in sectors],
             'cross_assets':{group:[{'ticker':r['ticker'],'price':r['quote'].get('price'),'timestamp':r['quote'].get('market_timestamp'),'status':r['quote'].get('status')} for r in rows] for group,rows in coverage.items()},
             'bullish_alerts':[{'ticker':r.get('ticker'),'rqg_score':r.get('rqg_score'),'setup':r.get('setup_type')} for r in alerts if r.get('signal')=='BOTTOM'][:12],
             'bearish_alerts':[{'ticker':r.get('ticker'),'rqg_score':r.get('rqg_score')} for r in alerts if r.get('signal')=='TOP'][:8],
             'improving':[{k:r.get(k) for k in ['ticker','price','technical_score','return_5d','relative_20d_vs_spy_pp']} for r in features if r.get('improving') and r.get('higher_low') and r.get('panel')=='stock_or_other'][:12],
             'options_coverage':[{'ticker':k,'pcr':v.get('pcr'),'pcr_status':v.get('pcr_status'),'iv':v.get('avg_iv')} for k,v in options.items()][:10],
             'country_etfs':[r.get('ticker') for r in features if r.get('panel')=='country'][:12],
             'tactical_etfs':[r.get('ticker') for r in features if r.get('panel')=='tactical'][:12],
             'changes':full.get('score_changes',[])[:12], 'missing':['Verified fundamentals','Institutional flow transactions','Current macro policy research','Complete Dow/Nasdaq constituent verification']}
    budget=int(os.getenv('SNAPSHOT_AI_MAX_BYTES','24000'))
    while len(json.dumps(compact,default=str).encode())>budget:
        candidates=[k for k in ['improving','bullish_alerts','bearish_alerts','changes','options_coverage','sectors'] if compact.get(k)]
        if candidates:compact[max(candidates,key=lambda k:len(json.dumps(compact[k])))].pop();continue
        groups=[g for g,v in compact['cross_assets'].items() if v]
        if groups:compact['cross_assets'][max(groups,key=lambda g:len(json.dumps(compact['cross_assets'][g])))].pop();continue
        raise ValueError('AI byte budget cannot fit minimum metadata')
    (docs/'snapshot_ai.json').write_text(json.dumps(compact,default=str,allow_nan=False,separators=(',',':')))
    html='<html><body style="font-family:Segoe UI,Arial;color:#1e293b;background:#ffffff"><h1 style="color:#1565ad">Full Market Intelligence | Technical Research</h1>'
    html+='<p style="background:#fff3cd;padding:12px">Generated '+escape(meta['generated_at'])+'. Historical and delayed data are labeled by source timestamp. No automatic entry approval or verified institutional flow.</p>'
    html+=section('1. Macro and scanner regime',['Regime','SPY RSI','Scan time','Completed'],[[report.get('regime'),report.get('spy_rsi'),report.get('scan_time'),report.get('completed_at')]])
    html+=section('2. All 11 sectors | relative strength',['ETF','Sector','Strength','RS vs SPY (pp)','Session %','Bar timestamp'],[[s.get('sector'),SECTORS.get(s.get('sector'),'Unknown'),s.get('strength'),s.get('diff'),s.get('session_return'),s.get('latest')] for s in sectors])
    for group,rows in coverage.items():
        html+=section('3. Cross-asset | '+group,['Instrument','Price','Source timestamp','Status'],[[r['ticker'],r['quote'].get('price'),r['quote'].get('market_timestamp'),r['quote'].get('status','Unavailable')] for r in rows])
    html+=section('4. Early growth themes | research hypotheses',['Theme','What to validate'],[['AI infrastructure','Earnings, capex and breadth; no verified fundamentals feed'],['Grid and nuclear','Order backlog, policy, deployment and financing'],['Cybersecurity','Recurring revenue, margins and valuations'],['Space infrastructure','Contracts, launch execution and dilution'],['Critical minerals','Supply/demand, permitting and balance sheets'],['Healthcare innovation','Clinical milestones and funding']])
    html+=section('5. Options coverage',['Ticker','PCR','Verification','IV','Expiration'],[[k,v.get('pcr'),v.get('pcr_status'),v.get('avg_iv'),v.get('expiration')] for k,v in options.items()])
    html+=section('6. Confirmed scanner setups',['Ticker','Direction','Setup','Alert price','RQG','Gate'],[[a.get('ticker'),a.get('signal'),a.get('setup_type'),a.get('alert_price'),a.get('rqg_score'),a.get('rqg_label')] for a in alerts])
    top=[r for r in features if r.get('panel')=='stock_or_other' and r.get('improving') and r.get('higher_low')]
    top.sort(key=lambda r:(r.get('technical_score') or 0,r.get('relative_20d_vs_spy_pp') or -999),reverse=True)
    html+=section('7. Early improving stocks | research watchlist',['Ticker','Price','Score','3D %','5D %','20D %','RS vs SPY pp','Bar time'],[[r.get('ticker'),r.get('price'),r.get('technical_score'),r.get('return_3d'),r.get('return_5d'),r.get('return_20d'),r.get('relative_20d_vs_spy_pp'),r.get('bar_timestamp')] for r in top],12)
    for group in ['Dow30','Nasdaq100']:
        rows=[r for r in features if group in r.get('groups',[])]
        html+=section('8. '+group+' | constituent coverage unverified' if not rows else '8. '+group+' | technical snapshot',['Ticker','Price','Score','20D %','Bar time'],[[r.get('ticker'),r.get('price'),r.get('technical_score'),r.get('return_20d'),r.get('bar_timestamp')] for r in rows],15)
    for panel in ['country','tactical']:
        rows=[r for r in features if r.get('panel')==panel]
        html+=section('9. '+panel.title()+' ETFs | technical observations',['Ticker','Price','Score','5D %','20D %','Timestamp'],[[r.get('ticker'),r.get('price'),r.get('technical_score'),r.get('return_5d'),r.get('return_20d'),r.get('bar_timestamp')] for r in rows],12)
    html+=section('10. Blocked / waiting | not confirmed outflows',['Ticker','Signal','Gate','Reasons'],[[r.get('ticker'),r.get('signal'),r.get('rqg_label'),r.get('rqg_reasons')] for r in blocked])
    html+=section('11. Changes since prior scan',['Ticker','Technical score change'],[[r.get('ticker'),r.get('score_change')] for r in full.get('score_changes',[])],15)
    html+='<p style="color:#64748b;font-size:11px">Green: bullish/strong. Red: bearish/rejected. Source: GitHub scanner. Technical observations only, not financial advice. Full audit: market_full_archive.json.</p></body></html>'
    (docs/'snapshot_tables.html').write_text(html,encoding='utf-8')
    audit={'all_11_sectors_present':len({s.get('sector') for s in sectors}&set(SECTORS))==11,'cross_asset_groups':list(coverage),'alerts_in_archive':len(alerts),'alerts_in_html':len(alerts),'options_included':len(options),'dow_configured':any('Dow30' in r.get('groups',[]) for r in features),'nasdaq_configured':any('Nasdaq100' in r.get('groups',[]) for r in features),'ai_bytes':(docs/'snapshot_ai.json').stat().st_size,'status':'PARTIAL' if len({s.get('sector') for s in sectors}&set(SECTORS))<11 else 'COVERAGE_REVIEW_REQUIRED'}
    (docs/'snapshot_coverage_audit.json').write_text(json.dumps(audit,indent=2))
    print('Full market reporting overlay created',audit)
