"""Additive reporting analytics. No trading thresholds changed."""
import json, math, os
from pathlib import Path
from datetime import datetime, timezone
from html import escape
import pandas as pd

COUNTRY = ['EWJ','EWG','EWZ','EWU','EWA','EWC','EWH','EWS','EWY','EWT','EWW','INDA','MCHI','FXI','KSA','KWEB']
TACTICAL = ['TQQQ','SQQQ','UPRO','SPXU','UDOW','SDOW','SOXL','SOXS','TNA','TZA','TECL','TECS']
BENCHMARKS = ['SPY','QQQ','DIA','IWM','ACWI']

def finite(v):
    try:
        n=float(v)
        return round(n,4) if math.isfinite(n) else None
    except (ValueError,TypeError): return None

def features(ticker, df):
    df=df.dropna(subset=['Close']).sort_index()
    if len(df)<60: return None
    c=df['Close']; h=df['High']; l=df['Low']; v=df['Volume']; last=float(c.iloc[-1])
    prev=c.shift(1)
    atr=pd.concat([h-l,(h-prev).abs(),(l-prev).abs()],axis=1).max(axis=1).rolling(14).mean().iloc[-1]
    ret=lambda n: finite((last/float(c.iloc[-n-1])-1)*100) if len(c)>n else None
    ma20=c.rolling(20).mean().iloc[-1]; ma50=c.rolling(50).mean().iloc[-1]
    resistance=h.iloc[-21:-1].max(); support=l.iloc[-11:-1].min()
    # Daily-bar volume is not intraday time-adjusted RVOL.
    volume_ratio=finite(v.iloc[-1]/v.iloc[-21:-1].mean()) if v.iloc[-21:-1].mean()>0 else None
    higher_low=bool(l.iloc[-5:].min()>l.iloc[-10:-5].min())
    improving=bool(last>ma20 and ma20>c.rolling(20).mean().iloc[-6])
    extension=finite((last-ma20)/atr) if atr>0 else None
    components={'above_ma20':20 if last>ma20 else 0,'above_ma50':20 if last>ma50 else 0,
                'rising_ma20':20 if improving else 0,'higher_low':20 if higher_low else 0,
                'volume_confirmation':20 if volume_ratio is not None and volume_ratio>=1.2 else 0}
    return {'ticker':ticker,'bar_timestamp':str(df.index[-1]),'price':finite(last),'return_3d':ret(3),'return_5d':ret(5),'return_20d':ret(20),
            'ma20':finite(ma20),'ma50':finite(ma50),'atr14':finite(atr),'extension_atr':extension,
            'daily_bar_volume_ratio':volume_ratio,'higher_low':higher_low,'improving':improving,
            'prior_20bar_high':finite(resistance),'prior_10bar_low':finite(support),
            'technical_score':sum(components.values()),'score_components':components,
            'price_history_5bars':[finite(x) for x in c.tail(5)],
            'status':'HISTORICAL_RESEARCH_NOT_ENTRY_READY','fundamental_quality':'NOT_EVALUATED'}

def run(namespace):
    docs=Path(namespace.get('script_dir','.'))/'docs'
    source=docs/'report_scan.json'
    if not source.exists(): raise RuntimeError('report_scan.json missing; run the prior reporting-enabled runner first')
    data=json.loads(source.read_text(encoding='utf-8'))
    config_path=Path(namespace.get('script_dir','.'))/'snapshot_universes.json'
    config=json.loads(config_path.read_text()) if config_path.exists() else {}
    groups=config.get('groups',{})
    membership=config.get('membership_verified_as_of')
    universe=sorted(set(namespace.get('ALL_STOCKS',[]))|set(COUNTRY+TACTICAL+BENCHMARKS)|{t for g in groups.values() for t in g})
    limit=int(os.getenv('SNAPSHOT_MAX_TICKERS','500'))
    selected=universe[:max(1,limit)]
    downloader=namespace['patched_yf_download']
    records=[]; failures=[]
    for offset in range(0,len(selected),40):
        chunk=selected[offset:offset+40]
        try:
            raw=downloader(chunk,period='1y',interval='1d',progress=False,auto_adjust=True,threads=False)
            for ticker in chunk:
                try:
                    if isinstance(raw.columns,pd.MultiIndex):
                        if ticker in raw.columns.get_level_values(0): frame=raw[ticker]
                        elif ticker in raw.columns.get_level_values(1): frame=raw.xs(ticker,axis=1,level=1)
                        else: raise ValueError('ticker absent in response')
                    elif len(chunk)==1: frame=raw
                    else: raise ValueError('unexpected columns')
                    row=features(ticker,frame)
                    if row is None: raise ValueError('fewer than 60 valid bars')
                    row['groups']=[g for g,tickers in groups.items() if ticker in tickers]
                    row['panel']='country' if ticker in COUNTRY else 'tactical' if ticker in TACTICAL else 'stock_or_other'
                    records.append(row)
                except Exception as exc: failures.append({'ticker':ticker,'error':str(exc)[:120]})
        except Exception as exc:
            failures.extend({'ticker':t,'error':str(exc)[:120]} for t in chunk)
    lookup={r['ticker']:r for r in records}; spy=lookup.get('SPY',{}).get('return_20d')
    for row in records:
        row['relative_20d_vs_spy_pp']=finite(row['return_20d']-spy) if row['return_20d'] is not None and spy is not None else None
    ranked=sorted(records,key=lambda r:(r['technical_score'],r['relative_20d_vs_spy_pp'] or -999),reverse=True)
    alerts=data.get('alerts',[])
    confirmed_bottom=[r for r in alerts if r.get('signal')=='BOTTOM']
    watch=[r for r in ranked if r['panel']=='stock_or_other' and r['improving'] and r['higher_low'] and r['extension_atr'] is not None and 0<=r['extension_atr']<=2]
    coverage={'requested':len(universe),'attempted':len(selected),'valid':len(records),'failed':len(failures),'omitted_by_cap':universe[len(selected):],
              'membership_verified_as_of':membership,'membership_status':'CONFIGURED_NOT_EXTERNALLY_VERIFIED' if membership else 'DOW30_NASDAQ100_MEMBERSHIP_NOT_VERIFIED'}
    previous_path=docs/'snapshot_previous.json'
    previous=json.loads(previous_path.read_text()) if previous_path.exists() else {}
    changes=[{'ticker':r['ticker'],'score_change':r['technical_score']-previous[r['ticker']]} for r in records if r['ticker'] in previous and r['technical_score']!=previous[r['ticker']]]
    full={'generated_at':datetime.now(timezone.utc).isoformat(),'coverage':coverage,'daily_features':records,'failures':failures,'score_changes':changes,'scanner':data}
    (docs/'snapshot_full.json').write_text(json.dumps(full,allow_nan=False,separators=(',',':')),encoding='utf-8')
    compact={'generated_at':full['generated_at'],'coverage':coverage,'confirmed_bottom_count':len(confirmed_bottom),
             'confirmed_bottom':[{k:r.get(k) for k in ['ticker','signal','setup_type','alert_price','rqg_score','rqg_label','confidence']} for r in confirmed_bottom[:15]],
             'early_improving':watch[:12],'countries':[r for r in ranked if r['panel']=='country'][:6],
             'tactical':[r for r in ranked if r['panel']=='tactical'][:6],'changes':changes[:12],
             'sectors':data.get('summary',{}).get('sectors',[]),
             'limitations':['Daily bars may be incomplete or prior session; not current entry approval.','Technical scores are not win probabilities or fundamental growth ratings.','ETF metadata and Dow/Nasdaq membership require verified registry.','No verified institutional flow or fundamentals feed.']}
    # Bound model input by dropping whole records, never slicing JSON text.
    budget=int(os.getenv('SNAPSHOT_AI_MAX_BYTES','24000'))
    while len(json.dumps(compact).encode())>budget:
        keys=[k for k in ['early_improving','confirmed_bottom','countries','tactical','changes','sectors'] if compact.get(k)]
        if not keys: raise RuntimeError('Input budget too small for metadata')
        key=max(keys,key=lambda k:len(json.dumps(compact[k])))
        compact[key].pop()
    (docs/'snapshot_ai.json').write_text(json.dumps(compact,allow_nan=False,separators=(',',':')),encoding='utf-8')
    previous_path.write_text(json.dumps({r['ticker']:r['technical_score'] for r in records}))
    def table(title,rows,columns):
        out='<h2>'+escape(title)+'</h2><table><tr>'+''.join('<th>'+escape(k)+'</th>' for k in columns)+'</tr>'
        for row in rows: out+='<tr>'+''.join('<td>'+escape(str(row.get(k,'Unavailable')))+'</td>' for k in columns)+'</tr>'
        return out+'</table>'
    html='<html><body><h1>Market Snapshot</h1><p>Historical technical research. Not entry approval. Leveraged products are tactical, not long-term growth holdings.</p><pre>'+escape(json.dumps(coverage,indent=2))+'</pre>'
    html+=table('ALL confirmed bullish scanner alerts',confirmed_bottom,['ticker','signal','setup_type','alert_price','rqg_score','rqg_label','confidence'])
    cols=['ticker','price','bar_timestamp','technical_score','return_3d','return_5d','return_20d','relative_20d_vs_spy_pp','daily_bar_volume_ratio','extension_atr','prior_20bar_high','prior_10bar_low','price_history_5bars']
    html+=table('Early improving research watchlist',watch,cols)
    for group in groups: html+=table(group,[r for r in ranked if group in r['groups']],cols)
    html+=table('Country ETF observations',[r for r in ranked if r['panel']=='country'],cols)
    html+=table('Tactical ETF observations: verify leverage and issuer before use',[r for r in ranked if r['panel']=='tactical'],cols)
    html+=table('ALL scanner alerts',alerts,['ticker','signal','setup_type','alert_price','rqg_score','rqg_label'])
    html+=table('ALL blocked scanner setups',data.get('summary',{}).get('blocked_alerts',[]),['ticker','signal','rqg_label','rqg_reasons'])
    html+='</body></html>'
    (docs/'snapshot_tables.html').write_text(html,encoding='utf-8')
    print('Snapshot built:',coverage,'AI bytes:',(docs/'snapshot_ai.json').stat().st_size)
