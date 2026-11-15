"""A1 1.1 data provenance, calendar, universe and formatting helpers."""
import json,math,re,os
from pathlib import Path
from datetime import datetime,timedelta,timezone
from zoneinfo import ZoneInfo
from functools import lru_cache
ET=ZoneInfo('America/New_York')
INDEX_SYMBOLS={'^TNX','^VIX','DX-Y.NYB'}
EXTRA_TACTICAL={'FNGD','FAS','FAZ','DRIP','GUSH','ERY','ERX','DFEN','DPST','SPXS','SPXL','LABU','LABD','BOIL','KOLD','NUGT','DUST','YINN','YANG','AGQ','SCO','UCO','QLD','SSO','USD','ROM'}
EXTRA_ETFS={'DIA','RSP','SPY','QQQ','IWM','GLD','SLV','GDX','SIL','USO','UNG','URA','URNM','COPX','LIT','TLT','HYG','LQD','SHY','IBIT','BITO','ETHA','BLOK','JETS','VNQ','IYT','XRT','PAVE','ITA','KRE','SMH','SOXX','TAN','ICLN','ARKG','ARKF','CIBR','HACK'}|{'XLK','XLF','XLV','XLI','XLY','XLP','XLE','XLU','XLB','XLRE','XLC'}

def number(v):
    try:
        x=float(v);return x if math.isfinite(x) else None
    except (ValueError,TypeError):return None

def parse_ts(v):
    if not v:return None
    s=str(v).strip()
    try:
        t=datetime.fromisoformat(s.replace('Z','+00:00'))
        return t if t.tzinfo else None
    except ValueError:pass
    for pattern in ['%Y-%m-%d %I:%M:%S %p ET','%Y-%m-%d %I:%M %p ET']:
        try:return datetime.strptime(s,pattern).replace(tzinfo=ET)
        except ValueError:pass
    return None

@lru_cache(maxsize=16)
def sessions(day):
    import pandas_market_calendars as mcal
    d=datetime.fromisoformat(day).date()
    return mcal.get_calendar('NYSE').schedule(start_date=d-timedelta(days=40),end_date=d+timedelta(days=2))

def freshness(ts,now,market='equity'):
    dt=parse_ts(ts)
    if not dt or not now.tzinfo:return 'TIMESTAMP NOT SUPPLIED',None
    age=(now-dt).total_seconds()/60
    if age < -2:return 'FUTURE TIMESTAMP',round(age,1)
    if market=='crypto':return ('RECENT BAR <=5m' if age<=5 else 'DELAYED BAR <=30m' if age<=30 else 'STALE CRYPTO BAR'),round(age,1)
    if market=='index':return ('RECENT INDEX OBSERVATION' if age<=30 else 'OLDER INDEX OBSERVATION'),round(age,1)
    sch=sessions(now.astimezone(ET).date().isoformat())
    active=sch[(sch.market_open<=now)&(sch.market_close>now)]
    if not active.empty:
        return ('RECENT BAR <=5m' if age<=5 else 'DELAYED BAR <=30m' if age<=30 else 'STALE - MARKET OPEN'),round(age,1)
    done=sch[sch.market_close<=now]
    if done.empty:return 'CALENDAR HISTORY INSUFFICIENT',round(age,1)
    last=done.iloc[-1]
    if last.market_open.to_pydatetime()<=dt<=last.market_close.to_pydatetime():
        near=(last.market_close.to_pydatetime()-dt).total_seconds()/60<=10
        return ('LATEST SESSION - NEAR-CLOSE BAR' if near else 'LATEST SESSION - EARLIER BAR'),round(age,1)
    if dt>last.market_close.to_pydatetime():return 'EXTENDED-HOURS OBSERVATION',round(age,1)
    return 'STALE - MISSED SESSION',round(age,1)

def daily_state(v,now,crypto=False):
    try:day=datetime.fromisoformat(str(v)[:10]).date()
    except ValueError:return 'DAILY DATE NOT SUPPLIED'
    today=now.astimezone(timezone.utc if crypto else ET).date()
    if day>today:return 'FUTURE DAILY DATE'
    if crypto:return 'CURRENT UTC DAY - PARTIAL' if day==today else 'LATEST COMPLETED UTC DAY' if day==today-timedelta(days=1) else 'OLDER DAILY CRYPTO'
    sch=sessions(now.astimezone(ET).date().isoformat())
    active=sch[(sch.market_open<=now)&(sch.market_close>now)]
    if not active.empty and day==today:return 'CURRENT SESSION DAILY BAR - PARTIAL'
    done=sch[sch.market_close<=now]
    if not done.empty and day==done.index[-1].date():return 'LATEST COMPLETED SESSION DAILY BAR'
    return 'OLDER DAILY SESSION'

def fmt(v,key=''):
    if v is None or v=='':return 'Not supplied'
    if isinstance(v,bool):return 'Yes' if v else 'No'
    if isinstance(v,(int,float)):
        if not math.isfinite(v):return 'Not supplied'
        if key.endswith('_pp'):return f'{v:+.2f} pp'
        if key in {'price','alert_price','trigger','invalidation','support','resistance','ma20','ma50'}:return f'${v:,.2f}'
        if 'return' in key or key.endswith('_pct'):return f'{v:+.2f}%'
        if key in {'expected','valid','missing','technical_score','rqg_score','confidence','dte','score_change'}:return str(int(v))
        return f'{v:,.2f}'
    if isinstance(v,list):return ', '.join(map(str,v[:7]))+(' ...' if len(v)>7 else '')
    return str(v)

def classify_entry(r,fresh='TIMESTAMP NOT SUPPLIED'):
    if str(r.get('rqg_label','')).startswith('REJECT'):return 'REJECTED BY SCANNER GATE'
    ext=number(r.get('extension_atr'))
    if ext is not None and ext>2:return 'OVEREXTENDED - RESEARCH ONLY'
    if r.get('status')=='HISTORICAL_RESEARCH_NOT_ENTRY_READY':
        state=r.get('daily_status','DAILY DATE NOT SUPPLIED')
        if 'OLDER' in state or 'FUTURE' in state:return 'OLD/INVALID DAILY DATA - REFRESH'
        if 'NOT SUPPLIED' in state:return 'DAILY DATE CHECK NEEDED'
        return 'DAILY RESEARCH - INTRADAY CHECK NEEDED'
    if 'STALE' in fresh:return 'OLD SIGNAL BAR - REFRESH NEEDED'
    if 'NOT SUPPLIED' in fresh or 'FUTURE' in fresh:return 'SIGNAL TIMESTAMP CHECK NEEDED'
    if 'LATEST SESSION' in fresh:return 'LAST SESSION SIGNAL - RECONFIRM'
    return 'SCANNER SIGNAL - TRIGGER/RISK CHECK NEEDED'

def signal_timestamp(r):
    details=r.get('rqg_details') or {}
    for v in [r.get('market_timestamp'),details.get('latest'),(details.get('data_meta') or {}).get('latest')]:
        if parse_ts(v):return v
    # Scan generation time is deliberately NOT a signal bar timestamp.
    return None

def frame_for(raw,ticker,only=False):
    import pandas as pd
    if raw is None or raw.empty:raise ValueError('empty download')
    if isinstance(raw.columns,pd.MultiIndex):
        for level in range(raw.columns.nlevels):
            if ticker in raw.columns.get_level_values(level):return raw.xs(ticker,axis=1,level=level)
        raise ValueError('ticker absent from response')
    if only:return raw
    raise ValueError('unexpected multi-ticker response')

def normalize_features(features,now):
    for r in features:
        r['daily_status']=daily_state(r.get('bar_timestamp',''),now,r.get('ticker','').endswith('-USD'))
        r['price_basis']='ADJUSTED DAILY CLOSE; CURRENT SESSION MAY BE PARTIAL'

def asset_row(t,group,q,daily,now):
    q=q or {};daily=daily or {};price=number(q.get('price'));stamp=q.get('market_timestamp')
    fr,age=freshness(stamp,now,'index' if t in INDEX_SYMBOLS else 'crypto' if t.endswith('-USD') else 'equity')
    basis='ADJUSTED 5-MINUTE BAR';reason=q.get('error') or ''
    if price is None:
        price=number(daily.get('price'))
        if price is not None:
            stamp=str(daily.get('bar_timestamp',''))[:10];age=None
            fr=daily_state(stamp,now,t.endswith('-USD'));basis='ADJUSTED DAILY OBSERVATION - NOT LIVE'
        else:fr='NO PRICE RETURNED';basis='NO QUOTE OR DAILY FALLBACK';reason=reason or 'No valid price in quote or daily data'
    if t in INDEX_SYMBOLS:basis='INDEX VALUE - NOT USD; '+basis
    return {'group':group,'ticker':t,'price':price,'return_3d':daily.get('return_3d'),'return_20d':daily.get('return_20d'),'timestamp':stamp or 'Quote time not supplied','age_min':age,'freshness':fr,'type':q.get('instrument_type','ETF proxy or index'),'basis':basis,'reason':reason,'daily_return_date':str(daily.get('bar_timestamp',''))[:10] or 'Daily history not returned'}

def json_write(path,data):
    def clean(x):
        if isinstance(x,dict):return {str(k):clean(v) for k,v in x.items()}
        if isinstance(x,(list,tuple)):return [clean(v) for v in x]
        if isinstance(x,float) and not math.isfinite(x):return None
        return x
    path=Path(path);tmp=path.with_name(path.name+'.tmp')
    tmp.write_text(json.dumps(clean(data),default=str,allow_nan=False,separators=(',',':')),encoding='utf-8');tmp.replace(path)

def resolve_universes(root):
    """Dated reference lists; optional full issuer QQQ refresh. Never certify index membership."""
    root=Path(root);cfg=json.loads((root/'a1_reference_universes.json').read_text())
    cfg['themes']=json.loads((root/'a1_universes.json').read_text()).get('themes',{}) if (root/'a1_universes.json').exists() else {}
    cfg['membership_verified_as_of']=None
    # Do not use ten top holdings as a complete universe. Refresh is opt-in and rejects partial responses.
    if os.getenv('A1_REFRESH_QQQ','false').lower()=='true':
        from urllib.request import Request,urlopen
        try:
            url=cfg['issuer_qqq_endpoint']
            with urlopen(Request(url,headers={'User-Agent':'A1MarketResearch/1.1'}),timeout=15) as r:data=json.load(r)
            rows=data.get('holdings',[])
            symbols=sorted({str(r['ticker']).replace('.','-') for r in rows if r.get('securityTypeCode')=='COM' and re.fullmatch(r'[A-Z][A-Z0-9.\-]{0,9}',str(r.get('ticker','')))})
            date=data.get('effectiveDate')
            if not 95<=len(symbols)<=110 or not date:raise ValueError('Incomplete or undated issuer response')
            cfg['groups']['Nasdaq100']=symbols
            cfg['group_metadata']['Nasdaq100']={'as_of':date,'source':url,'basis':'ISSUER QQQ COMMON-STOCK HOLDINGS PROXY; NOT INDEX CERTIFICATION'}
        except Exception as e:cfg['refresh_error']=str(e)[:200]
    json_write(root/'docs'/'a1_universe_resolved.json',cfg)
    return cfg
