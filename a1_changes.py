"""Section 11 comparisons. No signal scoring or trading decisions modified."""
import json,math,hashlib,re
from pathlib import Path
from html import escape
from datetime import datetime,timezone

def num(v):
    if isinstance(v,bool):return None
    try:
        n=float(v);return n if math.isfinite(n) else None
    except (ValueError,TypeError):return None

def stamp(v):
    try:
        d=datetime.fromisoformat(str(v).replace('Z','+00:00'))
        return d if d.tzinfo else None
    except ValueError:return None

def read_baseline(path):
    p=Path(path)
    if not p.exists():return None,'FIRST_RUN','No prior successful Section 11 baseline. This run establishes one.'
    try:
        data=json.loads(p.read_text(encoding='utf-8'))
        if data.get('schema')!=1 or not isinstance(data.get('daily'),dict) or not isinstance(data.get('alerts'),dict) or not stamp(data.get('generated_at')):
            raise ValueError('invalid schema, timestamp or record maps')
        return data,'OK',''
    except (OSError,ValueError,TypeError) as e:return None,'BASELINE_ERROR','Previous baseline could not be read: '+str(e)

def snapshot(report,snap,assessed_at):
    if not isinstance(report.get('alerts'),list):raise ValueError('report.alerts missing or not a list')
    if not isinstance(snap.get('daily_features'),list):raise ValueError('snapshot.daily_features missing or not a list')
    daily={}
    for r in snap['daily_features']:
        t=r.get('ticker')
        if not t:continue
        if t in daily:raise ValueError('duplicate daily ticker: '+t)
        daily[t]={k:r.get(k) for k in ['price','technical_score','bar_timestamp','improving','higher_low','extension_atr','entry_readiness']}
        daily[t]['basis']='ADJUSTED DAILY OBSERVATION'
        if daily[t].get('entry_readiness') is None:
            ext=num(r.get('extension_atr'))
            daily[t]['entry_readiness']='OVEREXTENDED - RESEARCH ONLY' if ext is not None and ext>2 else 'IMPROVING - INTRADAY CHECK NEEDED' if r.get('improving') and r.get('higher_low') else 'WATCH ONLY'
    alerts={}
    for r in report['alerts']:
        key='|'.join(str(r.get(k,'')) for k in ['ticker','signal','setup_type'])
        alerts[key]={k:r.get(k) for k in ['ticker','signal','setup_type','rqg_label','rqg_score','alert_price']}
    state={'schema':1,'generated_at':assessed_at,'scan_time':report.get('scan_time'),'daily':daily,'alerts':alerts}
    # Fingerprint excludes generated timestamp, so repeated source observations are identifiable.
    state['observation_id']=hashlib.sha256(json.dumps({'daily':daily,'alerts':alerts,'scan_time':report.get('scan_time')},sort_keys=True,default=str).encode()).hexdigest()
    return state

def compare(previous,current,state='OK',message=''):
    result={'schema':1,'status':state,'message':message,'previous_generated_at':previous.get('generated_at') if previous else None,'current_generated_at':current['generated_at'],'previous_scan_time':previous.get('scan_time') if previous else None,'current_scan_time':current.get('scan_time'),'events':[],'counts':{}}
    if previous is None:return result
    old_time=stamp(previous['generated_at']);new_time=stamp(current['generated_at'])
    if not old_time or not new_time or new_time<=old_time:
        result.update(status='OUT_OF_ORDER',message='Current assessment is not later than the baseline. Baseline retained.');return result
    events=result['events']
    def add(t,kind,field,old,new,delta=None,note='',tone='neutral'):
        events.append({'ticker':t,'event':kind,'field':field,'previous':old,'current':new,'change':delta,'note':note,'tone':tone})
    for t in sorted(set(previous['daily'])|set(current['daily'])):
        old=previous['daily'].get(t);new=current['daily'].get(t)
        if old is None:add(t,'NEW DAILY COVERAGE','coverage',None,new.get('bar_timestamp'),note='Newly observed in daily research, not a new buy signal.');continue
        if new is None:add(t,'NOT OBSERVED THIS RUN','coverage',old.get('bar_timestamp'),None,note='May be download failure or universe change; not a sell signal.',tone='warning');continue
        od=str(old.get('bar_timestamp') or '');nd=str(new.get('bar_timestamp') or '')
        if not od or not nd:
            add(t,'DATE CHECK NEEDED','bar date',od,nd,note='Price/score comparison skipped without both bar dates.',tone='warning');continue
        if nd[:10]<od[:10]:
            add(t,'OLDER DATA RETURNED','bar date',od,nd,note='Price/score comparison skipped.',tone='warning');continue
        for field in ['technical_score','price']:
            a=num(old.get(field));b=num(new.get(field))
            if a is None or b is None:continue
            delta=round(b-a,6)
            if abs(delta)<(0.000001 if field=='technical_score' else 0.00005):continue
            note=('Score points; heuristic, not win probability.' if field=='technical_score' else 'Adjusted daily observation change; not live return.')
            note+=' '+od+' -> '+nd
            if field=='price':note+='; '+f'{(b/a-1)*100:+.2f}%' if a>0 else '; percentage undefined'
            add(t,'SCORE CHANGE' if field=='technical_score' else 'PRICE OBSERVATION CHANGE',field,a,b,delta,note,'up' if delta>0 else 'down')
        for field in ['improving','higher_low','entry_readiness']:
            if old.get(field) is not None and new.get(field) is not None and old[field]!=new[field]:
                add(t,'RESEARCH STATUS CHANGE',field,old[field],new[field],note='Daily technical state, not entry approval.',tone='warning')
    for key in sorted(set(previous['alerts'])|set(current['alerts'])):
        a=previous['alerts'].get(key);b=current['alerts'].get(key);r=b or a;t=r.get('ticker')
        if a is None:add(t,'NEW SCANNER ALERT',r.get('signal'),None,r.get('setup_type'),note='Newly returned by scanner; confirm bar time and risk.',tone='new')
        elif b is None:add(t,'ALERT NO LONGER RETURNED',r.get('signal'),a.get('setup_type'),None,note='Not proof of invalidation or removal; scanner may not repeat an alert.',tone='warning')
        else:
            for field in ['rqg_label','rqg_score']:
                if a.get(field)!=b.get(field):add(t,'SCANNER GATE CHANGE',field,a.get(field),b.get(field),note='Scanner output change; no gate retuning.',tone='warning')
    for e in events:result['counts'][e['event']]=result['counts'].get(e['event'],0)+1
    result['status']='CHANGES' if events else 'NO_CHANGE'
    result['message']='Changes compared with the previous successful A1 report.' if events else 'Previous baseline exists. No comparable price, score, alert or research-state changes detected.'
    if previous.get('observation_id')==current.get('observation_id'):result['message']+=' Same source observations were returned.'
    return result

def render(result):
    colors={'up':'#DFF3E4','down':'#FDE2E1','warning':'#FFF3CD','new':'#E8F1FB','neutral':'#ECEFF3'}
    def text(x):return escape('Not supplied' if x is None else str(x))
    out='<h2 style="color:#123B66;font:700 18px Segoe UI,Arial">11. Changes since prior successful scan</h2>'
    out+='<p style="font:12px Segoe UI,Arial">'+text(result['status'])+': '+text(result['message'])+'<br>Previous report: '+text(result['previous_generated_at'])+' | Current report: '+text(result['current_generated_at'])+'<br>Previous scanner time: '+text(result['previous_scan_time'])+' | Current scanner time: '+text(result['current_scan_time'])+'</p>'
    if not result['events']:return out
    out+='<table cellspacing="0" cellpadding="7" style="border-collapse:collapse;width:100%;font:12px Segoe UI,Arial"><tr>'
    cols=['ticker','event','field','previous','current','change','note']
    out+=''.join('<th bgcolor="#1769AA" style="background:#1769AA;color:white;border:1px solid #B7C5D5">'+c.title()+'</th>' for c in cols)+'</tr>'
    for e in result['events']:
        color=colors[e['tone']];out+='<tr>'+''.join('<td bgcolor="'+color+'" style="background:'+color+';border:1px solid #D5DDE6">'+text(e[c])+'</td>' for c in cols)+'</tr>'
    return out+'</table><p style="font-size:11px">Green/red mean positive/negative numerical change, not buy/sell. Missing observations and non-repeated alerts are not trade instructions.</p>'

def atomic(path,data):
    p=Path(path);tmp=p.with_name(p.name+'.tmp');tmp.write_text(json.dumps(data,allow_nan=False,default=str,separators=(',',':')),encoding='utf-8');tmp.replace(p)

def finish(docs_dir,baseline_info):
    docs=Path(docs_dir);audit=json.loads((docs/'a1_audit.json').read_text())
    if not audit.get('publish_ok'):raise RuntimeError('Section 11 refused: A1 audit failed')
    report=json.loads((docs/'report_scan.json').read_text());snap=json.loads((docs/'snapshot_full.json').read_text())
    current=snapshot(report,snap,audit['generated_at']);previous,state,msg=baseline_info
    result=compare(previous,current,state,msg)
    htmlpath=docs/'a1_tables.html';html=htmlpath.read_text(encoding='utf-8')
    pattern=r'<h2\b[^>]*>11\. Changes since prior scan</h2>.*?</table>'
    html,count=re.subn(pattern,lambda _:render(result),html,count=1,flags=re.S)
    if count!=1:raise RuntimeError('Section 11 marker missing; no baseline advanced')
    ai=json.loads((docs/'a1_ai.json').read_text());ai['changes']=result['events'][:12]
    ai['comparison']={k:v for k,v in result.items() if k!='events'}
    ai['comparison']['events_in_full_archive']=len(result['events']);ai['comparison']['events_in_ai']=len(ai['changes'])
    import os
    budget=int(os.getenv('A1_AI_MAX_BYTES','26000'))
    while len(json.dumps(ai,default=str).encode())>budget:
        choices=[k for k in ['changes','improving','confirmed_bullish','confirmed_bearish','country','tactical','options'] if isinstance(ai.get(k),list) and ai[k]]
        if not choices:raise RuntimeError('AI comparison metadata exceeds byte budget')
        key=max(choices,key=lambda k:len(json.dumps(ai[k],default=str)));ai[key].pop()
        ai['comparison']['events_in_ai']=len(ai['changes'])
    # Validate all outputs before committing comparison baseline.
    json.dumps(result,allow_nan=False,default=str);json.dumps(current,allow_nan=False,default=str)
    atomic(docs/'a1_changes.json',result);htmlpath.write_text(html,encoding='utf-8');atomic(docs/'a1_ai.json',ai)
    audit['section11']={'status':result['status'],'events':len(result['events']),'previous_generated_at':result['previous_generated_at'],'baseline_advanced':result['status'] not in {'BASELINE_ERROR','OUT_OF_ORDER'}}
    audit['ai_bytes']=(docs/'a1_ai.json').stat().st_size;audit['html_bytes']=htmlpath.stat().st_size
    atomic(docs/'a1_audit.json',audit)
    if result['status'] not in {'BASELINE_ERROR','OUT_OF_ORDER'}:atomic(docs/'a1_comparison_baseline.json',current)
    return result
