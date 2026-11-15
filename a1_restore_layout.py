"""A1 1.2 presentation restoration. Does not change quotes, scores or baselines."""
import json,re,hashlib,os
from pathlib import Path
from html import escape
from html.parser import HTMLParser

MARKER='<!-- A1_PRESENTATION_RESTORED_V1 -->'

def load(path,default=None):
    p=Path(path)
    if not p.exists():return default
    return json.loads(p.read_text(encoding='utf-8'))

def plain(value):
    return re.sub(r'<[^>]+>','',value).strip()

def atomic(path,value):
    p=Path(path);tmp=p.with_name(p.name+'.layout_tmp')
    tmp.write_text(json.dumps(value,default=str,allow_nan=False,separators=(',',':')),encoding='utf-8');tmp.replace(p)

def esc(value):return escape('Not supplied' if value is None else str(value))

def info_box(title,body,color='#E8F1FB'):
    return '<div style="background:'+color+';border-left:4px solid #1769AA;padding:10px;margin:10px 0;font:12px Segoe UI,Arial;color:#1F2937"><b>'+esc(title)+'</b><br>'+body+'</div>'

def render_table(title,columns,rows):
    out='<h3 style="color:#123B66;font:700 16px Segoe UI,Arial">'+esc(title)+'</h3><table cellpadding="7" cellspacing="0" style="width:100%;border-collapse:collapse;font:12px Segoe UI,Arial"><tr>'
    out+=''.join('<th bgcolor="#1769AA" style="background:#1769AA;color:white;border:1px solid #B7C5D5;text-align:left">'+esc(label)+'</th>' for key,label in columns)+'</tr>'
    for row in rows:
        delta=row.get('score_change');bg='#DFF3E4' if isinstance(delta,(int,float)) and delta>0 else '#FDE2E1' if isinstance(delta,(int,float)) and delta<0 else '#FFF3CD'
        out+='<tr>'+''.join('<td bgcolor="'+bg+'" style="background:'+bg+';border:1px solid #D5DDE6">'+esc(row.get(key))+'</td>' for key,label in columns)+'</tr>'
    return out+'</table>'

class TableCounter(HTMLParser):
    def __init__(self):super().__init__();self.tables=0;self.rows=0
    def handle_starttag(self,tag,attrs):
        if tag=='table':self.tables+=1
        if tag=='tr':self.rows+=1

def inventory(html):
    p=TableCounter();p.feed(html)
    return {'tables':p.tables,'rows':p.rows}

def partition(html):
    """Split at H2 sections; retain every byte of each section, including unknown sections."""
    matches=list(re.finditer(r'<h2\b[^>]*>.*?</h2\s*>',html,re.S|re.I))
    if not matches:raise RuntimeError('No H2 sections found; refusing layout replacement')
    # Separate the original closing wrapper/footer from the last section.
    footer_start=html.rfind('<p style="font-size:11px')
    if footer_start<matches[-1].end():
        # Actual A1 footer uses background/color formatting; retain wrappers at least.
        closing=re.search(r'(</div>\s*</div>\s*</body>\s*</html>\s*)$',html,re.I)
        footer_start=closing.start() if closing else len(html)
    sections=[]
    for i,m in enumerate(matches):
        end=matches[i+1].start() if i+1<len(matches) else footer_start
        sections.append({'title':plain(m.group()),'html':html[m.start():end]})
    return html[:matches[0].start()],sections,html[footer_start:]

def group(section):
    t=section['title'].lower()
    if 'all 11 sectors' in t:return 0
    if 'changes since prior' in t:return 1
    if 'cross-asset' in t:return 2
    if 'confirmed scanner alerts' in t:return 3
    if 'early-improving' in t or 'early improving' in t:return 4
    if 'options' in t:return 5
    if 'country' in t:return 6
    if 'tactical' in t:return 7
    if 'blocked' in t:return 8
    if 'secular' in t:return 9
    if 'universe coverage' in t:return 10
    if 'dated reference universe' in t:return 11
    return 12

def restore(docs_dir):
    docs=Path(docs_dir);htmlpath=docs/'a1_tables.html'
    original=htmlpath.read_text(encoding='utf-8')
    if MARKER in original:return {'status':'ALREADY_RESTORED'}
    audit=load(docs/'a1_audit.json',{})
    if audit.get('publish_ok') is not True:raise RuntimeError('A1 audit did not pass; layout not published')
    report=load(docs/'report_scan.json',{})
    snap=load(docs/'snapshot_full.json',{})
    changes=load(docs/'a1_changes.json',{})
    ai=load(docs/'a1_ai.json',{})
    prefix,sections,footer=partition(original)
    sector=[s for s in sections if group(s)==0];comparison=[s for s in sections if group(s)==1]
    if len(sector)!=1 or len(comparison)!=1:raise RuntimeError('Expected exactly one sector and comparison section; no changes made')
    old_inventory=inventory(original)
    # These deterministic facts replace missing opening details, without LLM calculations.
    summary=report.get('summary') or {}
    alerts=report.get('alerts') or []
    bullish=sum(1 for r in alerts if r.get('signal')=='BOTTOM');bearish=sum(1 for r in alerts if r.get('signal')=='TOP')
    sector_rows=summary.get('sectors') or []
    strong=[str(r.get('sector')) for r in sector_rows if r.get('strength')=='STRONG']
    weak=[str(r.get('sector')) for r in sector_rows if r.get('strength')=='WEAK']
    assessed=audit.get('generated_at')
    opening=MARKER+info_box('Market overview and data coverage',
        '<b>Report assessed:</b> '+esc(assessed)+'<br><b>Scanner generated:</b> '+esc(report.get('scan_time'))+
        '<br><b>Regime:</b> '+esc(report.get('regime'))+' | <b>SPY RSI:</b> '+esc(report.get('spy_rsi'))+
        '<br><b>Scanner alerts:</b> '+str(bullish)+' BOTTOM / '+str(bearish)+' TOP'+
        '<br><b>Strong sectors:</b> '+esc(', '.join(strong) or 'None classified strong')+
        ' | <b>Weak sectors:</b> '+esc(', '.join(weak) or 'None classified weak')+
        '<br>Daily research scores are not confirmed intraday scanner signals. Latest-session data is historical, not live. All 11 sectors appear immediately below.')
    # Preserve the new comparison state AND any older score-change evidence still present in snapshot.
    # Do not pretend the legacy comparison shares the new baseline or known timestamps.
    legacy=snap.get('score_changes') or []
    legacy=[{'ticker':r.get('ticker'),'score_change':r.get('score_change'),'basis':'Earlier snapshot score comparison; previous assessment time not supplied'} for r in legacy if isinstance(r,dict) and r.get('ticker') and isinstance(r.get('score_change'),(int,float))]
    supplemental=''
    if changes.get('status')=='FIRST_RUN':
        supplemental+=info_box('New comparison baseline started',
            'The persistent Section 11 comparison starts with this successful run. This is <b>not</b> a claim that nothing changed. Earlier snapshot score changes, if supplied, are retained separately below. The next successful GitHub report can compare against this baseline.','#FFF3CD')
    if legacy:
        supplemental+=render_table('Additional evidence: existing snapshot score changes (separate baseline)',[('ticker','Ticker'),('score_change','Score-point change'),('basis','Comparison basis')],legacy)
    # Do not renumber original headings: Section 11 stays identifiable and no installer regex is broken.
    ordered=sorted(enumerate(sections),key=lambda pair:(group(pair[1]),pair[0]))
    body=[]
    for _,s in ordered:
        body.append(s['html'])
        if group(s)==1:body.append(supplemental)
        if group(s)==3 and alerts:
            details=[]
            for r in alerts:
                d=r.get('rqg_details') or {}
                details.append({'ticker':r.get('ticker'),'signal':r.get('signal'),
                    'confidence':r.get('confidence'),'rsi':r.get('rsi'),'mfi':r.get('mfi'),'cmf':r.get('cmf'),
                    'mtf':r.get('mtf'),'trend_3d':r.get('trend_3d'),'trend_5d':r.get('trend_5d'),
                    'volume_expansion':r.get('volume_expansion'),'vol_ratio':d.get('vol_ratio'),
                    'reasons':', '.join(map(str,r.get('rqg_reasons') or d.get('reasons') or [])) or 'Not supplied'})
            body.append(render_table('Scanner indicator evidence | supplied fields only',[
                ('ticker','Ticker'),('signal','Signal'),('confidence','Scanner confidence score (not probability)'),
                ('rsi','RSI'),('mfi','MFI'),('cmf','CMF'),('mtf','MTF'),('trend_3d','3D trend'),('trend_5d','5D trend'),
                ('volume_expansion','volume_expansion'),('vol_ratio','rqg_details.vol_ratio'),('reasons','Reasons')],details))
    new=prefix+opening+''.join(body)+footer
    new_inventory=inventory(new)
    if new_inventory['tables']<old_inventory['tables'] or new_inventory['rows']<old_inventory['rows']:
        raise RuntimeError('Layout inventory shrank; refusing publication')
    # Every original H2 block is retained byte-for-byte, only its position changes.
    if not all(s['html'] in new for s in sections):raise RuntimeError('An original section was lost')
    ai.setdefault('metadata',{})['presentation_version']='1.2-preserve-all-sections'
    ai['metadata']['commentary_constraints']=[
        'Use exact numeric units. relative_pp and change_pp are already percentage points; NEVER multiply by 100.',
        'Technical-score reference shortlists are daily research, not confirmed BOTTOM scanner alerts.',
        'Reference universes are dated secondary holdings proxies, not certified current index membership.',
        'Country names must come from supplied registry only; do not infer country from ticker.',
        'Options volume ratios do not identify buyer direction or prove put buying.',
        'FIRST_RUN is a new comparison baseline, not NO_CHANGE.',
        'Do not claim sector/theme changes absent explicit comparison evidence.',
        'Deterministic opening overview, all-sector table and changes are in the HTML; do not recreate them.'
    ]
    # Preserve legacy evidence in AI separately with explicit unknown comparison time.
    ai['legacy_score_changes']={'basis':'Earlier snapshot comparator; baseline timestamp not supplied','events':legacy[:6]}
    budget=int(os.getenv('A1_AI_MAX_BYTES','26000'))
    removed={}
    while len(json.dumps(ai,default=str).encode())>budget:
        keys=[k for k in ['improving','confirmed_bullish','confirmed_bearish','country','tactical','options'] if isinstance(ai.get(k),list) and ai[k]]
        if not keys:raise RuntimeError('Cannot fit presentation metadata without dropping required summaries')
        key=max(keys,key=lambda k:len(json.dumps(ai[k],default=str)));ai[key].pop();removed[key]=removed.get(key,0)+1
    ai['metadata']['presentation_shortlist_omissions']=removed
    # Recheck after metadata itself is added.
    while len(json.dumps(ai,default=str).encode())>budget:
        if not ai.get('improving'):raise RuntimeError('AI payload exceeds budget')
        ai['improving'].pop()
    details={'version':'1.2','original_sections':[s['title'] for s in sections],'new_section_order':[s['title'] for _,s in ordered],
        'all_original_sections_retained':True,'before':old_inventory,'after':new_inventory,'legacy_score_change_rows_retained':len(legacy),
        'comparison_status':changes.get('status'),'assessed_at':assessed}
    audit['presentation']=details
    audit['html_bytes']=len(new.encode());audit['ai_bytes']=len(json.dumps(ai,default=str,separators=(',',':')).encode())
    # Do not modify a1_changes.json or a1_comparison_baseline.json.
    backup=docs/'a1_tables_before_layout.html';backup.write_text(original,encoding='utf-8')
    htmlpath.write_text(new,encoding='utf-8');atomic(docs/'a1_ai.json',ai);atomic(docs/'a1_audit.json',audit);atomic(docs/'a1_layout_audit.json',details)
    return details
