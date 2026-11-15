"""Patch the exact uploaded A1 Ultimate v1 dashboard to v1.1.
Idempotent, checked, and refuses an unexpected source version.
"""
from pathlib import Path
import ast, shutil
p=Path('a1_ultimate_dashboard.py')
s=p.read_text(encoding='utf-8')
if 'A1_ULTIMATE_DASHBOARD_V1_1' in s:
    print('Final dashboard fix already installed')
    raise SystemExit(0)
markers=[
 "MARKER='<!-- A1_ULTIMATE_DASHBOARD_V1 -->'",
 "cell(trend_strip(hist.get(t)))",
 "sec_by={r.get('sector'):r for r in sectors}",
 "for r in alerts:",
 "A1 PROFESSIONAL MARKET DASHBOARD"
]
for m in markers:
    if m not in s:
        raise SystemExit('Unexpected dashboard version; missing marker: '+m)
# Version marker + safe raw HTML cell. Only locally generated trend_strip markup uses raw_cell.
s=s.replace("MARKER='<!-- A1_ULTIMATE_DASHBOARD_V1 -->'", "MARKER='<!-- A1_ULTIMATE_DASHBOARD_V1_1 -->'")
needle="""def cell(text,bg=None,bold=False,align='left'):
 return '<td bgcolor=\"{0}\" style=\"background:{0};border:1px solid {1};padding:7px 8px;vertical-align:middle;text-align:{2};font-weight:{3};color:{4}\">{5}</td>'.format(bg or C['white'],C['border'],align,'700' if bold else '400',C['dark'],e(text))
"""
addition=needle+"""def raw_cell(trusted_html,bg=None,align='left'):
 # trusted_html must be generated only by trend_strip(); never pass provider/user text.
 return '<td bgcolor=\"{0}\" style=\"background:{0};border:1px solid {1};padding:7px 8px;vertical-align:middle;text-align:{2};color:{3}\">{4}</td>'.format(bg or C['white'],C['border'],align,C['dark'],trusted_html)
"""
if needle not in s:raise SystemExit('cell() function differs; no file changed')
s=s.replace(needle,addition,1)
# Load calculated A1 sources that already contain stage/change and enriched alerts.
s=s.replace("report=load(docs/'report_scan.json',{});snap=load(docs/'snapshot_full.json',{});audit=load(docs/'a1_audit.json',{});changes=load(docs/'a1_changes.json',{})",
"report=load(docs/'report_scan.json',{});snap=load(docs/'snapshot_full.json',{});audit=load(docs/'a1_audit.json',{});changes=load(docs/'a1_changes.json',{})\n ai=load(docs/'a1_ai.json',{});archive=load(docs/'a1_full_archive.json',{})")
# Prefer complete enriched alert records from archive, never compact/omitted lists.
s=s.replace("alerts=report.get('alerts') or [];blocked=", "alerts=((archive.get('report') or {}).get('alerts') or report.get('alerts') or []);blocked=")
# Calculated sectors in a1_ai are joined by ticker. Raw rows continue to provide any missing source values.
old="""sec_by={r.get('sector'):r for r in sectors}
 rows=[]
 for t in SECTOR_ORDER:
  r=sec_by.get(t,{})
"""
new="""raw_sec_by={r.get('sector'):r for r in sectors}
 calc_sec_by={r.get('ticker'):r for r in (ai.get('sectors') or [])}
 sec_by={}
 for ticker in SECTOR_ORDER:
  merged=dict(raw_sec_by.get(ticker,{}) or {})
  calculated=calc_sec_by.get(ticker,{}) or {}
  # Explicitly map calculated names to dashboard names. Zero is a valid value.
  if 'relative_pp' in calculated: merged['diff']=calculated.get('relative_pp')
  if 'change_pp' in calculated: merged['change_pp']=calculated.get('change_pp')
  if calculated.get('stage') is not None: merged['stage']=calculated.get('stage')
  if calculated.get('timestamp'): merged['latest']=calculated.get('timestamp')
  if calculated.get('strength'): merged['strength']=calculated.get('strength')
  sec_by[ticker]=merged
 strong=[t for t,r in sec_by.items() if r.get('strength')=='STRONG']
 weak=[t for t,r in sec_by.items() if r.get('strength')=='WEAK']
 rows=[]
 for t in SECTOR_ORDER:
  r=sec_by.get(t,{})
"""
if old not in s:raise SystemExit('sector block differs; no file changed')
s=s.replace(old,new,1)
# The old strong/weak is now superseded by calculated merge.
s=s.replace("strong=[r.get('sector') for r in sectors if r.get('strength')=='STRONG'];weak=[r.get('sector') for r in sectors if r.get('strength')=='WEAK']", "strong=[];weak=[]")
# Render only internally generated trend markup without escaping.
s=s.replace("cell(trend_strip(hist.get(t)))", "raw_cell(trend_strip(hist.get(t)))")
s=s.replace("cell(trend_strip(hist.get(r.get('ticker'))))", "raw_cell(trend_strip(hist.get(r.get('ticker'))))")
# Field-specific sector fallbacks.
s=s.replace("cell(r.get('stage')),cell(r.get('latest'))", "cell(r.get('stage') if r.get('stage') is not None else 'No calculated stage'),cell(r.get('latest') or 'Bar time not supplied')")
# Make the early count label explicit and non-contradictory.
s=s.replace("('Early research',len(improving),C['cyan'],'Daily watch, not entries')", "('Screen matches',len(improving),C['cyan'],'Early-improving daily screen; appendix may use stricter criteria')")
# Alert joins from complete enriched archive; fallback wording is precise.
old="""for r in alerts:
  bg=C['green'] if r.get('signal')=='BOTTOM' else C['red']
  d=r.get('rqg_details') or {}
  setup_rows.append([cell(r.get('ticker'),bg,True),cell(r.get('signal'),bg,True),cell(r.get('setup_type')),cell(money(r.get('alert_price'))),cell(score(r.get('rqg_score')),bg_for(r.get('rqg_score'),'score')),cell(score(r.get('confidence')),bg_for(r.get('confidence'),'score')),cell(r.get('rsi')),cell(r.get('mfi')),cell(r.get('cmf')),cell(r.get('freshness'),freshness_color(r.get('freshness'))),cell(r.get('entry_readiness'),freshness_color(r.get('entry_readiness'))),raw_cell(trend_strip(hist.get(r.get('ticker'))))])
"""
new="""for r in alerts:
  bg=C['green'] if r.get('signal')=='BOTTOM' else C['red']
  d=r.get('rqg_details') or {}
  freshness=r.get('freshness') or ('Bar time supplied; freshness not calculated' if d.get('latest') else 'Signal bar timestamp not supplied')
  readiness=r.get('entry_readiness') or ('Last-session signal - reconfirm' if 'LATEST SESSION' in freshness else 'Intraday confirmation required')
  setup_rows.append([cell(r.get('ticker'),bg,True),cell(r.get('signal'),bg,True),cell(r.get('setup_type') or 'Setup type not supplied'),cell(money(r.get('alert_price'))),cell(score(r.get('rqg_score')),bg_for(r.get('rqg_score'),'score')),cell(score(r.get('confidence')),bg_for(r.get('confidence'),'score')),cell(r.get('rsi') if r.get('rsi') is not None else 'RSI not supplied'),cell(r.get('mfi') if r.get('mfi') is not None else 'MFI not supplied'),cell(r.get('cmf') if r.get('cmf') is not None else 'CMF not supplied'),cell(freshness,freshness_color(freshness)),cell(readiness,freshness_color(readiness)),raw_cell(trend_strip(hist.get(r.get('ticker'))))])
"""
if old not in s:raise SystemExit('alert block differs; no file changed')
s=s.replace(old,new,1)
# Precise missing labels in cross-assets/options.
s=s.replace("or 'Not supplied';basis=", "or 'Status not supplied';basis=")
s=s.replace("or 'Not supplied'\n   cross_rows", "or 'Price basis not supplied'\n   cross_rows")
s=s.replace("cell(v.get('quote_timestamp_status'))", "cell(v.get('quote_timestamp_status') or 'Option quote timestamp not supplied by source')")
# Stronger integrity statement without claiming all data is current.
s=s.replace("Missing values remain visible.</div>", "Missing values remain visible. Broad coverage does not mean uniform freshness: index observations and option timestamps can remain limited.</div>")
# Add final structural validation before writing.
validation="""\n # Final dashboard integrity checks: prevent recurrence of missing calculated fields or escaped strips.
 if len(sec_by)!=11 or any(sec_by[t].get('stage') is None or 'change_pp' not in sec_by[t] for t in SECTOR_ORDER):
  raise RuntimeError('Ultimate dashboard sector join incomplete')
 if '&lt;span title=' in out or '\\\\<span title=' in out:
  raise RuntimeError('Seven-session strip was escaped instead of rendered')
 if alerts and any((r.get('freshness') is None and not (r.get('rqg_details') or {}).get('latest')) for r in alerts):
  # Missing source timestamp remains allowed, but must be represented with the explicit fallback text.
  if 'Signal bar timestamp not supplied' not in out:raise RuntimeError('Alert timestamp fallback missing')
"""
write_marker="   if len(out.encode())<len(original.encode()):raise RuntimeError('Ultimate report lost content; refusing publication')"
if write_marker not in s:raise SystemExit('write marker differs; no file changed')
s=s.replace(write_marker,validation+"\n"+write_marker,1)
# Version and audit fields.
s=s.replace("'version':'ultimate-v1'", "'version':'ultimate-v1.1-final'")
s=s.replace("'history_symbols':len(hist)", "'sector_calculated_fields_complete':all(sec_by[t].get('stage') is not None and 'change_pp' in sec_by[t] for t in SECTOR_ORDER),'trend_strips_rendered_as_html':('&lt;span title=' not in out),'alert_rows_with_explicit_freshness':sum(1 for r in alerts if r.get('freshness') or (r.get('rqg_details') or {}).get('latest')),'history_symbols':len(hist)")
s+="\n# A1_ULTIMATE_DASHBOARD_V1_1\n"
ast.parse(s)
backup=p.with_name('a1_ultimate_dashboard.py.before_final_fix')
if not backup.exists():shutil.copy2(p,backup)
p.write_text(s,encoding='utf-8')
print('Installed final dashboard v1.1 fix; backup:',backup)
