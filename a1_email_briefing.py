"""A1 evidence briefing R2. Presentation only; no downloads or source-data writes."""
import json
import math
import re
from datetime import datetime
from html import escape, unescape
from pathlib import Path

VERSION = 'evidence-briefing-r2'
SECTORS = {'XLK':'Technology','XLF':'Financials','XLV':'Health Care',
           'XLI':'Industrials','XLY':'Consumer Discretionary','XLP':'Staples',
           'XLE':'Energy','XLU':'Utilities','XLB':'Materials',
           'XLRE':'Real Estate','XLC':'Communication Services'}
COLORS = {'bull':'#176640','bear':'#ac2940','watch':'#925d00',
          'blue':'#145b94','gray':'#526273'}


def number(value):
    if isinstance(value, bool):
        return None
    try:
        value = float(value)
        return value if math.isfinite(value) else None
    except (ValueError, TypeError, OverflowError):
        return None


def text(value):
    return escape(str(value if value is not None else 'Not recorded'))


def fmt(value, unit=''):
    value = number(value)
    if value is None:
        return 'Not recorded'
    if unit == '$':
        return '$' + format(value, ',.2f')
    return format(value, '+.2f') + unit


def tone(value, unit='%'):
    value = number(value)
    color = COLORS['gray'] if value is None or value == 0 else COLORS['bull'] if value > 0 else COLORS['bear']
    return '<span style="font-weight:bold;color:' + color + '">' + text(fmt(value, unit)) + '</span>'


def read(docs, name, default):
    path = Path(docs) / name
    return json.loads(path.read_text(encoding='utf-8-sig')) if path.exists() else default


def panel(title, body):
    return '<h2 style="font-size:17px;color:#12304a;margin:22px 0 10px;border-bottom:2px solid #1976a3;padding-bottom:7px">' + text(title) + '</h2>' + body


def table(headers, rows):
    head = ''.join('<th scope="col" style="background:#12304a;color:#fff;padding:7px;text-align:left;font-size:11px">' + text(h) + '</th>' for h in headers)
    body = ''.join('<tr style="background:' + ('#f2f6fa' if i % 2 == 0 else '#fff') + '">' + ''.join('<td style="padding:7px;border-bottom:1px solid #dce5ed;vertical-align:top;overflow-wrap:anywhere;font-size:12px">' + str(c) + '</td>' for c in row) + '</tr>' for i, row in enumerate(rows))
    return '<table width="100%" cellpadding="0" cellspacing="0" style="width:100%;border-collapse:collapse;table-layout:fixed"><thead><tr>' + head + '</tr></thead><tbody>' + body + '</tbody></table>'


def field(label, value, color='#243447'):
    return '<p style="margin:7px 0;line-height:1.5;color:' + color + '"><b>' + text(label) + ':</b> ' + value + '</p>'


def reason_label(value):
    return re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', str(value)).replace('_', ' ')


def ranked(rows):
    # Existing scores only. Display order is not a new strategy or probability.
    return sorted(rows, key=lambda r: (-(number(r.get('rqg_score')) or 0), -(number(r.get('confidence')) or 0), str(r.get('ticker', ''))))


def parse_time(value):
    if not value:
        return None
    try:
        result = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        return result if result.tzinfo is not None else None
    except ValueError:
        return None


def observations(report, ai, snapshot):
    """Use timestamped records, never the un-timestamped report.prices map."""
    result = {}
    for row in snapshot.get('daily_features', []) or []:
        if isinstance(row, dict) and row.get('ticker'):
            result[row['ticker']] = {'price':row.get('price'), 'timestamp':row.get('bar_timestamp'), 'basis':row.get('price_basis') or 'Daily observation; current session may be partial', 'freshness':row.get('daily_status') or 'Freshness not classified'}
    for row in ai.get('cross_assets', []) or []:
        if isinstance(row, dict) and row.get('ticker'):
            result[row['ticker']] = dict(row)
    raw = report.get('cross_assets') or {}
    for ticker, row in raw.items() if isinstance(raw, dict) else []:
        if not isinstance(row, dict):
            continue
        old = result.get(ticker, {})
        stamp = row.get('market_timestamp')
        if old.get('timestamp') == stamp:
            merged = dict(old)
            merged.update({'price':row.get('price'), 'timestamp':stamp, 'basis':row.get('price_basis') or old.get('basis')})
            result[ticker] = merged
        elif not old or (parse_time(stamp) and parse_time(old.get('timestamp')) and parse_time(stamp) > parse_time(old.get('timestamp'))):
            result[ticker] = {'price':row.get('price'), 'timestamp':stamp, 'basis':row.get('price_basis'), 'freshness':'Timestamp supplied; freshness classification unavailable'}
    return result


def comparison(alert, observed):
    """Only compare observations when timestamps and price bases support it."""
    ticker = alert.get('ticker')
    latest = observed.get(ticker) or {}
    before = number(alert.get('alert_price'))
    after = number(latest.get('price'))
    stamp = latest.get('timestamp')
    alert_time = alert.get('time') or (alert.get('rqg_details') or {}).get('latest')
    sentence = 'Alert reference ' + fmt(before, '$') + ' at ' + str(alert_time or 'time not recorded') + '.'
    if after is None or not stamp:
        return sentence + ' No separate timestamped latest observation; no change calculated.'
    sentence += ' Latest supplied ' + fmt(after, '$') + ' at ' + str(stamp) + ' (' + str(latest.get('basis') or 'basis not recorded') + ').'
    # Alert adjustment basis is not recorded by the supplied schema. Do not mix it
    # with adjusted quotes just to produce an attractive return percentage.
    basis = alert.get('price_basis')
    if basis and basis == latest.get('basis') and before and parse_time(alert_time) and parse_time(stamp) and parse_time(stamp) >= parse_time(alert_time):
        sentence += ' Change since alert ' + fmt((after / before - 1) * 100, '%') + '; not a trade outcome.'
    else:
        sentence += ' Change since alert withheld: matching adjustment basis and ordered timestamps not established.'
    return sentence


def conflicts(alert):
    details = alert.get('rqg_details') or {}
    warnings = []
    rr = number(details.get('rr'))
    if rr is None or rr <= 0:
        warnings.append('No usable recorded risk/reward')
    else:
        warnings.append('Recorded risk/reward ' + fmt(rr) + '; execution levels still require verification')
    rsi = number(alert.get('rsi'))
    if alert.get('signal') == 'TOP' and rsi is not None and rsi < 30:
        warnings.append('RSI below 30: downside-extension caution')
    if alert.get('signal') == 'BOTTOM' and rsi is not None and rsi > 70:
        warnings.append('RSI above 70: upside-extension caution')
    if alert.get('signal') == 'TOP' and alert.get('sector_strength') == 'STRONG':
        warnings.append('Bearish direction conflicts with strong sector')
    if alert.get('signal') == 'BOTTOM' and alert.get('sector_strength') == 'WEAK':
        warnings.append('Bullish direction conflicts with weak sector')
    if alert.get('signal') == 'BOTTOM' and alert.get('regime') == 'BEARISH':
        warnings.append('Bullish direction conflicts with bearish engine regime')
    return warnings


def alert_card(alert, observed):
    color = COLORS['bull'] if alert.get('signal') == 'BOTTOM' else COLORS['bear']
    reasons = alert.get('rqg_reasons') or alert.get('signals') or []
    if isinstance(reasons, str):
        reasons = [reasons]
    support = '; '.join(reason_label(x) for x in reasons[:4]) or 'Supporting reasons not recorded'
    body = '<div style="border:1px solid #d8e2eb;border-left:5px solid ' + color + ';padding:12px;margin:10px 0;border-radius:6px">'
    body += '<h3 style="font-size:16px;margin:0;color:' + color + '">' + text(alert.get('ticker')) + ' | ' + text(alert.get('signal')) + ' | ' + text(alert.get('setup_type')) + '</h3>'
    body += field('Why flagged', text(support))
    body += field('Recorded confirmation evidence', 'RQG ' + text(alert.get('rqg_score')) + '; confidence score ' + text(alert.get('confidence')) + '; timeframes ' + text(alert.get('mtf_status')) + '; sector ' + text(alert.get('sector')) + ' / ' + text(alert.get('sector_strength')))
    body += field('What conflicts / needs checking', text('; '.join(conflicts(alert))), COLORS['watch'])
    body += field('Then versus latest supplied', text(comparison(alert, observed)))
    body += field('Next verification', 'Confirm current bar, executable trigger, invalidation and target. Scanner direction is not an order; no entry approval is inferred.')
    body += '</div>'
    return body


def research_card(row):
    extended = number(row.get('extension_atr'))
    caution = 'Daily research only; not a qualified BOTTOM signal.'
    if extended is not None and extended > 2:
        caution += ' More than 2 ATR above MA20: extension warning (display only).'
    if 'PARTIAL' in str(row.get('daily_status', '')).upper():
        caution += ' Partial-session volume ratio is not full-day volume confirmation.'
    body = '<div style="border:1px solid #cbdcec;border-left:5px solid #145b94;padding:12px;margin:10px 0;border-radius:6px">'
    body += '<h3 style="margin:0;color:#145b94;font-size:16px">' + text(row.get('ticker')) + ' | Developing daily research</h3>'
    components = row.get('score_components') or {}
    supported = [reason_label(k) for k,v in components.items() if number(v) is not None and number(v) > 0]
    body += field('Why on the screen', text('; '.join(supported) or 'Component evidence not recorded') + '; technical score ' + text(row.get('technical_score')) + ' is a heuristic, not a probability.')
    body += field('Evidence', '3-session ' + tone(row.get('return_3d')) + '; 5-session ' + tone(row.get('return_5d')) + '; 20-session ' + tone(row.get('return_20d')) + '; 20-session relative performance ' + tone(row.get('relative_20d_vs_spy_pp'), ' pp'))
    body += field('Caution', text(caution), COLORS['watch'])
    body += field('Observation', text(fmt(row.get('price'), '$')) + ' | bar ' + text(row.get('bar_timestamp')) + ' | ' + text(row.get('daily_status') or row.get('status')))
    history = [number(x) for x in (row.get('price_history_5bars') or [])]
    if history and all(x is not None for x in history):
        body += field('Recent daily observations', ' &rarr; '.join(text(fmt(x, '$')) for x in history) + ' <small>(oldest to newest; final bar may be partial)</small>')
    body += '</div>'
    return body


def sector_rows(report, ai):
    rows = {}
    for row in (report.get('summary') or {}).get('sectors', []) or []:
        if isinstance(row, dict) and row.get('sector') in SECTORS:
            rows[row['sector']] = dict(row)
    for row in ai.get('sectors', []) or []:
        ticker = row.get('ticker')
        if ticker not in SECTORS:
            continue
        target = rows.setdefault(ticker, {})
        for source, dest in [('relative_pp','diff'),('change_pp','change_pp'),('stage','stage'),('timestamp','latest')]:
            if row.get(source) is not None:
                target[dest] = row[source]
        if row.get('strength') is not None:
            target['strength'] = row['strength']
    return rows


def event_order(event):
    priority = {'NEW SCANNER ALERT':0, 'ALERT NO LONGER RETURNED':1,
                'RESEARCH STATUS CHANGE':2, 'SCORE CHANGE':3,
                'PRICE OBSERVATION CHANGE':4}
    return (priority.get(event.get('event'), 9),
            -abs(number(event.get('change')) or 0) if event.get('event') == 'SCORE CHANGE' else 0,
            str(event.get('ticker', '')))


def build(docs):
    report = read(docs, 'report_scan.json', None)
    if not isinstance(report, dict) or not isinstance(report.get('alerts'), list):
        raise ValueError('Complete report_scan.json with alerts is required')
    ai = read(docs, 'a1_ai.json', {})
    changes = read(docs, 'a1_changes.json', {})
    snapshot = read(docs, 'snapshot_full.json', {})
    alerts = report['alerts']
    bull = [r for r in alerts if r.get('signal') == 'BOTTOM']
    bear = [r for r in alerts if r.get('signal') == 'TOP']
    sectors = sector_rows(report, ai)
    observed = observations(report, ai, snapshot)
    metadata = ai.get('metadata') or {}
    ordered = sorted(SECTORS, key=lambda t: -(number(sectors.get(t, {}).get('diff')) if number(sectors.get(t, {}).get('diff')) is not None else -1e12))
    events = changes.get('events') or []
    chosen_events = sorted(events, key=event_order)[:4]
    features = snapshot.get('daily_features')
    complete_research = isinstance(features, list)
    research = [r for r in features if r.get('panel') == 'stock_or_other' and r.get('improving') and r.get('higher_low')] if complete_research else list(ai.get('improving') or [])
    research.sort(key=lambda r: (number(r.get('extension_atr')) is None or number(r.get('extension_atr')) > 2, -(number(r.get('technical_score')) or 0), str(r.get('ticker', ''))))
    h = '<!doctype html><html lang="en"><head><meta name="viewport" content="width=device-width, initial-scale=1"><meta charset="utf-8"><title>A1 Evidence Briefing</title></head><body style="margin:0;background:#edf2f6;color:#243447;font:14px Arial,sans-serif;line-height:1.5"><div style="max-width:760px;margin:0 auto;background:#fff;padding:16px">'
    h += '<div style="background:#12304a;color:#fff;padding:20px;border-radius:8px"><div style="font-size:12px;letter-spacing:1px">A1 | EVIDENCE BRIEFING</div><h1 style="font-size:24px;margin:8px 0">What deserves attention?</h1><p style="margin:0">Evidence. Conflicts. Changes. Full dashboard attached.</p></div>'
    h += '<p><b>Executive Summary</b> | Engine regime <b>' + text(report.get('regime')) + '</b> | SPY RSI ' + text(report.get('spy_rsi')) + '</p>'
    h += table(['Bullish signals','Bearish signals','Blocked setups'], [[text(len(bull)),text(len(bear)),text((report.get('summary') or {}).get('blocked'))]])
    h += '<p style="font-size:11px;color:#526273">Scan started ' + text(report.get('scan_time')) + '; completed ' + text(report.get('completed_at')) + '. Freshness assessed ' + text(metadata.get('freshness_assessed_at')) + '. These are not interchangeable with market-observation times.</p>'
    insights = []
    if not bull:
        insights.append(('Opportunity discipline', 'No qualified BOTTOM signals returned. Developing research is kept separate; a rebound alone does not establish a confirmed turn.'))
    else:
        insights.append(('Bullish review', str(len(bull)) + ' BOTTOM signals returned. Display priority uses existing RQG then confidence, not a new probability model.'))
    valid = [t for t in ordered if number(sectors.get(t, {}).get('diff')) is not None]
    if valid:
        first, last = valid[0], valid[-1]
        insights.append(('Relative leadership', first + ' ' + fmt(sectors[first].get('diff'),' pp') + ' versus SPY; ' + last + ' ' + fmt(sectors[last].get('diff'),' pp') + '. Relative strength is not measured fund flow or necessarily an absolute gain.'))
    rr_missing = sum(number((r.get('rqg_details') or {}).get('rr')) is None or number((r.get('rqg_details') or {}).get('rr')) <= 0 for r in alerts)
    insights.append(('Main evidence gap', str(rr_missing) + ' of ' + str(len(alerts)) + ' alerts lack positive recorded risk/reward. Scores and timeframe agreement do not resolve missing execution evidence.'))
    h += panel('1. Market in 30 seconds', ''.join(field(k,text(v)) for k,v in insights))
    h += panel('2. Attention first', '<p style="font-size:12px">Bullish and bearish lists are separate. Existing RQG, then confidence determines display order; no signal thresholds changed.</p>')
    for label, rows, limit in [('Bullish scanner signals',bull,3),('Bearish scanner signals',bear,3)]:
        chosen = ranked(rows)[:limit]
        h += '<h3 style="font-size:15px">' + text(label) + ' (' + str(len(chosen)) + ' shown / ' + str(len(rows)) + ' total)</h3>'
        h += ''.join(alert_card(r, observed) for r in chosen) if chosen else '<p>No scanner signals returned in this direction.</p>'
    h += '<h3 style="font-size:15px">Developing research (up to 2)</h3><p style="font-size:12px">' + ('Full daily snapshot used.' if complete_research else 'Compact shortlist used; selection is incomplete because full snapshot was not supplied.') + '</p>'
    h += ''.join(research_card(r) for r in research[:2]) or '<p>No developing rows supplied.</p>'
    rows = []
    for ticker in ordered:
        row = sectors.get(ticker, {})
        value = number(row.get('diff'))
        bg = '#e7f4ec' if value is not None and value > 0 else '#fbeaec' if value is not None and value < 0 else '#f2f4f6'
        rows.append(['<span style="background:'+bg+';padding:3px"><b>'+text(ticker)+'</b></span><br>'+text(SECTORS[ticker]), tone(row.get('diff'),' pp')+'<br><small>change '+text(fmt(row.get('change_pp'),' pp'))+'</small>', text(row.get('stage') or row.get('strength'))+'<br><small>'+text(row.get('latest'))+'</small>'])
    h += panel('3. All-sector rotation', table(['Sector','RS vs SPY / change','Stage / observation'], rows) + '<p style="font-size:11px">Relative performance uses the scanner\'s recent window, not a one-day return. Change is versus the prior sector snapshot. Missing comparisons stay missing; zero is a valid value. No flow inference.</p>')
    body = '<p style="font-size:12px">' + text(changes.get('status')) + ' | ' + text(changes.get('previous_generated_at')) + ' to ' + text(changes.get('current_generated_at')) + '. Showing ' + str(len(chosen_events)) + ' of ' + str(len(events)) + ' events.</p>'
    for event in chosen_events:
        body += '<div style="border-left:4px solid #1976a3;padding:5px 10px;margin:7px 0"><b>'+text(event.get('ticker'))+' | '+text(event.get('event'))+'</b><br>'+text(event.get('previous'))+' &rarr; '+text(event.get('current'))+'<br><small>'+text(event.get('note'))+'</small></div>'
    if not chosen_events:
        body += '<p>' + text(changes.get('message') or 'No event details supplied; do not infer NO_CHANGE.') + '</p>'
    body += '<p style="font-size:11px">An alert no longer returned is not automatically invalidated. FIRST_RUN means a new baseline, not no change. Full Section 11 remains attached.</p>'
    h += panel('4. What materially changed', body)
    groups = {}
    for row in ai.get('cross_assets') or []:
        groups.setdefault(row.get('group') or 'Other', []).append(row)
    body = ''
    assets_by = {r.get('ticker'):r for r in ai.get('cross_assets') or []}
    spy = assets_by.get('SPY', {})
    qqq = assets_by.get('QQQ', {})
    if (spy.get('daily_return_date') and spy.get('daily_return_date') == qqq.get('daily_return_date')
            and number(spy.get('return_20d')) is not None and number(qqq.get('return_20d')) is not None):
        spread = number(qqq['return_20d']) - number(spy['return_20d'])
        body += field('Same-period cross-check', 'QQQ minus SPY over 20 sessions: ' + tone(spread, ' pp') + ' through ' + text(spy['daily_return_date']) + '. This is relative price performance, not ETF flows.')
    for group, items in groups.items():
        selected = items[:2]
        body += '<p><b>' + text(group) + '</b> ('+str(len(selected))+' of '+str(len(items))+' shown)</p>'
        for row in selected:
            unit = '' if 'index' in str(row.get('type','')).lower() or row.get('ticker') in ('^VIX','^TNX','DX-Y.NYB') else '$'
            body += '<p style="margin:5px 0 10px"><b>'+text(row.get('ticker'))+' '+text(fmt(row.get('price'),unit))+'</b> | 20-session '+tone(row.get('return_20d'))+'<br><small>'+text(row.get('timestamp'))+' | '+text(row.get('freshness'))+' | '+text(row.get('basis'))+'</small></p>'
    body += '<p style="font-size:11px">Price observations and 20-session returns may use different bases. No current-session return is inferred. ETF proxies are not spot prices; index units are not dollars. Relative price moves do not establish money flows.</p>'
    h += panel('5. Cross-asset context', body or '<p>See full attachment for cross-asset observations.</p>')
    options = report.get('options') or {}
    valid_options = [(t,r) for t,r in options.items() if isinstance(r,dict) and not r.get('error')]
    body = '<p><b>Options coverage:</b> '+str(len(valid_options))+' usable chain records / '+str(len(alerts))+' scanner alerts. Selected-expiration volume PCR is not buyer intent or institutional positioning.</p>'
    for ticker,row in valid_options[:2]:
        body += '<p style="font-size:12px">'+text(ticker)+' PCR '+text(row.get('pcr'))+'; expiry '+text(row.get('expiration'))+'; retrieved '+text(row.get('retrieved_at'))+'. '+text(row.get('quote_timestamp_status'))+'.</p>'
    for group in ('country','tactical'):
        available = [r for r in features if r.get('panel') == group] if complete_research else ai.get(group) or []
        chosen = sorted(available,key=lambda r: (-(number(r.get('technical_score')) or 0),str(r.get('ticker'))))[:2]
        body += field(group.title()+' research', text('; '.join(str(r.get('ticker'))+' score '+str(r.get('technical_score')) for r in chosen) or 'No rows supplied')+'; full tables attached.')
    body += '<p style="font-size:11px">Tactical leveraged/inverse ETFs are daily-reset tools, not ordinary long-term growth holdings.</p>'
    body += field('Index reference coverage', text('; '.join(str(r.get('universe'))+' '+str(r.get('valid'))+'/'+str(r.get('expected'))+' dated '+str(r.get('verified_as_of')) for r in ai.get('coverage') or []) or 'Not supplied')+'. Dated holdings references are not certified current membership.')
    body += field('Theme research (selected)', text('; '.join(str(r.get('theme')) for r in (ai.get('themes') or [])[:3]) or 'No theme map supplied')+'. Hypotheses only; fundamental quality not established.')
    h += panel('6. Coverage and research limits',body)
    warnings = report.get('warnings') or []
    h += panel('7. Data quality / what is not verified', ''.join('<p>'+text(w)+'</p>' for w in warnings[:3])+'<p>Fundamentals, analyst targets, company catalysts and actual fund flows are not added without dated sources. No verified feed was added in this release. Full source warnings and all evidence remain in the attached dashboard.</p>')
    h += '<div style="background:#fff3d4;padding:12px;border-left:4px solid #b77900;font-size:12px">Green/red numbers show direction, not trade approval. Blue marks research or changes; amber marks uncertainty. Setup scores are not win probabilities. This briefing changes presentation only.</div>'
    h += '</div></body></html>'
    plain = re.sub(r'\s+',' ',unescape(re.sub('<[^>]+>',' ',h)))
    return h, plain, str(report.get('regime') or 'Not recorded')
