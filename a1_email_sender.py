#!/usr/bin/env python3
"""A1 GitHub email sender. Replaces Power Automate.

Sends the final validated A1 dashboard (docs/a1_tables.html) once per trading day,
at the first successful scan at or after 3:30 PM ET (or at the close on early-close
days). A deterministic summary built from the report data replaces the AI commentary.
Nothing in the scanner, scores or report data is changed.

Required GitHub secrets: EMAIL_ADDRESS, EMAIL_PASSWORD, RECIPIENT_EMAILS.
Optional: SMTP_SERVER (default smtp.gmail.com), SMTP_PORT (default 587).
Manual run with A1_FORCE_EMAIL=true sends immediately, even if already sent today.
"""
import json, os, smtplib, ssl, sys
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from html import escape
from pathlib import Path
from zoneinfo import ZoneInfo

ET = ZoneInfo('America/New_York')
DOCS = Path('docs')
STATE = DOCS / 'a1_email_state.json'
SEND_AFTER = (14, 00)          # 3:30 PM Eastern
MAX_REPORT_AGE_MIN = 90        # never email an old report
SECTOR_NAMES = {'XLK': 'Technology', 'XLF': 'Financials', 'XLV': 'Health Care', 'XLI': 'Industrials',
                'XLY': 'Consumer Discretionary', 'XLP': 'Consumer Staples', 'XLE': 'Energy',
                'XLU': 'Utilities', 'XLB': 'Materials', 'XLRE': 'Real Estate', 'XLC': 'Communication Services'}


def load(name, default=None):
    path = DOCS / name
    if not path.is_file():
        return default
    return json.loads(path.read_text(encoding='utf-8'))


def num(value):
    try:
        x = float(value)
        return x if x == x and abs(x) != float('inf') else None
    except (TypeError, ValueError):
        return None


def send_time_today(now_et):
    """3:30 PM ET, or the market close if the exchange closes earlier that day."""
    target = now_et.replace(hour=SEND_AFTER[0], minute=SEND_AFTER[1], second=0, microsecond=0)
    try:
        import pandas_market_calendars as mcal
        sched = mcal.get_calendar('NYSE').schedule(start_date=now_et.date(), end_date=now_et.date())
        if sched.empty:
            return None  # weekend or exchange holiday: no scheduled email
        close = sched.iloc[0]['market_close'].to_pydatetime().astimezone(ET)
        return min(target, close)
    except ImportError:
        return target if now_et.weekday() < 5 else None


def decide(now_et, force, state):
    """Return (send: bool, reason: str)."""
    if force:
        return True, 'manual run requested email'
    send_at = send_time_today(now_et)
    if send_at is None:
        return False, 'not a trading day'
    if now_et < send_at:
        return False, 'before send time ' + send_at.strftime('%I:%M %p ET')
    if (state or {}).get('last_sent_date') == now_et.date().isoformat():
        return False, 'already sent today'
    return True, 'first validated report after ' + send_at.strftime('%I:%M %p ET')


def check_report(now_utc):
    """Refuse to send unless this run produced a passing, current report."""
    audit = load('a1_audit.json', {})
    ultimate = load('a1_ultimate_audit.json', {})
    if audit.get('publish_ok') is not True:
        raise SystemExit('Not sending: A1 audit did not pass')
    if not ultimate.get('all_prior_html_retained'):
        raise SystemExit('Not sending: dashboard audit did not pass')
    generated = datetime.fromisoformat(str(audit.get('generated_at')).replace('Z', '+00:00'))
    if generated.tzinfo is None or now_utc - generated > timedelta(minutes=MAX_REPORT_AGE_MIN):
        raise SystemExit('Not sending: report is older than %d minutes' % MAX_REPORT_AGE_MIN)
    html = (DOCS / 'a1_tables.html').read_text(encoding='utf-8')
    if 'A1 PROFESSIONAL MARKET DASHBOARD' not in html or len(html) < 5000:
        raise SystemExit('Not sending: dashboard HTML missing or incomplete')
    return audit, ultimate, html


def summary(audit, ultimate):
    """Deterministic executive summary from report data. No model, no invented facts."""
    ai = load('a1_ai.json', {}) or {}
    report = load('report_scan.json', {}) or {}
    changes = load('a1_changes.json', {}) or {}
    alerts = report.get('alerts') or []
    bullish = sum(1 for r in alerts if r.get('signal') == 'BOTTOM')
    bearish = sum(1 for r in alerts if r.get('signal') == 'TOP')
    sectors = [s for s in (ai.get('sectors') or []) if num(s.get('relative_pp')) is not None]
    sectors.sort(key=lambda s: num(s.get('relative_pp')), reverse=True)

    def sector_text(s):
        name = SECTOR_NAMES.get(s.get('ticker'), s.get('ticker'))
        return '%s %s (%+.2f pp vs SPY)' % (s.get('ticker'), name, num(s.get('relative_pp')))

    regime = report.get('regime') or ai.get('regime') or 'not supplied'
    rsi = num(report.get('spy_rsi') if report.get('spy_rsi') is not None else ai.get('spy_rsi'))
    lines = [
        ('Market', 'Regime <b>%s</b>; SPY RSI <b>%s</b>.' % (escape(str(regime)), '%.1f' % rsi if rsi is not None else 'not supplied')),
        ('Opportunities', '<b>%d</b> bullish (BOTTOM) and <b>%d</b> bearish (TOP) scanner setups. Top-ranked: <b>%s</b> (see section 1).'
         % (bullish, bearish, escape(str(ultimate.get('top_ranked') or 'none')))),
    ]
    if sectors:
        lines.append(('Sectors', 'Strongest: %s. Weakest: %s.' % (escape(sector_text(sectors[0])), escape(sector_text(sectors[-1])))))
    status = changes.get('status') or 'not supplied'
    events = len(changes.get('events') or [])
    change_text = {'FIRST_RUN': 'new comparison baseline started; changes appear from the next scan',
                   'NO_CHANGE': 'no changes since the prior scan',
                   'CHANGES': '%d change(s) since the prior scan (see section 3)' % events}.get(status, status)
    lines.append(('Changes', escape(change_text) + '.'))
    lines.append(('Data note', 'Equity signals use the latest completed session; crypto is timed separately. '
                  'Ranks are a sorting aid, not a buy signal or probability. Reconfirm before acting.'))
    rows = ''.join('<tr><td style="padding:6px 10px;font-weight:700;color:#0B2E4F;white-space:nowrap;vertical-align:top">%s</td>'
                   '<td style="padding:6px 10px">%s</td></tr>' % (escape(k), v) for k, v in lines)
    html = ('<div style="background:#E8F4FD;border-left:5px solid #1565C0;padding:10px;margin:12px 0;font:13px Segoe UI,Arial;color:#1F2937">'
            '<div style="font-weight:800;font-size:15px;color:#0B2E4F;margin-bottom:6px">Executive Summary</div>'
            '<table cellpadding="0" cellspacing="0" style="border-collapse:collapse">%s</table></div>' % rows)
    text = '\n'.join('%s: %s' % (k, v.replace('<b>', '').replace('</b>', '')) for k, v in lines)
    return html, text, regime


def build_message(html, summary_html, summary_text, subject, sender, recipients):
    marker = '<div style="padding:16px">'
    body = html.replace(marker, marker + summary_html, 1) if marker in html else summary_html + html
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ', '.join(recipients)
    msg.attach(MIMEText(summary_text + '\n\nOpen this email in an HTML-capable client to see the full dashboard.', 'plain', 'utf-8'))
    msg.attach(MIMEText(body, 'html', 'utf-8'))
    return msg


def send(msg, sender, password, recipients):
    server = os.getenv('SMTP_SERVER') or 'smtp.gmail.com'
    port = int(os.getenv('SMTP_PORT') or '587')
    context = ssl.create_default_context()
    if port == 465:
        with smtplib.SMTP_SSL(server, port, context=context, timeout=60) as smtp:
            smtp.login(sender, password)
            smtp.sendmail(sender, recipients, msg.as_string())
    else:
        with smtplib.SMTP(server, port, timeout=60) as smtp:
            smtp.starttls(context=context)
            smtp.login(sender, password)
            smtp.sendmail(sender, recipients, msg.as_string())


def main():
    now_utc = datetime.now(timezone.utc)
    now_et = now_utc.astimezone(ET)
    force = os.getenv('A1_FORCE_EMAIL', '').strip().lower() in ('1', 'true', 'yes')
    state = load('a1_email_state.json', {}) or {}
    ok, reason = decide(now_et, force, state)
    print('Email decision:', 'SEND' if ok else 'SKIP', '-', reason)
    if not ok:
        return 0
    sender = os.getenv('EMAIL_ADDRESS', '').strip()
    password = os.getenv('EMAIL_PASSWORD', '')
    recipients = [r.strip() for r in os.getenv('RECIPIENT_EMAILS', '').split(',') if r.strip()]
    missing = [n for n, v in [('EMAIL_ADDRESS', sender), ('EMAIL_PASSWORD', password), ('RECIPIENT_EMAILS', recipients)] if not v]
    if missing:
        raise SystemExit('Not sending: missing GitHub secret(s): ' + ', '.join(missing))
    audit, ultimate, html = check_report(now_utc)
    summary_html, summary_text, regime = summary(audit, ultimate)
    subject = 'A1 Market Dashboard - %s - %s - Top: %s' % (
        now_et.strftime('%a %b %d %I:%M %p ET'), regime, ultimate.get('top_ranked') or 'none')
    msg = build_message(html, summary_html, summary_text, subject, sender, recipients)
    send(msg, sender, password, recipients)
    # Record only after the mail server accepted the message.
    STATE.write_text(json.dumps({'last_sent_date': now_et.date().isoformat(),
                                 'sent_at': now_utc.isoformat(),
                                 'report_generated_at': audit.get('generated_at'),
                                 'recipients': len(recipients),
                                 'forced': force}, indent=2), encoding='utf-8')
    print('Email sent to %d recipient(s): %s' % (len(recipients), subject))
    return 0


if __name__ == '__main__':
    sys.exit(main())
