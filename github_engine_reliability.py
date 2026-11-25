"""GitHub-only engine reliability R1. Runtime adapters; original strategy source untouched.

Imported by run_a1 before run_github. No background workers or database resets.
"""
from collections import OrderedDict
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from functools import wraps, lru_cache
from html import escape
import ast
import hashlib
import inspect
import json
import logging
import math
from pathlib import Path
import ssl
import textwrap
import time
from zoneinfo import ZoneInfo

VERSION = 'github-engine-reliability-r1'
ET = ZoneInfo('America/New_York')
_REAL_SLEEP = time.sleep
_MONOTONIC = time.monotonic
LOGGER = logging.getLogger(__name__)


class ScanCache:
    """Bounded, opt-in per-scan cache; no disk writes, failures never cached."""
    def __init__(self, ttl=30, max_entries=32, clock=None):
        self.ttl = ttl
        self.max_entries = max_entries
        self.clock = clock or _MONOTONIC
        self.active = False
        self.items = OrderedDict()
        self.hits = self.misses = 0

    @contextmanager
    def scope(self):
        previous = self.active
        if not previous:
            self.items.clear()
            self.hits = self.misses = 0
        self.active = True
        try:
            yield self
        finally:
            self.active = previous
            if not previous:
                self.items.clear()

    def call(self, fn, *args, **kwargs):
        if not self.active:
            return fn(*args, **kwargs)
        try:
            key = json.dumps([args, kwargs], sort_keys=True, allow_nan=False)
        except (TypeError, ValueError):
            return fn(*args, **kwargs)
        now = self.clock()
        cached = self.items.get(key)
        if cached and now - cached[0] <= self.ttl:
            self.hits += 1
            self.items.move_to_end(key)
            return cached[1].copy(deep=True)
        self.misses += 1
        result = fn(*args, **kwargs)
        if result is not None and not result.empty:
            # All-NaN frames and frames without usable closes are not successes.
            try:
                valid = result['Close'].notna().to_numpy().any()
            except (KeyError, AttributeError):
                valid = False
            if valid:
                self.items[key] = (self.clock(), result.copy(deep=True))
                self.items.move_to_end(key)
                while len(self.items) > self.max_entries:
                    self.items.popitem(last=False)
        return result


@lru_cache(maxsize=8)
def sessions(day):
    """Authoritative installed exchange calendar, including holidays/early closes."""
    import pandas_market_calendars as mcal
    schedule = mcal.get_calendar('NYSE').schedule(
        start_date=day-timedelta(days=21), end_date=day)
    return [(str(index)[:10], row['market_open'].to_pydatetime(),
             row['market_close'].to_pydatetime())
            for index, row in schedule.iterrows()]


def assess_frame(frame, interval, now, limits, session_provider=None):
    """fresh means usable session observation, NOT an executable/live entry."""
    limit = limits.get(interval, 30)
    out = {'fresh':False, 'age_min':999999, 'latest_et':'N/A',
           'reason':'NO_VALID_TIMESTAMP', 'limit_min':limit,
           'live_eligible':False, 'calendar_basis':'NYSE calendar',
           'observation_kind':'UNAVAILABLE'}
    if now.tzinfo is None:
        out['reason'] = 'CURRENT_TIME_TIMEZONE_MISSING'
        return out
    now = now.astimezone(ET)
    try:
        import pandas as pd
        if frame is None or frame.empty:
            return out
        index = pd.DatetimeIndex(frame.index)
        if index.hasnans or not index.is_monotonic_increasing or index.has_duplicates:
            out['reason'] = 'INVALID_OR_UNSORTED_BAR_INDEX'
            return out
        latest = pd.Timestamp(index[-1])
        latest = latest.tz_localize(ET) if latest.tzinfo is None else latest.tz_convert(ET)
        latest = latest.to_pydatetime()
        out.update(latest_et=latest.strftime('%Y-%m-%d %I:%M:%S %p ET'),
                   bar_timestamp=latest.isoformat(),
                   age_min=round((now-latest).total_seconds()/60, 1))
        if latest > now + timedelta(minutes=2):
            out['reason'] = 'FUTURE_TIMESTAMP'
            return out
        calendar = (session_provider or sessions)(now.date())
        started = [r for r in calendar if r[1] <= now]
        if not started:
            out['reason'] = 'NO_EXPECTED_SESSION'
            return out
        day, opening, closing = started[-1]
        out['expected_session'] = day
        if latest.date().isoformat() != day:
            out['reason'] = 'MISSING_EXPECTED_SESSION'
            return out
        during = opening <= now <= closing
        if interval == '1d':
            out.update(fresh=True,
                       reason='CURRENT_SESSION_DAILY_PARTIAL' if during else 'LATEST_COMPLETED_SESSION_DAILY',
                       observation_kind='PARTIAL_DAILY_BAR' if during else 'LATEST_SESSION_DAILY_BAR')
            return out
        if latest < opening or latest > closing:
            out['reason'] = 'BAR_OUTSIDE_REGULAR_SESSION'
            return out
        reference = now if during else closing
        distance = (reference-latest).total_seconds()/60
        if distance > limit:
            out['reason'] = 'STALE_DURING_MARKET_HOURS' if during else 'INCOMPLETE_LAST_SESSION'
            return out
        out.update(fresh=True, live_eligible=during,
                   reason='CURRENT_SESSION_OBSERVATION' if during else 'LATEST_COMPLETED_SESSION_OBSERVATION',
                   observation_kind='INTRADAY_OBSERVATION' if during else 'HISTORICAL_SESSION_OBSERVATION')
        return out
    except Exception as error:
        out['reason'] = 'FRESHNESS_CHECK_FAILED'
        out['error'] = type(error).__name__ + ': ' + str(error)
        return out


def signed_change(row):
    try:
        value = float(row[14])
        if not math.isfinite(value):
            return 0.0
        return -value if row[3] == 'TOP' else value
    except (ValueError, TypeError, IndexError):
        return 0.0


class ReportCorrections(ast.NodeTransformer):
    """Surgical report-only AST edits; no strategy nodes are visited."""
    def __init__(self, kind):
        self.kind = kind
        self.changed = 0

    def visit_Compare(self, node):
        self.generic_visit(node)
        left = node.left
        if (self.kind == 'eod' and isinstance(left, ast.Subscript)
                and isinstance(left.value, ast.Name) and left.value.id == 'r'
                and isinstance(left.slice, ast.Constant) and left.slice.value == 14
                and any(isinstance(c, ast.Constant) and c.value in ('WIN','LOSS','PENDING') for c in node.comparators)):
            left.slice = ast.Constant(15)
            self.changed += 1
        return node

    def visit_Assign(self, node):
        self.generic_visit(node)
        names = [t.id for t in node.targets if isinstance(t, ast.Name)]
        if self.kind == 'eod' and any(n in ('gains','lvals') for n in names):
            if not isinstance(node.value, ast.ListComp):
                raise RuntimeError('Unexpected EOD gain/loss structure')
            node.value.elt = ast.Call(func=ast.Name(id='_r1_signed_change',ctx=ast.Load()),args=[ast.Name(id='r',ctx=ast.Load())],keywords=[])
            for condition in node.value.generators[0].ifs:
                for item in ast.walk(condition):
                    if (isinstance(item,ast.Subscript) and isinstance(item.value,ast.Name)
                            and item.value.id=='r' and isinstance(item.slice,ast.Constant)
                            and item.slice.value==13):
                        item.slice = ast.Constant(14)
            self.changed += 1
        if self.kind == 'weekly' and any(n in ('best','worst') for n in names):
            for item in ast.walk(node.value):
                if isinstance(item,ast.Lambda):
                    item.body = ast.Call(func=ast.Name(id='_r1_signed_change',ctx=ast.Load()),args=[ast.Name(id=item.args.args[0].arg,ctx=ast.Load())],keywords=[])
                    self.changed += 1
        if self.kind == 'weekly' and 'strong' in names:
            for item in ast.walk(node.value):
                if isinstance(item,ast.Compare) and isinstance(item.left,ast.Subscript):
                    left = item.left
                    if isinstance(left.value,ast.Name) and left.value.id=='r' and isinstance(left.slice,ast.Constant) and left.slice.value==14:
                        item.left = ast.Call(func=ast.Name(id='_r1_signed_change',ctx=ast.Load()),args=[ast.Name(id='r',ctx=ast.Load())],keywords=[])
                        self.changed += 1
        return node


def correct_report(fn, kind):
    source = textwrap.dedent(inspect.getsource(fn))
    tree = ast.parse(source)
    transformer = ReportCorrections(kind)
    tree = transformer.visit(tree)
    ast.fix_missing_locations(tree)
    expected = 7 if kind == 'eod' else 3
    if transformer.changed != expected:
        raise RuntimeError('Unexpected '+kind+' report version; refusing partial correction ('+str(transformer.changed)+')')
    namespace = dict(fn.__globals__)
    namespace['_r1_signed_change'] = signed_change
    exec(compile(tree, fn.__code__.co_filename, 'exec'), namespace)
    fixed = namespace[fn.__name__]
    @wraps(fn)
    def wrapped(*args, **kwargs):
        # Refresh globals so later runner adapters and test stubs are respected.
        for name,value in fn.__globals__.items():
            if name != fn.__name__:
                namespace[name] = value
        result = fixed(*args, **kwargs)
        note = '<p>Recorded outcomes are directional price comparisons, not validated stop/target trades. No costs, fills or probabilities are established. Weekly rankings use direction-adjusted change; table Change remains raw price change.</p>'
        return result.replace('</body>', note+'</body>')
    return wrapped


def bounded_email(engine, subject, html_content):
    if not engine.EMAIL_ADDRESS or not engine.EMAIL_PASSWORD or not engine.RECIPIENT_EMAIL:
        engine.logger.warning('Legacy engine email not configured')
        return False
    pending = list(dict.fromkeys(engine.RECIPIENT_EMAIL))
    for attempt in range(1,4):
        try:
            with engine.smtplib.SMTP(engine.SMTP_SERVER,587,timeout=30) as server:
                server.starttls(context=ssl.create_default_context())
                server.login(engine.EMAIL_ADDRESS,engine.EMAIL_PASSWORD)
                for recipient in list(pending):
                    msg = engine.MIMEMultipart('alternative')
                    msg['Subject'] = subject
                    msg['From'] = engine.EMAIL_ADDRESS
                    msg['To'] = recipient
                    msg.attach(engine.MIMEText(html_content,'html','utf-8'))
                    refused = server.sendmail(engine.EMAIL_ADDRESS,[recipient],msg.as_string())
                    if refused:
                        raise RuntimeError('SMTP recipient refused')
                    pending.remove(recipient)
            if not pending:
                return True
        except Exception as error:
            # Do not log recipient addresses, credentials or provider error text.
            engine.logger.warning('Legacy email attempt %d/3 failed (%s); %d pending',attempt,type(error).__name__,len(pending))
            if attempt < 3:
                _REAL_SLEEP(2 ** attempt)
    engine.logger.error('Legacy email exhausted retries; %d recipients not confirmed',len(pending))
    return False


def archive_before_delete(engine):
    """One SQLite transaction; retain complete row JSON before any deletion."""
    cutoff = (engine.get_eastern_now()-timedelta(days=engine.RETENTION_DAYS)).strftime('%Y-%m-%d')
    conn = engine.get_db_connection()
    try:
        with conn:
            conn.execute('BEGIN IMMEDIATE')
            cursor = conn.execute('SELECT * FROM alerts WHERE date < ?', (cutoff,))
            columns = [c[0] for c in cursor.description]
            rows = cursor.fetchall()
            if not rows:
                return 0
            if 'id' not in columns:
                raise RuntimeError('Alerts primary identifier unavailable; no rows deleted')
            conn.execute('CREATE TABLE IF NOT EXISTS engine_retained_alerts (digest TEXT PRIMARY KEY, payload TEXT NOT NULL, archived_at TEXT NOT NULL)')
            stamp = datetime.now(timezone.utc).isoformat()
            for row in rows:
                payload = json.dumps(dict(zip(columns,row)),sort_keys=True,allow_nan=False)
                digest = hashlib.sha256(payload.encode()).hexdigest()
                conn.execute('INSERT OR IGNORE INTO engine_retained_alerts VALUES (?,?,?)',(digest,payload,stamp))
                saved = conn.execute('SELECT payload FROM engine_retained_alerts WHERE digest=?',(digest,)).fetchone()
                if not saved or saved[0] != payload:
                    raise RuntimeError('Archive verification failed; transaction rolled back')
            id_index = columns.index('id')
            conn.executemany('DELETE FROM alerts WHERE id=?',[(r[id_index],) for r in rows])
        return len(rows)
    finally:
        conn.close()


def install(engine=None):
    if engine is None:
        import market_scanner_v5 as engine
    if getattr(engine,'_github_reliability_r1',False):
        return engine
    required = ['safe_yf_download','get_data_freshness','download_with_freshness',
                'build_eod_report','build_weekly_report','send_email','cleanup_old_data']
    for name in required:
        if not callable(getattr(engine,name,None)):
            raise RuntimeError('Engine interface missing: '+name)
    # Prepare all replacements before changing any module binding.
    eod = correct_report(engine.build_eod_report,'eod')
    weekly = correct_report(engine.build_weekly_report,'weekly')
    cache = ScanCache()
    original_download = engine.safe_yf_download
    original_validated = engine.download_with_freshness
    @wraps(original_download)
    def download(*args,**kwargs):
        return cache.call(original_download,*args,**kwargs)
    def freshness(frame,interval='5m'):
        return assess_frame(frame,interval,engine.get_eastern_now(),engine.STALE_LIMIT_MINUTES)
    @wraps(original_validated)
    def validated(*args,**kwargs):
        frame,meta = original_validated(*args,**kwargs)
        interval = kwargs.get('interval',args[2] if len(args)>2 else '5m')
        state = freshness(frame,interval)
        meta = dict(meta)
        meta['provider_reason'] = meta.get('reason')
        meta.update(state)
        if frame is not None and not state['fresh']:
            # The legacy function accepts any nonempty daily frame before trying
            # its fallback. Give that fallback one chance under the same policy.
            fallback = getattr(engine,'yahoo_chart_download',None)
            allow = kwargs.get('allow_fallback',args[4] if len(args)>4 else True)
            if interval == '1d' and allow and callable(fallback) and not meta.get('fallback_used'):
                ticker = kwargs.get('ticker',args[0] if args else None)
                period = kwargs.get('period',args[1] if len(args)>1 else '5d')
                backup = fallback(ticker,period=period,interval=interval)
                backup_state = freshness(backup,interval)
                if backup is not None and backup_state['fresh']:
                    meta.update(backup_state)
                    meta.update(source='YahooChartDirect',fallback_used=True)
                    return backup,meta
            engine.logger.warning('R1 rejected unusable %s history: %s',interval,state['reason'])
            return None,meta
        return frame,meta
    engine.safe_yf_download = download
    engine.get_data_freshness = freshness
    engine.download_with_freshness = validated
    engine.build_eod_report = eod
    engine.build_weekly_report = weekly
    engine.send_email = lambda subject,html_content: bounded_email(engine,subject,html_content)
    engine.cleanup_old_data = lambda: archive_before_delete(engine)
    engine._github_r1_cache = cache
    engine._github_reliability_r1 = True
    engine.logger.info('GitHub reliability R1 installed; strategy formulas/thresholds unchanged')
    return engine


def attach_runner(runner,engine):
    if getattr(runner,'_github_reliability_r1',False):
        return
    original_scan = runner.run_single_scan
    @wraps(original_scan)
    def scan(*args,**kwargs):
        started = _MONOTONIC()
        with engine._github_r1_cache.scope() as cache:
            result = original_scan(*args,**kwargs)
            alerts,regime,rsi,summary = result
            summary['engine_reliability'] = {
                'version':VERSION,'cache_hits':cache.hits,'download_cache_misses':cache.misses,
                'cache_ttl_seconds':cache.ttl,'scan_seconds':round(_MONOTONIC()-started,2),
                'strategy_thresholds_changed':False,
                'confirmation_policy':'LEGACY_UNCHANGED',
                'vwap_policy':'LEGACY_CUMULATIVE_UNCHANGED',
                'outcome_policy':'LEGACY_DIRECTIONAL_NOT_TRADE_VALIDATED',
                'actual_rqg_min_score':getattr(engine,'RQG_MIN_SCORE',None),
                'actual_confirmation_calls':getattr(engine,'MIN_5MIN_CONFIRMATIONS',None)}
            now = engine.get_eastern_now()
            try:
                rows = sessions(now.astimezone(ET).date())
                current = any(opening <= now <= closing for _,opening,closing in rows)
                context = 'CURRENT_SESSION_RESEARCH' if current else 'HISTORICAL_RESEARCH_NOT_ENTRY_READY'
            except Exception:
                context = 'SESSION_CONTEXT_UNVERIFIED'
            summary['report_context'] = context
            for alert in alerts:
                alert['report_context'] = context
                alert['engine_version'] = VERSION
                alert['outcome_measurement'] = 'DIRECTIONAL_OBSERVATION_NOT_VALIDATED_TRADE'
            runner.logger.info('R1 scan complete: %d cache hits; context=%s',cache.hits,context)
            return alerts,regime,rsi,summary
    original_fetch = runner.fetch_fresh_ohlcv
    @wraps(original_fetch)
    def fetch(ticker,period='5d',interval='5m'):
        frame,meta = original_fetch(ticker,period,interval)
        state = engine.get_data_freshness(frame,interval)
        meta = dict(meta)
        meta.update(state)
        return frame,meta
    runner.run_single_scan = scan
    runner.fetch_fresh_ohlcv = fetch
    runner._github_reliability_r1 = True


def atomic_json(path, value):
    import os
    import tempfile
    path = Path(path)
    path.parent.mkdir(parents=True,exist_ok=True)
    fd,name = tempfile.mkstemp(prefix=path.name+'.',suffix='.tmp',dir=str(path.parent))
    try:
        with os.fdopen(fd,'w',encoding='utf-8') as stream:
            json.dump(value,stream,indent=1,allow_nan=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name,path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def attach_history_retention(runner):
    if getattr(runner,'_r1_history_retention',False):
        return
    original = runner.save_history
    @wraps(original)
    def save(history):
        # Match the original runner's 30-day cutoff exactly; only add an archive.
        cutoff = (datetime.now()-timedelta(days=30)).strftime('%Y-%m-%d')
        expired = [r for r in history or [] if r.get('date','') < cutoff]
        if expired:
            path = Path(runner.script_dir)/'docs'/'scan_history_archive.json'
            existing = json.loads(path.read_text(encoding='utf-8')) if path.exists() else []
            if not isinstance(existing,list):
                raise ValueError('History archive is not a list; refusing retention trim')
            dedup = {}
            for row in existing+expired:
                payload = json.dumps(row,sort_keys=True,allow_nan=False)
                dedup[hashlib.sha256(payload.encode()).hexdigest()] = row
            atomic_json(path,list(dedup.values()))
        return original(history)
    runner.save_history = save
    runner._r1_history_retention = True
