"""Offline tests for a1_email_sender. No real email is sent."""
import json, os, tempfile, unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
import a1_email_sender as s

ET = s.ET


def write_docs(root, generated):
    docs = root / 'docs'
    docs.mkdir()
    (docs / 'a1_audit.json').write_text(json.dumps({'publish_ok': True, 'generated_at': generated}))
    (docs / 'a1_ultimate_audit.json').write_text(json.dumps({'all_prior_html_retained': True, 'top_ranked': 'AAA'}))
    (docs / 'a1_tables.html').write_text('<html><body><div style="padding:16px">A1 PROFESSIONAL MARKET DASHBOARD' + 'x' * 6000 + '</div></body></html>')
    (docs / 'report_scan.json').write_text(json.dumps({'regime': 'BULLISH', 'spy_rsi': 65.1,
        'alerts': [{'signal': 'BOTTOM'}, {'signal': 'BOTTOM'}, {'signal': 'TOP'}]}))
    (docs / 'a1_ai.json').write_text(json.dumps({'sectors': [{'ticker': 'XLE', 'relative_pp': 0.154}, {'ticker': 'XLB', 'relative_pp': -0.368}]}))
    (docs / 'a1_changes.json').write_text(json.dumps({'status': 'CHANGES', 'events': [1, 2]}))


class Tests(unittest.TestCase):
    def test_before_send_time(self):
        now = datetime(2026, 11, 16, 13, 50, tzinfo=ET)
        self.assertFalse(s.decide(now, False, {})[0])

    def test_after_send_time(self):
        now = datetime(2026, 11, 16, 14, 5, tzinfo=ET)
        self.assertTrue(s.decide(now, False, {})[0])

    def test_once_per_day(self):
        now = datetime(2026, 11, 16, 15, 50, tzinfo=ET)
        self.assertFalse(s.decide(now, False, {'last_sent_date': '2026-11-16'})[0])

    def test_weekend_skipped(self):
        now = datetime(2026, 11, 15, 14, 20, tzinfo=ET)
        self.assertFalse(s.decide(now, False, {})[0])

    def test_force(self):
        now = datetime(2026, 11, 15, 9, 0, tzinfo=ET)
        self.assertTrue(s.decide(now, True, {'last_sent_date': '2026-11-15'})[0])

    def test_early_close_uses_close(self):
        now = datetime(2026, 11, 27, 13, 5, tzinfo=ET)   # day after Thanksgiving, 1 PM close
        self.assertTrue(s.decide(now, False, {})[0])

    def test_full_send_records_state_and_keeps_units(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            now = datetime.now(timezone.utc)
            write_docs(root, now.isoformat())
            sent = {}
            cwd = os.getcwd()
            os.chdir(root)
            try:
                env = {'A1_FORCE_EMAIL': 'true', 'EMAIL_ADDRESS': 'a@x.com', 'EMAIL_PASSWORD': 'p', 'RECIPIENT_EMAILS': 'b@x.com, c@x.com'}
                with patch.dict(os.environ, env), patch.object(s, 'send', side_effect=lambda m, *a: sent.update(msg=''.join(part.get_payload(decode=True).decode('utf-8') for part in m.walk() if part.get_content_maintype()=='text'))):
                    self.assertEqual(s.main(), 0)
                state = json.loads(Path('docs/a1_email_state.json').read_text())
                self.assertEqual(state['recipients'], 2)
            finally:
                os.chdir(cwd)
            self.assertIn('Executive Summary', sent['msg'])
            self.assertIn('+0.15 pp', sent['msg'])        # never multiplied by 100
            self.assertNotIn('15.4', sent['msg'])

    def test_refuses_old_report(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            write_docs(root, '2020-01-01T00:00:00+00:00')
            cwd = os.getcwd()
            os.chdir(root)
            try:
                env = {'A1_FORCE_EMAIL': 'true', 'EMAIL_ADDRESS': 'a@x.com', 'EMAIL_PASSWORD': 'p', 'RECIPIENT_EMAILS': 'b@x.com'}
                with patch.dict(os.environ, env), patch.object(s, 'send') as fake:
                    with self.assertRaises(SystemExit):
                        s.main()
                    fake.assert_not_called()
                (Path('docs/a1_email_state.json').exists())
            finally:
                os.chdir(cwd)

    def test_missing_secret(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            write_docs(root, datetime.now(timezone.utc).isoformat())
            cwd = os.getcwd()
            os.chdir(root)
            try:
                with patch.dict(os.environ, {'A1_FORCE_EMAIL': 'true', 'EMAIL_ADDRESS': '', 'EMAIL_PASSWORD': '', 'RECIPIENT_EMAILS': ''}):
                    with self.assertRaises(SystemExit):
                        s.main()
            finally:
                os.chdir(cwd)


if __name__ == '__main__':
    unittest.main(verbosity=2)
