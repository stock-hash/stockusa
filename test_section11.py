import unittest,tempfile,json
from pathlib import Path
from a1_changes import *
class Tests(unittest.TestCase):
 def state(self,score=80,price=100,alerts=None,time='2026-11-15T12:00:00+00:00'):
  return snapshot({'scan_time':'source','alerts':alerts or []},{'daily_features':[{'ticker':'AAA','technical_score':score,'price':price,'bar_timestamp':'2026-11-13','improving':True}]},time)
 def test_first(self):self.assertEqual(compare(None,self.state(),'FIRST_RUN')['status'],'FIRST_RUN')
 def test_no_change(self):self.assertEqual(compare(self.state(),self.state(time='2026-11-15T13:00:00+00:00'))['status'],'NO_CHANGE')
 def test_score_and_price(self):
  r=compare(self.state(),self.state(100,102,time='2026-11-15T13:00:00+00:00'));self.assertEqual(len(r['events']),2);self.assertEqual(r['events'][0]['change'],20)
 def test_alert_new(self):
  r=compare(self.state(),self.state(alerts=[{'ticker':'AAA','signal':'BOTTOM','setup_type':'REVERSAL'}],time='2026-11-15T13:00:00+00:00'));self.assertEqual(r['events'][0]['event'],'NEW SCANNER ALERT')
 def test_alert_missing_not_sell(self):
  r=compare(self.state(alerts=[{'ticker':'AAA','signal':'TOP'}]),self.state(time='2026-11-15T13:00:00+00:00'));self.assertIn('Not proof',r['events'][0]['note'])
 def test_corrupt(self):
  with tempfile.TemporaryDirectory() as d:
   p=Path(d)/'x';p.write_text('{');self.assertEqual(read_baseline(p)[1],'BASELINE_ERROR')
 def test_order(self):self.assertEqual(compare(self.state(),self.state())['status'],'OUT_OF_ORDER')
 def test_old_daily(self):
  a=self.state();b=self.state(100,102,time='2026-11-15T13:00:00+00:00');b['daily']['AAA']['bar_timestamp']='2026-11-12';r=compare(a,b);self.assertEqual(r['events'][0]['event'],'OLDER DATA RETURNED')
 def test_html_escape(self):
  r=compare(None,self.state(),'FIRST_RUN','<bad>');self.assertIn('&lt;bad&gt;',render(r))
 def test_no_zero_as_missing(self):
  r=compare(self.state(0),self.state(20,time='2026-11-15T13:00:00+00:00'));self.assertEqual(r['events'][0]['change'],20)
 def test_integration(self):
  with tempfile.TemporaryDirectory() as d:
   p=Path(d)
   def setup(time):
    atomic(p/'a1_audit.json',{'publish_ok':True,'generated_at':time});atomic(p/'report_scan.json',{'alerts':[]});atomic(p/'snapshot_full.json',{'daily_features':[]});atomic(p/'a1_ai.json',{});(p/'a1_tables.html').write_text('<h2>11. Changes since prior scan</h2><table></table>')
   setup('2026-11-15T12:00:00+00:00');r=finish(p,read_baseline(p/'a1_comparison_baseline.json'));self.assertEqual(r['status'],'FIRST_RUN')
   setup('2026-11-15T13:00:00+00:00');r=finish(p,read_baseline(p/'a1_comparison_baseline.json'));self.assertEqual(r['status'],'NO_CHANGE')
 def test_failed_audit_preserves_baseline(self):
  with tempfile.TemporaryDirectory() as d:
   p=Path(d);atomic(p/'a1_audit.json',{'publish_ok':False});atomic(p/'a1_comparison_baseline.json',{'keep':1})
   with self.assertRaises(RuntimeError):finish(p,(None,'FIRST_RUN',''))
   self.assertEqual(json.loads((p/'a1_comparison_baseline.json').read_text()),{'keep':1})
if __name__=='__main__':unittest.main(verbosity=2)
