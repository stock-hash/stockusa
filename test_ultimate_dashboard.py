import unittest,tempfile,json
from pathlib import Path
from unittest.mock import patch
import a1_ultimate_dashboard as u
class Tests(unittest.TestCase):
 def fixture(self,p):
  def w(n,v):(p/n).write_text(json.dumps(v))
  base='<html><body><h2>Legacy Complete Evidence</h2><table><tr><td>KEEP</td></tr></table></body></html>'
  (p/'a1_tables.html').write_text(base)
  w('a1_audit.json',{'publish_ok':True,'generated_at':'2026-11-15T15:00:00+00:00'})
  sectors=[{'sector':t,'strength':'NEUTRAL','diff':0,'change_pp':0,'stage':'NEUTRAL','latest':'time'} for t in u.SECTOR_ORDER]
  w('report_scan.json',{'regime':'BULLISH','spy_rsi':60,'scan_time':'scan','alerts':[{'ticker':'AAA','signal':'BOTTOM','rqg_score':60}],'summary':{'sectors':sectors,'blocked_alerts':[]},'cross_assets':{},'options':{}})
  w('snapshot_full.json',{'daily_features':[{'ticker':'AAA','panel':'stock_or_other','improving':True,'higher_low':True,'technical_score':80}],'failures':[]})
  w('a1_changes.json',{'status':'NO_CHANGE','events':[]})
  return base
 def test_preserves_and_builds(self):
  with tempfile.TemporaryDirectory() as d:
   p=Path(d);old=self.fixture(p)
   with patch.object(u,'history_fetch',return_value=({'AAA':{'closes':[1,2,3],'dates':['1','2','3']}},{})):r=u.build(p)
   html=(p/'a1_tables.html').read_text();self.assertIn('A1 PROFESSIONAL MARKET DASHBOARD',html);self.assertIn('KEEP',html);self.assertGreaterEqual(r['ultimate_html_bytes'],r['prior_html_bytes']);self.assertTrue(r['all_prior_html_retained'])
 def test_failed_audit_preserves(self):
  with tempfile.TemporaryDirectory() as d:
   p=Path(d);old=self.fixture(p);(p/'a1_audit.json').write_text('{"publish_ok":false}')
   with self.assertRaises(RuntimeError):u.build(p)
   self.assertEqual((p/'a1_tables.html').read_text(),old)
 def test_idempotent(self):
  with tempfile.TemporaryDirectory() as d:
   p=Path(d);self.fixture(p)
   with patch.object(u,'history_fetch',return_value=({},{})):u.build(p)
   self.assertEqual(u.build(p)['status'],'ALREADY_BUILT')
 def test_trend(self):self.assertIn('+200.00%',u.trend_strip({'closes':[1,2,3]}))
if __name__=='__main__':unittest.main(verbosity=2)
