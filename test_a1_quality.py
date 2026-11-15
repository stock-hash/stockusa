"""Offline tests. All prices and ticker fixtures are synthetic, not market data."""
import unittest,json,tempfile,ast,types,math,logging
from pathlib import Path
from datetime import datetime,timezone
from unittest.mock import patch
import pandas as pd
import numpy as np
import a1_quality as q
import a1_market_intelligence as a1
ROOT=Path(__file__).parent
NOW=datetime.fromisoformat('2026-11-15T12:05:00+00:00')

class QualityTests(unittest.TestCase):
 def test_et_timestamp(self):
  self.assertEqual(q.parse_ts('2026-11-13 03:55:00 PM ET').isoformat(),'2026-11-13T15:55:00-05:00')
 def test_naive_not_guessed(self):self.assertIsNone(q.parse_ts('2026-11-13 15:55:00'))
 def test_weekend_latest(self):self.assertEqual(q.freshness('2026-11-13T15:55:00-05:00',NOW)[0],'LATEST SESSION - NEAR-CLOSE BAR')
 def test_weekend_earlier(self):self.assertEqual(q.freshness('2026-11-13T11:00:00-05:00',NOW)[0],'LATEST SESSION - EARLIER BAR')
 def test_missed_session(self):self.assertIn('MISSED SESSION',q.freshness('2026-11-12T15:55:00-05:00',NOW)[0])
 def test_open_stale(self):self.assertIn('STALE',q.freshness('2026-11-13T09:30:00-05:00',datetime.fromisoformat('2026-11-13T11:00:00-05:00'))[0])
 def test_crypto_not_weekend_exempt(self):self.assertEqual(q.freshness('2026-11-13T15:55:00-05:00',NOW,'crypto')[0],'STALE CRYPTO BAR')
 def test_early_close(self):self.assertIn('NEAR-CLOSE',q.freshness('2026-11-27T12:55:00-05:00',datetime.fromisoformat('2026-11-28T12:00:00-05:00'))[0])
 def test_holiday(self):self.assertIn('LATEST SESSION',q.freshness('2026-11-25T15:55:00-05:00',datetime.fromisoformat('2026-11-26T12:00:00-05:00'))[0])
 def test_future(self):self.assertEqual(q.freshness('2026-11-16T12:00:00-05:00',NOW)[0],'FUTURE TIMESTAMP')
 def test_pp(self):self.assertEqual(q.fmt(1.25,'change_pp'),'+1.25 pp')
 def test_zero_pp(self):self.assertEqual(q.fmt(0,'relative_pp'),'+0.00 pp')
 def test_index_units(self):self.assertNotIn('$',q.fmt(19.2,'index_value'))
 def test_daily_not_stale(self):
  r={'status':'HISTORICAL_RESEARCH_NOT_ENTRY_READY','daily_status':'LATEST COMPLETED SESSION DAILY BAR'}
  self.assertEqual(q.classify_entry(r),'DAILY RESEARCH - INTRADAY CHECK NEEDED')
 def test_overextended(self):self.assertIn('OVEREXTENDED',q.classify_entry({'extension_atr':2.5}))
 def test_scan_time_not_bar_time(self):self.assertIsNone(q.signal_timestamp({'time':'2026-11-13 03:55:00 PM ET'}))
 def test_nested_timestamp(self):self.assertIsNotNone(q.signal_timestamp({'rqg_details':{'data_meta':{'latest':'2026-11-13 03:55:00 PM ET'}}}))
 def test_daily_fallback(self):
  r=q.asset_row('DIA','Equity',{}, {'price':100,'bar_timestamp':'2026-11-13 00:00:00','return_20d':3},NOW)
  self.assertEqual(r['price'],100);self.assertIn('DAILY',r['basis']);self.assertEqual(r['timestamp'],'2026-11-13');self.assertIsNone(r['age_min'])
 def test_column_orders(self):
  idx=pd.date_range('2026-11-13',periods=2,tz='UTC')
  d=pd.DataFrame([[1,2],[3,4]],index=idx,columns=pd.MultiIndex.from_tuples([('Close','AAA'),('Close','BBB')]))
  self.assertEqual(q.frame_for(d,'AAA')['Close'].iloc[-1],3)
  self.assertEqual(q.frame_for(d.swaplevel(axis=1),'BBB')['Close'].iloc[-1],4)
 def test_reference_counts(self):
  cfg=json.loads((ROOT/'a1_reference_universes.json').read_text());self.assertEqual(len(cfg['groups']['Dow30']),30);self.assertEqual(len(cfg['groups']['Nasdaq100']),101);self.assertIsNone(cfg['membership_verified_as_of'])
 def test_html_and_baseline(self):
  with tempfile.TemporaryDirectory() as td:
   d=Path(td)
   report={'regime':'BULLISH','spy_rsi':60,'alerts':[{'ticker':'AAA','signal':'BOTTOM','rqg_label':'VALID_REVERSAL','rqg_details':{'latest':'2026-11-13 03:55:00 PM ET'}}],'summary':{'sectors':[{'sector':t,'strength':'STRONG','diff':0.2,'latest':'2026-11-13 03:55:00 PM ET'} for t in a1.SECTORS],'blocked_alerts':[]},'cross_assets':{t:{'price':100,'market_timestamp':'2026-11-13T15:55:00-05:00'} for group in a1.CROSS_ASSET_GROUPS.values() for t in group},'options':{}}
   snap={'daily_features':[{'ticker':'AAA','price':100,'panel':'stock_or_other','improving':True,'higher_low':True,'technical_score':80,'bar_timestamp':'2026-11-13','status':'HISTORICAL_RESEARCH_NOT_ENTRY_READY'}],'score_changes':[],'failures':[]}
   q.json_write(d/'report_scan.json',report);q.json_write(d/'snapshot_full.json',snap)
   q.json_write(d/'a1_universe_resolved.json',{'groups':{'Dow30':['AAA'],'Nasdaq100':['AAA']},'group_metadata':{}})
   result=a1.build(d,NOW);self.assertTrue(result['publish_ok']);self.assertEqual(result['alerts_rendered'],1)
   html=(d/'a1_tables.html').read_text();self.assertIn('No prior baseline',html);self.assertNotIn('NEW LEADERSHIP',html);self.assertIn('dated reference universe',html)
   self.assertLessEqual((d/'a1_ai.json').stat().st_size,26000)
   self.assertEqual(json.loads((d/'a1_previous.json').read_text())['sectors']['XLK'],.2)
 def test_failed_build_preserves_baseline(self):
  with tempfile.TemporaryDirectory() as td:
   d=Path(td);q.json_write(d/'report_scan.json',{'summary':{'sectors':[]}});q.json_write(d/'snapshot_full.json',{})
   q.json_write(d/'a1_previous.json',{'sectors':{'XLK':9}})
   with self.assertRaises(RuntimeError):a1.build(d,NOW)
   self.assertEqual(json.loads((d/'a1_previous.json').read_text())['sectors']['XLK'],9)
 def test_pcr_one_not_fallback(self):
  src=(ROOT/'run_github.py').read_text();tree=ast.parse(src)
  names={'safe_int','safe_float','fetch_options_data'}
  mod=ast.Module(body=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in names],type_ignores=[])
  calls=pd.DataFrame({'strike':[99.,101.],'bid':[1.,1.],'ask':[2.,2.],'volume':[10.,10.],'openInterest':[100,100],'impliedVolatility':[.3,.3]})
  class Ticker:
   options=['2026-12-01']
   def history(self,**kw):return pd.DataFrame({'Close':[100.]})
   def option_chain(self,exp):return types.SimpleNamespace(calls=calls,puts=calls.copy())
  ns={'pd':pd,'np':np,'math':math,'datetime':datetime,'yf':types.SimpleNamespace(Ticker=lambda t:Ticker()),'_HAS_PATCH':False,'logger':logging.getLogger('test')}
  exec(compile(mod,'options','exec'),ns)
  r=ns['fetch_options_data']('AAA');self.assertEqual(r['pcr'],1.0);self.assertEqual(r['pcr_status'],'CHAIN_VOLUME_RATIO_SELECTED_EXPIRATION')
  calls['volume']=float('nan');r=ns['fetch_options_data']('AAA');self.assertIsNone(r['pcr'])
if __name__=='__main__':unittest.main(verbosity=2)
