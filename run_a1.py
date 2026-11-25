#!/usr/bin/env python3
"""A1 1.1 coordinated correction plus persistent Section 11 comparisons.
GitHub reliability R1 integration. Existing layout installers remain required.
"""
import os,json
from pathlib import Path
from github_engine_reliability import install,attach_runner,attach_history_retention
_engine_r1=install()
import run_github
attach_runner(run_github,_engine_r1)
attach_history_retention(run_github)
from snapshot_upgrade import run as build_snapshot
from a1_market_intelligence import build,CROSS_ASSET_GROUPS
from a1_quality import resolve_universes,json_write
from a1_changes import read_baseline,finish

def main():
    root=Path(run_github.script_dir);(root/'docs').mkdir(exist_ok=True)
    result=run_github.main()
    if result not in (None,0):return result
    cfg=resolve_universes(root)
    extra={t for ts in cfg['groups'].values() for t in ts}|{t for ts in CROSS_ASSET_GROUPS.values() for t in ts}
    namespace=dict(vars(run_github));namespace['ALL_STOCKS']=sorted(set(run_github.ALL_STOCKS)|extra)
    old_cap=os.environ.get('SNAPSHOT_MAX_TICKERS')
    if old_cap is None:os.environ['SNAPSHOT_MAX_TICKERS']=str(max(500,len(namespace['ALL_STOCKS'])))
    try:build_snapshot(namespace)
    finally:
        if old_cap is None:os.environ.pop('SNAPSHOT_MAX_TICKERS',None)
    p=root/'docs'/'snapshot_full.json';snap=json.loads(p.read_text())
    for row in snap.get('daily_features',[]):row['groups']=[g for g,ts in cfg['groups'].items() if row.get('ticker') in ts]
    json_write(p,snap)
    baseline=read_baseline(root/'docs'/'a1_comparison_baseline.json')
    build(root/'docs')
    finish(root/'docs',baseline)
    return 0
if __name__=='__main__':raise SystemExit(main())
