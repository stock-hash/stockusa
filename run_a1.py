#!/usr/bin/env python3
"""Run existing scanner and snapshot, then A1 reporting. No signal thresholds changed."""
import os
import run_github
from snapshot_upgrade import run as build_snapshot
from a1_market_intelligence import build as build_a1
if __name__=='__main__':
    rc=run_github.main()
    if rc not in (0,None):raise SystemExit(rc)
    build_snapshot(vars(run_github))
    build_a1(os.path.join(run_github.script_dir,'docs'))
