"""Runs original scanner, existing snapshot, then restores full cross-asset report coverage."""
import run_github
from snapshot_upgrade import run as build_snapshot
from full_market_overlay import build as build_full_market
if __name__ == '__main__':
    result=run_github.main()
    if result not in (0,None): raise SystemExit(result)
    build_snapshot(vars(run_github))
    build_full_market(run_github.os.path.join(run_github.script_dir,'docs'))
