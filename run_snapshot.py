"""Use only after reporting-enabled run_github.py is installed."""
import run_github
from snapshot_upgrade import run
if __name__ == '__main__':
    result=run_github.main()
    if result not in (None,0): raise SystemExit(result)
    run(vars(run_github))
