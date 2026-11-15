"""Checked, idempotent integration after A1 layout/Section 11 generation."""
from pathlib import Path
import ast,shutil
p=Path('run_a1.py');s=p.read_text(encoding='utf-8')
if '# A1_ULTIMATE_DASHBOARD_V1' in s:print('Ultimate dashboard already installed');raise SystemExit(0)
# Prefer placement after layout restore; fall back after section 11 finish.
needle="    restore(root/'docs')" if "    restore(root/'docs')" in s else "    finish(root/'docs',baseline)"
if s.count(needle)!=1:raise SystemExit('Compatible A1 runner marker not found. No files changed.')
s=s.replace(needle,needle+"\n    # A1_ULTIMATE_DASHBOARD_V1\n    from a1_ultimate_dashboard import build as build_ultimate_dashboard\n    build_ultimate_dashboard(root/'docs')")
ast.parse(s)
backup=p.with_name('run_a1.py.before_ultimate')
if not backup.exists():shutil.copy2(p,backup)
p.write_text(s,encoding='utf-8');print('Ultimate dashboard installed after validated A1 report')
