"""Checked, idempotent integration into existing Section 11 run_a1.py."""
from pathlib import Path
import ast,shutil
p=Path('run_a1.py');s=p.read_text(encoding='utf-8')
if '# A1_LAYOUT_RESTORE_V1' in s:print('Presentation restoration already installed');raise SystemExit(0)
needle="    finish(root/'docs',baseline)"
if s.count(needle)!=1:raise SystemExit('Expected Section 11 runner not found. No files changed; use the installed run_a1.py for a compatible patch.')
s=s.replace(needle,needle+"\n    # A1_LAYOUT_RESTORE_V1\n    from a1_restore_layout import restore\n    restore(root/'docs')")
ast.parse(s)
backup=p.with_name('run_a1.py.before_layout')
if not backup.exists():shutil.copy2(p,backup)
p.write_text(s,encoding='utf-8');print('Installed presentation restoration after Section 11')
