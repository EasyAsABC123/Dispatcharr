"""Exact fixed dispatch: default read-only audit OR one new TS cohort."""
import os,runpy
from pathlib import Path
root=Path(__file__).resolve().parent
stage=os.environ.get('SPECIAL_STAGE')
if stage=='audit':runpy.run_path(str(root/'default_audit.py'),run_name='__main__')
elif stage=='profile':runpy.run_path(str(root/'profile_fixture.py'),run_name='__main__')
elif stage=='exercise':runpy.run_path(str(root/'source_ts_fixture.py'),run_name='__main__')
else:raise ValueError('OnlyFixedAuditOrExerciseStage')
