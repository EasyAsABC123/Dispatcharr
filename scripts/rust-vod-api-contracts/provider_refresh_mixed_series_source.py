"""Actual Series mixed Source/native HTTP schedule; no performance claim."""
import os,runpy
from pathlib import Path
os.environ["VOD_REFRESH_MIXED_KIND"]="series"
runpy.run_path(str(Path(__file__).with_name("provider_refresh_mixed_source.py")))
