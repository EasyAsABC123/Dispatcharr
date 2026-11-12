"""Owned deterministic local origin only; app is never imported."""
import os,sys
from pathlib import Path
from base_origin import mp4_command
from local_command import LocalCommand
if os.environ.get('PENDING_MAIN_FIXTURE')!='playback-fixture-only':raise RuntimeError('OwnedHeaderOriginRequired')
media=Path('/tmp/fixture.mp4');LocalCommand(mp4_command(media),stderr_path=Path('/evidence/origin-generate.stderr')).wait(60)
from header_origin import main
sys.argv=['header_origin','--media',str(media),'--terminal','/evidence/header-origin-terminal.json'];main()
