"""Owned normal fixture origin bootstrap; no Source app or arbitrary commands."""
import os,sys
from pathlib import Path
from base_origin import mp4_command
from local_command import LocalCommand
if os.environ.get('PENDING_MAIN_FIXTURE')!='playback-fixture-only':raise RuntimeError('OwnedOriginOnly')
mode=os.environ.get('NORMAL_BACKEND');assert mode in['source','native'];path=Path('/tmp/fixture.mp4');LocalCommand(mp4_command(path),stderr_path=Path('/evidence/origin-'+mode+'-generate.stderr')).wait(60)
from ledger_origin import main
sys.argv=['ledger_origin','--media',str(path),'--terminal','/evidence/origin-'+mode+'-terminal.json'];main()
