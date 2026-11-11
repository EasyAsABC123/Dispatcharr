#!/usr/bin/env python3
"""Pinned actual nginx/DRF static oracle; no Native/performance acceptance.
Original Dispatcharr/nginx/DRF and bundled authors retain all credit/licenses.
"""
import argparse,hashlib,json,subprocess,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
    out=args.output.resolve();out.mkdir(parents=True,exist_ok=True)
    if any(out.iterdir()):parser.error('Fresh empty owned output required')
    out.chmod(0o755);name='dispatcharr-static-source-'+uuid.uuid4().hex[:12]
    report={'passed':False,'scope':__doc__,'image':IMAGE,'container':name,'network':'none','performance_verified':False,'native_verified':False}
    report['tools']={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest()for p in [Path(__file__).resolve(),Path(__file__).with_name('source.py'),ROOT/'rust/runtime/collect_static.py']}
    command=['docker','run','--rm','--name',name,'--network','none','-e','STATIC_SOURCE_DISPOSABLE_FIXTURE=owned-static-only','--entrypoint','/dispatcharrpy/bin/python','-v',str(Path(__file__).resolve().parent)+':/probe:ro','-v',str(ROOT/'rust/runtime')+':/runtime:ro','-v',str(out)+':/out','-v',str(out)+':/app/static:ro',IMAGE,'/probe/source.py']
    try:
        result=subprocess.run(command,capture_output=True,text=True,timeout=120);(out/'controller.log').write_text(result.stdout+result.stderr)
        report['exit_code']=result.returncode
        if result.returncode==0:
            data=json.loads((out/'source-nginx-static.json').read_text());report.update(passed=data['passed'],assets=data['assets'],observations=len(data['records']),Source_report_sha256=hashlib.sha256((out/'source-nginx-static.json').read_bytes()).hexdigest())
    except (OSError,ValueError,subprocess.SubprocessError) as error:
        report['error_type']=type(error).__name__
    finally:
        subprocess.run(['docker','rm','-f',name],capture_output=True,text=True,timeout=20)
    remaining=subprocess.check_output(['docker','ps','-aq','--filter','name=^/'+name+'$'],text=True).split();report['remaining_containers']=remaining;report['passed']=report['passed']and not remaining
    (out/'verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({'passed':report['passed'],'evidence':str(out)}));return 0 if report['passed']else 1
if __name__=='__main__':raise SystemExit(main())
