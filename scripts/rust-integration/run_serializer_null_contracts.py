#!/usr/bin/env python3
"""Capture original serializer/null family on an owned disposable source stack.
Original Dispatcharr authors retain all application credit. No native acceptance.
"""
import argparse,hashlib,json,os,subprocess,time,urllib.request,uuid
from pathlib import Path
from fixture_ports import write_port_overlay
from tool_snapshot import snapshot_sources
from run import BASELINE,fixture_source
ROOT=Path(__file__).resolve().parents[2]
def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',required=True,type=Path);args=parser.parse_args()
    output=args.output.resolve();output.mkdir(parents=True,exist_ok=True)
    if any(output.iterdir()):parser.error('Output must be empty')
    tools=output/'tool-source';hashes=snapshot_sources(ROOT,tools);project='dispatcharr-serializer-null-'+uuid.uuid4().hex[:10]
    report={'passed':False,'scope':'actual original generic serializer/null/nonobject HTTP discovery; native comparison separate','source_baseline':BASELINE,'source_capture_only':True,'application_image_verified':False,'performance_verified':False,'project':project,'tool_source_sha256':hashes};status=1
    with fixture_source(report,False,output)as directory:
        source=Path(directory)/'source';source.mkdir();source.chmod(0o755)
        archive=subprocess.check_output(['git','archive',BASELINE],cwd=ROOT)
        subprocess.run(['tar','-x','--same-permissions','--no-same-owner','-C',str(source)],input=archive,check=True);(source/'media').mkdir(exist_ok=True)
        report['baseline_archive_sha256']=hashlib.sha256(archive).hexdigest()
        environment=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(output),DISPATCHARR_ENV='aio')
        ports=output/'fixture-ports.yml';report['fixture_ports']=write_port_overlay(ports,{'baseline':[5656]})
        compose=['docker','compose','-p',project,'-f',str(tools/'docker/compose.rust-playback.yml'),'-f',str(ports)]
        def command(args,timeout=180):return subprocess.check_output(args,env=environment,text=True,stderr=subprocess.PIPE,timeout=timeout)
        try:
            command(compose+['up','-d','baseline']);url='http://'+command(compose+['port','baseline','5656']).strip();deadline=time.monotonic()+180
            while True:
                try:
                    with urllib.request.urlopen(url+'/api/core/version/',timeout=2)as response:
                        if response.status==200:break
                except OSError:
                    if time.monotonic()>deadline:raise RuntimeError('Source readiness failed')
                    time.sleep(.2)
            program=(tools/'scripts/rust-integration/serializer_null_fixture.py').read_text()
            result=subprocess.run(compose+['exec','-T','-e','SERIALIZER_NULL_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','-'],env=environment,input=program,text=True,capture_output=True,timeout=180)
            (output/'source-worker.log').write_text(result.stderr);result.check_returncode();golden=json.loads(result.stdout.splitlines()[-1]);path=output/'serializer-null-fixture.json';path.write_text(json.dumps(golden,indent=2)+'\n')
            report.update(passed=golden['all_no_model_mutations'],source_cases=len(golden['cases']),fixture_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),all_no_model_mutations=golden['all_no_model_mutations']);status=0 if report['passed']else 1
        except (OSError,ValueError,RuntimeError,subprocess.SubprocessError)as error:
            report['error_type']=type(error).__name__
            if isinstance(error,subprocess.CalledProcessError):(output/'failed-command.log').write_text(str(error.stderr or '')[:32768])
        finally:
            try:command(compose+['down','--volumes','--remove-orphans']);report['cleanup']='project removed'
            except subprocess.SubprocessError:report.update(cleanup_error=True,passed=False);status=1
    print(json.dumps({'passed':report['passed'],'evidence':str(output)}));return status
if __name__=='__main__':raise SystemExit(main())
