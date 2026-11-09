#!/usr/bin/env python3
"""Capture source-pinned fresh PostgreSQL DDL/defaults without external network.

This generates developer artifacts only; it does not initialize native schema
or establish clean-install acceptance. Original Dispatcharr authors retain credit.
"""
import argparse,hashlib,json,os,subprocess,tempfile
from pathlib import Path
from run import BASELINE
ROOT=Path(__file__).resolve().parents[2]
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',required=True,type=Path);args=parser.parse_args();output=args.output.resolve();output.mkdir(parents=True,exist_ok=True)
    if any(output.iterdir()):parser.error('Output must be empty')
    report={'passed':False,'scope':'Original fresh schema/default capture only; native application remains unverified','source_image':IMAGE,'baseline':BASELINE}
    with tempfile.TemporaryDirectory(prefix='dispatcharr-original-schema-') as temporary:
        source=Path(temporary)/'source';source.mkdir();source.chmod(0o755)
        archive=subprocess.check_output(['git','archive',BASELINE],cwd=ROOT);subprocess.run(['tar','-x','--same-permissions','--no-same-owner','-C',str(source)],input=archive,check=True)
        report['baseline_archive_sha256']=hashlib.sha256(archive).hexdigest()
        tools=output/'tools';tools.mkdir()
        for name in ['capture_clean_schema.sh','capture_clean_schema.py']:(tools/name).write_bytes((ROOT/'scripts/rust-integration'/name).read_bytes())
        command=['docker','run','--rm','--network','none','--tmpfs','/data:mode=1777,size=1073741824','--tmpfs','/tmp:mode=1777,size=1073741824','--mount','type=bind,src='+str(source)+',dst=/app,readonly','--mount','type=bind,src='+str(tools)+',dst=/schema-tools,readonly','--mount','type=bind,src='+str(output)+',dst=/evidence','--workdir','/app']
        env={'DISPATCHARR_ENV':'aio','DJANGO_SECRET_KEY':'playback-fixture-only','POSTGRES_DB':'dispatcharr','POSTGRES_USER':'dispatch','POSTGRES_PASSWORD':'playback-fixture-only','POSTGRES_HOST':'/var/run/postgresql','POSTGRES_PORT':'5432','POSTGRES_DIR':'/data/db','REDIS_HOST':'localhost','REDIS_PORT':'6379','REDIS_PASSWORD':'playback-fixture-only','PUID':'1000','PGID':'1000','LANG':'C.UTF-8','LC_ALL':'C.UTF-8','PYTHONDONTWRITEBYTECODE':'1','DISPATCHARR_LOG_LEVEL':'WARNING'}
        for key,value in env.items():command+=['-e',key+'='+value]
        command+=['--entrypoint','bash',IMAGE,'/schema-tools/capture_clean_schema.sh']
        try:
            with (output/'capture.log').open('w')as log:result=subprocess.run(command,stdout=log,stderr=subprocess.STDOUT,timeout=240)
            report['exit_code']=result.returncode
            if result.returncode:raise RuntimeError('Source schema capture failed')
            inventory=json.loads((output/'defaults.json').read_text())
            for name in ['schema','defaults']:
                raw=(output/(name+'.raw.sql')).read_text()
                # psql-only random restrict directives and session settings are
                # not schema. Keep qualified DDL/DML, no COPY protocol required.
                lines=[line for line in raw.splitlines()if not line.startswith('\\') and not line.startswith('SET ') and not line.startswith("SELECT pg_catalog.set_config('search_path'")]
                normalized='\n'.join(lines)+'\n'
                if 'playback-fixture-only' in normalized or 'COPY ' in normalized:raise RuntimeError('Unexpected secret/COPY protocol in schema artifact')
                (output/(name+'.sql')).write_text(normalized)
            report.update(passed=True,table_count=len(inventory['tables']),default_rows=sum(inventory['tables'].values()),migration_count=len(inventory['migrations']),files={name:hashlib.sha256((output/name).read_bytes()).hexdigest()for name in ['schema.sql','defaults.sql','defaults.json']})
        except (OSError,ValueError,RuntimeError,subprocess.SubprocessError)as error:report['error_type']=type(error).__name__
    (output/'capture.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({'passed':report['passed'],'evidence':str(output)}));return 0 if report['passed']else 1
if __name__=='__main__':raise SystemExit(main())
