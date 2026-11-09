#!/usr/bin/env python3
"""Read-only PostgreSQL session/private/PSS diagnostic for isolated fixtures.

No SQL texts, credentials or client addresses are captured. Original Dispatcharr
application authors retain full credit. Run outside resource measurements unless
its observer work is explicitly part of the declared measurement protocol.
"""
import argparse
import datetime
import json
import os
from pathlib import Path
import shutil
import subprocess

FIELDS=('Rss','Pss','Private_Clean','Private_Dirty','Private_Hugetlb','Shared_Clean','Shared_Dirty','Shared_Hugetlb','Swap')
SQL="""SELECT json_build_object('observer_pid',pg_backend_pid(),'sessions',COALESCE((SELECT json_agg(json_build_object('pid',pid,'backend_type',backend_type,'state',state,'transport',CASE WHEN client_addr IS NULL THEN 'local' ELSE 'tcp' END,'wait_event_type',wait_event_type)) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()),'[]'::json))"""


def parse_rollup(text):
    if len(text)>65536:raise ValueError('smaps_rollup exceeds64KiB')
    values={}
    for line in text.splitlines():
        key,separator,value=line.partition(':')
        if separator and key in FIELDS:
            fields=value.split()
            if len(fields)!=2 or fields[1]!='kB' or not fields[0].isdigit():raise ValueError('invalid smaps byte field')
            values[key.lower()+'_bytes']=int(fields[0])*1024
    if 'rss_bytes' not in values or 'pss_bytes' not in values:raise ValueError('missing RSS/PSS')
    values['private_bytes']=sum(values.get(key.lower()+'_bytes',0) for key in ('Private_Clean','Private_Dirty','Private_Hugetlb'))
    return values


def collect():
    raw=subprocess.check_output(['psql','-X','-U',os.environ.get('POSTGRES_USER','dispatch'),'-d',os.environ.get('POSTGRES_DB','dispatcharr'),'-At','-v','ON_ERROR_STOP=1','-c',SQL],text=True,timeout=5)
    if len(raw)>65536:raise ValueError('activity response exceeds64KiB')
    activity=json.loads(raw);observer=activity['observer_pid'];processes=[];unreadable=[]
    entries=list(Path('/proc').glob('[0-9]*'))
    if len(entries)>4096:raise ValueError('process enumeration exceeds4096')
    for process in entries:
        try:
            if int(process.name)==observer or (process/'comm').read_text().strip()!='postgres':continue
            values=parse_rollup((process/'smaps_rollup').read_text());values['pid']=int(process.name);processes.append(values)
        except (FileNotFoundError,ProcessLookupError):continue
        except PermissionError:unreadable.append(int(process.name))
    return {'format_version':1,'scope':'same-OS-user PostgreSQL smaps_rollup plus pg_stat_activity, observer excluded',
        'timestamp_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'observer_pid_excluded':observer,
        'client_backend_count':sum(row['backend_type']=='client backend' for row in activity['sessions']),
        'sessions':activity['sessions'],'processes':processes,'unreadable_processes':unreadable,
        'postgres_pss_bytes_sum':sum(row['pss_bytes'] for row in processes),
        'postgres_private_bytes_sum':sum(row['private_bytes'] for row in processes),
        'postgres_rss_bytes_sum_diagnostic':sum(row['rss_bytes'] for row in processes),
        'notes':'RSS sums overcount shared pages. PSS is proportional and not identical to container working-memory. Snapshot is not atomic across SQL and process files.'}


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--inside',action='store_true');parser.add_argument('--container');parser.add_argument('--os-user',default='dispatch');parser.add_argument('--docker',default='docker');parser.add_argument('--output',type=Path);args=parser.parse_args()
    if args.inside:print(json.dumps(collect()));return 0
    if not args.container or not args.output:parser.error('container and output required')
    docker=shutil.which(args.docker)
    if not docker:parser.error('Docker not found')
    result=subprocess.run([docker,'exec','-i','-u',args.os_user,args.container,'/dispatcharrpy/bin/python','-','--inside'],input=Path(__file__).read_text(),text=True,capture_output=True,timeout=15,check=True)
    value=json.loads(result.stdout);value['container']=args.container;args.output.write_text(json.dumps(value,indent=2)+'\n');return 0


if __name__=='__main__':raise SystemExit(main())
