"""Read-only owned Native fixture SQL/cache observations; never imports Django."""
import hashlib,json,os,re,subprocess,sys
from pathlib import Path
TABLES=('accounts_user','m3u_m3uaccount','m3u_m3uaccountprofile','vod_movie','vod_m3umovierelation','vod_series','vod_episode','vod_m3uepisoderelation','core_coresettings','core_systemnotification','core_notificationdismissal')
MAX_ROW=131072;MAX_TOTAL=8388608
ROOT=Path('/evidence')
def require(ok,label):
    if not ok:raise ValueError(label)
def owned(name):
    require(type(name)is str and re.fullmatch(r'rust_probe_backup_jobs_physical_[a-f0-9]{10}',name),'OwnedNativeDatabase');return name
def psql(database,sql,label):
    path=ROOT/('audit-'+label+'.stderr')
    # PG server enforces read-only and statement duration before SQL starts.
    env=dict(os.environ,PGCONNECT_TIMEOUT='3',PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=5000')
    with path.open('xb')as errors:
        process=subprocess.Popen(['psql','-XAtq','-h','/var/run/postgresql','-U','dispatch','-d',database,'-v','ON_ERROR_STOP=1','-c',sql],env=env,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=errors)
        try:
            used=0
            while True:
                row=process.stdout.readline(MAX_ROW*2+65536)
                if not row:break
                used+=len(row);require(len(row)<MAX_ROW*2+65536 and used<=MAX_TOTAL*2+1048576,'PSQLBoundedCollection')
                yield row.decode().strip()
            require(process.wait(timeout=8)==0,'PSQLReadOnlyExit')
        finally:
            if process.poll()is None:
                process.terminate()
                try:process.wait(timeout=2)
                except subprocess.TimeoutExpired:process.kill();process.wait(timeout=2)
            process.stdout.close()
    require(path.stat().st_size<=65536,'AuditErrorCapacity')
def audit(database,label,raw=False):
    binding=list(psql(database,'SELECT current_database()',label+'-binding'));require(binding==[database],'AuditCurrentDatabaseBinding')
    tables={};sequences={};used=0;raw_rows={}
    for name in (('accounts_user','core_systemevent')if raw else TABLES):
        sql='SELECT json_build_array(octet_length(to_jsonb(t)::text),CASE WHEN octet_length(to_jsonb(t)::text)<=131072 THEN to_jsonb(t)::text ELSE NULL END) FROM '+name+' t ORDER BY id LIMIT 10001'
        count=0;digest=hashlib.sha256();digest.update(b'[');rows=[]
        for line in psql(database,sql,label+'-'+name):
            size,text=json.loads(line);require(type(size)is int and size<=MAX_ROW and type(text)is str,'AuditRowCapacity');encoded=text.encode();used+=len(encoded);count+=1
            require(count<=10000 and used<=MAX_TOTAL,'AuditTotalCapacity')
            if count>1:digest.update(b',')
            digest.update(encoded)
            if raw:rows.append(json.loads(text))
        digest.update(b']');tables[name]={'rows':count,'sha256':digest.hexdigest()}
        sequence=list(psql(database,'SELECT json_build_object(\'last_value\',last_value,\'is_called\',is_called) FROM public.'+name+'_id_seq',label+'-'+name+'-sequence'));require(len(sequence)==1,'IDSequenceRequired');sequences['public.'+name+'_id_seq']=json.loads(sequence[0])
        if raw:raw_rows[name]=rows
    payload={'tables':tables,'sequences':sequences}
    return {'database':database,**payload,'dataset_sha256':hashlib.sha256(json.dumps(payload,sort_keys=True).encode()).hexdigest(),'capture_bytes':used,'scope':'two Native credential-observation tables'if raw else'eleven named tables/sequences only, not full database',**({'raw_rows':raw_rows}if raw else{})}
def cache(database):
    import redis
    client=redis.Redis(host='127.0.0.1',port=6379,password='playback-fixture-only',db=0,socket_connect_timeout=2,socket_timeout=2)
    require(client.get('rust-probe-backup-job-physical-owner')==database.encode(),'OwnedRedisMarker')
    rows=[];used=0;cursor=0
    try:
        for _ in range(128):
            cursor,keys=client.scan(cursor=cursor,count=100)
            for key in keys:
                require(len(key)<=1024 and len(rows)<1000,'CacheKeyCapacity');value=client.dump(key)
                if value is None:continue
                used+=len(key)+len(value);require(len(value)<=65536 and used<=1048576,'CacheValueCapacity');rows.append({'key_hex':key.hex(),'dump_hex':value.hex(),'pttl':client.pttl(key)})
            if cursor==0:break
        else:raise ValueError('CacheScanCapacity')
    finally:client.close()
    return {'rows':sorted({r['key_hex']:r for r in rows}.values(),key=lambda r:r['key_hex']),'capture_bytes':used,'scope':'raw owned Native Redis DUMP/PTTL; credential/cache effects not paired with Source force-auth or normalized away'}
def full_public_manifest(database):
    # Raw driver only: no Django app/setup/import. Same bounded legacy algorithm.
    import psycopg,types
    from public_manifest import manifest
    require(database=='dispatcharr','FullDefaultReadOnlyOnly')
    expected=json.loads(Path('/native-inputs/schema62-authority.json').read_text())
    connection=psycopg.connect(host='/var/run/postgresql',user='dispatch',dbname=database,connect_timeout=3,options='-c default_transaction_read_only=on -c statement_timeout=5000',autocommit=False)
    class Adapter:
        ops=types.SimpleNamespace(quote_name=lambda name:'"'+name.replace('"','""')+'"')
        def cursor(self):return connection.cursor()
    try:
        with connection.cursor()as cursor:
            cursor.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            cursor.execute('SHOW transaction_read_only');require(cursor.fetchone()==('on',),'ActualFullDefaultReadOnlyOn')
            cursor.execute('SHOW transaction_isolation');require(cursor.fetchone()==('repeatable read',),'ActualFullDefaultRepeatableRead')
            cursor.execute('SELECT current_database()');require(cursor.fetchone()==(database,),'ExactFullDefaultDatabase')
        return manifest(Adapter(),expected)
    finally:connection.close()
def main():
    require(os.environ.get('BACKUP_PHYSICAL_AUDIT')=='owned-native-only','AuditAuthority')
    stage=os.environ['BACKUP_PHYSICAL_AUDIT_STAGE'];database=os.environ['BACKUP_PHYSICAL_AUDIT_DATABASE'];label=os.environ['BACKUP_PHYSICAL_AUDIT_LABEL'];require(re.fullmatch('[a-z-]{1,40}',label),'AuditLabel')
    if stage=='default':
        require(database=='dispatcharr','DefaultReadOnlyOnly');value={'named11':audit(database,label),'public62':full_public_manifest(database)}
    elif stage=='credentials':owned(database);value=audit(database,label,raw=True)
    elif stage=='cache':owned(database);value=cache(database)
    else:raise ValueError('KnownAuditStage')
    result={'passed':True,'stage':stage,'value':value,'Django_or_Source_application_loaded':False};path=ROOT/(label+'.json');require(not path.exists(),'FreshAuditOutput');path.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({'passed':True,'output':str(path),'stage':stage}))
if __name__=='__main__':main()
