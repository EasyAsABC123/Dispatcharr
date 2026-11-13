"""Pure notification fixture ownership, audit and seal checks; no I/O on import."""
import hashlib,json,re
TABLES=('accounts_user','m3u_m3uaccount','m3u_m3uaccountprofile','vod_movie','vod_m3umovierelation','vod_series','vod_episode','vod_m3uepisoderelation','core_coresettings','core_systemnotification','core_notificationdismissal')
def require(value,label):
    if not value:raise ValueError(label)
def database(name):
    require(type(name)is str and re.fullmatch(r'rust_probe_notifforms_[a-f0-9]{10}',name),'OwnedCopiedDatabase')
    return name
def dataset(value,expected):
    require(value['database']==expected,'AuditDatabaseBinding')
    require(set(value['tables'])==set(TABLES),'ExactElevenTables')
    require(set(value['sequences'])=={'public.'+table+'_id_seq' for table in TABLES},'ExactElevenSequences')
    for row in value['tables'].values():
        require(type(row['rows'])is int and 0<=row['rows']<=10000 and re.fullmatch('[0-9a-f]{64}',row['sha256']),'TableFingerprintShape')
    for row in value['sequences'].values():
        require(type(row['last_value'])is int and row['last_value']>=1 and type(row['is_called'])is bool,'SequenceFingerprintShape')
    actual=hashlib.sha256(json.dumps({'tables':value['tables'],'sequences':value['sequences']},sort_keys=True).encode()).hexdigest()
    require(actual==value['dataset_sha256'],'FingerprintDigestBinding')
    return actual
def unchanged_default(before,after):
    return dataset(before,'dispatcharr')==dataset(after,'dispatcharr')
def stop_record(row,role,nonce,read_starttime):
    require(row.get('role')==role and row.get('nonce')==nonce,'OwnedPIDBinding')
    pid=row.get('pid');require(type(pid)is int and 1<pid<2**31,'OwnedPIDRange')
    require(type(row.get('starttime'))is str and row['starttime'].isdigit() and read_starttime(pid)==row['starttime'],'OwnedPIDReused')
    return pid
