"""Portable read-only recomputation; no Docker, SQL, app or binary execution."""
from pathlib import Path
import gzip,hashlib,importlib.util,io,json,tarfile,tempfile
ROOT=Path(__file__).resolve().parent
def need(x,label):
 if not x:raise ValueError(label)
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def archived(case):
 row=json.loads((ROOT/'manifest.json').read_text())['cases'][str(case)];path=ROOT/row['archive'];need(sha(path)==row['archive_sha256'],'ExactRawArchive');data=gzip.decompress(path.read_bytes());need(len(data)<=32*1048576,'RawArchive32MiBCap');result={}
 with tarfile.open(fileobj=io.BytesIO(data))as tar:
  members=tar.getmembers();need(len(members)==len(row['raw_files'])<=128,'ExactFiniteRawArchiveMembers')
  for member in members:
   need(member.isfile()and'/'not in member.name and member.name not in result and member.size<=8*1048576,'OnlyFiniteFlatRegularCapturedFiles');raw=tar.extractfile(member).read();wanted=row['raw_files'][member.name];need(len(raw)==wanted['bytes']and hashlib.sha256(raw).hexdigest()==wanted['sha256'],'EveryCapturedFileByteExact');result[member.name]=raw
 need(set(result)==set(row['raw_files']),'ExactRawOutputFileSet');return result

def check_case(case,files=None):
 if files is None:files=archived(case)
 actual=json.loads(files['rest-reads-private.json']);spec=importlib.util.spec_from_file_location('retained_rest_actual_validator',ROOT/'closure/controller/validate_physical.py');mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
 with tempfile.TemporaryDirectory()as d:
  (Path(d)/'proxy-process.json').write_bytes(files['proxy-process.json']);qualified=mod.validate(actual,case,d)
 need(qualified==json.loads(files['effect-validation.json']),'ExactHistoricalEffectRecomputation')
 for prefix in('default','owned'):
  need(files[prefix+'-before.json']==files[prefix+'-after.json'],'Full6259LiteralAuditBytesEqual');value=json.loads(files[prefix+'-before.json'])['value'];full=value['public62'];schema=json.loads((ROOT/'closure/inputs/schema62-authority.json').read_text());need(sorted(full['tables'])==schema['tables']and sorted(full['sequences'])==schema['sequences']and len(full['tables'])==62 and len(full['sequences'])==59,'ExactFull6259Sets');need(len(value['named11']['tables'])==len(value['named11']['sequences'])==11,'ExactNamedElevenSubset')
 need(json.loads(files['cache-before.json'])['value']==json.loads(files['cache-after.json'])['value'],'AllFourRawRedisUnchanged');cache=json.loads(files['cache-before.json'])['value'];need([r['database']for r in cache]==[0,1,2,3],'ExactFourRedisDatabases')
 report=json.loads(files['execution-report.json']);need(report['passed']is True and type(report['actual_controller_exit'])is int and report['actual_controller_exit']==0 and report['actual_command_count']==len(report['processes'])==26,'ActualController0AndAll26Receipts')
 for row in report['processes']:need(type(row['starttime'])is str and row['starttime'].isdigit()and int(row['starttime'])>0 and row['actual_exit']==0 and row['collection_invalid']is None and row['physically_settled']is True and row['pipes_settled']is True,'ActualKnownHostIdentitiesWaitAndPipes0')
 need(report['all_command_processes_settled']is True and report['cleanup_within_declared_90_seconds']is True and 0<=report['cleanup_elapsed_seconds']<=90 and report['owned_backends_absent_after_process_exit']is True,'ActualBoundedOwnedCleanup')
 inventories=json.loads(files['cleanup-project-inventories.json']);need(set(inventories)=={'containers','networks','volumes'}and all(not x.strip()for x in inventories.values()),'ExactOwnedProjectAllResourceKindsEmpty');need(not json.loads(files['exact-CID-absence.json'])['raw'].strip(),'ExactFormerCIDAbsent');need(all(r['original_process_absent']is True for r in json.loads(files['cleanup-PID-absence.json'])),'ActualServiceHostIdentitiesAbsent')
 # Deliberately public disposable credentials; refuse unforeseen real users.
 users=actual['before']['tables']['accounts_user'];need(len(users)==1 and users[0]['api_key']=='owned-rest-read-key-not-production'and users[0]['password']=='!unusable'and actual['credentials_before']['events']==[],'OnlyDeclaredPublicSyntheticCredentialContext')
 return {'case':case,'passed':True,'actual_controller_exit':0,'actual_closed_size':actual['closed_pool_size'],'literal_idle_diagnostic':actual['closed_pool_idle_diagnostic'],'original_writer_slots':actual['writer_slots_after_settlement'],'while_ELF_subject_backends':actual['while_ELF_alive_subject_SQL_backends'],'same_backend_error57014':actual['held']['held']['backend_pid'],'scope':'historical component only'}
def verify():
 manifest=json.loads((ROOT/'manifest.json').read_text());need(manifest['source_count']==852 and manifest['rust_count']==847 and manifest['coupled_count']==5,'HistoricalSourceCountNotCurrentUnion')
 need(sha(ROOT/'source-inputs.json')==manifest['source_map_sha256'],'ExactHistoricalSourceMap')
 for rel,h in manifest['overlay'].items():need(sha(ROOT/'source-overlay'/rel)==h,'ExactSourceOverlay')
 seal=json.loads((ROOT/'closure/seal.json').read_text());need(sha(ROOT/'closure/seal.json')==manifest['executed_seal_sha256'],'ExactExecutedControllerSeal')
 for rel,h in seal['local_inputs'].items():need(sha(ROOT/'closure'/rel)==h,'ExactOriginalControllerInput')
 build=json.loads((ROOT/'closure/build/build.json').read_text());need(build['passed']is True and len(build['commands'])==5 and all(x['actual_wait_exit']==x['actual_poll_exit']==0 for x in build['commands']),'ActualFiveBuildWaits0');need(manifest['ELF']['source_map_sha256']==manifest['source_map_sha256']and manifest['ELF']['copied_epoch']>=max(x['completed_epoch']for x in build['commands']),'OriginalOutsideELFCopyAfterWaitsBound')
 return {'passed':True,'cases':[check_case(1),check_case(2)],'Source_runs':0,'Main_runs':0,'current_union_physical_transfer':False,'binary_required_for_new_run':True}
if __name__=='__main__':print(json.dumps(verify(),indent=2))
