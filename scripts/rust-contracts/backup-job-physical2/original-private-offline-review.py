"""Separate Source-derived OFFLINE qualification; no app/runtime/ELF execution."""
import datetime,hashlib,importlib.util,json,re,tarfile
from pathlib import Path
ROOT=Path(__file__).resolve().parent

def require(ok,label):
 if not ok:raise ValueError(label)
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def module(name,path):
 spec=importlib.util.spec_from_file_location(name,path);value=importlib.util.module_from_spec(spec);spec.loader.exec_module(value);return value
EXECUTED=module('backup_physical_executed',ROOT/'validation/executed-validator.py')
CORRECTED=module('backup_physical_offline_corrected',ROOT/'validation/corrected-validator.py')

def source_proof():
 mapping=json.loads((ROOT/'inputs/compiled830-map.json').read_text());require(len(mapping)==830,'ExactCompiled830Map')
 for p in(ROOT/'compiled-source').rglob('*'):
  if p.is_file():require(sha(p)==mapping[str(p.relative_to(ROOT/'compiled-source'))],'ExactCompiledSourceSubset')
 peer=(ROOT/'compiled-source/rust/crates/job-transport/src/backup_job_physical_tests.rs').read_text();require(re.findall(r'let store = Store::connect\(&url,\s*([0-9]+)\)',peer)==['4'],'SameFinalCompiledPoolMax4')
 store=(ROOT/'compiled-source/rust/crates/store/src/lib.rs').read_text();require('pub async fn connect(url: &str, max_connections: u32)'in store and'.max_connections(max_connections)'in store,'ActualMaxPassedToSQLxBuilder')
 entry=next(v for v in(ROOT/'compiled-source/rust/Cargo.lock').read_text().split('[[package]]')if '\nname = "sqlx-core"\n'in v);digest=re.search(r'checksum = "([a-f0-9]+)"',entry)[1];require(digest=='ee6798b1838b6a0f69c007c133b8df5866302197e404e8b6ee8ed3e3a5e68dc6'and sha(ROOT/'upstream/sqlx-core-0.8.6.crate')==digest,'ExactLockChecksumArchive')
 with tarfile.open(ROOT/'upstream/sqlx-core-0.8.6.crate','r:gz')as tar:
  for p in(ROOT/'upstream').rglob('*'):
   if p.is_file()and p.name!='sqlx-core-0.8.6.crate':require(tar.extractfile('sqlx-core-0.8.6/'+str(p.relative_to(ROOT/'upstream'))).read()==p.read_bytes(),'ExactArchiveSourceAndLicenses')
 inner=(ROOT/'upstream/src/pool/inner.rs').read_text();close=inner[inner.index('    pub(super) fn close<'):inner.index('    pub(crate) fn close_event')]
 require('self.idle_conns.pop()'in close and'num_idle'not in close and'if self.size() == 0'in close,'CloseDirectQueueDrainWithoutIdleCounterReset')
 require('self.num_idle.load(Ordering::Acquire)'in inner and'self.num_idle.fetch_sub(1, Ordering::AcqRel)'in inner and'self.num_idle.fetch_add(1, Ordering::AcqRel)'in inner and'self.pool.size.fetch_sub(1, Ordering::AcqRel)'in inner,'IndependentIdleAndSizeCounterCode')
 connection=(ROOT/'upstream/src/pool/connection.rs').read_text();require('let _ = self.inner.raw.close().await;'in connection,'RealRawCloseBeforeSizeGuardDrop')
 options=(ROOT/'upstream/src/pool/options.rs').read_text();require('self.max_connections = max;'in options,'PinnedMaxSetter')
 return {'sqlx_core':'0.8.6','crate_archive_sha256':digest,'same_compiled_final_pool_max':4,'closed_idle_counter_not_decremented_by_close':True,'preclose_idle_or_idlequeue_length':'UNOBSERVED'}

def retained_host(case,report,execution,backends,before_bytes,after_bytes,cleanup):
 require(execution['actual_controller_exit']==1 and execution['passed']is False and execution['error']['message']=='ActualSinglePoolCloseSizeIdle0'and execution['effect_validation_error']=='ValueError:ActualSinglePoolCloseSizeIdle0','OriginalHost1OracleNegativePreserved')
 rows=execution['processes'];require(rows and all(type(r['actual_exit'])is int and r['actual_exit']==0 and r['physically_settled']is True and r['pipes_settled']is True and r['collection_invalid']is None and type(r['starttime'])is str and r['starttime'].isdigit()for r in rows),'AllActualCommandsWaitPipesIdentities')
 elf=[r for r in rows if r['kind']==f'actual-backup-physical-case-{case}-once'];require(len(elf)==1,'OneActualELFCaseNoReplay')
 require(backends and all(equal['actual_backends']==[]for equal in backends),'ExternalOwnedBackendsEmptyAfterELF')
 done=datetime.datetime.fromisoformat(elf[0]['completed_utc']);require(all(datetime.datetime.fromisoformat(v['utc'])>=done for v in backends),'ObserverAfterELFNotWhileAlive')
 require(before_bytes==after_bytes and execution['default11_full62tables59seq_exact']is True,'LiteralDefault11Full62_59Exact')
 defaults=json.loads(before_bytes);value=defaults['value'];require(len(value['named11']['tables'])==len(value['named11']['sequences'])==11 and len(value['public62']['tables'])==62 and len(value['public62']['sequences'])==59,'ExactDefaultAuditScope')
 require(execution['all_command_processes_settled']is True and execution['cleanup_within_declared_90_seconds']is True and 0<=execution['cleanup_elapsed_seconds']<=90,'ActualCompleteCleanup90')
 require(set(cleanup)=={'containers','networks','volumes'}and all(type(v)is str and not v.strip()for v in cleanup.values()),'ActualProjectInventoriesEmpty')
 require(report['closed_pool_idle']==2 and type(report['closed_pool_idle'])is int,'RetainedLiteralIdle2NoNormalization')
 return {'original_host_exit':1,'actual_ELF_exit':0,'external_after_ELF_backends':[],'while_alive_outside_backend_observer':'UNOBSERVED','default_scope':'named11 plusfull62tables59sequences exactbytes'}

def validate():
 seal=json.loads((ROOT/'seal.json').read_text())
 for name,digest in seal['inputs'].items():require(sha(ROOT/name)==digest,'ExactSealedRetainedBytes:'+name)
 source=source_proof();qualified=[]
 for case in(5,6):
  d=ROOT/f'retained-case{case}';report=json.loads((d/'capture-private.json').read_text())
  try:EXECUTED.validate(report,case,d)
  except ValueError as error:require(str(error)=='ActualSinglePoolCloseSizeIdle0','OnlyOriginalFinalOracleFails')
  else:raise ValueError('OriginalNegativeMustRemainNegative')
  result=CORRECTED.validate(report,case,d)
  witness=retained_host(case,report,json.loads((d/'execution-report.json').read_text()),json.loads((d/'backend-after-ELF-exit.json').read_text()),(d/'default-before.json').read_bytes(),(d/'default-after.json').read_bytes(),json.loads((d/'cleanup-project-inventories.json').read_text()))
  qualified.append({**result,**witness})
 return {'state':'OFFLINE_SOURCE_DERIVED_ORACLE_CORRECTION','qualified_cases':qualified,'source_proof':source,'original_plan1_host_exits':[1,1],'original_plan1_ELF_requests':0,'original_plan2_host_exits':[1,1],'original_plan2_ELF_exits':[0,0],'Original_idle0_oracle_unmet':True,'new_app_or_request_runs':0,'runtime_ELF_or_raw_fields_changed':False,'Main_or_fullM8_or_prior_max1_failure_transfer':False}
if __name__=='__main__':print(json.dumps(validate(),indent=2))
