"""Strict lossless QA stderr refs, not business normalization or raw fallback."""
import copy,hashlib,json,os,re,stat
from pathlib import Path
BLOB_CAP=65536
BLOBS=2
DIRECTORY='native-dvr-stderr-by-sha256'
def need(v,label):
 if not v:raise ValueError(label)
def expand_report(encoded,evidence):
 root=Path(evidence);original=copy.deepcopy(encoded);value=copy.deepcopy(encoded);seen={};refs=[];commands=value.get('commands');need(type(commands)is list and len(commands)<=16,'BoundedActualCommandList16')
 def rows(items,location):
  need(type(items)is list and len(items)<=2,'ActualCaptureRemuxSlots2')
  for slot,row in enumerate(items):
   stderr=row.get('stderr')
   if stderr is None:continue
   tail=stderr.get('tail');need(type(tail)is dict and set(tail)=={'$native_dvr_stderr_ref'},'ExactEncodedTailReferenceOnlyNoRawFallback');ref=tail['$native_dvr_stderr_ref'];need(type(ref)is dict and set(ref)=={'path','sha256','bytes','codec'},'ExactReferenceSchema');digest=ref['sha256'];need(type(digest)is str and re.fullmatch('[a-f0-9]{64}',digest)is not None,'CanonicalSHA256');need(ref['codec']=='raw-u8-tail-v1'and type(ref['bytes'])is int and 0<=ref['bytes']<=BLOB_CAP,'ExactRawByteCodecLength');relative=DIRECTORY+'/'+digest+'.bin';need(ref['path']==relative,'CanonicalPerSampleOwnedPath')
   if relative not in seen:
    folder=root/DIRECTORY;need(stat.S_ISDIR(folder.lstat().st_mode),'OwnedRegularDirectoryNoSymlink');path=root/relative;need(stat.S_ISREG(path.lstat().st_mode),'OwnedRegularFileNoSymlink');fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
    with os.fdopen(fd,'rb')as stream:
     before=os.fstat(stream.fileno());need(stat.S_ISREG(before.st_mode)and before.st_size==ref['bytes'],'ActualFileDeclaredLength');data=stream.read(BLOB_CAP+1);after=os.fstat(stream.fileno())
    need(len(data)==ref['bytes']and before.st_ino==after.st_ino and before.st_size==after.st_size and before.st_mtime_ns==after.st_mtime_ns and hashlib.sha256(data).hexdigest()==digest,'ActualStableBytesAndSHA256');seen[relative]=data;need(len(seen)<=BLOBS and sum(map(len,seen.values()))<=BLOBS*BLOB_CAP,'TwoImmutableTailTotal131072')
   data=seen[relative];need(len(data)==ref['bytes']and hashlib.sha256(data).hexdigest()==digest,'EveryRepeatedReferenceBindsSameExactBytes');stderr['tail']=list(data);refs.append({'command_location':location,'process_slot':slot,**ref})
 for index,command in enumerate(commands):
  process=command.get('diagnostics',{}).get('processes')
  if process is not None:rows(process['rows'],f'commands/{index}/diagnostics/processes/rows')
  outcome=command.get('actual_observer_result',{})
  if 'processes'in outcome:rows(outcome['processes'],f'commands/{index}/actual_observer_result/processes')
 folder=root/DIRECTORY
 if folder.exists()or folder.is_symlink():
  need(stat.S_ISDIR(folder.lstat().st_mode),'OwnedRegularDirectoryNoSymlink');actual={DIRECTORY+'/'+p.name for p in folder.iterdir()};need(actual==set(seen),'NoUnknownOrOrphanSidecarSilentlyOmitted')
 return value,{'encoding':'lossless-raw-u8-tail-v1','unique_blobs':len(seen),'raw_total_bytes':sum(map(len,seen.values())),'references':refs,'no_fields_or_clocks_removed':True,'encoded_report_unchanged':encoded==original}


def load_report(path,evidence):
 path=Path(path);need(stat.S_ISREG(path.lstat().st_mode),'EncodedReportRegularNoSymlink');fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
 with os.fdopen(fd,'rb')as stream:
  before=os.fstat(stream.fileno());need(before.st_size<=2*1024*1024,'UnchangedEncodedReport2MiBCap');data=stream.read(2*1024*1024+1);after=os.fstat(stream.fileno())
 need(len(data)<=2*1024*1024 and len(data)==before.st_size==after.st_size and before.st_ino==after.st_ino and before.st_mtime_ns==after.st_mtime_ns,'ActualStableBoundedEncodedReport');encoded=json.loads(data.decode('utf8',errors='strict'));expanded,proof=expand_report(encoded,evidence);proof.update(encoded_report_sha256=hashlib.sha256(data).hexdigest(),encoded_report_bytes=len(data));return encoded,expanded,proof
