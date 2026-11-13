"""Bounded owned-stop CLI collection; deliberately no container-stop recursion."""
import datetime,hashlib,json,time,os
from pathlib import Path
from bounded_process import Collected
class StopCollectionFailure(RuntimeError):pass

def collect(argv,environment,output,receipt,limit=8388608,timeout=15,save=None):
 output=Path(output);began=time.monotonic();job=None;failure=None
 try:
  # A stop-CLI overflow/timeout terminates ONLY this owned CLI. Its neutral
  # callback cannot recursively start another container stop command.
  job=Collected(argv,environment,limit,lambda:None,stdout_path=output/'owned-stop.stdout',stderr_path=output/'owned-stop.stderr')
  try:identity=Path('/proc',str(job.process.pid),'stat').read_text().rsplit(')',1)[1].split()[19]
  except FileNotFoundError:identity=None
  receipt.update(argv=argv,pid=job.process.pid,starttime=identity,proc_PID_namespace=os.readlink('/proc/self/ns/pid'),started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),output_limit=limit,deadline_seconds=timeout,recursive_stop_callback=False)
  job.wait(timeout)
  if type(identity)is not str or not identity.isdigit()or int(identity)<=0:raise ValueError("ActualOwnedStopPIDIdentityRequired")
 except BaseException as error:
  failure=error;receipt['failure']={'type':type(error).__name__,'message':str(error)[:4096],'message_bytes':len(str(error).encode()),'message_sha256':hashlib.sha256(str(error).encode()).hexdigest()}
 finally:
  receipt.update(elapsed_seconds=time.monotonic()-began,completed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
  if job is not None:receipt.update(actual_exit=job.process.poll(),collection_invalid=job.invalid,physically_settled=job.physically_settled,pipes_settled=not any(t.is_alive()for t in job.threads),capture_complete=job.invalid is None)
  receipt['acknowledged']=failure is None and receipt.get('actual_exit')==0 and receipt.get('physically_settled')is True and receipt.get('pipes_settled')is True and receipt.get('capture_complete')is True
  try:
   if save is None:(output/'owned-stop.json').write_text(json.dumps(receipt,indent=2)+'\n')
   else:save(output/'owned-stop.json',receipt)
  except BaseException as error:
   receipt['acknowledged']=False;receipt['receipt_write_failure']={'type':type(error).__name__,'message':str(error)[:4096]};raise StopCollectionFailure('OwnedStopReceiptWriteFailed')from error
 if not receipt['acknowledged']:raise StopCollectionFailure('OwnedStopNotAcknowledged')from failure
 return receipt
