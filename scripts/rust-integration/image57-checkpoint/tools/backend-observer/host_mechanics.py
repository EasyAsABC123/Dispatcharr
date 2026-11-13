"""Host-only initial environment and actual child process receipts; no app imports."""
import os,re
from pathlib import Path
PREFIXES=('DOCKER_','GIT_','COMPOSE_','POSTGRES_','REDIS_','DJANGO_','CELERY_','DATABASE_','BASELINE_','BENCHMARK_','FIXTURE_','ENABLE_VOD')
SAFE={'PATH','DOCKER_HOST','DOCKER_CONFIG','CARGO_HOME','RUSTUP_HOME','RUSTUP_TOOLCHAIN'}
def environment(ambient,selected):
 if set(selected)!=SAFE or not all(type(v)is str and v for v in selected.values()):raise ValueError('ExactSixBoundExports')
 result={k:v for k,v in ambient.items()if not k.startswith(PREFIXES)};result.update(selected);return result

def identity(process,cwd=None):
 pid=process.pid
 if type(pid)is not int or pid<=1:raise ValueError('ActualChildPID')
 cwd=os.getcwd()if cwd is None else cwd
 # Capture before wait/poll can reap a short-lived child. A missing record is
 # explicit incomplete coverage, never invented zero/parent identity.
 try:
  raw=Path('/proc',str(pid),'stat').read_text();tail=raw.rsplit(')',1)[1].split();start=tail[19]
  if not start.isdigit():raise ValueError('ActualChildStarttime')
  return {'cwd':cwd,'host_PID':pid,'host_PID_starttime':start,'actual_identity_observed':True}
 except FileNotFoundError:return{'cwd':cwd,'host_PID':pid,'host_PID_starttime':None,'actual_identity_observed':False}

def settled(collector):
 return {'stdout_pipe_closed':collector.process.stdout.closed,'stderr_pipe_closed':collector.process.stderr.closed,'pipe_reader_threads_settled':all(not t.is_alive()for t in collector.threads),'stdin_writer_settled':collector.writer is None or not collector.writer.is_alive(),'pipes_physically_settled':collector.physically_settled}
