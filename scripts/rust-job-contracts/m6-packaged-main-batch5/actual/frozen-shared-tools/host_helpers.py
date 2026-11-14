"""Qualified bounded host collection patterns copied from SPA prep3; no app calls."""
import os
from pathlib import Path
from bounded_process import CollectionFailure
import tooling_guard as T

def require(v,k):
 if not v:raise ValueError(k)
def start(pid):
 try:return Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split()[19]
 except FileNotFoundError:return None
def environment(meta):
 prefixes=('COMPOSE_','POSTGRES_','REDIS_','DJANGO_','CELERY_','DATABASE_','BASELINE_','BENCHMARK_','FIXTURE_','ENABLE_VOD','SOURCE_CAPTURE_','SPECIAL_','HEALTH_SOURCE_','GIT_','DOCKER_','RUST_','DISPATCHARR_')
 value={k:v for k,v in os.environ.items()if not k.startswith(prefixes)};value.update(meta['selected_environment']);T.assert_inputs(value,meta);return value
def collected(job,timeout,stdout):
 job.wait(timeout);require(job.process.returncode==0 and job.physically_settled and not any(t.is_alive()for t in job.threads),'ActualSuccessfulWaitAndPipes');return Path(stdout).read_bytes()
