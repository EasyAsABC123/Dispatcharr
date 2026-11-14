"""Default DB read-only full6259 and optional named11 under real Source shell."""
import os,sys,json
from pathlib import Path
from capture_common import connected_database,audit
from public_manifest import manifest

def main():
 if os.environ.get('PLUGIN_REPO_AUTHORITY')!='owned-source-repository-only' or os.environ.get('POSTGRES_DB')!='dispatcharr' or len(sys.argv)<2 or sys.argv[1]!='shell':raise RuntimeError('ExactReadonlyDefaultShellContext')
 label=os.environ['PLUGIN_REPO_AUDIT_LABEL']
 if label not in ['before','after']:raise RuntimeError('FixedAuditLabel')
 from dispatcharr.app_initialization import should_skip_initialization
 from django.db import connection,connections,transaction
 if should_skip_initialization()is not True or connected_database(connection)!='dispatcharr':raise RuntimeError('ActualDefaultDBAndManagementSkip')
 try:
  with transaction.atomic():
   with connection.cursor()as cursor:
    cursor.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY');cursor.execute('SHOW transaction_read_only')
    if cursor.fetchone()[0]!='on':raise RuntimeError('ActualReadonlyTransaction')
   result={'named11':audit(connection),'public62':manifest(connection,json.loads(Path('/plugin-inputs/schema62-authority.json').read_text()))}
  Path('/evidence/default-'+label+'-private.json').write_text(json.dumps(result,indent=2)+'\n')
 finally:connections.close_all()
if __name__=='__main__':main()
