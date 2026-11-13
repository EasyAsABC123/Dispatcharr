"""Reconstruct immutable measured support tools from Git and exact public overlays.
No network, build, Docker, workload dispatch, mutable HEAD or private fallback.
"""
import argparse,hashlib,io,json,os,subprocess,tarfile
from pathlib import Path,PurePosixPath
HERE=Path(__file__).resolve().parent
sha=lambda b:hashlib.sha256(b).hexdigest()
def reconstruct(repo,output):
    repo=Path(repo).resolve();output=Path(output).resolve()
    if output.exists():raise ValueError('FreshOutputRequired')
    manifest=json.loads((HERE/'execution-inputs.json').read_text());revision=manifest['support_revision']
    if revision!='28122faf28a0acf1c136ca0d0831961e786cb686':raise ValueError('PinnedSupportRequired')
    env={k:v for k,v in os.environ.items()if not k.startswith('GIT_')};env['GIT_OPTIONAL_LOCKS']='0'
    def git(*args):return subprocess.check_output(['git','-C',str(repo),*args],env=env)
    archive=git('archive',revision,'--','docker','scripts','docs')
    if sha(archive)!=manifest['support_archive_sha256']:raise ValueError('SupportArchiveChanged')
    files={}
    with tarfile.open(fileobj=io.BytesIO(archive))as t:
        for member in t:
            if member.isdir():continue
            name=member.name;parts=PurePosixPath(name).parts
            if not member.isfile()or name in files or name.startswith('/')or '..'in parts:raise ValueError('ArchiveShape')
            files[name]=t.extractfile(member).read()
    if {k:sha(v)for k,v in files.items()}!=manifest['support_sha256']:raise ValueError('SupportMapChanged')
    expected=manifest['actual_executed_tools_sha256']
    for name,digest in expected.items():
        if name in files and sha(files[name])==digest:continue
        if not name.startswith('scripts/rust-live-perf/')or len(PurePosixPath(name).parts)!=3:raise ValueError('UnknownOverlay')
        content=(HERE/PurePosixPath(name).name).read_bytes()
        if sha(content)!=digest:raise ValueError('OverlayChanged')
        files[name]=content
    if {k:sha(v)for k,v in files.items()}!=expected:raise ValueError('ExecutedMapChanged')
    objects=Path(git('rev-parse','--path-format=absolute','--git-path','objects').decode().strip())
    output.mkdir();tree=output/'support';metadata=output/'support-git'
    for name,content in files.items():
        path=tree/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(content)
    (metadata/'objects/info').mkdir(parents=True);(metadata/'refs').mkdir()
    (metadata/'HEAD').write_text(revision+'\n');(metadata/'config').write_text('[core]\n\trepositoryformatversion = 0\n\tbare = false\n\tworktree = '+str(tree)+'\n');(metadata/'objects/info/alternates').write_text(str(objects)+'\n')
    isolated={**env,'GIT_DIR':str(metadata),'GIT_WORK_TREE':str(tree)}
    if subprocess.check_output(['git','rev-parse','HEAD'],cwd=tree,env=isolated).decode().strip()!=revision:raise ValueError('DetachedHead')
    if subprocess.check_output(['git','archive','HEAD','--','docker','scripts','docs'],cwd=tree,env=isolated)!=archive:raise ValueError('IsolatedArchive')
    for path in metadata.rglob('*'):
        if path.is_file():path.chmod(0o444)
    result={'passed':True,'support_files':len(manifest['support_sha256']),'executed_tools':len(files),'support_revision':revision,'support_archive_sha256':sha(archive),'all_executed_hashes_exact':True,'isolated_HEAD_archive_exact':True,'application_or_resource_executed':False}
    (output/'reconstruction.json').write_text(json.dumps(result,indent=2)+'\n');return result
if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();print(json.dumps(reconstruct(a.repo,a.output),indent=2))
