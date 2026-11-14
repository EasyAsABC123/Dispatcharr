"""Bounded owned DVR file snapshots; no external path traversal or deletion."""
import hashlib,os,stat,time
from pathlib import Path
from seed_spec import SOURCE_FILE_LIMIT,SOURCE_FILES_TOTAL,SOURCE_FILE_COUNT
ROOT=Path('/data/recordings')
def relative_path(path,root=ROOT):
 path=Path(path);root=Path(root)
 if path.is_symlink()or root.is_symlink():raise ValueError('NoOwnedMediaSymlink')
 path.resolve().relative_to(root.resolve())
 return path.relative_to(root).as_posix()
def snapshot(output=None):
 rows=[];total=0
 if not ROOT.exists():return {'root':str(ROOT),'exists':False,'files':[],'observed_bytes':0,'snapshot_atomic':False}
 for folder,dirs,files in os.walk(ROOT,followlinks=False):
  relative=Path(folder).relative_to(ROOT)
  if len(relative.parts)>8 or len(dirs)+len(files)>128:raise ValueError('OwnedMediaTraversalBound')
  for name in dirs:
   if(Path(folder)/name).is_symlink():raise ValueError('OwnedMediaDirectorySymlink')
  for name in files:
   path=Path(folder)/name;rel=relative_path(path,ROOT)
   if len(rows)>=SOURCE_FILE_COUNT:raise ValueError('OwnedFileCount128')
   row={'path':str(path),'relative':rel,'began_epoch':time.time(),'gone_during_observation':False}
   try:
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
    with os.fdopen(fd,'rb')as stream:
     before=os.fstat(stream.fileno())
     if not stat.S_ISREG(before.st_mode)or before.st_size>SOURCE_FILE_LIMIT:raise ValueError('OwnedRegularFile32MiB')
     data=stream.read(SOURCE_FILE_LIMIT+1);after=os.fstat(stream.fileno())
    total+=len(data)
    if len(data)>SOURCE_FILE_LIMIT or total>SOURCE_FILES_TOTAL:raise ValueError('OwnedFiles64MiB')
    stable=before.st_ino==after.st_ino and before.st_size==after.st_size==len(data)and before.st_mtime_ns==after.st_mtime_ns
    row.update(bytes=len(data),sha256=hashlib.sha256(data).hexdigest(),stat_before={'size':before.st_size,'mtime_ns':before.st_mtime_ns,'mode':before.st_mode,'uid':before.st_uid,'gid':before.st_gid,'inode':before.st_ino},stat_after={'size':after.st_size,'mtime_ns':after.st_mtime_ns,'inode':after.st_ino},stable_across_this_read=stable)
    if output is not None:
     dest=Path(output)/rel;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(data);row['retained_copy']=str(dest)
   except FileNotFoundError:row['gone_during_observation']=True
   row['finished_epoch']=time.time();rows.append(row)
 return {'root':str(ROOT),'exists':True,'files':rows,'observed_bytes':total,'snapshot_atomic':False}
