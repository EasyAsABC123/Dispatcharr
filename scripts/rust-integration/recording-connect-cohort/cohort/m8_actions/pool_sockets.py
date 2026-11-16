"""Pure /proc reader joined to the exact owned Main PID namespace. No app imports."""
import argparse,json,os
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--generation',required=True);p.add_argument('--host-pid',type=int,required=True);p.add_argument('--CID',required=True);a=p.parse_args();assert os.getuid()==0 and len(a.CID)==64
self_status=Path('/proc/self/status').read_text();assert int(self_status.split('CapEff:',1)[1].splitlines()[0].strip(),16)==(1<<19)|(1<<2);assert self_status.split('NoNewPrivs:',1)[1].splitlines()[0].strip()=='1'
root=Path('/proc/1')
def generation():return(root/'stat').read_text().rsplit(')',1)[1].split()[19]
assert generation()==a.generation
uids=(root/'status').read_text().split('Uid:',1)[1].splitlines()[0].split();assert uids==['65532']*4
exe=os.readlink(root/'exe');assert exe.endswith('/dispatcharr-rs')
fd_stat=(root/'fd').stat();fd_metadata={'mode':oct(fd_stat.st_mode&0o777),'uid':fd_stat.st_uid,'gid':fd_stat.st_gid}
inodes={};errors=[]
for descriptor in(root/'fd').iterdir():
 try:target=os.readlink(descriptor)
 except FileNotFoundError:continue
 except PermissionError as error:errors.append({'fd':descriptor.name,'error':type(error).__name__});continue
 if target.startswith('socket:['):inodes[target[8:-1]]=descriptor.name
sockets=[]
for family in('tcp','tcp6'):
 for line in(root/'net'/family).read_text().splitlines()[1:]:
  fields=line.split();inode=fields[9]
  if inode not in inodes:continue
  local_addr,local_port=fields[1].split(':');remote_addr,remote_port=fields[2].split(':')
  sockets.append({'fd':inodes[inode],'inode':inode,'family':family,'state':fields[3],'local_address_hex':local_addr,'local_port':int(local_port,16),'remote_address_hex':remote_addr,'remote_port':int(remote_port,16)})
assert generation()==a.generation and(root/'status').read_text().split('Uid:',1)[1].splitlines()[0].split()==uids
print(json.dumps({'actual_native_pid':1,'actual_host_PID':a.host_pid,'actual_Main_CID':a.CID,'actual_generation':a.generation,'actual_executable':exe,'actual_UID':uids,'sockets':sockets,'errors':errors,'observer_effective_capability':'SYS_PTRACE and DAC_READ_SEARCH only','observer_no_new_privileges':True,'no_app_imports':True,'fd_directory':fd_metadata}))
