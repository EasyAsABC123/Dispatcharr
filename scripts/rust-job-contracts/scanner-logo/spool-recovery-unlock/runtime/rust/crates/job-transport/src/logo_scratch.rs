//! Versioned, bounded private scratch ownership. No generic filesystem cleanup.
//! Single managed Linux directory; external mutation and power-loss recovery
//! are unsupported. Stable lock inode is never renamed or removed.
use super::Error;
use std::{fs::{File,OpenOptions,Metadata},io::{Read,Write,Seek,SeekFrom},path::{Path,PathBuf}};
use std::os::unix::fs::{MetadataExt,OpenOptionsExt};
const NAME:&str=".scanner-owner-v2";
const MAGIC:&[u8;8]=b"LSPOOL02";
const LENGTH:usize=136;
// Linux O_NOFOLLOW; this module has no non-Linux authority.
const NOFOLLOW:i32=0o400000;
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
struct Record{generation:u64,id:Option<uuid::Uuid>}
fn encode(record:Record)->Result<[u8;64],Error>{
 if record.generation==0{return Err(Error::Unsupported)}
 let mut out=[0;64];out[..8].copy_from_slice(&record.generation.to_le_bytes());
 if let Some(id)=record.id{if id.get_version_num()!=4||id.get_variant()!=uuid::Variant::RFC4122{return Err(Error::Unsupported)}out[8]=1;out[9..25].copy_from_slice(id.as_bytes())}
 for i in 0..32{out[i+32] = !out[i]}Ok(out)
}
fn decode(bytes:&[u8])->Option<Record>{
 if bytes.len()!=64||(0..32).any(|i|bytes[i]!=!bytes[i+32])||bytes[25..32].iter().any(|b|*b!=0){return None}
 let generation=u64::from_le_bytes(bytes[..8].try_into().ok()?);if generation==0{return None}
 let value=uuid::Uuid::from_slice(&bytes[9..25]).ok()?;
 let id=match bytes[8]{0 if value.is_nil()=>None,1 if value.get_version_num()==4&&value.get_variant()==uuid::Variant::RFC4122=>Some(value),_=>return None};Some(Record{generation,id})
}
fn select(bytes:&[u8])->Result<(usize,Record),Error>{
 if bytes.len()!=LENGTH||&bytes[..8]!=MAGIC{return Err(Error::Unsupported)}
 match(decode(&bytes[8..72]),decode(&bytes[72..136])){
 (Some(a),None)=>Ok((0,a)),(None,Some(b))=>Ok((1,b)),(Some(a),Some(b))if a==b=>Ok((0,a)),
 (Some(a),Some(b))if a.generation.checked_add(1)==Some(b.generation)=>Ok((1,b)),
 (Some(a),Some(b))if b.generation.checked_add(1)==Some(a.generation)=>Ok((0,a)),_=>Err(Error::Unsupported)}
}
fn same(a:&Metadata,b:&Metadata)->bool{a.dev()==b.dev()&&a.ino()==b.ino()}
fn checked(path:&Path,uid:u32,max:u64)->Result<Metadata,Error>{
 let m=std::fs::symlink_metadata(path).map_err(|_|Error::Filesystem)?;
 if !m.is_file()||m.file_type().is_symlink()||m.uid()!=uid||m.mode()&0o777!=0o600||m.nlink()!=1||m.len()>max{return Err(Error::Unsupported)}Ok(m)
}
fn open()->OpenOptions{let mut o=OpenOptions::new();o.read(true).write(true).mode(0o600).custom_flags(NOFOLLOW);o}
/// A logical owner explicitly unlocks once its physical work is settled. A
/// forked unrelated child may inherit CLOEXEC descriptors until exec; relying
/// only on File::drop would extend the flock beyond this owner's lifetime.
struct LockedControl{file:File,held:bool}
impl std::ops::Deref for LockedControl{type Target=File;fn deref(&self)->&File{&self.file}}
impl std::ops::DerefMut for LockedControl{fn deref_mut(&mut self)->&mut File{&mut self.file}}
impl LockedControl{fn release(&mut self)->std::io::Result<()>{if self.held{self.file.unlock()?;self.held=false;}Ok(())}}
impl Drop for LockedControl{fn drop(&mut self){let _=self.release();}}
/// Holds the actual flock descriptor through physical cleanup and all effects.
pub(super) struct Lease{control:LockedControl,directory:PathBuf,uid:u32,slot:usize,record:Record}
impl Lease{
 pub(super) fn acquire(directory:&Path,max:u64)->Result<Self,Error>{
  if !cfg!(target_os="linux"){return Err(Error::Unsupported)}
  let directory_meta=std::fs::symlink_metadata(directory).map_err(|_|Error::Filesystem)?;
  if !directory_meta.is_dir()||directory_meta.file_type().is_symlink(){return Err(Error::Unsupported)}
  let uid=directory_meta.uid();let path=directory.join(NAME);
  let(new,control)=match open().create_new(true).open(&path){Ok(f)=>(true,f),Err(e)if e.kind()==std::io::ErrorKind::AlreadyExists=>(false,open().open(&path).map_err(|_|Error::Filesystem)?),Err(_)=>return Err(Error::Filesystem)};
  let meta=checked(&path,uid,LENGTH as u64)?;if !same(&meta,&control.metadata().map_err(|_|Error::Filesystem)?){return Err(Error::Unsupported)}
  match control.try_lock(){Ok(())=>{},Err(std::fs::TryLockError::WouldBlock)=>return Err(Error::Capacity),Err(_)=>return Err(Error::Filesystem)}
  let mut control=LockedControl{file:control,held:true};
  if new{
   // Bootstrap is never retried for an existing empty/partial control.
   let mut bytes=[0;LENGTH];bytes[..8].copy_from_slice(MAGIC);let idle=encode(Record{generation:1,id:None})?;bytes[8..72].copy_from_slice(&idle);bytes[72..136].copy_from_slice(&idle);
   control.write_all(&bytes).map_err(|_|Error::Filesystem)?;control.sync_all().map_err(|_|Error::Filesystem)?;
  }
  if control.metadata().map_err(|_|Error::Filesystem)?.len()!=LENGTH as u64{return Err(Error::Unsupported)}
  control.seek(SeekFrom::Start(0)).map_err(|_|Error::Filesystem)?;let mut bytes=[0;LENGTH];control.read_exact(&mut bytes).map_err(|_|Error::Filesystem)?;let(slot,record)=select(&bytes)?;
  let mut lease=Self{control,directory:directory.into(),uid,slot,record};
  // Validate complete bounded directory BEFORE deleting anything.
  let expected=record.id.map(|id|lease.path(id));let mut found=false;let mut count=0;
  for entry in std::fs::read_dir(directory).map_err(|_|Error::Filesystem)?{
   count+=1;if count>2{return Err(Error::Unsupported)}let path=entry.map_err(|_|Error::Filesystem)?.path();
   if path==lease.directory.join(NAME){let m=checked(&path,uid,LENGTH as u64)?;if !same(&m,&lease.control.metadata().map_err(|_|Error::Filesystem)?){return Err(Error::Unsupported)}}
   else if expected.as_ref()==Some(&path){checked(&path,uid,max)?;found=true}else{return Err(Error::Unsupported)}
  }
  if found{lease.remove(expected.as_ref().ok_or(Error::Operational)?,max)?}
  if record.id.is_some(){lease.write(None)?}
  Ok(lease)
 }
 fn path(&self,id:uuid::Uuid)->PathBuf{self.directory.join(format!(".rust-owned-logo-spool-v2-{id}"))}
 fn write(&mut self,id:Option<uuid::Uuid>)->Result<(),Error>{
  if !self.control.held{return Err(Error::Operational)}
  let record=Record{generation:self.record.generation.checked_add(1).ok_or(Error::Unsupported)?,id};let bytes=encode(record)?;let slot=1-self.slot;
  self.control.seek(SeekFrom::Start((8+64*slot)as u64)).map_err(|_|Error::Filesystem)?;self.control.write_all(&bytes).map_err(|_|Error::Filesystem)?;self.control.sync_all().map_err(|_|Error::Filesystem)?;
  self.record=record;self.slot=slot;Ok(())
 }
 pub(super) fn create(&mut self)->Result<(PathBuf,File),Error>{
  if !self.control.held{return Err(Error::Operational)}
  if self.record.id.is_some(){return Err(Error::Operational)}let id=uuid::Uuid::new_v4();self.write(Some(id))?;let path=self.path(id);let file=open().create_new(true).open(&path).map_err(|_|Error::Filesystem)?;
  let m=checked(&path,self.uid,0)?;if !same(&m,&file.metadata().map_err(|_|Error::Filesystem)?){return Err(Error::Unsupported)}Ok((path,file))
 }
 fn remove(&self,path:&Path,max:u64)->Result<(),Error>{
  if self.record.id.map(|id|self.path(id)).as_deref()!=Some(path){return Err(Error::Unsupported)}
  let before=checked(path,self.uid,max)?;let file=open().open(path).map_err(|_|Error::Filesystem)?;let after=file.metadata().map_err(|_|Error::Filesystem)?;if !same(&before,&after)||!same(&before,&checked(path,self.uid,max)?){return Err(Error::Unsupported)}drop(file);std::fs::remove_file(path).map_err(|_|Error::Filesystem)
 }
 pub(super) fn finish(&mut self,path:&Path,max:u64)->Result<(),Error>{self.remove(path,max)?;self.write(None)?;self.control.release().map_err(|_|Error::Filesystem)}
 pub(super) fn aliases(&self,meta:&Metadata)->Result<bool,Error>{Ok(same(meta,&self.control.metadata().map_err(|_|Error::Filesystem)?))}
}

#[cfg(test)]mod tests{
 use super::*;
 #[test]fn bounded_slots_reject_corruption_and_generation_conflicts(){
  let a=Record{generation:7,id:None};let b=Record{generation:8,id:Some(uuid::Uuid::new_v4())};let aa=encode(a).unwrap();let bb=encode(b).unwrap();assert_eq!(decode(&aa),Some(a));assert_eq!(decode(&bb),Some(b));
  for i in 0..64{let mut bad=aa;bad[i]^=1;assert_eq!(decode(&bad),None)}
  let mut bytes=[0;LENGTH];bytes[..8].copy_from_slice(MAGIC);bytes[8..72].copy_from_slice(&aa);bytes[72..136].copy_from_slice(&bb);assert_eq!(select(&bytes).unwrap(),(1,b));
  for end in 0..=64{let mut partial=bytes;partial[72..136].copy_from_slice(&aa);partial[72..72+end].copy_from_slice(&bb[..end]);let selected=select(&partial).unwrap().1;assert!(selected==a||selected==b)}
  bytes[72..136].copy_from_slice(&encode(Record{generation:10,id:None}).unwrap());assert!(select(&bytes).is_err());assert!(encode(Record{generation:0,id:None}).is_err());
 }
}

#[cfg(test)]mod process_tests{
 use super::*;
 use std::{process::{Command,Stdio},time::{Duration,Instant}};
 struct Child(std::process::Child);
 impl std::ops::Deref for Child{type Target=std::process::Child;fn deref(&self)->&Self::Target{&self.0}}
 impl std::ops::DerefMut for Child{fn deref_mut(&mut self)->&mut Self::Target{&mut self.0}}
 impl Drop for Child{fn drop(&mut self){let _=self.0.kill();let _=self.0.wait();}}
 struct Fixture(PathBuf);
 impl Fixture{fn new()->Self{let root=std::env::temp_dir().join(format!("rust-owned-scratch-recovery-{}",uuid::Uuid::new_v4()));std::fs::create_dir(&root).unwrap();Self(root)}}
 impl Drop for Fixture{fn drop(&mut self){std::fs::remove_dir_all(&self.0).unwrap()}}
 #[test]#[ignore]
 fn recovery_child(){
  let Some(root)=std::env::var_os("RUST_OWNED_SCRATCH_CHILD")else{return};let root=PathBuf::from(root);let scratch=root.join("scratch");let mode=std::env::var("RUST_OWNED_SCRATCH_MODE").unwrap();let mut lease=Lease::acquire(&scratch,262144).unwrap();
  if mode=="record-before-create"{lease.write(Some(uuid::Uuid::new_v4())).unwrap()}
  else{let(path,mut file)=lease.create().unwrap();file.write_all(b"owned incomplete spool").unwrap();file.sync_all().unwrap();drop(file);
   if mode=="after-unlink"{std::fs::remove_file(path).unwrap()}
   if mode=="partial-next-slot"{let record=encode(Record{generation:lease.record.generation+1,id:None}).unwrap();let slot=1-lease.slot;lease.control.seek(SeekFrom::Start((8+64*slot)as u64)).unwrap();lease.control.write_all(&record[..17]).unwrap();lease.control.sync_all().unwrap()}
  }
  std::fs::write(root.join("ready"),b"actual lock and state settled").unwrap();loop{std::thread::sleep(Duration::from_secs(1))}
 }
 #[test]
 fn actual_active_owner_and_sigkill_windows_recover_only_recorded_scratch(){
  use std::os::unix::process::ExitStatusExt;
  for mode in ["partial-scratch","record-before-create","after-unlink","partial-next-slot"]{
   let f=Fixture::new();let scratch=f.0.join("scratch");std::fs::create_dir(&scratch).unwrap();let mut child=Child(Command::new(std::env::current_exe().unwrap()).args(["logo_scanner::spool::scratch::process_tests::recovery_child","--exact","--ignored","--nocapture"]).env("RUST_OWNED_SCRATCH_CHILD",&f.0).env("RUST_OWNED_SCRATCH_MODE",mode).stdout(Stdio::null()).stderr(Stdio::inherit()).spawn().unwrap());
   let deadline=Instant::now()+Duration::from_secs(10);while !f.0.join("ready").exists(){if let Some(status)=child.try_wait().unwrap(){panic!("Child exited before witness: {status}")}assert!(Instant::now()<deadline);std::thread::sleep(Duration::from_millis(5))}
   assert!(matches!(Lease::acquire(&scratch,262144),Err(Error::Capacity)));child.kill().unwrap();assert_eq!(child.wait().unwrap().signal(),Some(9));
   let mut fresh=Lease::acquire(&scratch,262144).unwrap();assert!(fresh.record.id.is_none());assert_eq!(std::fs::read_dir(&scratch).unwrap().count(),1);
   let(path,mut file)=fresh.create().unwrap();file.write_all(b"fresh owned run").unwrap();drop(file);fresh.finish(&path,262144).unwrap();drop(fresh);assert_eq!(std::fs::read_dir(&scratch).unwrap().count(),1);
  }
 }
 #[test]
 fn unknown_and_invalid_owned_shapes_refuse_without_any_deletion(){
  use std::os::unix::fs::{symlink,PermissionsExt};
  for mode in ["foreign","v1","symlink","hardlink","mode","oversize","partial-bootstrap"]{
   let f=Fixture::new();let scratch=f.0.join("scratch");std::fs::create_dir(&scratch).unwrap();
   if mode=="partial-bootstrap"{std::fs::write(scratch.join(NAME),b"LSPOOL").unwrap();std::fs::set_permissions(scratch.join(NAME),std::fs::Permissions::from_mode(0o600)).unwrap()}
   else{let mut lease=Lease::acquire(&scratch,262144).unwrap();let(path,file)=lease.create().unwrap();drop(file);match mode{
    "foreign"=>std::fs::write(scratch.join("foreign"),b"retain").unwrap(),
    "v1"=>std::fs::write(scratch.join(".rust-owned-logo-spool-foreign"),b"retain").unwrap(),
    "symlink"=>{std::fs::remove_file(&path).unwrap();symlink(f.0.join("outside"),&path).unwrap()},
    "hardlink"=>std::fs::hard_link(&path,f.0.join("outside")).unwrap(),
    "mode"=>std::fs::set_permissions(&path,std::fs::Permissions::from_mode(0o644)).unwrap(),
    "oversize"=>OpenOptions::new().write(true).open(&path).unwrap().set_len(262145).unwrap(),_=>unreachable!()};drop(lease)}
   let before=std::fs::read_dir(&scratch).unwrap().count();assert!(Lease::acquire(&scratch,262144).is_err());assert_eq!(std::fs::read_dir(&scratch).unwrap().count(),before);
  }
 }
 #[test]
 fn settled_cleanup_releases_and_stale_lease_cannot_reenter(){
  let f=Fixture::new();let mut lease=Lease::acquire(&f.0,262144).unwrap();let(path,mut file)=lease.create().unwrap();file.write_all(b"owned completed spool").unwrap();drop(file);
  assert!(matches!(Lease::acquire(&f.0,262144),Err(Error::Capacity)));lease.finish(&path,262144).unwrap();assert!(!path.exists());assert!(lease.record.id.is_none());assert!(!lease.control.held);
  let fresh=Lease::acquire(&f.0,262144).unwrap();assert!(matches!(lease.create(),Err(Error::Operational)));assert!(matches!(lease.write(Some(uuid::Uuid::new_v4())),Err(Error::Operational)));drop(lease);assert!(matches!(Lease::acquire(&f.0,262144),Err(Error::Capacity)));drop(fresh);assert!(Lease::acquire(&f.0,262144).is_ok());
 }
 #[test]
 fn stable_control_inode_and_same_process_contention(){
  let f=Fixture::new();let mut lease=Lease::acquire(&f.0,262144).unwrap();let inode=std::fs::metadata(f.0.join(NAME)).unwrap().ino();assert!(matches!(Lease::acquire(&f.0,262144),Err(Error::Capacity)));
  let(path,file)=lease.create().unwrap();drop(file);lease.finish(&path,262144).unwrap();assert_eq!(std::fs::metadata(f.0.join(NAME)).unwrap().ino(),inode);drop(lease);let result=Lease::acquire(&f.0,262144);if let Err(error)=&result{eprintln!("REACQUIRE_TYPED_ERROR={error:?}");}assert!(result.is_ok());
 }
}

#[cfg(test)]mod inheritance_diagnostic{
 use super::*;
 use std::{io::{Read,Write},os::{fd::AsRawFd,unix::process::CommandExt},process::{Command,Stdio},time::Duration};
 unsafe extern "C"{fn read(fd:i32,bytes:*mut std::ffi::c_void,n:usize)->isize;fn write(fd:i32,bytes:*const std::ffi::c_void,n:usize)->isize;fn getpid()->i32;fn close(fd:i32)->i32;}
 #[test]
 fn explicit_release_preserves_active_guard_then_releases_despite_inherited_descriptor(){
  let root=std::env::temp_dir().join(format!("rust-owned-flock-inherited-{}",uuid::Uuid::new_v4()));std::fs::create_dir(&root).unwrap();let lease=Lease::acquire(&root,262144).unwrap();let fd=lease.control.as_raw_fd();let original=lease.control.metadata().unwrap();
  let(mut ready_parent,ready_child)=std::os::unix::net::UnixStream::pair().unwrap();let(mut gate_parent,gate_child)=std::os::unix::net::UnixStream::pair().unwrap();ready_parent.set_read_timeout(Some(Duration::from_secs(5))).unwrap();gate_parent.set_write_timeout(Some(Duration::from_secs(5))).unwrap();let unused_ready=ready_parent.as_raw_fd();let unused_gate=gate_parent.as_raw_fd();let ready_fd=ready_child.as_raw_fd();let gate_fd=gate_child.as_raw_fd();
  let(done,finished)=std::sync::mpsc::channel();
  let thread=std::thread::spawn(move||{
   let mut command=Command::new("/bin/true");command.stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null());
   // Test-only pre_exec uses only async-signal-safe POSIX calls. No allocation,
   // Rust mutex, formatting, managed service or runtime callback in the child.
   unsafe{command.pre_exec(move||{close(unused_ready);close(unused_gate);let pid=getpid().to_ne_bytes();if write(ready_fd,pid.as_ptr().cast(),pid.len())!=pid.len()as isize{return Err(std::io::Error::last_os_error())}let mut release=0u8;if read(gate_fd,(&mut release as*mut u8).cast(),1)!=1{return Err(std::io::Error::last_os_error())}Ok(())});}
   let child=command.spawn();drop(ready_child);drop(gate_child);let result=child.and_then(|mut child|{let deadline=std::time::Instant::now()+Duration::from_secs(5);loop{if let Some(exit)=child.try_wait()?{return Ok(exit)}
   if std::time::Instant::now()>=deadline{child.kill()?;return child.wait()}std::thread::sleep(Duration::from_millis(1));}});let _=done.send(result);
  });
  let mut pid=[0u8;4];let ready=ready_parent.read_exact(&mut pid);if ready.is_err(){let _=gate_parent.write_all(b"x");let _=finished.recv_timeout(Duration::from_secs(10));drop(thread);panic!("Controlled child did not reach pre_exec: {ready:?}")}
  let pid=i32::from_ne_bytes(pid);let active=matches!(Lease::acquire(&root,262144),Err(Error::Capacity));drop(lease);
  let parent_descriptor=std::fs::metadata(format!("/proc/self/fd/{fd}"));let parent_closed=parent_descriptor.as_ref().map_or(true,|m|!same(m,&original));let fd_number_reused=parent_descriptor.as_ref().is_ok_and(|m|!same(m,&original));let inherited=std::fs::metadata(format!("/proc/{pid}/fd/{fd}")).is_ok_and(|m|same(&m,&original));let reacquire=Lease::acquire(&root,262144);let released=reacquire.is_ok();let typed=match &reacquire{Ok(_)=>"Ok".into(),Err(e)=>format!("{e:?}")};drop(reacquire);
  // Always release/wait the owned child before assertions, including failure.
  gate_parent.write_all(b"x").unwrap();let exit=finished.recv_timeout(Duration::from_secs(10)).expect("owned fork/exec cleanup deadline").unwrap();drop(thread);let after=Lease::acquire(&root,262144);let after_ok=after.is_ok();drop(after);
  eprintln!("INHERITED_FLOCK_WITNESS parent_control_fd_gone={parent_closed} fd_number_reused={fd_number_reused} child_control_fd_open={inherited} active_guard_denied={active} typed_reacquire={typed} post_exec_reacquire={after_ok} child_exit={exit}");std::fs::remove_dir_all(&root).unwrap();assert!(parent_closed&&inherited&&active&&released&&after_ok&&exit.success());
 }
}
