//! UNREGISTERED / UNCOMPILED draft: bounded original v2 database-only backups.
//! Original Dispatcharr authors retain full application credit and AGPL license.
//! No HTTP, durable jobs, migrations, SQLite, timezone resolution or app wiring.
//! Restore is intentionally restricted to explicitly owned disposable DB names.
use serde::{Deserialize,Serialize};
use std::{collections::BTreeMap,fs::{self,File,OpenOptions},io::{self,Read,Seek,SeekFrom,Write},path::{Component,Path,PathBuf},process::{Child,Command,ExitStatus,Stdio},sync::{Arc,atomic::{AtomicBool,Ordering}},thread,time::{Duration,Instant}};
use zip::{ZipArchive,ZipWriter,write::SimpleFileOptions};

#[derive(Clone)]
pub struct Limits {
    pub database_bytes:u64,
    pub archive_bytes:u64,
    pub central_directory_bytes:u64,
    pub entries:u64,
    pub metadata_bytes:u64,
    pub diagnostic_bytes:usize,
    pub command_timeout:Duration,
}
impl Default for Limits {
    fn default()->Self{Self{database_bytes:128<<30,archive_bytes:128<<30,central_directory_bytes:8<<20,entries:128,metadata_bytes:64<<10,diagnostic_bytes:64<<10,command_timeout:Duration::from_secs(3600)}}
}
impl Limits {
    fn validate(&self)->Result<(),Error>{
        if self.database_bytes==0||self.archive_bytes==0||self.metadata_bytes==0||self.metadata_bytes>1<<20
            ||self.central_directory_bytes==0||self.central_directory_bytes>16<<20||self.entries==0||self.entries>1024
            ||self.diagnostic_bytes>1<<20||self.command_timeout.is_zero()||self.command_timeout>Duration::from_secs(86400)
            ||self.database_bytes>i64::MAX as u64||self.archive_bytes>i64::MAX as u64 {return Err(Error::Capacity)}
        Ok(())
    }
}
#[derive(Clone)]
pub struct PgConfig {
    pub host:String,pub port:u16,pub user:String,pub database:String,
    pub password:Option<String>,
    /// Allowed keys: sslmode, sslrootcert, sslcert, sslkey. No secret Debug output.
    pub options:BTreeMap<String,String>,
}
#[derive(Clone)]
pub struct Runtime {
    pub pg_dump:PathBuf,pub pg_restore:PathBuf,pub psql:PathBuf,
}
#[derive(Debug)]
pub enum Error {Io,Zip,Metadata,Capacity,Busy,Cancelled,Timeout,Command{stage:&'static str,code:Option<i32>},WrongDatabaseType,UnsafeMember,FixtureAuthorization,WorkerPanic}
impl From<io::Error> for Error {fn from(_:io::Error)->Self{Self::Io}}
impl From<zip::result::ZipError> for Error {fn from(_:zip::result::ZipError)->Self{Self::Zip}}
#[derive(Serialize,Deserialize)]
struct Metadata {
    format:String,version:u32,database_type:String,database_file:String,created_at:String,
}
/// All clocks are supplied by a future Source-compatible timezone resolver.
/// The archive component does not pretend host localtime reproduces pytz.
pub trait SourceClock {
    fn filename_timestamp(&self)->Result<String,Error>;
    fn created_at_utc(&self)->Result<String,Error>;
    fn zip_timestamp(&self,time:SystemTime)->Result<zip::DateTime,Error>;
}
#[derive(Clone,Default)]
pub struct Cancellation(Arc<AtomicBool>);
impl Cancellation {pub fn cancel(&self){self.0.store(true,Ordering::Release)}fn check(&self)->Result<(),Error>{if self.0.load(Ordering::Acquire){Err(Error::Cancelled)}else{Ok(())}}}
/// One owned worker per service. Dropping its join handle does not cancel it;
/// explicit cancellation does. Durable job persistence is a separate component.
#[derive(Clone,Default)]
pub struct Admission(Arc<AtomicBool>);
struct Permit(Arc<AtomicBool>);
impl Drop for Permit {fn drop(&mut self){self.0.store(false,Ordering::Release)}}
impl Admission {
    pub fn submit<T:Send+'static>(&self,job:impl FnOnce()->Result<T,Error>+Send+'static)->Result<thread::JoinHandle<Result<T,Error>>,Error>{
        self.0.compare_exchange(false,true,Ordering::AcqRel,Ordering::Acquire).map_err(|_|Error::Busy)?;
        let permit=Permit(self.0.clone());
        thread::Builder::new().name("dispatcharr-backup".into()).spawn(move||{let _permit=permit;job()}).map_err(|_|Error::Io)
    }
}
/// A draft restore may only destroy its explicitly owned disposable database.
/// Promotion to a production restore authority requires separate API/job gates.
pub struct DisposableRestore {database:String}
impl DisposableRestore {
    pub fn authorize(database:&str)->Result<Self,Error>{
        if !database.starts_with("rust_probe_backup_")||database.len()>63||!database.bytes().all(|b|b.is_ascii_lowercase()||b.is_ascii_digit()||b==b'_'){return Err(Error::FixtureAuthorization)}
        Ok(Self{database:database.into()})
    }
}
struct OwnedDirectory(PathBuf);
impl OwnedDirectory {
    fn new(root:&Path)->Result<Self,Error>{
        fs::create_dir_all(root)?;
        for _ in 0..8 {
            let path=root.join(format!("dispatcharr-backup-{}",uuid::Uuid::new_v4()));
            let mut directory=fs::DirBuilder::new();
            #[cfg(unix)]{use std::os::unix::fs::DirBuilderExt;directory.mode(0o700);}
            match directory.create(&path){Ok(())=>{
                return Ok(Self(path))
            },Err(error)if error.kind()==io::ErrorKind::AlreadyExists=>{},Err(_)=>return Err(Error::Io)}
        }
        Err(Error::Io)
    }
}
impl Drop for OwnedDirectory {fn drop(&mut self){let _=fs::remove_dir_all(&self.0);}}

/// Diagnostics are retained privately and bounded; public summaries expose only
/// stage/status/byte counts. Never serialize stderr or DB config into evidence.
struct Diagnostic {prefix:Vec<u8>,bytes:u64,critical:bool,utf8_valid:bool}
fn drain(mut reader:impl Read,limit:usize)->io::Result<Diagnostic>{
    let mut output=Diagnostic{prefix:Vec::with_capacity(limit.min(8192)),bytes:0,critical:false,utf8_valid:true};
    let mut buffer=[0u8;8192];let mut search_tail=Vec::new();let mut utf8_tail=Vec::new();
    loop {
        let count=match reader.read(&mut buffer){Err(error)if error.kind()==io::ErrorKind::Interrupted=>continue,other=>other?};
        if count==0{break}
        let chunk=&buffer[..count];output.bytes=output.bytes.saturating_add(count as u64);
        output.prefix.extend_from_slice(&chunk[..chunk.len().min(limit.saturating_sub(output.prefix.len()))]);
        search_tail.extend(chunk.iter().map(u8::to_ascii_lowercase));
        output.critical|=search_tail.windows(5).any(|v|v==b"fatal")||search_tail.windows(17).any(|v|v==b"could not connect");
        if search_tail.len()>16{search_tail.drain(..search_tail.len()-16);}
        if output.utf8_valid {
            utf8_tail.extend_from_slice(chunk);
            match std::str::from_utf8(&utf8_tail){Ok(_)=>utf8_tail.clear(),Err(error)if error.error_len().is_none()=>{utf8_tail.drain(..error.valid_up_to());},Err(_)=>{output.utf8_valid=false;utf8_tail.clear();}}
        }
    }
    output.utf8_valid&=utf8_tail.is_empty();Ok(output)
}
struct ChildOwner {child:Child,cleaned:bool}
impl ChildOwner {
    fn kill_and_wait(&mut self){
        #[cfg(unix)]unsafe{libc::kill(-(self.child.id()as i32),libc::SIGKILL);}
        let _=self.child.kill();let _=self.child.wait();self.cleaned=true;
    }
}
impl Drop for ChildOwner {fn drop(&mut self){if !self.cleaned{self.kill_and_wait()}}}
struct CommandOutcome {status:ExitStatus,stderr:Diagnostic}
fn connection_args(config:&PgConfig)->Vec<String>{vec!["-h".into(),config.host.clone(),"-p".into(),config.port.to_string(),"-U".into(),config.user.clone(),"-d".into(),config.database.clone()]}
fn prepare_command(binary:&Path,config:&PgConfig,args:&[String],file_limit:u64)->Command {
    let mut command=Command::new(binary);command.args(args).stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    for key in ["PGPASSWORD","PGSSLMODE","PGSSLROOTCERT","PGSSLCERT","PGSSLKEY"]{command.env_remove(key);}
    if let Some(password)=config.password.as_ref().filter(|v|!v.is_empty()){command.env("PGPASSWORD",password);}
    for(key,env)in [("sslmode","PGSSLMODE"),("sslrootcert","PGSSLROOTCERT"),("sslcert","PGSSLCERT"),("sslkey","PGSSLKEY")]{
        if let Some(value)=config.options.get(key).filter(|v|!v.is_empty()){command.env(env,value);}
    }
    #[cfg(unix)]{
        use std::os::unix::process::CommandExt;
        command.process_group(0);
        // Only async-signal-safe libc call in the child before exec. Child file
        // growth is hard-limited rather than checked after a large write.
        unsafe{command.pre_exec(move||{
            let limit=libc::rlimit{rlim_cur:file_limit as libc::rlim_t,rlim_max:file_limit as libc::rlim_t};
            if libc::setrlimit(libc::RLIMIT_FSIZE,&limit)!=0{return Err(io::Error::last_os_error())}Ok(())
        });}
    }
    command
}
fn run(mut command:Command,limits:&Limits,cancel:&Cancellation)->Result<CommandOutcome,Error>{
    cancel.check()?;
    let mut child=ChildOwner{child:command.spawn()?,cleaned:false};
    let stdout=child.child.stdout.take().ok_or(Error::Io)?;let stderr=child.child.stderr.take().ok_or(Error::Io)?;
    let cap=limits.diagnostic_bytes;
    let out=thread::spawn(move||drain(stdout,cap));let err=thread::spawn(move||drain(stderr,cap));
    let deadline=Instant::now()+limits.command_timeout;
    let status=loop {
        if let Err(error)=cancel.check(){child.kill_and_wait();let _=out.join();let _=err.join();return Err(error)}
        if Instant::now()>=deadline{child.kill_and_wait();let _=out.join();let _=err.join();return Err(Error::Timeout)}
        match child.child.try_wait(){Ok(Some(status))=>break status,Ok(None)=>thread::sleep(Duration::from_millis(20)),Err(_)=>{child.kill_and_wait();let _=out.join();let _=err.join();return Err(Error::Io)}}
    };
    // Reap/kill the owned process group before joining pipe readers, including
    // any descendant that inherited a pipe after the leader exited.
    child.kill_and_wait();
    let stdout=out.join().map_err(|_|Error::WorkerPanic)??;let stderr=err.join().map_err(|_|Error::WorkerPanic)??;
    if !stdout.utf8_valid||!stderr.utf8_valid{return Err(Error::Io)}
    Ok(CommandOutcome{status,stderr})
}
fn checked(outcome:CommandOutcome,stage:&'static str)->Result<(),Error>{if outcome.status.success(){Ok(())}else{Err(Error::Command{stage,code:outcome.status.code()})}}
struct LimitedFile {file:File,position:u64,limit:u64}
impl Write for LimitedFile {
    fn write(&mut self,bytes:&[u8])->io::Result<usize>{
        if bytes.len()as u64>self.limit.saturating_sub(self.position){return Err(io::Error::other("archive capacity"))}
        let count=self.file.write(bytes)?;self.position+=count as u64;Ok(count)
    }
    fn flush(&mut self)->io::Result<()>{self.file.flush()}
}
impl Seek for LimitedFile {fn seek(&mut self,pos:SeekFrom)->io::Result<u64>{let offset=self.file.seek(pos)?;if offset>self.limit{return Err(io::Error::other("archive capacity"))}self.position=offset;Ok(offset)}}
fn copy(mut reader:impl Read,mut writer:impl Write,limit:u64,cancel:&Cancellation)->Result<u64,Error>{
    let mut buffer=[0u8;8192];let mut bytes=0_u64;
    loop{cancel.check()?;let count=reader.read(&mut buffer)?;if count==0{return Ok(bytes)}bytes=bytes.checked_add(count as u64).ok_or(Error::Capacity)?;if bytes>limit{return Err(Error::Capacity)}writer.write_all(&buffer[..count])?;}
}
/// Successful archives remain DB-only v2; compressed ZIP bytes need not match
/// Python's deflater. Current bounded/malformed input limits are explicit deltas.
pub fn create_backup(config:&PgConfig,runtime:&Runtime,backup_root:&Path,scratch_root:&Path,clock:&dyn SourceClock,limits:&Limits,cancel:&Cancellation)->Result<PathBuf,Error>{
    limits.validate()?;cancel.check()?;
    let timestamp=clock.filename_timestamp()?;
    if timestamp.len()!=19||!timestamp.bytes().all(|v|v.is_ascii_digit()||v==b'.'){return Err(Error::Metadata)}
    fs::create_dir_all(backup_root)?;let scratch=OwnedDirectory::new(scratch_root)?;
    let dump=scratch.0.join("database.dump");
    let mut args=connection_args(config);args.extend(["-Fc".into(),"-v".into(),"-f".into(),dump.to_string_lossy().into_owned()]);
    checked(run(prepare_command(&runtime.pg_dump,config,&args,limits.database_bytes),limits,cancel)?,"pg_dump")?;
    if fs::metadata(&dump)?.len()>limits.database_bytes{return Err(Error::Capacity)}
    let destination=backup_root.join(format!("dispatcharr-backup-{timestamp}.zip"));
    // Source opens directly with truncate semantics; no atomic rename invented.
    let file=OpenOptions::new().write(true).create(true).truncate(true).open(&destination)?;
    let mut zip=ZipWriter::new(LimitedFile{file,position:0,limit:limits.archive_bytes});
    let options=SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated).large_file(true)
        .last_modified_time(clock.zip_timestamp(fs::metadata(&dump)?.modified()?)?);
    zip.start_file("database.dump",options)?;copy(File::open(dump)?,&mut zip,limits.database_bytes,cancel)?;
    let created_at=clock.created_at_utc()?;if created_at.len()>128{return Err(Error::Metadata)}
    let metadata=Metadata{format:"dispatcharr-backup".into(),version:2,database_type:"postgresql".into(),database_file:"database.dump".into(),created_at};
    let data=serde_json::to_vec_pretty(&metadata).map_err(|_|Error::Metadata)?;
    if data.len()as u64>limits.metadata_bytes{return Err(Error::Capacity)}
    zip.start_file("metadata.json",options.last_modified_time(clock.zip_timestamp(SystemTime::now())?))?;zip.write_all(&data)?;zip.finish()?.file.sync_all()?;Ok(destination)
}
/// Check the final ZIP directory before a library can allocate from it.
/// ZIP64 supported; split archives and oversized metadata deliberately reject.
fn preflight(file:&mut File,limits:&Limits)->Result<(),Error>{
    let size=file.metadata()?.len();if size>limits.archive_bytes{return Err(Error::Capacity)}
    let tail_len=size.min(65557)as usize;file.seek(SeekFrom::End(-(tail_len as i64)))?;let mut tail=vec![0;tail_len];file.read_exact(&mut tail)?;
    let index=(0..tail.len().saturating_sub(21)).rev().find(|&i|tail[i..].starts_with(b"PK\x05\x06")&&i+22+u16::from_le_bytes([tail[i+20],tail[i+21]])as usize==tail.len()).ok_or(Error::Zip)?;
    let end=&tail[index..];let u16_at=|at|u16::from_le_bytes([end[at],end[at+1]]);let u32_at=|at|u32::from_le_bytes(end[at..at+4].try_into().unwrap());
    if u16_at(4)!=0||u16_at(6)!=0||u16_at(8)!=u16_at(10){return Err(Error::Zip)}
    let(mut entries,mut directory_size,mut offset)=(u64::from(u16_at(10)),u64::from(u32_at(12)),u64::from(u32_at(16)));
    if entries==65535||directory_size==u32::MAX as u64||offset==u32::MAX as u64 {
        let absolute=size-tail_len as u64+index as u64;if absolute<20{return Err(Error::Zip)}
        file.seek(SeekFrom::Start(absolute-20))?;let mut locator=[0;20];file.read_exact(&mut locator)?;
        if &locator[..4]!=b"PK\x06\x07"||locator[4..8]!=[0;4]||u32::from_le_bytes(locator[16..20].try_into().unwrap())!=1{return Err(Error::Zip)}
        file.seek(SeekFrom::Start(u64::from_le_bytes(locator[8..16].try_into().unwrap())))?;let mut record=[0;56];file.read_exact(&mut record)?;
        if &record[..4]!=b"PK\x06\x06"||record[16..24]!=[0;8]||record[24..32]!=record[32..40]{return Err(Error::Zip)}
        entries=u64::from_le_bytes(record[32..40].try_into().unwrap());directory_size=u64::from_le_bytes(record[40..48].try_into().unwrap());offset=u64::from_le_bytes(record[48..56].try_into().unwrap());
    }
    if entries>limits.entries||directory_size>limits.central_directory_bytes||offset.checked_add(directory_size).is_none_or(|end|end>size){return Err(Error::Capacity)}
    file.rewind()?;Ok(())
}
/// Draft guarded restore. A completed return does NOT include migrations or
/// establish application/job completion. Source runs Django migrate afterward.
pub fn restore_fixture(config:&PgConfig,runtime:&Runtime,archive:&Path,scratch_root:&Path,limits:&Limits,cancel:&Cancellation,authority:&DisposableRestore)->Result<(),Error>{
    limits.validate()?;cancel.check()?;
    if authority.database!=config.database{return Err(Error::FixtureAuthorization)}
    let scratch=OwnedDirectory::new(scratch_root)?;let mut file=File::open(archive)?;preflight(&mut file,limits)?;
    let mut zip=ZipArchive::new(file)?;
    let mut metadata=Vec::new();copy(zip.by_name("metadata.json")?,&mut metadata,limits.metadata_bytes,cancel)?;
    // Preserve Source's missing database_type/database_file defaults, and do
    // not suddenly require metadata.format/version on legacy valid archives.
    let value:serde_json::Value=serde_json::from_slice(&metadata).map_err(|_|Error::Metadata)?;
    let object=value.as_object().ok_or(Error::Metadata)?;
    let member=object.get("database_file").map_or(Some("database.dump"),serde_json::Value::as_str).ok_or(Error::Metadata)?;
    if member.is_empty()||Path::new(member).components().any(|part|!matches!(part,Component::Normal(_))){return Err(Error::UnsafeMember)}
    let dump=scratch.0.join("database.dump");let mut output=OpenOptions::new().write(true).create_new(true).open(&dump)?;
    copy(zip.by_name(member)?,&mut output,limits.database_bytes,cancel)?;output.sync_all()?;
    let kind=object.get("database_type").map_or(Some("postgresql"),serde_json::Value::as_str).ok_or(Error::Metadata)?;
    if kind!="postgresql"{return Err(Error::WrongDatabaseType)}
    let mut args=connection_args(config);args.extend(["-c".into(),"DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public; GRANT ALL ON SCHEMA public TO public;".into()]);
    checked(run(prepare_command(&runtime.psql,config,&args,limits.database_bytes),limits,cancel)?,"psql schema cleanup")?;
    let mut args=vec!["--no-owner".into()];args.extend(connection_args(config));args.extend(["-v".into(),dump.to_string_lossy().into_owned()]);
    let restored=run(prepare_command(&runtime.pg_restore,config,&args,limits.database_bytes),limits,cancel)?;
    if !restored.status.success()&&restored.stderr.critical{return Err(Error::Command{stage:"pg_restore",code:restored.status.code()})}
    // Other nonzero pg_restore exits are Source-compatible warnings, not proof
    // of a complete DB restore. Real copied-data manifests remain mandatory.
    Ok(())
}
