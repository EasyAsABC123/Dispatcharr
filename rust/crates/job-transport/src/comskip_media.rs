//! Real manual Comskip mark processing, owned by the existing DVR process guard.
//! Original Dispatcharr/Comskip/FFmpeg authors retain algorithm/application credit.
use crate::recording_process::{self, Transcript};
use dispatcharr_contracts::{BoxFuture, UpdateEventSink};
use dispatcharr_store::{
    comskip_task::{self as task, Media, Outcome, Owner, Work},
    StoreError,
};
use serde_json::{json, Value};
use std::{
    fs::{self, File, OpenOptions},
    io::Read,
    os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
    sync::{atomic::AtomicBool, Arc},
};
pub(crate) struct ActualMedia {
    pub shutdown: Arc<AtomicBool>,
    pub events: Arc<dyn UpdateEventSink>,
    pub transcript: Arc<Transcript>,
}
fn unsupported() -> StoreError {
    StoreError::Unsupported("Comskip media ownership or bounded mark domain unavailable")
}
struct FileOwner {
    _file: File,
    _directory: File,
    path: PathBuf,
    dev: u64,
    ino: u64,
}
impl FileOwner {
    fn open(path: &Path) -> Result<Self, StoreError> {
        let canonical = fs::canonicalize(path).map_err(|_| unsupported())?;
        let root = fs::canonicalize("/data").map_err(|_| unsupported())?;
        if !canonical.starts_with(&root)
            || canonical != path
            || canonical.extension().and_then(|v| v.to_str()) != Some("mkv")
        {
            return Err(unsupported());
        }
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&canonical)
            .map_err(|_| unsupported())?;
        let m = file.metadata().map_err(|_| unsupported())?;
        if !m.is_file() || m.len() > 512 * 1024 * 1024 {
            return Err(unsupported());
        }
        // Retain the actual completed input and parent directory, never a JSON claim.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(unsupported());
        }
        let directory =
            File::open(canonical.parent().ok_or_else(unsupported)?).map_err(|_| unsupported())?;
        Ok(Self {
            _file: file,
            _directory: directory,
            path: canonical,
            dev: m.dev(),
            ino: m.ino(),
        })
    }
    fn unchanged(&self) -> Result<(), StoreError> {
        let m = fs::metadata(&self.path).map_err(|_| unsupported())?;
        if m.dev() != self.dev || m.ino() != self.ino {
            return Err(unsupported());
        }
        Ok(())
    }
}
fn outcome(result: &'static str, properties: Value, status: &'static str, extra: Value) -> Outcome {
    Outcome {
        result,
        properties,
        status,
        extra,
    }
}
fn bounded(path: &Path, limit: u64) -> Result<Vec<u8>, StoreError> {
    let f = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|_| unsupported())?;
    if !f.metadata().map_err(|_| unsupported())?.is_file() {
        return Err(unsupported());
    }
    let mut bytes = Vec::new();
    f.take(limit + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| unsupported())?;
    if bytes.len() as u64 > limit {
        return Err(unsupported());
    }
    Ok(bytes)
}
impl Media for ActualMedia {
    fn process(&self, work: Work, owner: Owner) -> BoxFuture<'_, Result<Outcome, StoreError>> {
        Box::pin(async move {
            // Missing binary is an actual Source terminal, not simulated success.
            match fs::metadata("/usr/local/bin/comskip") {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(outcome(
                        "comskip_missing",
                        json!({"status":"skipped","reason":"comskip_not_installed"}),
                        "skipped",
                        json!({"reason":"comskip_not_installed"}),
                    ))
                }
                Err(_) => return Err(unsupported()),
                Ok(m) if !m.is_file() => return Err(unsupported()),
                Ok(_) => {}
            }
            let actual = FileOwner::open(&work.file)?;
            task::started(&self.events, &work, owner.clone()).await;
            let shutdown = self.shutdown.clone();
            let transcript = self.transcript.clone();
            // The detached blocking task owns all files and caller admission even when
            // its async observer disappears. No SQL writer is held during media work.
            tokio::task::spawn_blocking(move||{
    let directory=work.file.parent().ok_or_else(unsupported)?;
    let mut argv=vec!["--threads=1".to_owned(),"--output".into(),directory.to_str().ok_or_else(unsupported)?.to_owned()];
    if let Some(hw)=&work.hw{argv.insert(0,hw.clone());}
    let mut candidates=Vec::new();if let Some(p)=work.custom_ini{candidates.push(p);}candidates.extend([PathBuf::from("/etc/comskip/comskip.ini"),PathBuf::from("/app/docker/comskip.ini")]);
    let ini=candidates.into_iter().find(|p|p.exists());if let Some(path)=&ini{argv.push(format!("--ini={}",path.to_str().ok_or_else(unsupported)?));}
    argv.push(work.file.to_str().ok_or_else(unsupported)?.to_owned());
    let receipt=recording_process::comskip_program("/usr/local/bin/comskip",&argv,None,shutdown.clone(),owner.clone(),transcript.clone(),300).map_err(|_|unsupported())?;
    actual.unchanged()?;
    if receipt.code==Some(1){let mut cp=json!({"status":"completed","skipped":true});if let Some(ini)=ini{cp["ini_path"]=json!(ini);}return Ok(outcome("no_commercials",cp,"skipped",json!({"reason":"no_commercials_detected"})));}
    if receipt.code!=Some(0){
     let returncode=receipt.code.unwrap_or_else(||-receipt.signal.unwrap_or(0));
     let mut cp=json!({"status":"error","reason":"comskip_failed","returncode":returncode});
     let text=String::from_utf8(receipt.stderr.tail).map_err(|_|unsupported())?;let lines:Vec<_>=text.trim().lines().collect();if !lines.is_empty(){cp["stderr"]=json!(lines[lines.len().saturating_sub(5)..].join("\n"));}
     if let Some(ini)=ini{cp["ini_path"]=json!(ini);}
     // Known signal names only; do not fabricate Source's Python enum labels.
     if returncode<0{return Err(unsupported());}
     return Ok(outcome("comskip_failed",cp,"error",json!({"reason":"comskip_failed","returncode":returncode})));
    }
    let edl=work.file.with_extension("edl");
    if !edl.exists(){return Ok(outcome("no_edl",json!({"status":"error","reason":"edl_not_found"}),"error",json!({"reason":"edl_not_found"})));}
    let probe=directory.join(format!(".dispatcharr-comskip-probe-{}.txt",uuid::Uuid::new_v4()));
    let sink=OpenOptions::new().write(true).create_new(true).mode(0o600).open(&probe).map_err(|_|unsupported())?;
    let args=vec!["-v".into(),"error".into(),"-show_entries".into(),"format=duration".into(),"-of".into(),"default=noprint_wrappers=1:nokey=1".into(),work.file.to_str().ok_or_else(unsupported)?.to_owned()];
    let execution=recording_process::comskip_program("/usr/local/bin/ffprobe",&args,Some(sink),shutdown,owner,transcript,30);
    let data=bounded(&probe,4096);let removal=fs::remove_file(&probe);removal.map_err(|_|unsupported())?;
    let probe=execution.map_err(|_|unsupported())?;actual.unchanged()?;
    let duration=if probe.code==Some(0){data.ok().and_then(|d|String::from_utf8(d).ok()).and_then(|s|s.trim().parse::<f64>().ok()).filter(|v|v.is_finite()&&*v>0.0)}else{None};
    let Some(duration)=duration else{return Ok(outcome("no_duration",json!({"status":"error","reason":"duration_unknown"}),"error",json!({"reason":"duration_unknown"})));};
    let raw=bounded(&edl,1024*1024)?;let text=String::from_utf8(raw).map_err(|_|unsupported())?;
    // Non-finite EDL is outside the admitted real-generator domain;
    // do not turn such an interval into an empty/no-commercial result.
    if text.lines().any(|line|line.split_whitespace().take(2).any(|part|part.parse::<f64>().is_ok_and(|v|!v.is_finite()))) {return Err(unsupported());}
    let commercials=task::intervals(&text,duration);let skipped=commercials.is_empty()||commercials.iter().map(|(a,b)|b-a).sum::<f64>()<=0.5;
    let mut cp=if skipped{json!({"status":"completed","skipped":true,"edl":edl.file_name().and_then(|name|name.to_str()).ok_or_else(unsupported)?})}else{json!({"status":"completed","mode":"mark","edl":edl.file_name().and_then(|name|name.to_str()).ok_or_else(unsupported)?,"commercials":commercials.len()})};
    if let Some(ini)=ini{cp["ini_path"]=json!(ini);}
    Ok(if skipped{outcome("no_commercials",cp,"skipped",json!({"reason":"no_commercials","commercials":0}))}else{outcome("ok",cp,"completed",json!({"commercials":commercials.len(),"mode":"mark"}))})
   }).await.map_err(|_|unsupported())?
        })
    }
}
