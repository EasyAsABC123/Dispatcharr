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
    io::{Read, Write},
    os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
    sync::{atomic::AtomicBool, Arc, Mutex},
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
    fn open(path: &Path, mode: task::Mode) -> Result<Self, StoreError> {
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
        // Linux media sizes use signed 64-bit off_t. Check representability,
        // without a byte admission cap or a file-sized Rust allocation.
        let m = file.metadata().map_err(|_| unsupported())?;
        if !m.is_file() || i64::try_from(m.len()).is_err() {
            return Err(unsupported());
        }
        // Retain the actual completed input and parent directory, never a JSON claim.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(unsupported());
        }
        let directory =
            File::open(canonical.parent().ok_or_else(unsupported)?).map_err(|_| unsupported())?;
        if mode == task::Mode::Cut
            && unsafe { libc::flock(directory.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0
        {
            return Err(unsupported());
        }
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
/// Emergency child ownership includes the actual filesystem locks, not only a
/// logical task slot. Transcript.pending_owner may outlive this blocking stack.
struct MediaOwner {
    _filesystem: Arc<Mutex<FileOwner>>,
    _caller: Owner,
}
fn outcome(
    result: impl Into<String>,
    properties: Value,
    status: &'static str,
    extra: Value,
) -> Outcome {
    Outcome {
        result: result.into(),
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
            let filesystem = Arc::new(Mutex::new(FileOwner::open(&work.file, work.mode)?));
            let owner: Owner = Arc::new(MediaOwner {
                _filesystem: filesystem.clone(),
                _caller: owner,
            });
            task::started(&self.events, &work, owner.clone()).await;
            let shutdown = self.shutdown.clone();
            let transcript = self.transcript.clone();
            // The detached blocking task owns all files and caller admission even when
            // its async observer disappears. No SQL writer is held during media work.
            tokio::task::spawn_blocking(move||{
    let mut actual=filesystem.lock().map_err(|_|unsupported())?;
    let directory=work.file.parent().ok_or_else(unsupported)?;
    let mut argv=vec!["--threads=1".to_owned(),"--output".into(),directory.to_str().ok_or_else(unsupported)?.to_owned()];
    if let Some(hw)=&work.hw{argv.insert(0,hw.clone());}
    let mut candidates=Vec::new();if let Some(p)=&work.custom_ini{candidates.push(p.clone());}candidates.extend([PathBuf::from("/etc/comskip/comskip.ini"),PathBuf::from("/app/docker/comskip.ini")]);
    let ini=candidates.into_iter().find(|p|p.exists());if let Some(path)=&ini{argv.push(format!("--ini={}",path.to_str().ok_or_else(unsupported)?));}
    argv.push(work.file.to_str().ok_or_else(unsupported)?.to_owned());
    let receipt=recording_process::comskip_program("/usr/local/bin/comskip",&argv,None,shutdown.clone(),owner.clone(),transcript.clone(),None).map_err(|_|unsupported())?;
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
    let execution=recording_process::comskip_program("/usr/local/bin/ffprobe",&args,Some(sink),shutdown.clone(),owner.clone(),transcript.clone(),None);
    let data=bounded(&probe,4096);let removal=fs::remove_file(&probe);removal.map_err(|_|unsupported())?;
    let probe=execution.map_err(|_|unsupported())?;actual.unchanged()?;
    let duration=if probe.code==Some(0){data.ok().and_then(|d|String::from_utf8(d).ok()).and_then(|s|s.trim().parse::<f64>().ok()).filter(|v|v.is_finite()&&*v>0.0)}else{None};
    let Some(duration)=duration else{return Ok(outcome("no_duration",json!({"status":"error","reason":"duration_unknown"}),"error",json!({"reason":"duration_unknown"})));};
    let raw=bounded(&edl,1024*1024)?;let text=String::from_utf8(raw).map_err(|_|unsupported())?;
    // Non-finite EDL is outside the admitted real-generator domain;
    // do not turn such an interval into an empty/no-commercial result.
    if text.lines().any(|line|line.split_whitespace().take(2).any(|part|part.parse::<f64>().is_ok_and(|v|!v.is_finite()))) {return Err(unsupported());}
    let commercials=task::intervals(&text,duration);let skipped=commercials.is_empty()||commercials.iter().map(|(a,b)|b-a).sum::<f64>()<=0.5;
    if !skipped && work.mode==task::Mode::Cut {return cut_recording(&work,&mut actual,&commercials,duration,ini.as_deref(),CutExecution { shutdown, owner, transcript });}
    let mut cp=if skipped{json!({"status":"completed","skipped":true,"edl":edl.file_name().and_then(|name|name.to_str()).ok_or_else(unsupported)?})}else{json!({"status":"completed","mode":"mark","edl":edl.file_name().and_then(|name|name.to_str()).ok_or_else(unsupported)?,"commercials":commercials.len()})};
    if let Some(ini)=ini{cp["ini_path"]=json!(ini);}
    Ok(if skipped{outcome("no_commercials",cp,"skipped",json!({"reason":"no_commercials","commercials":0}))}else{outcome("ok",cp,"completed",json!({"commercials":commercials.len(),"mode":"mark"}))})
   }).await.map_err(|_|unsupported())?
        })
    }
}

/// Retain directory/input ownership until every real child and replacement settles.
/// Existing stage paths are outside this bounded slice; never overwrite a foreign file.
impl FileOwner {
    fn replace_with(&mut self, output: &Path) -> Result<(), StoreError> {
        self.unchanged()?;
        let output_file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(output)
            .map_err(|_| unsupported())?;
        // FFmpeg writes to disk; rename and std::fs::copy do not buffer the
        // whole recording in Rust. Preserve only the platform size check.
        let output_identity = output_file.metadata().map_err(|_| unsupported())?;
        if !output_identity.is_file() || i64::try_from(output_identity.len()).is_err() {
            return Err(unsupported());
        }
        // Source attempts rename first, then copy (which intentionally retains .cut).
        let renamed = fs::rename(output, &self.path).is_ok();
        if !renamed {
            fs::copy(output, &self.path).map_err(|_| unsupported())?;
        }
        let replacement = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&self.path)
            .map_err(|_| unsupported())?;
        let metadata = replacement.metadata().map_err(|_| unsupported())?;
        if !metadata.is_file()
            || (renamed
                && (metadata.dev() != output_identity.dev()
                    || metadata.ino() != output_identity.ino()))
        {
            return Err(unsupported());
        }
        if !renamed {
            // Copy fallback writes the original still-locked inode. A second
            // independently opened descriptor must not attempt to relock it.
            if metadata.dev() != self.dev || metadata.ino() != self.ino {
                return Err(unsupported());
            }
            return Ok(());
        }
        if unsafe { libc::flock(replacement.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(unsupported());
        }
        self.dev = metadata.dev();
        self.ino = metadata.ino();
        self._file = replacement;
        Ok(())
    }
}
enum CutProblem {
    Ownership(StoreError),
    Business(String),
}
impl From<StoreError> for CutProblem {
    fn from(value: StoreError) -> Self {
        Self::Ownership(value)
    }
}
fn python_string(value: &str) -> Result<String, StoreError> {
    // CPython repr uses double quotes for strings containing only single quotes.
    // Restrict only failure-message formatting until Unicode repr is qualified.
    if !value.bytes().all(|byte| (0x20..=0x7e).contains(&byte)) {
        return Err(unsupported());
    }
    let quote = if value.contains('\'') && !value.contains('"') {
        '"'
    } else {
        '\''
    };
    let escaped = value
        .replace('\\', "\\\\")
        .replace(quote, &format!("\\{quote}"));
    Ok(format!("{quote}{escaped}{quote}"))
}
fn cut_command(
    args: &[String],
    shutdown: Arc<AtomicBool>,
    owner: Owner,
    transcript: Arc<Transcript>,
) -> Result<(), CutProblem> {
    let receipt = recording_process::comskip_cut_program(args, shutdown, owner, transcript)
        .map_err(|_| unsupported())?;
    if receipt.code == Some(0) {
        return Ok(());
    }
    let code = receipt.code.ok_or_else(unsupported)?;
    let mut argv = vec![python_string("ffmpeg")?];
    for arg in args {
        argv.push(python_string(arg)?);
    }
    Err(CutProblem::Business(format!(
        "Command '[{}]' returned non-zero exit status {code}.",
        argv.join(", ")
    )))
}
struct CutExecution {
    shutdown: Arc<AtomicBool>,
    owner: Owner,
    transcript: Arc<Transcript>,
}
fn cut_recording(
    work: &Work,
    actual: &mut FileOwner,
    commercials: &[(f64, f64)],
    duration: f64,
    ini: Option<&Path>,
    execution: CutExecution,
) -> Result<Outcome, StoreError> {
    let CutExecution {
        shutdown,
        owner,
        transcript,
    } = execution;
    let directory = work.file.parent().ok_or_else(unsupported)?;
    let edl = work.file.with_extension("edl");
    let list = directory.join("concat_list.txt");
    let output = directory.join(format!(
        "{}.cut.mkv",
        work.file
            .file_stem()
            .and_then(|n| n.to_str())
            .ok_or_else(unsupported)?
    ));
    let intervals = task::keep_intervals(commercials, duration);
    if intervals.len() > 256 || list.exists() || output.exists() {
        return Err(unsupported());
    }
    let planned: Vec<_> = intervals
        .into_iter()
        .enumerate()
        .filter(|(_, (s, e))| e - s > 0.01)
        .map(|(index, (start, end))| {
            (
                directory.join(format!("segment_{index:03}.mkv")),
                start,
                (end - start).max(0.0),
            )
        })
        .collect();
    if planned.iter().any(|(p, _, _)| p.exists()) {
        return Err(unsupported());
    }
    let observed = (|| -> Result<usize, CutProblem> {
        let mut parts = Vec::new();
        for (seg, start, duration) in planned {
            actual.unchanged()?;
            let reserved = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o666)
                .custom_flags(libc::O_NOFOLLOW)
                .open(&seg)
                .map_err(|_| unsupported())?;
            let args = vec![
                "-y".into(),
                "-ss".into(),
                format!("{start:.3}"),
                "-i".into(),
                work.file.to_str().ok_or_else(unsupported)?.into(),
                "-t".into(),
                format!("{duration:.3}"),
                "-c".into(),
                "copy".into(),
                "-avoid_negative_ts".into(),
                "1".into(),
                seg.to_str().ok_or_else(unsupported)?.into(),
            ];
            cut_command(&args, shutdown.clone(), owner.clone(), transcript.clone())?;
            let expected = reserved.metadata().map_err(|_| unsupported())?;
            let observed = fs::metadata(&seg).map_err(|_| unsupported())?;
            if expected.dev() != observed.dev() || expected.ino() != observed.ino() {
                return Err(unsupported().into());
            }
            parts.push(seg);
        }
        if parts.is_empty() {
            return Err(CutProblem::Business("no_parts".into()));
        }
        let mut concat = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o666)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&list)
            .map_err(|_| unsupported())?;
        for part in &parts {
            let escaped = part
                .to_str()
                .ok_or_else(unsupported)?
                .replace('\'', "'\\''");
            writeln!(concat, "file '{escaped}'").map_err(|_| unsupported())?;
        }
        drop(concat);
        let reserved = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o666)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&output)
            .map_err(|_| unsupported())?;
        let args = vec![
            "-y".into(),
            "-f".into(),
            "concat".into(),
            "-safe".into(),
            "0".into(),
            "-i".into(),
            list.to_str().ok_or_else(unsupported)?.into(),
            "-c".into(),
            "copy".into(),
            output.to_str().ok_or_else(unsupported)?.into(),
        ];
        cut_command(&args, shutdown, owner, transcript)?;
        let expected = reserved.metadata().map_err(|_| unsupported())?;
        let observed = fs::metadata(&output).map_err(|_| unsupported())?;
        if expected.dev() != observed.dev() || expected.ino() != observed.ino() {
            return Err(unsupported().into());
        }
        actual.replace_with(&output)?;
        let _ = fs::remove_file(&list);
        for part in &parts {
            let _ = fs::remove_file(part);
        }
        let _ = fs::remove_file(&edl);
        Ok(parts.len())
    })();
    match observed {
        Ok(kept) => {
            let mut cp = json!({"status":"completed","edl":edl.file_name().and_then(|n|n.to_str()).ok_or_else(unsupported)?,"segments_kept":kept,"commercials":commercials.len()});
            if let Some(ini) = ini {
                cp["ini_path"] = json!(ini);
            }
            Ok(outcome(
                "ok",
                cp,
                "completed",
                json!({"commercials":commercials.len(),"segments_kept":kept}),
            ))
        }
        Err(CutProblem::Business(reason)) => Ok(outcome(
            format!("error:{reason}"),
            json!({"status":"error","reason":reason}),
            "error",
            json!({"reason":reason}),
        )),
        Err(CutProblem::Ownership(error)) => Err(error),
    }
}
#[cfg(test)]
mod cut_format_tests {
    use super::*;
    #[test]
    fn source_concat_and_checked_failure_repr_keep_apostrophes_and_backslashes() {
        assert_eq!(
            python_string("/data/O'Brien/file.mkv").unwrap(),
            "\"/data/O'Brien/file.mkv\""
        );
        assert_eq!(python_string("a\\b").unwrap(), "'a\\\\b'");
        assert!(python_string("a\nb").is_err());
        assert!(python_string("a\u{200b}b").is_err());
        assert!(python_string("a\u{2028}b").is_err());
        assert!(python_string("café").is_err());
    }
}
