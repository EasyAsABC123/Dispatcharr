//! Bounded owned input-profile stdout transport. Original Dispatcharr credit retained.
//! No shell is involved. The original provider/channel/worker owners outlive all
//! child/group/pipe work, including cancellation of the observer future.
use bytes::Bytes;
use dispatcharr_contracts::StreamCommand;
use futures_util::StreamExt;
use std::{collections::VecDeque, io, process::Stdio, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::Command,
    sync::{mpsc, watch},
    task::JoinHandle,
    time::{self, Instant},
};

const FRAME: usize = 8192;
const STDERR_TAIL: usize = 65536;
#[derive(Debug)]
#[allow(dead_code)] // Bounded diagnostic receipt inspected by owned-process acceptance tests.
pub(super) struct Receipt {
    pub pid: u32,
    pub code: Option<i32>,
    pub signal: Option<i32>,
    pub stdout_bytes: u64,
    pub stderr_bytes: u64,
    pub stderr_tail: Vec<u8>,
    pub stdout_eof: bool,
    pub stderr_eof: bool,
    pub read_failed: bool,
    pub input_failed: bool,
    pub stdin_bytes: u64,
    pub stdin_eof: bool,
    pub stdin_failed: bool,
    pub forced_kill: bool,
    pub group_settled: bool,
}
pub(super) struct InputProcess {
    receiver: mpsc::Receiver<io::Result<Bytes>>,
    stop: watch::Sender<bool>,
    done: Option<JoinHandle<Receipt>>,
}
fn group_exists(pid: u32) -> bool {
    let result = unsafe { libc::kill(-(pid as i32), 0) };
    result == 0 || io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
}
fn signal_group(pid: u32, signal: i32) {
    // The group is created by this exact owned child. Never target the app group.
    unsafe {
        libc::kill(-(pid as i32), signal);
    }
}
fn reap_descendants(pid: u32) {
    loop {
        let mut status = 0;
        if unsafe { libc::waitpid(-(pid as i32), &mut status, libc::WNOHANG) } <= 0 {
            break;
        }
    }
}
/// Original StreamManager maps only literal vlc/cvlc command names to its
/// VLC parser. The marker is matched case-insensitively within a stderr line;
/// arbitrary pipe-read boundaries must not hide a real input failure.
struct VlcInputFailure {
    enabled: bool,
    suffix: VecDeque<u8>,
}
impl VlcInputFailure {
    const MARKER: &'static [u8] = b"unable to open the mrl";
    fn new(command: &str) -> Self {
        Self {
            enabled: command.eq_ignore_ascii_case("vlc") || command.eq_ignore_ascii_case("cvlc"),
            suffix: VecDeque::with_capacity(Self::MARKER.len()),
        }
    }
    fn observe(&mut self, bytes: &[u8]) -> bool {
        if !self.enabled {
            return false;
        }
        for &byte in bytes {
            if matches!(byte, b'\r' | b'\n') {
                self.suffix.clear();
                continue;
            }
            if self.suffix.len() == Self::MARKER.len() {
                self.suffix.pop_front();
            }
            self.suffix.push_back(byte.to_ascii_lowercase());
            if self.suffix.iter().copied().eq(Self::MARKER.iter().copied()) {
                return true;
            }
        }
        false
    }
}

impl InputProcess {
    pub fn start(
        command: &StreamCommand,
        read_timeout: Duration,
        owner: Arc<dyn Send + Sync>,
    ) -> io::Result<Self> {
        Self::start_inner(command, None, read_timeout, owner)
    }

    /// One producer-owned stdin transport for an output-profile process. The
    /// supplied raw subscription remains owned by the supervisor through actual
    /// writer settlement, including cancellation of the HTTP observer.
    #[allow(dead_code)] // Owner-dispatch integration is a separate gated port.
    pub fn start_transform(
        command: &StreamCommand,
        input: super::MediaStream,
        read_timeout: Duration,
        owner: Arc<dyn Send + Sync>,
    ) -> io::Result<Self> {
        Self::start_inner(command, Some(input), read_timeout, owner)
    }

    fn start_inner(
        command: &StreamCommand,
        input: Option<super::MediaStream>,
        read_timeout: Duration,
        owner: Arc<dyn Send + Sync>,
    ) -> io::Result<Self> {
        if !command.spawn_compatible() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Invalid input profile argv",
            ));
        }
        // Adopt profile-script orphans, so the owned group can be physically
        // reaped after its original leader exits. Other PGIDs are never waited.
        if unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let mut input_failure = VlcInputFailure::new(if input.is_some() {
            ""
        } else {
            &command.argv[0]
        });
        let mut process = Command::new(&command.argv[0]);
        process
            .args(&command.argv[1..])
            .stdin(if input.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        // Source posix_spawn uses setsid=True. Only an async-signal-safe syscall
        // runs between fork/exec; no allocation, lock or Python hook runs here.
        unsafe {
            process.pre_exec(|| {
                if libc::setsid() < 0 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(())
                }
            });
        }
        let mut child = process.spawn()?;
        let pid = child.id().expect("newly spawned child has a PID");
        let mut stdout = child.stdout.take().expect("requested stdout pipe");
        let mut stderr = child.stderr.take().expect("requested stderr pipe");
        let (sender, receiver) = mpsc::channel(2);
        let (stop, mut stopped) = watch::channel(false);
        let is_transform = input.is_some();
        // Exactly one bounded raw frame is held while a pipe write is blocked.
        // The write itself is cancellable; dropping it closes stdin before the
        // original producer owner can be retired by the supervisor below.
        let writer = input.map(|mut input| {
            let mut stdin = child.stdin.take().expect("requested stdin pipe");
            let mut writer_stop = stop.subscribe();
            tokio::spawn(async move {
                let mut bytes = 0u64;
                let mut eof = false;
                let mut failed = false;
                'writer: loop {
                    if *writer_stop.borrow() {
                        break;
                    }
                    let frame = tokio::select! {
                        biased;
                        _ = writer_stop.changed() => break,
                        frame = input.next() => match frame {
                            None => { eof = true; break; },
                            Some(Err(_)) => { failed = true; break; },
                            Some(Ok(frame)) => frame,
                        },
                    };
                    let mut remaining = frame.data.as_ref();
                    while !remaining.is_empty() {
                        tokio::select! {
                            biased;
                            _ = writer_stop.changed() => break 'writer,
                            count = stdin.write(remaining) => match count {
                                Ok(0) | Err(_) => { failed = true; break 'writer; },
                                Ok(count) => {
                                    bytes = bytes.saturating_add(count as u64);
                                    remaining = &remaining[count..];
                                },
                            },
                        }
                    }
                }
                // No flush buffer exists for Tokio's child pipe. Dropping this
                // real FD communicates EOF to the transcoder, not a fake ACK.
                drop(stdin);
                drop(input);
                (bytes, eof, failed)
            })
        });
        let supervisor_stop = stop.clone();
        let done = tokio::spawn(async move {
            use std::os::unix::process::ExitStatusExt;
            let _owner = owner;
            let mut receipt = Receipt {
                pid,
                code: None,
                signal: None,
                stdout_bytes: 0,
                stderr_bytes: 0,
                stderr_tail: Vec::new(),
                stdout_eof: false,
                stderr_eof: false,
                read_failed: false,
                input_failed: false,
                stdin_bytes: 0,
                stdin_eof: false,
                stdin_failed: false,
                forced_kill: false,
                group_settled: false,
            };
            let mut tail = VecDeque::with_capacity(STDERR_TAIL);
            let mut output = [0u8; FRAME];
            let mut errors = [0u8; 4096];
            let mut pending = None;
            let mut out_closed = false;
            let mut err_closed = false;
            let mut exit = None;
            let mut termination = None;
            let mut discard_output = false;
            let mut last_output = Instant::now();
            let mut tick = time::interval(Duration::from_millis(100));
            tick.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
            loop {
                if *stopped.borrow() || sender.is_closed() {
                    discard_output = true;
                }
                if discard_output {
                    pending = None;
                }
                if termination.is_none() && (discard_output || out_closed || exit.is_some()) {
                    supervisor_stop.send_replace(true);
                    // Source output manager first closes stdin, waits five
                    // seconds, then kills. Retain the existing input-profile
                    // TERM behavior; transformed input receives that EOF grace.
                    if !is_transform {
                        signal_group(pid, libc::SIGTERM);
                    }
                    termination = Some(Instant::now());
                }
                if termination.is_some_and(|at| at.elapsed() >= Duration::from_secs(5))
                    && !receipt.forced_kill
                {
                    signal_group(pid, libc::SIGKILL);
                    receipt.forced_kill = true;
                }
                if exit.is_some() {
                    // Tokio has reaped its leader before this waitpid boundary.
                    reap_descendants(pid);
                    if out_closed && err_closed && pending.is_none() && !group_exists(pid) {
                        receipt.group_settled = true;
                        break;
                    }
                }
                tokio::select! {
                    _ = stopped.changed(), if !discard_output => { discard_output = true; },
                    _ = sender.closed(), if !discard_output => { discard_output = true; },
                    permit = sender.reserve(), if pending.is_some() && !discard_output => {
                        match permit { Ok(permit) => permit.send(Ok(pending.take().unwrap())), Err(_) => { discard_output=true;pending=None; } }
                    },
                    read = stderr.read(&mut errors), if !err_closed => {
                        match read {
                            Ok(0)=>{err_closed=true;receipt.stderr_eof=true;},
                            Ok(count)=>{
                                receipt.stderr_bytes=receipt.stderr_bytes.saturating_add(count as u64);
                                if input_failure.observe(&errors[..count]) {
                                    receipt.input_failed=true;
                                    // Existing settlement retains provider/session ownership until
                                    // TERM/KILL, both EOFs and actual process-group absence.
                                    discard_output=true;
                                }
                                for byte in &errors[..count] { if tail.len()==STDERR_TAIL { tail.pop_front(); } tail.push_back(*byte); }
                            },
                            Err(_)=>{err_closed=true;receipt.read_failed=true;discard_output=true;},
                        }
                    },
                    read = stdout.read(&mut output), if !out_closed && (pending.is_none() || discard_output) => {
                        match read {
                            Ok(0)=>{out_closed=true;receipt.stdout_eof=true;},
                            Ok(count)=>{
                                receipt.stdout_bytes=receipt.stdout_bytes.saturating_add(count as u64);last_output=Instant::now();
                                if !discard_output { pending=Some(Bytes::copy_from_slice(&output[..count])); }
                            },
                            Err(_)=>{out_closed=true;receipt.read_failed=true;discard_output=true;},
                        }
                    },
                    _ = tick.tick() => {
                        if exit.is_none() {
                            match child.try_wait() { Ok(Some(status))=>{receipt.code=status.code();receipt.signal=status.signal();exit=Some(status);}, Ok(None)=>{}, Err(_)=>{receipt.read_failed=true;discard_output=true;} }
                        }
                        if !is_transform && !discard_output && !out_closed && last_output.elapsed() >= read_timeout && pending.is_none() {
                            receipt.read_failed=true;discard_output=true;
                        }
                    },
                }
            }
            supervisor_stop.send_replace(true);
            if let Some(writer) = writer {
                match writer.await {
                    Ok((bytes, eof, failed)) => {
                        receipt.stdin_bytes = bytes;
                        receipt.stdin_eof = eof;
                        receipt.stdin_failed = failed;
                    }
                    Err(_) => receipt.stdin_failed = true,
                }
            }
            receipt.stderr_tail = tail.into_iter().collect();
            // No timeout/abort can retire the original owner early. An actually
            // stuck process stays charged to its original bounded channel slot.
            receipt
        });
        Ok(Self {
            receiver,
            stop,
            done: Some(done),
        })
    }
    pub async fn next(&mut self) -> Option<io::Result<Bytes>> {
        self.receiver.recv().await
    }
    pub async fn finish(mut self) -> Result<Receipt, tokio::task::JoinError> {
        self.stop.send_replace(true);
        self.receiver.close();
        self.done.take().unwrap().await
    }
}
impl Drop for InputProcess {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    #[test]
    fn vlc_input_failure_survives_every_pipe_read_boundary() {
        let line = b"main input error: VLC is UNABLE TO OPEN THE MRL 'http://owned.invalid'.\n";
        for cut in 0..line.len() {
            let mut parser = VlcInputFailure::new("cvlc");
            let first = parser.observe(&line[..cut]);
            let second = parser.observe(&line[cut..]);
            assert!(first || second, "marker lost at pipe offset {cut}");
        }
    }
    #[test]
    fn unrelated_vlc_errors_and_other_command_stderr_do_not_trigger_failover() {
        for command in ["vlc", "CVLC"] {
            let mut parser = VlcInputFailure::new(command);
            assert!(!parser
                .observe(b"vlcpulse audio output error: PulseAudio server connection failure\n"));
            assert!(!parser.observe(b"unable to open the \nmrl\n"));
        }
        for command in ["ffmpeg", "streamlink", "/usr/bin/vlc", "custom"] {
            assert!(!VlcInputFailure::new(command).observe(b"unable to open the mrl\n"));
        }
    }
    fn shell(script: &str) -> StreamCommand {
        StreamCommand {
            argv: vec!["/bin/sh".into(), "-c".into(), script.into()],
        }
    }
    #[tokio::test]
    async fn transform_stdin_closes_and_owned_output_settles_without_a_second_upstream() {
        let input: super::super::MediaStream = Box::pin(futures_util::stream::iter([
            Ok(super::super::MediaFrame {
                data: Bytes::from_static(b"raw-one"),
                discontinuity: false,
            }),
            Ok(super::super::MediaFrame {
                data: Bytes::from_static(b"raw-two"),
                discontinuity: true,
            }),
        ]));
        let mut process = InputProcess::start_transform(
            &shell("cat; printf stderr-receipt >&2"),
            input,
            Duration::from_secs(10),
            Arc::new(()),
        )
        .unwrap();
        let mut output = Vec::new();
        while let Some(frame) = process.next().await {
            output.extend_from_slice(&frame.unwrap());
        }
        let receipt = process.finish().await.unwrap();
        assert_eq!(output, b"raw-oneraw-two");
        assert_eq!(receipt.stdin_bytes, 14);
        assert!(receipt.stdin_eof && !receipt.stdin_failed);
        assert_eq!(receipt.code, Some(0));
        assert_eq!(receipt.stderr_tail, b"stderr-receipt");
        assert!(receipt.group_settled && receipt.stdout_eof && receipt.stderr_eof);
    }

    #[tokio::test]
    async fn transform_observer_loss_cancels_blocked_raw_reader_before_owner_retirement() {
        struct Held(Arc<AtomicBool>);
        impl Drop for Held {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }
        let retired = Arc::new(AtomicBool::new(false));
        let held = Held(retired.clone());
        let input: super::super::MediaStream = Box::pin(async_stream::stream! {
            let _held = held;
            yield Ok(super::super::MediaFrame { data: Bytes::from_static(b"raw"), discontinuity: false });
            std::future::pending::<()>().await;
        });
        let mut process = InputProcess::start_transform(
            &shell("cat"),
            input,
            Duration::from_secs(10),
            Arc::new(()),
        )
        .unwrap();
        assert_eq!(process.next().await.unwrap().unwrap().as_ref(), b"raw");
        let receipt = time::timeout(Duration::from_secs(8), process.finish())
            .await
            .unwrap()
            .unwrap();
        assert!(retired.load(Ordering::Acquire));
        assert!(receipt.group_settled && receipt.stdout_eof && receipt.stderr_eof);
        assert_eq!(receipt.stdin_bytes, 3);
        assert!(!receipt.forced_kill);
    }
    #[tokio::test]
    async fn actual_child_stdout_and_bounded_lossless_stderr_tail_settle() {
        let mut child = InputProcess::start(
            &shell("printf payload; head -c 131072 /dev/zero >&2"),
            Duration::from_secs(10),
            Arc::new(()),
        )
        .unwrap();
        let mut output = Vec::new();
        while let Some(bytes) = child.next().await {
            output.extend_from_slice(&bytes.unwrap());
        }
        let receipt = child.finish().await.unwrap();
        assert_eq!(output, b"payload");
        assert_eq!(receipt.code, Some(0));
        assert_eq!(receipt.stdout_bytes, 7);
        assert_eq!(receipt.stderr_bytes, 131072);
        assert_eq!(receipt.stderr_tail, vec![0; STDERR_TAIL]);
        assert!(receipt.stdout_eof && receipt.stderr_eof && receipt.group_settled);
        assert!(!receipt.read_failed);
    }
    #[tokio::test]
    async fn observer_loss_keeps_original_owner_until_ignoring_child_is_physically_reaped() {
        struct Owner(Arc<AtomicBool>);
        impl Drop for Owner {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }
        let retired = Arc::new(AtomicBool::new(false));
        let owner: Arc<dyn Send + Sync> = Arc::new(Owner(retired.clone()));
        let mut child = InputProcess::start(
            &shell("trap '' TERM; printf '%s\n' $$; while :; do sleep 1; done"),
            Duration::from_secs(10),
            owner,
        )
        .unwrap();
        let pid: u32 = std::str::from_utf8(&child.next().await.unwrap().unwrap())
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        drop(child);
        time::sleep(Duration::from_millis(100)).await;
        assert!(!retired.load(Ordering::Acquire));
        time::timeout(Duration::from_secs(8), async {
            while !retired.load(Ordering::Acquire) {
                time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert!(!group_exists(pid));
    }
    #[tokio::test]
    async fn stop_reaps_the_actual_profile_script_grandchild_group() {
        let mut child = InputProcess::start(
            &shell("sleep 30 & printf '%s\n' $!; wait"),
            Duration::from_secs(10),
            Arc::new(()),
        )
        .unwrap();
        let grandchild: i32 = std::str::from_utf8(&child.next().await.unwrap().unwrap())
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let receipt = time::timeout(Duration::from_secs(8), child.finish())
            .await
            .unwrap()
            .unwrap();
        assert!(receipt.group_settled && receipt.stdout_eof && receipt.stderr_eof);
        assert!(!group_exists(receipt.pid));
        assert_eq!(unsafe { libc::kill(grandchild, 0) }, -1);
        assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::ESRCH));
    }
}
