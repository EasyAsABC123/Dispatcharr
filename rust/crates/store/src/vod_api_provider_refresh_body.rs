//! Private terminal-body supervision. One bounded raw-frame channel, no second
//! pool. Decoder/consumer failure switches the owned pump to raw discard until
//! actual EOF/error, retaining the same admitted session/work token throughout.
//! Original Dispatcharr authors retain application and algorithm credit.
use super::{vod_api_owned_read::ReadError, vod_api_provider_refresh_http::Fault};
use bytes::Bytes;
use futures_util::StreamExt;
use std::{io, sync::Arc};
use tokio::time::Instant;
const FRAME: usize = 8192;
#[derive(Debug)]
struct ControlDeadline;
impl std::fmt::Display for ControlDeadline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("provider refresh control deadline")
    }
}
impl std::error::Error for ControlDeadline {}
fn contains<E: std::error::Error + 'static>(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(error) = current {
        if error.is::<E>() {
            return true;
        }
        current = error
            .downcast_ref::<io::Error>()
            .and_then(io::Error::get_ref)
            .map(|inner| inner as &(dyn std::error::Error + 'static))
            .or_else(|| error.source());
    }
    false
}
fn classify(error: io::Error) -> Fault {
    if contains::<ControlDeadline>(&error) {
        Fault::Control(ReadError::Deadline)
    } else if contains::<dispatcharr_upstream_http::WireLimitExceeded>(&error)
        || contains::<dispatcharr_upstream_http::ResourceLimitExceeded>(&error)
    {
        Fault::Capacity
    } else if error.kind() == io::ErrorKind::InvalidData {
        Fault::BodyDecode
    } else {
        Fault::Remote
    }
}
pub(super) trait Observer: Send + Sync {
    fn pump_started(&self) {}
    fn consumer_stopped(&self, _capacity: bool) {}
    fn raw_finished(&self) {}
}
pub(super) async fn collect(
    response: reqwest::Response,
    guard: Arc<dyn Send + Sync>,
    maximum: usize,
    deadline: Instant,
    discard: Option<Fault>,
) -> Result<Vec<u8>, Fault> {
    collect_inner(response, guard, maximum, deadline, discard, None, true).await
}
pub(super) async fn consume_redirect(
    response: reqwest::Response,
    guard: Arc<dyn Send + Sync>,
    maximum: usize,
    deadline: Instant,
    discard: Option<Fault>,
) -> Result<(), Fault> {
    collect_inner(response, guard, maximum, deadline, discard, None, false)
        .await
        .map(|_| ())
}
pub(super) async fn collect_inner(
    response: reqwest::Response,
    guard: Arc<dyn Send + Sync>,
    maximum: usize,
    deadline: Instant,
    discard: Option<Fault>,
    observer: Option<Arc<dyn Observer>>,
    retain_output: bool,
) -> Result<Vec<u8>, Fault> {
    let headers = response.headers().clone();
    let status = response.status();
    let (sender, receiver) = tokio::sync::mpsc::channel::<io::Result<Bytes>>(1);
    let observed = observer.clone();
    let pump = tokio::spawn(async move {
        // Declare the guard before the stream: stream cleanup precedes the
        // guard's destructor on every normal/error/unwind path.
        let _physical = guard;
        if let Some(observer) = &observed {
            observer.pump_started();
        }
        let mut source = response.bytes_stream();
        let mut discard = false;
        while let Some(frame) = source.next().await {
            let frame = match frame {
                Ok(frame) => frame,
                Err(error) => {
                    if !discard {
                        let error = if Instant::now() >= deadline {
                            io::Error::other(ControlDeadline)
                        } else {
                            io::Error::other(error)
                        };
                        let _ = sender.send(Err(error)).await;
                    }
                    break;
                }
            };
            if discard {
                continue;
            }
            for offset in (0..frame.len()).step_by(FRAME) {
                let part = frame.slice(offset..frame.len().min(offset + FRAME));
                if sender.send(Ok(part)).await.is_err() {
                    discard = true;
                    break;
                }
            }
        }
        if let Some(observer) = observed {
            observer.raw_finished();
        }
    });
    let outcome = if let Some(error) = discard {
        drop(receiver);
        Err(error)
    } else {
        let incoming = futures_util::stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|item| (item, receiver))
        });
        let mut synthetic = axum::http::Response::new(reqwest::Body::wrap_stream(incoming));
        *synthetic.status_mut() = status;
        *synthetic.headers_mut() = headers;
        let response = reqwest::Response::from(synthetic);
        async {
            let mut stream =
                dispatcharr_upstream_http::body_limited(response, maximum).map_err(classify)?;
            let mut bytes = Vec::new();
            let mut received = 0usize;
            while let Some(frame) = stream.next().await {
                let frame = frame.map_err(classify)?;
                let needed = received
                    .checked_add(frame.len())
                    .filter(|n| *n <= maximum)
                    .ok_or(Fault::Capacity)?;
                received = needed;
                if !retain_output {
                    continue;
                }
                if needed > bytes.capacity() {
                    bytes
                        .try_reserve_exact(needed - bytes.len())
                        .map_err(|_| Fault::Capacity)?;
                }
                bytes.extend_from_slice(&frame);
            }
            Ok(bytes)
        }
        .await
    };
    if let Some(observer) = observer {
        if outcome.is_err() {
            observer.consumer_stopped(matches!(&outcome, Err(Fault::Capacity)));
        }
    }
    // Dropping this await future must not abort the pump: JoinHandle drop only
    // detaches it, and that task retains its physical guard through raw EOF.
    pump.await
        .map_err(|_| Fault::Control(ReadError::WorkerFailed))?;
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vod_api_draft::vod_api_owned_read::{ReadLimits, ReadRuntime};
    use std::{
        io::{Read, Write},
        sync::atomic::{AtomicBool, Ordering},
        time::Duration,
    };
    #[derive(Default)]
    struct Trace {
        started: AtomicBool,
        failed: AtomicBool,
        capacity: AtomicBool,
        drained: AtomicBool,
        wake: tokio::sync::Notify,
    }
    impl Observer for Trace {
        fn pump_started(&self) {
            self.started.store(true, Ordering::Release);
        }
        fn consumer_stopped(&self, capacity: bool) {
            self.capacity.store(capacity, Ordering::Release);
            self.failed.store(true, Ordering::Release);
            self.wake.notify_waiters();
        }
        fn raw_finished(&self) {
            self.drained.store(true, Ordering::Release);
            self.wake.notify_waiters();
        }
    }
    enum Command {
        Probe(tokio::sync::oneshot::Sender<bool>),
        Release,
    }
    async fn held_capacity(gzip: bool, abort_join: bool) {
        let maximum = 4096usize;
        let initial = if gzip {
            include_bytes!("../datasets/provider-refresh-body-overflow-gzip.bin").to_vec()
        } else {
            vec![b'x'; maximum * 2]
        };
        if gzip {
            assert!(initial.len() < maximum);
        }
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (commands, receive) = std::sync::mpsc::channel();
        let (closed, closed_receive) = tokio::sync::oneshot::channel();
        let server = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut headers = Vec::new();
            let mut byte = [0u8; 1];
            while !headers.ends_with(b"\r\n\r\n") {
                socket.read_exact(&mut byte).unwrap();
                headers.push(byte[0]);
            }
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n",
                )
                .unwrap();
            if gzip {
                socket.write_all(b"Content-Encoding: gzip\r\n").unwrap();
            }
            socket.write_all(b"\r\n").unwrap();
            write!(socket, "{:x}\r\n", initial.len()).unwrap();
            socket.write_all(&initial).unwrap();
            socket.write_all(b"\r\n").unwrap();
            while let Command::Probe(reply) = receive.recv_timeout(Duration::from_secs(5)).unwrap()
            {
                socket
                    .set_read_timeout(Some(Duration::from_millis(50)))
                    .unwrap();
                let open = match socket.peek(&mut byte) {
                    Ok(0) => false,
                    Ok(_) => true,
                    Err(error) => matches!(
                        error.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                    ),
                };
                let _ = reply.send(open);
            }
            // A valid terminal chunk, not an artificial task/process exit,
            // ends the actual physical response after the owner assertion.
            socket.write_all(b"1\r\nz\r\n0\r\n\r\n").unwrap();
            socket.shutdown(std::net::Shutdown::Write).unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            let eof = matches!(socket.read(&mut byte), Ok(0));
            let _ = closed.send(eof);
        });
        let runtime = ReadRuntime::new(ReadLimits::default()).unwrap();
        let trace = Arc::new(Trace::default());
        let watched = trace.clone();
        let (handle_send, handle_receive) = tokio::sync::oneshot::channel();
        let frame = runtime
            .run(1, move |work| async move {
                let client = Arc::new(
                    dispatcharr_upstream_http::Attempt::new(
                        Duration::from_secs(5),
                        Duration::from_secs(5),
                    )
                    .unwrap(),
                );
                let response = client
                    .request(&format!("http://{address}/owned"), "fixture-agent", &[])
                    .unwrap()
                    .send()
                    .await
                    .map_err(|_| ReadError::WorkerFailed)?;
                let guard: Arc<dyn Send + Sync> =
                    Arc::new((work.transport_guard()?.erased(), client));
                let deadline = work.control_deadline();
                let collector = tokio::spawn(collect_inner(
                    response,
                    guard,
                    maximum,
                    deadline,
                    None,
                    Some(watched),
                    true,
                ));
                handle_send
                    .send(collector)
                    .map_err(|_| ReadError::WorkerFailed)?;
                Ok(Vec::new())
            })
            .await
            .unwrap();
        drop(frame);
        let collector = handle_receive.await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !trace.failed.load(Ordering::Acquire) {
                let notified = trace.wake.notified();
                if trace.failed.load(Ordering::Acquire) {
                    break;
                }
                notified.await;
            }
        })
        .await
        .unwrap();
        assert!(trace.capacity.load(Ordering::Acquire));
        assert!(!trace.drained.load(Ordering::Acquire));
        let collector = if abort_join {
            collector.abort();
            assert!(collector.await.unwrap_err().is_cancelled());
            None
        } else {
            Some(collector)
        };
        assert!(runtime.physical_workers() > 0);
        assert!(runtime.retained_memory_bytes() >= ReadLimits::default().work_bytes);
        assert!(!runtime.shutdown(Duration::from_millis(20)).await);
        let (reply, receive) = tokio::sync::oneshot::channel();
        commands.send(Command::Probe(reply)).unwrap();
        assert!(
            receive.await.unwrap(),
            "fixture TCP connection closed before the terminal HTTP chunk"
        );
        commands.send(Command::Release).unwrap();
        assert!(tokio::time::timeout(Duration::from_secs(3), closed_receive)
            .await
            .unwrap()
            .unwrap());
        if let Some(collector) = collector {
            assert!(matches!(collector.await.unwrap(), Err(Fault::Capacity)));
        }
        assert!(runtime.shutdown(Duration::from_secs(2)).await);
        assert!(trace.drained.load(Ordering::Acquire));
        assert_eq!(runtime.retained_memory_bytes(), 0);
        assert_eq!(runtime.physical_workers(), 0);
        server.join().unwrap();
    }
    #[tokio::test]
    async fn actual_chunked_wire_cap_retains_owner_after_join_cancel_until_body_completion() {
        held_capacity(false, true).await;
    }
    #[tokio::test]
    async fn actual_gzip_expansion_cap_retains_owner_after_join_cancel_until_body_completion() {
        held_capacity(true, true).await;
    }
    #[tokio::test]
    async fn actual_capacity_reply_waits_for_body_completion_without_decode_on_discard() {
        held_capacity(false, false).await;
    }
    #[tokio::test]
    async fn actual_body_ready_waits_for_held_http_frames_and_survives_observer_abort() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (request_send, request_seen) = tokio::sync::oneshot::channel();
        let (headers_send, headers_receive) = std::sync::mpsc::channel();
        let (body_send, body_ready) = tokio::sync::oneshot::channel();
        let (finish_send, finish_receive) = std::sync::mpsc::channel();
        let server = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut headers = Vec::new();
            let mut byte = [0u8; 1];
            while !headers.ends_with(b"\r\n\r\n") {
                socket.read_exact(&mut byte).unwrap();
                headers.push(byte[0]);
            }
            request_send.send(()).unwrap();
            headers_receive
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
            socket.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n2000\r\n").unwrap();
            socket.write_all(&[b'x'; 8192]).unwrap();
            socket.write_all(b"\r\n").unwrap();
            body_send.send(()).unwrap();
            finish_receive.recv_timeout(Duration::from_secs(5)).unwrap();
            socket.write_all(b"0\r\n\r\n").unwrap();
        });
        let runtime = ReadRuntime::new_fair(ReadLimits::default()).unwrap();
        let trace = Arc::new(Trace::default());
        let observed = trace.clone();
        let own = runtime.clone();
        let request = tokio::spawn(async move {
            let scope = own
                .acquire_http(10, Instant::now() + Duration::from_secs(3))
                .await
                .unwrap();
            own.run_scoped(10, None, Some(scope), move |work| async move {
                let snapshot = work.freeze_snapshot(vec![0; 1024])?;
                let snapshot = work.admit_refresh(snapshot).await?;
                drop(work.consume_snapshot(snapshot)?);
                assert!(matches!(
                    super::super::vod_api_provider_refresh_http::observed_test_json(
                        &work,
                        &format!("http://{address}/credit-wait"),
                        observed
                    )
                    .await,
                    Err(Fault::Capacity)
                ));
                let mut document = work.document();
                document.write_all(b"{}").map_err(|_| ReadError::Capacity)?;
                Ok(document.finish())
            })
            .await
        });
        request_seen.await.unwrap();
        // Each actual retained response frame holds its HTTP slot, even after
        // its SQL/serialization worker has completed.
        let mut frames = Vec::new();
        for _ in 0..2 {
            let scope = runtime
                .acquire_http(10, Instant::now() + Duration::from_secs(3))
                .await
                .unwrap();
            let frame = runtime
                .run_scoped(10, None, Some(scope), |work| async move {
                    let mut document = work.document();
                    document.write_all(b"{}").map_err(|_| ReadError::Capacity)?;
                    Ok(document.finish())
                })
                .await
                .unwrap();
            frames.push(frame);
        }
        headers_send.send(()).unwrap();
        body_ready.await.unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(
            !trace.started.load(Ordering::Acquire),
            "body pump ran without active continuation credits"
        );
        assert_eq!(runtime.physical_workers(), 0);
        assert!(runtime.retained_memory_bytes() >= ReadLimits::default().work_bytes);
        // Dropping the public observer must not drop the admitted continuation.
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        assert!(!runtime.shutdown(Duration::from_millis(20)).await);
        drop(frames.pop());
        tokio::time::timeout(Duration::from_secs(1), async {
            while !trace.failed.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(trace.started.load(Ordering::Acquire));
        assert!(trace.capacity.load(Ordering::Acquire));
        assert!(!trace.drained.load(Ordering::Acquire));
        assert!(runtime.physical_workers() > 0);
        assert!(!runtime.shutdown(Duration::from_millis(20)).await);
        finish_send.send(()).unwrap();
        drop(frames);
        assert!(runtime.shutdown(Duration::from_secs(2)).await);
        assert!(trace.drained.load(Ordering::Acquire));
        assert_eq!(runtime.retained_memory_bytes(), 0);
        server.join().unwrap();
    }
}
