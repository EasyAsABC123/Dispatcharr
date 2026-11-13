from pathlib import Path
P=Path(__file__).resolve().parent/'source/rust/crates/store/src/backup_callback_physical_tests.rs';s=P.read_text();mark='// PRIVATE, UNEXECUTED. Cases1–4 only; caller abort and HTTP FD loss are distinct.'
trace=r'''
// Owned test-only transport witness. A peer FD close does not imply that Hyper
// attempted a response write. Retain actual read/write/handler/drop observations.
#[derive(Default, serde::Serialize)]
struct HttpFate {
    accepted: usize,
    handler_started: usize,
    handler_cancelled: usize,
    response_statuses: Vec<u16>,
    bytes_read: usize,
    bytes_written: usize,
    read_eof: bool,
    write_errors: Vec<String>,
    socket_dropped: bool,
    capacity_exceeded: bool,
}
type SharedFate = Arc<std::sync::Mutex<HttpFate>>;
struct FateIo { stream: TcpStream, fate: SharedFate }
impl tokio::io::AsyncRead for FateIo {
    fn poll_read(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>, buf: &mut tokio::io::ReadBuf<'_>) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        let result = std::pin::Pin::new(&mut this.stream).poll_read(cx, buf);
        if let std::task::Poll::Ready(Ok(())) = &result {
            let n = buf.filled().len() - before;
            let mut fate = this.fate.lock().unwrap();
            fate.bytes_read += n;
            fate.read_eof |= n == 0;
            fate.capacity_exceeded |= fate.bytes_read > 65536;
        }
        result
    }
}
impl tokio::io::AsyncWrite for FateIo {
    fn poll_write(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>, buf: &[u8]) -> std::task::Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        let result = std::pin::Pin::new(&mut this.stream).poll_write(cx, buf);
        match &result {
            std::task::Poll::Ready(Ok(n)) => {
                let mut fate = this.fate.lock().unwrap();
                fate.bytes_written += n;
                fate.capacity_exceeded |= fate.bytes_written > 65536;
            }
            std::task::Poll::Ready(Err(error)) => {
                let mut fate = this.fate.lock().unwrap();
                if fate.write_errors.len() < 8 { fate.write_errors.push(format!("{:?}", error.kind())); }
                else { fate.capacity_exceeded = true; }
            }
            std::task::Poll::Pending => {}
        }
        result
    }
    fn poll_flush(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().stream).poll_flush(cx)
    }
    fn poll_shutdown(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }
}
impl Drop for FateIo {
    fn drop(&mut self) { self.fate.lock().unwrap().socket_dropped = true; }
}
struct FateListener { listener: tokio::net::TcpListener, fate: SharedFate }
impl axum::serve::Listener for FateListener {
    type Io = FateIo;
    type Addr = SocketAddr;
    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        let (stream, address) = self.listener.accept().await.expect("OwnedHTTPAccept");
        self.fate.lock().unwrap().accepted += 1;
        (FateIo { stream, fate: self.fate.clone() }, address)
    }
    fn local_addr(&self) -> std::io::Result<Self::Addr> { self.listener.local_addr() }
}
struct HandlerFate { fate: SharedFate, returned: bool }
impl Drop for HandlerFate {
    fn drop(&mut self) { if !self.returned { self.fate.lock().unwrap().handler_cancelled += 1; } }
}
fn held_duration(stats: &Value, case: u64) -> f64 {
    let (rows, start, end) = if case == 4 {
        (stats["rows"].as_array().unwrap(), "backend-target-reply-read-held", "backend-target-reply-released")
    } else {
        (stats["events"].as_array().unwrap(), "ready-held", "ready-forwarded")
    };
    let starts: Vec<_> = rows.iter().filter(|v| v["kind"] == start).collect();
    let ends: Vec<_> = rows.iter().filter(|v| v["kind"] == end).collect();
    assert_eq!(starts.len(), 1);
    assert_eq!(ends.len(), 1);
    let elapsed = ends[0]["monotonic"].as_f64().unwrap() - starts[0]["monotonic"].as_f64().unwrap();
    assert!(elapsed.is_finite() && (0.0..=25.0).contains(&elapsed), "ActualPhysicalHold25");
    elapsed
}
'''
assert s.count(mark)==1;s=s.replace(mark,trace+'\n'+mark)
s=s.replace('let mut http_stream=None;','let mut http_stream=None;\n    let http_fate: SharedFate = Arc::new(std::sync::Mutex::new(HttpFate::default()));')
old='let router=files.clone().router();';new='''let handler_fate = http_fate.clone();
        let router=files.clone().router().layer(axum::middleware::from_fn(move |request: Request<Body>, next: axum::middleware::Next| {
            let fate = handler_fate.clone();
            async move {
                fate.lock().unwrap().handler_started += 1;
                let mut guard = HandlerFate { fate: fate.clone(), returned: false };
                let response = next.run(request).await;
                fate.lock().unwrap().response_statuses.push(response.status().as_u16());
                guard.returned = true;
                response
            }
        }));
        let listener = FateListener { listener, fate: http_fate.clone() };''';assert s.count(old)==1;s=s.replace(old,new)
old='report["HTTP_server_graceful_wait0"]=json!(true);}';new='''report["HTTP_server_graceful_wait0"]=json!(true);
        let fate = http_fate.lock().unwrap();
        report["actual_HTTP_response_write_fate"] = serde_json::to_value(&*fate).unwrap();
        assert_eq!(fate.accepted, 1); assert_eq!(fate.handler_started, 1);
        assert_eq!(fate.response_statuses.len() + fate.handler_cancelled, 1);
        assert!(fate.socket_dropped && !fate.capacity_exceeded);
        report["HTTP_write_failure_or_response_status_inferred_from_client_loss"] = json!(false);
    }''';assert s.count(old)==1;s=s.replace(old,new)
old='report["proxy_final"]=peer.finish();';new='''report["actual_physical_hold_seconds"] = json!(held_duration(&peer.stats(), case));
    report["proxy_final"]=peer.finish();''';assert s.count(old)==1;s=s.replace(old,new)
P.write_text(s)
print('Completed bounded test-only actual HTTP fate and full relay monotonic hold<=25 checks; uncompiled')
