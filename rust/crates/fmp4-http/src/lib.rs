//! Native fMP4 remux transport ported from Dispatcharr's original implementation.
//! Full credit for the application belongs to its original authors/contributors.
use axum::{body::Body, http::header, response::Response};
use bytes::Bytes;
use dispatcharr_clients::{ClientLease, ClientStore};
use dispatcharr_contracts::{PlaybackError, PlaybackSource};
use dispatcharr_fmp4_core::{Event, FragmentParser};
use dispatcharr_output_store::{OutputLease, OutputStore, SegmentBytes};
use dispatcharr_stream_http::StreamEngine;
use futures_util::StreamExt;
use std::{
    collections::HashMap,
    io,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::Command,
    sync::{watch, OwnedSemaphorePermit, Semaphore},
    time,
};

#[derive(Clone, Debug)]
pub struct Config {
    pub executable: String,
    pub max_remuxers: usize,
    pub max_init_bytes: usize,
    pub max_fragment_bytes: usize,
    pub initialization_timeout: Duration,
    pub stream_timeout: Duration,
    pub poll_interval: Duration,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            executable: "ffmpeg".into(),
            max_remuxers: 32,
            max_init_bytes: 10 * 1024 * 1024,
            max_fragment_bytes: 16 * 1024 * 1024,
            initialization_timeout: Duration::from_secs(15),
            stream_timeout: Duration::from_secs(60),
            poll_interval: Duration::from_millis(50),
        }
    }
}
#[derive(Clone)]
pub struct Fmp4Gateway {
    inner: Arc<Inner>,
}
struct Inner {
    engine: StreamEngine,
    clients: ClientStore,
    outputs: OutputStore,
    config: Config,
    slots: Arc<Semaphore>,
    managers: Mutex<HashMap<String, Arc<Manager>>>,
}
struct Manager {
    stop: watch::Sender<bool>,
    _slot: OwnedSemaphorePermit,
}
struct Frame {
    data: SegmentBytes,
    _client: ClientLease,
    _slot: OwnedSemaphorePermit,
}
impl AsRef<[u8]> for Frame {
    fn as_ref(&self) -> &[u8] {
        self.data.as_ref()
    }
}
fn failure(status: u16, detail: &str) -> PlaybackError {
    PlaybackError::new(status, detail)
}
fn wall() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}
impl Fmp4Gateway {
    pub fn new(
        engine: StreamEngine,
        clients: ClientStore,
        outputs: OutputStore,
        config: Config,
    ) -> Result<Self, PlaybackError> {
        if config.executable.is_empty()
            || config.max_remuxers == 0
            || config.max_remuxers > Semaphore::MAX_PERMITS
            || config.initialization_timeout.is_zero()
            || config.stream_timeout.is_zero()
            || config.poll_interval.is_zero()
            || FragmentParser::new(config.max_init_bytes, config.max_fragment_bytes).is_err()
        {
            return Err(failure(500, "Invalid fMP4 configuration"));
        }
        Ok(Self {
            inner: Arc::new(Inner {
                engine,
                clients,
                outputs,
                slots: Arc::new(Semaphore::new(config.max_remuxers)),
                config,
                managers: Mutex::new(HashMap::new()),
            }),
        })
    }
    pub async fn play(&self, source: PlaybackSource) -> Result<Response<Body>, PlaybackError> {
        let viewer = source
            .viewer
            .as_ref()
            .ok_or_else(|| failure(503, "Viewer metadata unavailable"))?;
        let client = self
            .inner
            .clients
            .register(&source.channel_id, viewer, "fmp4")
            .await
            .map_err(|error| match error {
                dispatcharr_clients::Error::UserLimit => failure(
                    429,
                    &format!(
                        "Stream limit exceeded ({} concurrent streams allowed)",
                        viewer.stream_limit
                    ),
                ),
                _ => failure(503, "Client admission unavailable"),
            })?;
        self.ensure(source.clone()).await?;
        let inner = self.inner.clone();
        let channel = source.channel_id;
        let behind = source.startup_behind;
        let output = async_stream::stream! {
            let client=client;let mut stopped=client.stopped();let started=time::Instant::now();
            let init=loop{
                if *stopped.borrow() || started.elapsed()>=inner.config.initialization_timeout{break None;}
                match inner.outputs.get_init(&channel,"fmp4").await{Ok(Some(data))=>break Some(data),Ok(None)=>{},Err(_)=>break None}
                tokio::select!{_=stopped.changed()=>break None,_=time::sleep(inner.config.poll_interval)=>{}}
            };
            if let Some(init)=init{
                let slots=Arc::new(Semaphore::new(1));
                let permit=slots.clone().acquire_owned().await.unwrap();
                client.record_bytes(init.as_ref().len());
                yield Ok::<Bytes,io::Error>(Bytes::from_owner(Frame{data:init,_client:client.clone(),_slot:permit}));
                let mut index=match inner.outputs.snapshot(&channel,"fmp4").await{
                    Ok(snapshot)=>if behind.is_zero(){snapshot.index.saturating_sub(1)}else{let cutoff=wall()-behind.as_secs_f64();snapshot.segments.iter().filter(|(_,stamp)|*stamp<=cutoff).next_back().map_or(0,|(sequence,_)|*sequence)},Err(_)=>0,
                };
                let mut last_data=time::Instant::now();
                'stream:loop{
                    if *stopped.borrow() || last_data.elapsed()>=inner.config.stream_timeout{break;}
                    let permit=tokio::select!{_=stopped.changed()=>break,result=slots.clone().acquire_owned()=>match result{Ok(permit)=>permit,Err(_)=>break}};
                    let snapshot=match inner.outputs.snapshot(&channel,"fmp4").await{Ok(snapshot)=>snapshot,Err(_)=>break};
                    if snapshot.owner.is_none(){break;}
                    let Some((sequence,_))=snapshot.segments.into_iter().find(|(sequence,_)|*sequence>index)else{drop(permit);tokio::select!{_=stopped.changed()=>break 'stream,_=time::sleep(inner.config.poll_interval)=>{}}continue;};
                    match inner.outputs.get(&channel,"fmp4",sequence).await{
                        Ok(Some(data))=>{index=sequence;last_data=time::Instant::now();client.record_bytes(data.as_ref().len());yield Ok(Bytes::from_owner(Frame{data,_client:client.clone(),_slot:permit}));},
                        Ok(None)=>{index=sequence;},Err(_)=>break,
                    }
                }
            }
        };
        let mut response = Response::new(Body::from_stream(output));
        response
            .headers_mut()
            .insert(header::CONTENT_TYPE, "video/mp4".parse().unwrap());
        response
            .headers_mut()
            .insert(header::CACHE_CONTROL, "no-cache".parse().unwrap());
        Ok(response)
    }
    async fn ensure(&self, source: PlaybackSource) -> Result<(), PlaybackError> {
        if self
            .inner
            .managers
            .lock()
            .unwrap()
            .contains_key(&source.channel_id)
        {
            return Ok(());
        }
        let slot = self
            .inner
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| failure(503, "fMP4 capacity reached"))?;
        let manager = Arc::new(Manager {
            stop: watch::channel(false).0,
            _slot: slot,
        });
        {
            let mut managers = self.inner.managers.lock().unwrap();
            if managers.contains_key(&source.channel_id) {
                return Ok(());
            }
            managers.insert(source.channel_id.clone(), manager.clone());
        }
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let token = uuid::Uuid::new_v4().to_string();
            if let Ok(Some(lease)) = inner
                .outputs
                .claim(&source.channel_id, "fmp4", &token)
                .await
            {
                let mut stop = manager.stop.subscribe();
                let _ = lease.set_state("initializing").await;
                let mut run = Box::pin(remux(inner.clone(), source.clone(), lease.clone()));
                let mut timer = time::interval(Duration::from_secs(1));
                let mut empty_since = None;
                let mut finished = false;
                loop {
                    tokio::select! {
                        _=&mut run,if !finished=>{finished=true;},
                        _=stop.changed()=>break,
                        _=timer.tick()=>{
                            if lease.renew().await.is_err(){break;}
                            match inner.clients.channel_clients(&source.channel_id).await{
                                Ok(clients)if clients.is_empty()=>{let start=empty_since.get_or_insert(time::Instant::now());if start.elapsed()>=source.shutdown_delay{break;}},
                                Ok(_)=>empty_since=None,Err(_)=>break,
                            }
                        }
                    }
                }
                drop(run);
                let _ = lease.release().await;
            }
            let mut managers = inner.managers.lock().unwrap();
            if managers
                .get(&source.channel_id)
                .is_some_and(|current| Arc::ptr_eq(current, &manager))
            {
                managers.remove(&source.channel_id);
            }
        });
        Ok(())
    }
    pub async fn shutdown(&self) {
        self.inner.slots.close();
        let managers = std::mem::take(&mut *self.inner.managers.lock().unwrap());
        for manager in managers.into_values() {
            manager.stop.send_replace(true);
        }
    }
}
const BASE: &[&str] = &[
    "-loglevel",
    "error",
    "-f",
    "mpegts",
    "-i",
    "pipe:0",
    "-c",
    "copy",
];
const TAIL: &[&str] = &[
    "-use_editlist",
    "0",
    "-flush_packets",
    "1",
    "-f",
    "mp4",
    "-movflags",
    "frag_keyframe+delay_moov+default_base_moof",
    "pipe:1",
];
async fn remux(inner: Arc<Inner>, source: PlaybackSource, lease: OutputLease) -> Result<(), ()> {
    for bsf in [true, false] {
        if run_process(inner.clone(), source.clone(), lease.clone(), bsf).await? {
            continue;
        }
        break;
    }
    Ok(())
}
async fn run_process(
    inner: Arc<Inner>,
    source: PlaybackSource,
    lease: OutputLease,
    bsf: bool,
) -> Result<bool, ()> {
    let mut input = inner.engine.open_input(source).await.map_err(|_| ())?;
    let mut command = Command::new(&inner.config.executable);
    command.args(BASE);
    if bsf {
        command.args([
            "-map",
            "0",
            "-max_interleave_delta",
            "100K",
            "-bsf:a",
            "aac_adtstoasc",
        ]);
    }
    command
        .args(TAIL)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn().map_err(|_| ())?;
    let mut stdin = child.stdin.take().ok_or(())?;
    let mut stdout = child.stdout.take().ok_or(())?;
    let mut stderr = child.stderr.take().ok_or(())?;
    let writer = async {
        while let Some(frame) = input.next().await {
            let frame = frame.map_err(|_| ())?;
            stdin.write_all(&frame.data).await.map_err(|_| ())?;
            stdin.flush().await.map_err(|_| ())?;
        }
        drop(stdin);
        Ok::<(), ()>(())
    };
    let reader = async {
        let mut parser =
            FragmentParser::new(inner.config.max_init_bytes, inner.config.max_fragment_bytes)
                .map_err(|_| ())?;
        let mut data = [0u8; 65536];
        loop {
            let count = stdout.read(&mut data).await.map_err(|_| ())?;
            if count == 0 {
                break;
            }
            for event in parser.push(&data[..count]).map_err(|_| ())? {
                publish(&lease, event).await?;
            }
        }
        if let Some(event) = parser.finish().map_err(|_| ())? {
            publish(&lease, event).await?;
        }
        Ok::<(), ()>(())
    };
    let errors = async {
        let mut data = [0u8; 4096];
        let mut tail = Vec::new();
        loop {
            let count = stderr.read(&mut data).await.map_err(|_| ())?;
            if count == 0 {
                return std::future::pending::<Result<bool, ()>>().await;
            }
            tail.extend_from_slice(&data[..count]);
            let message = String::from_utf8_lossy(&tail);
            if bsf
                && message.contains("aac_adtstoasc")
                && message.contains("is not supported by the bitstream filter")
            {
                return Ok(true);
            }
            if tail.len() > 512 {
                tail.drain(..tail.len() - 512);
            }
        }
    };
    tokio::pin!(writer, reader, errors);
    let result = tokio::select! {
        result=&mut reader=>result.map(|_|false),
        result=&mut errors=>result,
        result=&mut writer=>{result?;time::timeout(Duration::from_secs(3),&mut reader).await.map_err(|_|())?.map(|_|false)},
    };
    let _ = child.kill().await;
    let _ = time::timeout(Duration::from_secs(3), child.wait()).await;
    result
}
async fn publish(lease: &OutputLease, event: Event) -> Result<(), ()> {
    match event {
        Event::Init(data) => {
            lease.put_init(&data).await.map_err(|_| ())?;
            lease.set_state("active").await.map_err(|_| ())?;
        }
        Event::Fragment(data) => {
            lease.put(&data, wall()).await.map_err(|_| ())?;
        }
    }
    Ok(())
}
