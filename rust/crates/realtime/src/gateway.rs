//! Source-shaped WebSocket consumer with shared serialization and bounded frames.
//! Original Dispatcharr authors retain full credit; preview parity remains gated.
use crate::{
    hub::{self, Delivery, Hub, HubConfig},
    policy::Visibility,
    RealtimeBus,
};
use axum::{
    body::Body,
    extract::State,
    http::{Request, Response, StatusCode},
    routing::get,
    Router,
};
use bytes::Bytes;
use dispatcharr_contracts::{BoxFuture, UpdateEventError};
use dispatcharr_store::AuthService;
use futures_util::{stream::SplitSink, SinkExt, StreamExt};
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use rand::RngCore;
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, Weak,
    },
    time::Duration,
};
use tokio::{
    sync::{mpsc, watch, Mutex as AsyncMutex, Notify, OwnedSemaphorePermit, Semaphore},
    time::{self, Instant},
};
use tokio_tungstenite::{
    tungstenite::{
        self,
        protocol::{
            frame::{
                coding::{CloseCode, Data, OpCode},
                Frame,
            },
            CloseFrame, Role, WebSocketConfig,
        },
        Message,
    },
    WebSocketStream,
};

const FRAGMENT: usize = 16 * 1024;
const SOCKET_OUTPUT_CREDIT: usize = 64 * 1024;
type Socket = WebSocketStream<TokioIo<Upgraded>>;
type Writer = SplitSink<Socket, Message>;

pub trait PreviewHandler: Send + Sync {
    fn render(&self, request: Value) -> BoxFuture<'_, Result<Value, UpdateEventError>>;
}
#[derive(Clone, Debug)]
pub struct GatewayConfig {
    pub max_buffered_events: usize,
    pub max_buffered_bytes: usize,
    pub max_shared_wire_bytes: usize,
    pub max_wire_message_bytes: usize,
    pub max_private_wire_bytes: usize,
    pub max_socket_output_bytes: usize,
    pub max_inbound_message_bytes: usize,
    pub send_timeout: Duration,
    pub handshake_timeout: Duration,
    pub ping_interval: Duration,
    pub pong_timeout: Duration,
    pub max_connection_lifetime: Duration,
}
impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            max_buffered_events: 100,
            max_buffered_bytes: 32 * 1024 * 1024,
            max_shared_wire_bytes: 64 * 1024 * 1024,
            max_wire_message_bytes: 25 * 1024 * 1024,
            max_private_wire_bytes: 16 * 1024 * 1024,
            max_socket_output_bytes: 32 * 1024 * 1024,
            max_inbound_message_bytes: 1024 * 1024,
            send_timeout: Duration::from_secs(2),
            handshake_timeout: Duration::from_secs(5),
            ping_interval: Duration::from_secs(20),
            pong_timeout: Duration::from_secs(30),
            max_connection_lifetime: Duration::from_secs(86400),
        }
    }
}
#[derive(Clone)]
pub struct RealtimeGateway {
    inner: Arc<Inner>,
}
struct Inner {
    auth: AuthService,
    bus: RealtimeBus,
    hub: Arc<Hub>,
    config: GatewayConfig,
    preview: Mutex<Option<Arc<dyn PreviewHandler>>>,
    private_bytes: Arc<Semaphore>,
    private_encoder: AsyncMutex<()>,
    socket_buffers: Arc<Semaphore>,
    shutdown: watch::Sender<bool>,
    workers: AtomicUsize,
    done: Notify,
}
struct Worker(Weak<Inner>);
impl Drop for Worker {
    fn drop(&mut self) {
        if let Some(inner) = self.0.upgrade() {
            inner.workers.fetch_sub(1, Ordering::AcqRel);
            inner.done.notify_waiters();
        }
    }
}
#[derive(Clone, Debug)]
enum End {
    Close(u16),
    PeerClosed,
    TransportEnded,
}

impl RealtimeGateway {
    pub fn new(
        auth: AuthService,
        bus: RealtimeBus,
        config: GatewayConfig,
    ) -> Result<Self, UpdateEventError> {
        if config.max_private_wire_bytes < 1024
            || config.max_private_wire_bytes > u32::MAX as usize
            || config.max_socket_output_bytes < SOCKET_OUTPUT_CREDIT
            || config.max_socket_output_bytes > u32::MAX as usize
            || config.max_inbound_message_bytes < 1024
            || config.max_inbound_message_bytes > 1024 * 1024
            || config.send_timeout.is_zero()
            || config.handshake_timeout.is_zero()
            || config.ping_interval.is_zero()
            || config.pong_timeout.is_zero()
            || config.max_connection_lifetime.is_zero()
        {
            return Err(UpdateEventError::InvalidMessage);
        }
        let hub = Hub::new(HubConfig {
            max_events: config.max_buffered_events,
            max_buffered_bytes: config.max_buffered_bytes,
            max_live_bytes: config.max_shared_wire_bytes,
            max_wire_bytes: config.max_wire_message_bytes,
            capacity_wait: config.send_timeout,
        })?;
        let inner = Arc::new(Inner {
            auth,
            bus,
            hub,
            private_bytes: Arc::new(Semaphore::new(config.max_private_wire_bytes)),
            private_encoder: AsyncMutex::new(()),
            socket_buffers: Arc::new(Semaphore::new(config.max_socket_output_bytes)),
            config,
            preview: Mutex::new(None),
            shutdown: watch::channel(false).0,
            workers: AtomicUsize::new(1),
            done: Notify::new(),
        });
        let weak = Arc::downgrade(&inner);
        let shutdown = inner.shutdown.subscribe();
        tokio::spawn(async move {
            let _worker = Worker(weak.clone());
            receiver(weak, shutdown).await;
        });
        Ok(Self { inner })
    }
    pub fn with_preview_handler(self, handler: Arc<dyn PreviewHandler>) -> Self {
        *self.inner.preview.lock().unwrap() = Some(handler);
        self
    }
    pub fn router(&self) -> Router {
        Router::new()
            .route("/ws/", get(upgrade))
            .with_state(self.clone())
    }
    pub fn diagnostics(&self) -> Value {
        json!({"format_version":1,"local_sockets":self.inner.hub.clients(),
        "reserved_connections":self.inner.bus.config().max_connections-self.inner.bus.available_connections(),
        "shared_live_bytes":self.inner.hub.live_bytes(),"shared_buffered_bytes":self.inner.hub.buffered_bytes(),
        "private_live_bytes":self.inner.config.max_private_wire_bytes-self.inner.private_bytes.available_permits(),
        "socket_output_reserved_bytes":self.inner.config.max_socket_output_bytes-self.inner.socket_buffers.available_permits()})
    }
    pub async fn shutdown(&self) -> bool {
        self.inner.shutdown.send_replace(true);
        self.inner.bus.stop_accepting();
        self.inner.hub.shutdown();
        self.inner.socket_buffers.close();
        self.inner.private_bytes.close();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if self.inner.workers.load(Ordering::Acquire) == 0
                && self.inner.bus.available_connections() == self.inner.bus.config().max_connections
                && self.inner.bus.available_read_bytes() == self.inner.bus.config().max_read_bytes
                && self.inner.bus.available_publish_bytes()
                    == self.inner.bus.config().max_publish_bytes
            {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            time::sleep(Duration::from_millis(10)).await;
        }
    }
}

async fn upgrade(
    State(gateway): State<RealtimeGateway>,
    mut request: Request<Body>,
) -> Response<Body> {
    let inner = &gateway.inner;
    if *inner.shutdown.borrow() {
        return status(StatusCode::SERVICE_UNAVAILABLE);
    }
    // The trusted handshake factory validates HTTP/1 upgrade headers. Original
    // auth is query access JWT only; Authorization/API-key headers are ignored.
    let response =
        match tungstenite::handshake::server::create_response_with_body(&request, Body::empty) {
            Ok(response) => response,
            Err(_) => return status(StatusCode::BAD_REQUEST),
        };
    let buffers = match inner
        .socket_buffers
        .clone()
        .try_acquire_many_owned(SOCKET_OUTPUT_CREDIT as u32)
    {
        Ok(permit) => permit,
        Err(_) => return status(StatusCode::SERVICE_UNAVAILABLE),
    };
    let member = match inner.bus.reserve_member() {
        Ok(member) => member,
        Err(_) => return status(StatusCode::SERVICE_UNAVAILABLE),
    };
    let query = url::form_urlencoded::parse(request.uri().query().unwrap_or("").as_bytes())
        .into_owned()
        .collect::<Vec<_>>();
    let user = match inner.auth.websocket_user(&query).await {
        Ok(Some(user)) => user,
        _ => return status(StatusCode::FORBIDDEN),
    };
    if *inner.shutdown.borrow() {
        return status(StatusCode::SERVICE_UNAVAILABLE);
    }
    let upgraded = hyper::upgrade::on(&mut request);
    let inner = inner.clone();
    let role = user.user_level;
    inner.workers.fetch_add(1, Ordering::AcqRel);
    tokio::spawn(async move {
        let _worker = Worker(Arc::downgrade(&inner));
        if let Ok(Ok(upgraded)) = time::timeout(inner.config.handshake_timeout, upgraded).await {
            serve(inner, upgraded, member, buffers, role).await;
        }
    });
    response
}
fn status(code: StatusCode) -> Response<Body> {
    Response::builder()
        .status(code)
        .body(Body::empty())
        .unwrap()
}

async fn receiver(weak: Weak<Inner>, mut shutdown: watch::Receiver<bool>) {
    loop {
        let Some(inner) = weak.upgrade() else { return };
        if *shutdown.borrow() {
            return;
        }
        let bus = inner.bus.clone();
        let hub = inner.hub.clone();
        let activity = hub.activity();
        let interval = bus.config().poll_interval;
        drop(inner);
        if hub.clients() == 0 {
            let changed = activity.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if hub.clients() == 0 {
                tokio::select! {_=shutdown.changed()=>return,_=changed=>{}}
            }
            continue;
        }
        match bus.pop().await {
            Ok(Some(mut incoming)) => {
                let _ = hub
                    .publish(&incoming.message, std::mem::take(&mut incoming.recipients))
                    .await;
                let _ = incoming.ack().await;
            }
            _ => {
                tokio::select! {_=shutdown.changed()=>return,_=time::sleep(interval)=>{}}
            }
        }
    }
}

async fn private_wire(inner: &Inner, message: &Value) -> Result<Bytes, End> {
    let mut shutdown = inner.shutdown.subscribe();
    if *shutdown.borrow() {
        return Err(End::Close(1001));
    }
    let _encoder = tokio::select! {
        biased;
        _=shutdown.changed()=>return Err(End::Close(1001)),
        result=time::timeout(inner.config.send_timeout,inner.private_encoder.lock())=>result.map_err(|_|End::Close(1013))?,
    };
    if *shutdown.borrow() {
        return Err(End::Close(1001));
    }
    let maximum = inner
        .config
        .max_wire_message_bytes
        .min(inner.config.max_private_wire_bytes.saturating_sub(128));
    let wire = hub::serialize(message, maximum).map_err(|_| End::Close(1011))?;
    let weight = wire.len() + 128;
    let budget = time::timeout(
        inner.config.send_timeout,
        inner
            .private_bytes
            .clone()
            .acquire_many_owned(weight as u32),
    )
    .await
    .map_err(|_| End::Close(1013))?
    .map_err(|_| End::Close(1001))?;
    Ok(hub::leased_wire(wire, budget))
}
async fn fragments(writer: &mut Writer, wire: Bytes) -> Result<(), tungstenite::Error> {
    for offset in (0..wire.len()).step_by(FRAGMENT) {
        let end = (offset + FRAGMENT).min(wire.len());
        let kind = if offset == 0 {
            Data::Text
        } else {
            Data::Continue
        };
        writer
            .send(Message::Frame(Frame::message(
                wire.slice(offset..end),
                OpCode::Data(kind),
                end == wire.len(),
            )))
            .await?;
    }
    Ok(())
}
async fn transmit(
    writer: &mut Writer,
    wire: Bytes,
    control: &mut watch::Receiver<Option<End>>,
    shutdown: &mut watch::Receiver<bool>,
    timeout: Duration,
) -> Result<(), End> {
    if let Some(end) = control.borrow().clone() {
        return Err(end);
    }
    if *shutdown.borrow() {
        return Err(End::Close(1001));
    }
    tokio::select! {
        biased;
        _=shutdown.changed()=>Err(End::Close(1001)),
        _=control.changed()=>Err(control.borrow().clone().unwrap_or(End::TransportEnded)),
        result=time::timeout(timeout,fragments(writer,wire))=>match result {Ok(Ok(()))=>Ok(()),Ok(Err(_))=>Err(End::TransportEnded),Err(_)=>Err(End::Close(1013))},
    }
}
async fn serve(
    inner: Arc<Inner>,
    upgraded: Upgraded,
    member: crate::Membership,
    buffers: OwnedSemaphorePermit,
    role: i32,
) {
    let config = WebSocketConfig::default()
        .read_buffer_size(16 * 1024)
        .write_buffer_size(0)
        .max_write_buffer_size(FRAGMENT * 2 + 28)
        .max_message_size(Some(inner.config.max_inbound_message_bytes))
        .max_frame_size(Some(inner.config.max_inbound_message_bytes));
    let socket =
        WebSocketStream::from_raw_socket(TokioIo::new(upgraded), Role::Server, Some(config)).await;
    let (mut writer, mut reader) = socket.split();
    let mut subscription = match inner.hub.register(member.id().to_owned()) {
        Ok(subscription) => subscription,
        Err(_) => return,
    };
    let mut subscription_stopped = subscription.stopped();
    let mut shutdown = inner.shutdown.subscribe();
    let (control_sender, mut control) = watch::channel(None::<End>);
    let (command_sender, mut commands) = mpsc::channel(1);
    let (pong_sender, mut pongs) = watch::channel(Bytes::new());
    let reader_task = tokio::spawn(async move {
        while let Some(message) = reader.next().await {
            match message {
                Ok(Message::Text(text)) => {
                    if command_sender.try_send(text).is_err() {
                        control_sender.send_replace(Some(End::Close(1013)));
                        return;
                    }
                }
                Ok(Message::Binary(_)) => {
                    control_sender.send_replace(Some(End::Close(1011)));
                    return;
                }
                Ok(Message::Ping(_)) => {}
                Ok(Message::Pong(data)) => {
                    pong_sender.send_replace(data);
                }
                Ok(Message::Close(_)) => {
                    control_sender.send_replace(Some(End::PeerClosed));
                    return;
                }
                Ok(Message::Frame(_)) => {}
                Err(tungstenite::Error::Capacity(_)) => {
                    control_sender.send_replace(Some(End::Close(1009)));
                    return;
                }
                Err(tungstenite::Error::Utf8(_)) => {
                    control_sender.send_replace(Some(End::Close(1007)));
                    return;
                }
                Err(tungstenite::Error::Protocol(_)) => {
                    control_sender.send_replace(Some(End::Close(1002)));
                    return;
                }
                Err(_) => {
                    control_sender.send_replace(Some(End::TransportEnded));
                    return;
                }
            }
        }
        control_sender.send_replace(Some(End::TransportEnded));
    });
    let work = async {
        tokio::select! {_=shutdown.changed()=>return Err(End::Close(1001)),result=member.join()=>result.map_err(|_|End::Close(1011))?};
        let first = private_wire(
            &inner,
            &dispatcharr_store::realtime::connection_established(),
        )
        .await?;
        transmit(
            &mut writer,
            first,
            &mut control,
            &mut shutdown,
            inner.config.send_timeout,
        )
        .await?;
        if let Ok(Some(payload)) = inner.auth.websocket_cached_ip().await {
            let mut data = serde_json::Map::new();
            data.insert("type".into(), Value::String("ip_lookup_complete".into()));
            if let Value::Object(payload) = payload {
                data.extend(payload);
            }
            let wire = private_wire(&inner, &json!({"data":data})).await?;
            transmit(
                &mut writer,
                wire,
                &mut control,
                &mut shutdown,
                inner.config.send_timeout,
            )
            .await?;
        }
        let mut ping = time::interval_at(
            Instant::now() + inner.config.ping_interval,
            inner.config.ping_interval,
        );
        ping.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        let mut waiting_pong: Option<(Bytes, Instant)> = None;
        loop {
            let deadline = waiting_pong
                .as_ref()
                .map(|(_, since)| *since + inner.config.pong_timeout);
            let pong_deadline = async move {
                match deadline {
                    Some(deadline) => time::sleep_until(deadline).await,
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::select! {
                biased;
                _=shutdown.changed()=>return Err(End::Close(1001)),
                _=subscription_stopped.changed()=>return Err(End::Close(1001)),
                _=control.changed()=>return Err(control.borrow().clone().unwrap_or(End::TransportEnded)),
                _=pongs.changed()=>{if waiting_pong.as_ref().is_some_and(|(expected,_)|expected==&*pongs.borrow()){waiting_pong=None;}},
                _=pong_deadline=>return Err(End::TransportEnded),
                _=ping.tick()=>{
                    if waiting_pong.is_none(){let mut nonce=[0;12];rand::rngs::OsRng.try_fill_bytes(&mut nonce).map_err(|_|End::Close(1011))?;
                        let data=Bytes::copy_from_slice(&nonce);
                        tokio::select! {
                            biased;
                            _=shutdown.changed()=>return Err(End::Close(1001)),
                            _=control.changed()=>return Err(control.borrow().clone().unwrap_or(End::TransportEnded)),
                            result=time::timeout(inner.config.send_timeout,writer.send(Message::Ping(data.clone())))=>match result {
                                Ok(Ok(()))=>{},Ok(Err(_))=>return Err(End::TransportEnded),Err(_)=>return Err(End::Close(1013)),
                            },
                        }
                        waiting_pong=Some((data,Instant::now()));}
                },
                command=commands.recv()=>{
                    let Some(command)=command else{return Err(End::TransportEnded)};
                    let request:Value=serde_json::from_str(&command).map_err(|_|End::Close(1011))?;
                    let kind=request.as_object().and_then(|object|object.get("type")).ok_or(End::Close(1011))?;
                    if kind=="m3u_profile_test"&&role>=10 {
                        let preview=inner.preview.lock().unwrap().clone().ok_or(End::Close(1011))?;
                        let response=tokio::select! {
                            biased;
                            _=shutdown.changed()=>return Err(End::Close(1001)),
                            _=control.changed()=>return Err(control.borrow().clone().unwrap_or(End::TransportEnded)),
                            result=time::timeout(inner.config.send_timeout,preview.render(request))=>result.map_err(|_|End::Close(1011))?.map_err(|_|End::Close(1011))?,
                        };
                        let wire=private_wire(&inner,&response).await?;
                        transmit(&mut writer,wire,&mut control,&mut shutdown,inner.config.send_timeout).await?;
                    }
                },
                packet=subscription.next()=>match packet {
                    Delivery::Stopped=>return Err(End::Close(1001)),Delivery::Lagged=>return Err(End::Close(1013)),
                    Delivery::Packet(packet)=>{
                        if packet.visibility==Visibility::Invalid{return Err(End::Close(1011));}
                        if packet.visibility==Visibility::Administrative&&role<10{continue;}
                        transmit(&mut writer,packet.wire.clone(),&mut control,&mut shutdown,inner.config.send_timeout).await?;
                    }
                },
            }
        }
    };
    let ended: Result<(), End> =
        match time::timeout(inner.config.max_connection_lifetime, work).await {
            Ok(ended) => ended,
            Err(_) => Err(End::Close(1000)),
        };
    match ended.err().unwrap_or(End::TransportEnded) {
        End::Close(code) => {
            let _ = time::timeout(
                Duration::from_millis(250),
                writer.send(Message::Close(Some(CloseFrame {
                    code: CloseCode::from(code),
                    reason: "".into(),
                }))),
            )
            .await;
        }
        End::PeerClosed => {
            let _ = time::timeout(Duration::from_millis(250), writer.flush()).await;
        }
        End::TransportEnded => {}
    }
    drop(writer);
    reader_task.abort();
    let _ = reader_task.await;
    drop(subscription);
    drop(buffers);
    drop(member);
}
