//! A loopback RESP command gate for the second real peer-tail snapshot.
use std::{
    io,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::watch,
    task::{JoinHandle, JoinSet},
};

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum Decision {
    Hold,
    Forward,
    Disconnect,
}

pub(super) struct Gate {
    pub(super) url: String,
    pub(super) reached: watch::Receiver<bool>,
    pub(super) decision: watch::Sender<Decision>,
    pub(super) claims: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}

impl Drop for Gate {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn unexpected() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "unexpected Redis command framing",
    )
}

fn number(line: &[u8], marker: u8) -> io::Result<usize> {
    let bytes = line
        .strip_prefix(&[marker])
        .and_then(|line| line.strip_suffix(b"\r\n"))
        .ok_or_else(unexpected)?;
    let value = std::str::from_utf8(bytes).map_err(|_| unexpected())?;
    value.parse().map_err(|_| unexpected())
}

struct Command {
    wire: Vec<u8>,
    prefix: Vec<Vec<u8>>,
}

impl Command {
    async fn read(reader: &mut (impl AsyncBufRead + Unpin)) -> io::Result<Option<Self>> {
        let mut wire = Vec::new();
        if reader.read_until(b'\n', &mut wire).await? == 0 {
            return Ok(None);
        }
        let parts = number(&wire, b'*')?;
        if !(1..=128).contains(&parts) {
            return Err(unexpected());
        }
        let mut prefix = Vec::with_capacity(4);
        for index in 0..parts {
            let start = wire.len();
            reader.read_until(b'\n', &mut wire).await?;
            let length = number(&wire[start..], b'$')?;
            let payload = wire.len();
            if length > 262_144 || payload.saturating_add(length) > 524_288 {
                return Err(unexpected());
            }
            wire.resize(payload + length + 2, 0);
            reader.read_exact(&mut wire[payload..]).await?;
            if &wire[payload + length..] != b"\r\n" {
                return Err(unexpected());
            }
            if index < 4 {
                prefix.push(wire[payload..payload + length].to_vec());
            }
        }
        Ok(Some(Self { wire, prefix }))
    }

    fn is_script(&self, script: &[u8], key: &[u8]) -> bool {
        match self.prefix.as_slice() {
            [command, sha, _, pool] => {
                command.as_slice() == b"EVALSHA"
                    && sha.as_slice() == script
                    && pool.as_slice() == key
            }
            _ => false,
        }
    }
}

impl Gate {
    pub(super) async fn start(
        redis: &str,
        snapshot_script: String,
        claim_script: String,
        key: String,
    ) -> Self {
        let mut target = url::Url::parse(redis).unwrap();
        assert_eq!(target.scheme(), "redis");
        let address = (
            target.host_str().unwrap().to_owned(),
            target.port().unwrap_or(6379),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        target.set_host(Some("127.0.0.1")).unwrap();
        target
            .set_port(Some(listener.local_addr().unwrap().port()))
            .unwrap();
        let (parked, reached) = watch::channel(false);
        let (decision, action) = watch::channel(Decision::Hold);
        let matched = Arc::new(AtomicUsize::new(0));
        let claims = Arc::new(AtomicUsize::new(0));
        let observed_claims = claims.clone();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (client, _) = accepted.unwrap();
                        let target = address.clone();
                        let snapshot_script = snapshot_script.clone();
                        let claim_script = claim_script.clone();
                        let claims = observed_claims.clone();
                        let key = key.clone();
                        let matched = matched.clone();
                        let parked = parked.clone();
                        let mut action = action.clone();
                        connections.spawn(async move {
                            let server = TcpStream::connect((target.0.as_str(), target.1)).await?;
                            let (client_read, mut client_write) = client.into_split();
                            let (mut server_read, mut server_write) = server.into_split();
                            let commands = async {
                                let mut reader = BufReader::new(client_read);
                                while let Some(command) = Command::read(&mut reader).await? {
                                    if command.is_script(claim_script.as_bytes(), key.as_bytes()) {
                                        claims.fetch_add(1, Ordering::SeqCst);
                                    }
                                    if command.is_script(snapshot_script.as_bytes(), key.as_bytes())
                                        && matched.fetch_add(1, Ordering::SeqCst) == 1
                                    {
                                        parked.send_replace(true);
                                        let action = *action.wait_for(|state| *state != Decision::Hold)
                                            .await.map_err(|_| unexpected())?;
                                        if action == Decision::Disconnect {
                                            return Err(io::Error::new(io::ErrorKind::ConnectionReset, "isolated revalidation disconnect"));
                                        }
                                    }
                                    server_write.write_all(&command.wire).await?;
                                }
                                server_write.shutdown().await
                            };
                            let replies = tokio::io::copy(&mut server_read, &mut client_write);
                            tokio::try_join!(commands, replies)
                        });
                    }
                    _ = connections.join_next(), if !connections.is_empty() => {}
                }
            }
        });
        Self {
            url: target.to_string(),
            reached,
            decision,
            claims,
            task,
        }
    }
}
