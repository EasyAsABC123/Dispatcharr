//! Test-only transparent real-Redis reply gate; application authors keep credit.
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    task::{JoinHandle, JoinSet},
};
pub struct Proxy {
    pub url: String,
    pub hold: watch::Sender<bool>,
    pub blocked: watch::Receiver<bool>,
    task: JoinHandle<()>,
}
impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}
pub async fn start() -> Proxy {
    let mut target = url::Url::parse(&std::env::var("REDIS_TEST_URL").unwrap()).unwrap();
    let address = format!(
        "{}:{}",
        target.host_str().unwrap(),
        target.port().unwrap_or(6379)
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    target.set_host(Some("127.0.0.1")).unwrap();
    target
        .set_port(Some(listener.local_addr().unwrap().port()))
        .unwrap();
    let (hold, receiver) = watch::channel(false);
    let (blocked, observed) = watch::channel(false);
    let task = tokio::spawn(async move {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                accepted=listener.accept()=>{
                    let(client,_)=accepted.unwrap();let address=address.clone();let mut hold=receiver.clone();let blocked=blocked.clone();
                    connections.spawn(async move {
                        let server=TcpStream::connect(address).await.unwrap();
                        let(mut client_read,mut client_write)=client.into_split();let(mut server_read,mut server_write)=server.into_split();
                        let inbound=async move {
                            let mut buffer=[0u8;16384];
                            loop {
                                let count=server_read.read(&mut buffer).await?;
                                if count==0{return Ok::<(),std::io::Error>(());}
                                if *hold.borrow(){blocked.send_replace(true);}
                                if hold.wait_for(|paused|!*paused).await.is_err(){return Ok(());}
                                client_write.write_all(&buffer[..count]).await?;
                            }
                        };
                        let _=tokio::try_join!(async{tokio::io::copy(&mut client_read,&mut server_write).await.map(|_|())},inbound);
                    });
                },
                _=connections.join_next(),if !connections.is_empty()=>{},
            }
        }
    });
    Proxy {
        url: target.to_string(),
        hold,
        blocked: observed,
        task,
    }
}
pub async fn observed(proxy: &mut Proxy) {
    tokio::time::timeout(
        Duration::from_secs(3),
        proxy.blocked.wait_for(|value| *value),
    )
    .await
    .unwrap()
    .unwrap();
}
