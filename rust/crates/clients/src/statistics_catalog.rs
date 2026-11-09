//! Bounded global live metadata projection inputs, from original ChannelStatus.
//! Full original Dispatcharr authorship credit retained; no media is read here.
use super::*;
use serde_json::Value;
const PAGE_BYTES: usize = 4 * 1024 * 1024;

type ClientRow = (
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    String,
    Option<String>,
);
type Row = (String, Vec<String>, Option<String>, usize, Vec<ClientRow>);
pub struct ChannelStatisticsRecord {
    pub channel_id: String,
    pub metadata: BTreeMap<String, String>,
    pub index: u64,
    pub client_count: usize,
    pub clients: Vec<Value>,
}
pub struct ChannelStatisticsPage {
    pub cursor: u64,
    pub examined: usize,
    pub records: Vec<ChannelStatisticsRecord>,
    pub payload_bytes: usize,
    // Includes decoded strings. Cancellation retains this admission until the
    // actual multiplexed reply is received and the owned page is dropped.
    _budget: OwnedSemaphorePermit,
}
impl ClientStore {
    pub async fn channel_statistics_page(
        &self,
        cursor: u64,
    ) -> Result<ChannelStatisticsPage, Error> {
        let budget = time::timeout(
            self.inner.config.command_timeout,
            self.inner
                .statistics_bytes
                .clone()
                .acquire_many_owned(PAGE_BYTES as u32),
        )
        .await
        .map_err(|_| Error::Capacity)?
        .map_err(|_| Error::Stopped)?;
        let maximum = self.inner.config.max_clients;
        era::supervised(self.inner.clone(),move|mut connection|async move {
            let (next,examined,payload_bytes,rows):(u64,usize,usize,Vec<Row>)=Script::new(PAGE)
                .arg(cursor).arg(PAGE_BYTES).arg(maximum).invoke_async(&mut connection).await.map_err(|_|Error::RedisUnavailable)?;
            let mut records=Vec::with_capacity(rows.len());
            for (channel_id,flat,index,client_count,clients) in rows {
                if flat.len()%2!=0{return Err(Error::CorruptMetadata)}
                let metadata=flat.chunks_exact(2).map(|pair|(pair[0].clone(),pair[1].clone())).collect();
                let index=match index {Some(index)=>match index.parse::<u64>(){Ok(index)=>index,Err(_)=>continue},None=>0};
                let mut projected=Vec::with_capacity(clients.len());
                let mut valid=true;
                for (id,agent,ip,connected,user,format,profile) in clients {
                    let mut client=json!({"client_id":id,"user_agent":agent,"output_format":format,"output_profile_id":null});
                    if let Some(ip)=ip.filter(|value|!value.is_empty()){client["ip_address"]=json!(ip)}
                    if let Some(user)=user.filter(|value|!value.is_empty()){client["user_id"]=json!(user)}
                    if let Some(at)=connected.filter(|value|!value.is_empty()) {
                        match at.parse::<f64>() {Ok(at) if at.is_finite()=>client["connected_at"]=json!(at),_=>{valid=false;break}}
                    }
                    if let Some(profile)=profile.filter(|value|!matches!(value.as_str(),""|"0"|"None")) {
                        match profile.parse::<i64>(){Ok(profile)=>client["output_profile_id"]=json!(profile),Err(_)=>{valid=false;break}}
                    }
                    projected.push(client);
                }
                if valid {records.push(ChannelStatisticsRecord {channel_id,metadata,index,client_count,clients:projected});}
            }
            Ok(ChannelStatisticsPage {cursor:next,examined,records,payload_bytes,_budget:budget})
        }).await
    }
}
const PAGE: &str = r#"
local result=redis.call('SCAN',ARGV[1],'MATCH','live:channel:*:metadata','COUNT',64)
local limit=tonumber(ARGV[2]);local maxclients=tonumber(ARGV[3]);local used=1024
if #result[2]>512 then return redis.error_reply('statistics page limit') end
local function account(value)
 used=used+string.len(value)+64
 if used>limit then error('statistics byte limit') end
 return value
end
local rows={}
for _,key in ipairs(result[2]) do
 if string.len(key)>512 then return redis.error_reply('statistics key limit') end
 local id=string.sub(key,14,-10)
 if string.len(id)>0 and string.len(id)<=256 and redis.call('TYPE',key).ok=='hash' then
  local prefix='live:channel:'..id
  local set=prefix..':clients';local indexkey=prefix..':input:buffer:index'
  local settype=redis.call('TYPE',set).ok;local indextype=redis.call('TYPE',indexkey).ok
  if (settype=='none' or settype=='set') and (indextype=='none' or indextype=='string') then
   if redis.call('HLEN',key)>128 or redis.call('STRLEN',indexkey)>32 then return redis.error_reply('statistics field limit') end
   local count=redis.call('SCARD',set)
   if count>maxclients then return redis.error_reply('statistics client limit') end
   local meta=redis.call('HGETALL',key);for _,value in ipairs(meta) do account(value) end
   local clients=redis.call('SMEMBERS',set);local concise={};local ghosts={};local corrupt=false
   for ordinal,client in ipairs(clients) do
    account(client)
    if string.len(client)>256 then return redis.error_reply('statistics client key limit') end
    local clientkey=prefix..':clients:'..client;local kind=redis.call('TYPE',clientkey).ok
    if kind=='none' then table.insert(ghosts,client)
    elseif kind~='hash' then corrupt=true
    elseif ordinal<=10 then
     local values=redis.call('HMGET',clientkey,'user_agent','ip_address','connected_at','user_id','output_format','output_profile_id')
     for _,value in ipairs(values) do if value then account(value) end end
     local ua=values[1]
     local format=values[5];if not format or format=='' then format='mpegts' end
     table.insert(concise,{client,ua,values[2],values[3],values[4],format,values[6]})
    end
   end
   -- Match original remove_ghost_clients: remove only missing hashes, never a
   -- successor token or a client whose hash still exists.
   for _,client in ipairs(ghosts) do redis.call('SREM',set,client) end
   if not corrupt then
    local index=redis.call('GET',indexkey);if index then account(index) end
    table.insert(rows,{account(id),meta,index,count-#ghosts,concise})
   end
  end
 end
end
return {result[1],#result[2],used,rows}
"#;

#[cfg(test)]
mod tests {
    use super::*;
    use redis::AsyncCommands;
    async fn store() -> ClientStore {
        ClientStore::connect(
            &std::env::var("REDIS_TEST_URL").expect("owned disposable Redis required"),
            ClientConfig::default(),
        )
        .await
        .unwrap()
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires owned disposable Redis; serial with pause tests"]
    async fn global_catalog_reads_foreign_channel_and_removes_only_missing_hash_ghosts() {
        let store = store().await;
        let id = uuid::Uuid::new_v4().to_string();
        let prefix = format!("live:channel:{id}");
        let metadata = format!("{prefix}:metadata");
        let set = format!("{prefix}:clients");
        let index = format!("{prefix}:input:buffer:index");
        let client = format!("{prefix}:clients:source-client");
        let mut connection = store.inner.connection.clone();
        let _: () = redis::pipe()
            .cmd("HSET")
            .arg(&metadata)
            .arg("state")
            .arg("active")
            .arg("init_time")
            .arg("123")
            .arg("owner")
            .arg("foreign-source-worker")
            .cmd("EXPIRE")
            .arg(&metadata)
            .arg(30)
            .cmd("SET")
            .arg(&index)
            .arg(17)
            .arg("EX")
            .arg(30)
            .cmd("SADD")
            .arg(&set)
            .arg("source-client")
            .arg("expired-client")
            .cmd("EXPIRE")
            .arg(&set)
            .arg(30)
            .cmd("HSET")
            .arg(&client)
            .arg("user_agent")
            .arg("")
            .arg("ip_address")
            .arg("127.0.0.1")
            .arg("connected_at")
            .arg("123.5")
            .arg("user_id")
            .arg("0")
            .arg("output_format")
            .arg("")
            .arg("output_profile_id")
            .arg("None")
            .cmd("EXPIRE")
            .arg(&client)
            .arg(30)
            .ignore()
            .query_async(&mut connection)
            .await
            .unwrap();
        let before: i64 = connection.pttl(&metadata).await.unwrap();
        let mut cursor = 0;
        let mut found = None;
        loop {
            let page = store.channel_statistics_page(cursor).await.unwrap();
            assert!(page.payload_bytes <= PAGE_BYTES);
            cursor = page.cursor;
            for row in page.records {
                if row.channel_id == id {
                    found = Some(row);
                }
            }
            if cursor == 0 {
                break;
            }
        }
        let row = found.expect("metadata from a foreign worker must not be omitted");
        assert_eq!(row.index, 17);
        assert_eq!(row.client_count, 1);
        assert_eq!(row.metadata["owner"], "foreign-source-worker");
        assert_eq!(
            row.clients,
            vec![
                json!({"client_id":"source-client","user_agent":"","ip_address":"127.0.0.1","connected_at":123.5,"user_id":"0","output_format":"mpegts","output_profile_id":null})
            ]
        );
        let ids: Vec<String> = connection.smembers(&set).await.unwrap();
        assert_eq!(ids, vec!["source-client"]);
        let after: i64 = connection.pttl(&metadata).await.unwrap();
        assert!(
            after > 0 && after <= before,
            "status reads must not refresh metadata TTL"
        );
        let _: usize = connection
            .del(&[metadata, set, index, client])
            .await
            .unwrap();
        store.shutdown().await;
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires owned disposable Redis; pauses all commands250ms"]
    async fn cancelled_catalog_reads_retain_global_byte_admission_through_actual_reply() {
        let store = store().await;
        let mut connection = store.inner.connection.clone();
        let _: String = redis::cmd("CLIENT")
            .arg("PAUSE")
            .arg(250)
            .query_async(&mut connection)
            .await
            .unwrap();
        let mut tasks = Vec::new();
        for _ in 0..4 {
            let store = store.clone();
            tasks.push(tokio::spawn(async move {
                store.channel_statistics_page(0).await
            }));
        }
        time::sleep(Duration::from_millis(20)).await;
        assert_eq!(store.inner.statistics_bytes.available_permits(), 0);
        for task in tasks {
            task.abort();
            let _ = task.await;
        }
        time::sleep(Duration::from_millis(30)).await;
        assert_eq!(
            store.inner.statistics_bytes.available_permits(),
            0,
            "cancelled callers cannot free pending Redis reply memory"
        );
        time::timeout(Duration::from_secs(3), async {
            while store.inner.statistics_bytes.available_permits() != 4 * PAGE_BYTES {
                time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        store.shutdown().await;
    }
}
