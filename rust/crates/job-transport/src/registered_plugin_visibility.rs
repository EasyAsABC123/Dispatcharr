//! Typed plugin-only crash visibility recovery; original Kombu authors retain credit.
use super::*;
impl Jobs {
    pub(crate) async fn recover_registered_plugin_visible(
        &self,
        runtime: dispatcharr_store::PluginRuntime,
        owner: Arc<dyn Send + Sync>,
        offset: usize,
    ) -> Result<usize, Error> {
        if offset > 131072 {
            return Err(Error::Capacity);
        }
        let token = Uuid::new_v4().to_string();
        let keys = vec!["unacked_mutex".into()];
        let acquired = self
            .plugin_broker_settled::<i64>(
                ACQUIRE,
                &keys,
                &[token.as_bytes(), b"300000"],
                owner.clone(),
            )
            .await;
        let owns = match acquired {
            Ok(1) => true,
            Ok(0) => false,
            Err(Error::Backend) => {
                self.plugin_broker_settled::<i64>(OWNS, &keys, &[token.as_bytes()], owner.clone())
                    .await?
                    == 1
            }
            Ok(_) => return Err(Error::Corrupt),
            Err(e) => return Err(e),
        };
        if !owns {
            return Ok(offset);
        }
        let result = self
            .recover_registered_plugin_locked(&token, runtime, owner.clone(), offset)
            .await;
        let release = self
            .plugin_broker_settled::<i64>(RELEASE, &keys, &[token.as_bytes()], owner)
            .await;
        match (result, release) {
            (Err(e), _) | (_, Err(e)) => Err(e),
            (Ok(next), Ok(0 | 1)) => Ok(next),
            _ => Err(Error::Corrupt),
        }
    }
    async fn recover_registered_plugin_locked(
        &self,
        token: &str,
        runtime: dispatcharr_store::PluginRuntime,
        owner: Arc<dyn Send + Sync>,
        offset: usize,
    ) -> Result<usize, Error> {
        let ceiling = format!(
            "{:.6}",
            chrono::Utc::now().timestamp_micros() as f64 / 1_000_000.0 - 3600.0
        );
        let position = offset.to_string();
        let (code, items): (i64, Vec<(String, String)>) = self
            .plugin_broker_settled(
                SELECT,
                &["unacked_index".into()],
                &[ceiling.as_bytes(), b"10", position.as_bytes()],
                owner.clone(),
            )
            .await?;
        if code != 1 {
            return Err(Error::Corrupt);
        }
        let selected = items.len();
        let mut restored = 0usize;
        for (tag, score) in items {
            let tag = Uuid::parse_str(&tag).map_err(|_| Error::Corrupt)?;
            // Read only this selected actual tuple, with HSTRLEN checked in
            // Redis before allocating it in the client. Unsupported foreign
            // entries advance the bounded cursor; they never grant authority.
            let raw: (i64, Vec<u8>) = self
                .plugin_broker_settled(
                    READ_TUPLE,
                    &["unacked".into()],
                    &[tag.to_string().as_bytes()],
                    owner.clone(),
                )
                .await?;
            if raw.0 == 0 {
                continue;
            }
            if raw.0 != 1 {
                return Err(Error::Corrupt);
            }
            let tuple: serde_json::Value = match serde_json::from_slice(&raw.1) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let Some(parts) = tuple
                .as_array()
                .filter(|p| p.len() == 3 && p[1] == "" && p[2] == "celery")
            else {
                continue;
            };
            let queued = serde_json::to_vec(&parts[0]).map_err(|_| Error::Corrupt)?;
            let capability = match runtime.envelope_capability(&queued) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let receipt = match self
                .read_registered_plugin_eta(tag, capability, owner.clone(), Default::default())
                .await
            {
                Ok(Some(v)) => v,
                Ok(None) | Err(Error::Corrupt | Error::UnsupportedTask) => continue,
                Err(e) => return Err(e),
            };
            let keys = vec![
                "celery".into(),
                "unacked".into(),
                "unacked_index".into(),
                "unacked_mutex".into(),
            ];
            let tag = tag.to_string();
            match self
                .plugin_broker_settled::<i64>(
                    RESTORE_VISIBLE,
                    &keys,
                    &[
                        tag.as_bytes(),
                        receipt.tuple_bytes(),
                        &receipt.receipt.restored_bytes,
                        score.as_bytes(),
                        ceiling.as_bytes(),
                        token.as_bytes(),
                    ],
                    Arc::new((owner.clone(), receipt.clone())),
                )
                .await?
            {
                1 => {
                    restored += 1;
                }
                0 => {}
                -5 => return Err(Error::LostMutex),
                _ => return Err(Error::Corrupt),
            }
        }
        if selected < 10 {
            Ok(0)
        } else {
            offset
                .checked_add(selected - restored)
                .filter(|v| *v <= 131072)
                .ok_or(Error::Capacity)
        }
    }
}
const ACQUIRE: &str = r#"local n=redis.call('SET',KEYS[1],ARGV[1],'NX','PX',ARGV[2]);if n then return 1 end;return 0"#;
const OWNS: &str = r#"local t=redis.call('TYPE',KEYS[1]).ok;if t=='none'then return 0 end;if t~='string'then return -1 end;if redis.call('STRLEN',KEYS[1])~=#ARGV[1]then return 0 end;if redis.call('GET',KEYS[1])==ARGV[1]then return 1 end;return 0"#;
const RELEASE: &str = r#"if redis.call('TYPE',KEYS[1]).ok=='none'then return 0 end;if redis.call('TYPE',KEYS[1]).ok~='string'then return -1 end;if redis.call('STRLEN',KEYS[1])~=#ARGV[1]then return 0 end;if redis.call('GET',KEYS[1])==ARGV[1]then redis.call('DEL',KEYS[1]);return 1 end;return 0"#;
const SELECT: &str = r#"local t=redis.call('TYPE',KEYS[1]).ok;if t=='none'then return{1,{}}end;if t~='zset'then return{-1,{}}end;local r=redis.call('ZREVRANGEBYSCORE',KEYS[1],ARGV[1],0,'WITHSCORES','LIMIT',ARGV[3],ARGV[2]);local out={};for i=1,#r,2 do if #r[i]~=36 or #r[i+1]>64 then return{-1,{}}end;out[#out+1]={r[i],r[i+1]};end;return{1,out}"#;
const RESTORE_VISIBLE: &str = r#"
local m=redis.call('TYPE',KEYS[4]).ok
if m~='string'then return -5 end
if redis.call('STRLEN',KEYS[4])~=#ARGV[6]then return -5 end
if redis.call('GET',KEYS[4])~=ARGV[6]then return -5 end
local h=redis.call('TYPE',KEYS[2]).ok;local z=redis.call('TYPE',KEYS[3]).ok
if h=='none'and z=='none'then return 0 end
if h~='hash'or z~='zset'then return -1 end
local score=redis.call('ZSCORE',KEYS[3],ARGV[1]);if not score then return 0 end
if tonumber(score)~=tonumber(ARGV[4])or tonumber(score)>tonumber(ARGV[5])then return 0 end
if #ARGV[1]~=36 or #ARGV[2]>131072 or #ARGV[3]>65536 then return -1 end
if redis.call('HSTRLEN',KEYS[2],ARGV[1])~=#ARGV[2]then return 0 end
if redis.call('HGET',KEYS[2],ARGV[1])~=ARGV[2]then return 0 end
local q=redis.call('TYPE',KEYS[1]).ok;if q~='none'and q~='list'then return -1 end
redis.call('ZREM',KEYS[3],ARGV[1]);redis.call('HDEL',KEYS[2],ARGV[1]);redis.call('RPUSH',KEYS[1],ARGV[3]);return 1
"#;

const READ_TUPLE: &str = r#"local t=redis.call('TYPE',KEYS[1]).ok;if t=='none'then return{0,''}end;if t~='hash'then return{-1,''}end;local n=redis.call('HSTRLEN',KEYS[1],ARGV[1]);if n==0 then return{0,''}end;if n>131072 then return{-1,''}end;return{1,redis.call('HGET',KEYS[1],ARGV[1])}"#;
