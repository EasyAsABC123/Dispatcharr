//! Fenced, bounded request/receipt state for a warm Rust raw producer.
//! Original Dispatcharr authors retain all application credit.
use super::{Error, OutputStore};
use serde_json::{json, Value};
use uuid::Uuid;

const ASSIGNED_SWITCH_EVENT: &str = "dispatcharr_rust_assigned_source_switch";
const NEXT_SWITCH_EVENT: &str = "dispatcharr_rust_next_source_switch";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RemoteSwitchKind {
    Assigned,
    Next,
}

impl RemoteSwitchKind {
    fn event(self) -> &'static str {
        match self {
            Self::Assigned => ASSIGNED_SWITCH_EVENT,
            Self::Next => NEXT_SWITCH_EVENT,
        }
    }

    fn redis_name(self) -> &'static [u8] {
        match self {
            Self::Assigned => b"assigned",
            Self::Next => b"next",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteSwitchWake {
    pub channel: Uuid,
    pub request: Uuid,
    pub kind: RemoteSwitchKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteSwitchSource {
    pub owner: Uuid,
    pub stream: i64,
    pub profile: i64,
    deadline_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteSwitchIntent {
    pub source: RemoteSwitchSource,
    pub target: i64,
    pub url_sha256: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RemoteSwitchVerdict {
    Switched,
    Failed,
}

fn canonical_uuid(raw: &str) -> Option<Uuid> {
    Uuid::parse_str(raw)
        .ok()
        .filter(|id| !id.is_nil() && id.to_string() == raw)
}

fn positive(raw: &str) -> Option<i64> {
    raw.parse::<i64>()
        .ok()
        .filter(|value| *value > 0 && value.to_string() == raw)
}

fn is_digest(raw: &str) -> bool {
    raw.len() == 64
        && raw
            .as_bytes()
            .iter()
            .all(|v| matches!(v, b'0'..=b'9' | b'a'..=b'f'))
}

impl RemoteSwitchWake {
    pub(crate) fn decode(topic: &[u8], data: &Value) -> Result<Option<Self>, Error> {
        let kind = match data["event"].as_str() {
            Some(ASSIGNED_SWITCH_EVENT) => RemoteSwitchKind::Assigned,
            Some(NEXT_SWITCH_EVENT) => RemoteSwitchKind::Next,
            _ => return Ok(None),
        };
        if data.as_object().is_none_or(|object| object.len() != 3) {
            return Err(Error::Corrupt);
        }
        let channel = data["channel_id"]
            .as_str()
            .and_then(canonical_uuid)
            .ok_or(Error::Corrupt)?;
        let request = data["request_id"]
            .as_str()
            .and_then(canonical_uuid)
            .ok_or(Error::Corrupt)?;
        if topic != format!("live:events:{channel}").as_bytes() {
            return Err(Error::Corrupt);
        }
        Ok(Some(Self {
            channel,
            request,
            kind,
        }))
    }

    fn payload(&self) -> Vec<u8> {
        json!({"event": self.kind.event(), "channel_id": self.channel.to_string(),
               "request_id": self.request.to_string()})
        .to_string()
        .into_bytes()
    }
}

impl RemoteSwitchSource {
    fn from_reply(reply: &[String]) -> Result<Self, Error> {
        let [owner, stream, profile, deadline] = reply else {
            return Err(Error::Corrupt);
        };
        let owner = owner
            .strip_prefix("rust:")
            .and_then(canonical_uuid)
            .ok_or(Error::Corrupt)?;
        Ok(Self {
            owner,
            stream: positive(stream).ok_or(Error::Corrupt)?,
            profile: positive(profile).ok_or(Error::Corrupt)?,
            deadline_ms: positive(deadline).ok_or(Error::Corrupt)?,
        })
    }

    pub fn fence(&self) -> String {
        format!("rust:{}", self.owner)
    }
}

impl OutputStore {
    async fn remote_switch_call(
        &self,
        channel: Uuid,
        args: &[&[u8]],
    ) -> Result<Vec<String>, Error> {
        if channel.is_nil() {
            return Err(Error::InvalidKey);
        }
        let prefix = format!("live:channel:{channel}");
        self.invoke(
            REMOTE_SWITCH,
            &[
                format!("{prefix}:owner"),
                format!("{prefix}:metadata"),
                format!("{prefix}:stopping"),
                format!("{prefix}:rust_assigned_switch"),
                format!("live:events:{channel}"),
            ],
            args,
        )
        .await
    }

    pub async fn observe_remote_switch(
        &self,
        channel: Uuid,
    ) -> Result<Option<RemoteSwitchSource>, Error> {
        let reply = self.remote_switch_call(channel, &[b"observe"]).await?;
        if reply.is_empty() {
            return Ok(None);
        }
        RemoteSwitchSource::from_reply(&reply).map(Some)
    }

    pub async fn begin_remote_switch(
        &self,
        wake: &RemoteSwitchWake,
        intent: &RemoteSwitchIntent,
    ) -> Result<bool, Error> {
        if wake.request.is_nil()
            || intent.target <= 0
            || (wake.kind == RemoteSwitchKind::Next && intent.target == intent.source.stream)
            || !is_digest(&intent.url_sha256)
        {
            return Err(Error::InvalidKey);
        }
        let source = &intent.source;
        let result = self
            .remote_switch_call(
                wake.channel,
                &[
                    b"begin",
                    source.fence().as_bytes(),
                    wake.request.to_string().as_bytes(),
                    source.stream.to_string().as_bytes(),
                    source.profile.to_string().as_bytes(),
                    source.deadline_ms.to_string().as_bytes(),
                    intent.target.to_string().as_bytes(),
                    intent.url_sha256.as_bytes(),
                    &wake.payload(),
                    wake.kind.redis_name(),
                ],
            )
            .await?;
        Ok(result == ["accepted"])
    }

    pub async fn claim_remote_switch(
        &self,
        wake: &RemoteSwitchWake,
        owner: &str,
        claimant: Uuid,
    ) -> Result<Option<RemoteSwitchIntent>, Error> {
        let reply = self
            .remote_switch_call(
                wake.channel,
                &[
                    b"claim",
                    owner.as_bytes(),
                    wake.request.to_string().as_bytes(),
                    claimant.to_string().as_bytes(),
                    wake.kind.redis_name(),
                ],
            )
            .await?;
        if reply.is_empty() {
            return Ok(None);
        }
        let source = RemoteSwitchSource::from_reply(reply.get(..4).ok_or(Error::Corrupt)?)?;
        let [target, digest] = &reply[4..] else {
            return Err(Error::Corrupt);
        };
        if !is_digest(digest) {
            return Err(Error::Corrupt);
        }
        Ok(Some(RemoteSwitchIntent {
            source,
            target: positive(target).ok_or(Error::Corrupt)?,
            url_sha256: digest.clone(),
        }))
    }

    pub async fn finish_remote_switch(
        &self,
        wake: &RemoteSwitchWake,
        source: &RemoteSwitchSource,
        claimant: Uuid,
        verdict: RemoteSwitchVerdict,
    ) -> Result<(), Error> {
        let result = self
            .remote_switch_call(
                wake.channel,
                &[
                    b"finish",
                    source.fence().as_bytes(),
                    wake.request.to_string().as_bytes(),
                    claimant.to_string().as_bytes(),
                    match verdict {
                        RemoteSwitchVerdict::Switched => b"switched",
                        RemoteSwitchVerdict::Failed => b"failed",
                    },
                ],
            )
            .await?;
        if result == ["accepted"] {
            Ok(())
        } else {
            Err(Error::LostOwnership)
        }
    }

    pub async fn read_remote_switch(
        &self,
        wake: &RemoteSwitchWake,
        source: &RemoteSwitchSource,
    ) -> Result<Option<RemoteSwitchVerdict>, Error> {
        let reply = self
            .remote_switch_call(
                wake.channel,
                &[
                    b"read",
                    source.fence().as_bytes(),
                    wake.request.to_string().as_bytes(),
                ],
            )
            .await?;
        match reply.first().map(String::as_str) {
            None => Ok(None),
            Some("switched") => Ok(Some(RemoteSwitchVerdict::Switched)),
            Some("failed") => Ok(Some(RemoteSwitchVerdict::Failed)),
            _ => Err(Error::Corrupt),
        }
    }

    pub async fn acknowledge_remote_switch(
        &self,
        wake: &RemoteSwitchWake,
        source: &RemoteSwitchSource,
    ) -> Result<(), Error> {
        self.remote_switch_call(
            wake.channel,
            &[
                b"ack",
                source.fence().as_bytes(),
                wake.request.to_string().as_bytes(),
            ],
        )
        .await?;
        Ok(())
    }

    pub async fn cancel_pending_remote_switch(
        &self,
        wake: &RemoteSwitchWake,
        source: &RemoteSwitchSource,
    ) -> Result<(), Error> {
        let _ = self
            .remote_switch_call(
                wake.channel,
                &[
                    b"cancel",
                    source.fence().as_bytes(),
                    wake.request.to_string().as_bytes(),
                ],
            )
            .await?;
        Ok(())
    }
}

// Publication, claim, read and acknowledgment fence every operation. Keep
// receipt read side-effect-free so a broken reply cannot erase the evidence.
// A claimed transition has bounded retention and cannot be cancelled.
const REMOTE_SWITCH: &str = r#"
local operation=ARGV[1]
local fence=redis.call('GET',KEYS[1])
if not fence or string.sub(fence,1,5)~='rust:' or redis.call('EXISTS',KEYS[3])~=0 then return {} end
local function active()
    local fields=redis.call('HMGET',KEYS[2],'rust_token','owner','state','stream_id','m3u_profile')
    local state=fields[3]
    if fields[1]~=fence or fields[2]~=fence
        or (state~='active' and state~='buffering' and state~='waiting_for_clients')
        or not fields[4] or not fields[5] then return nil end
    return fields
end
local function millis()
    local stamp=redis.call('TIME')
    return tonumber(stamp[1])*1000+math.floor(tonumber(stamp[2])/1000)
end
if operation=='observe' then
    local fields=active()
    if not fields then return {} end
    return {fence,fields[4],fields[5],tostring(millis()+15000)}
end
if fence~=ARGV[2] then return {} end
local state=redis.call('HMGET',KEYS[4],'fence','request','phase','stream','profile','deadline','target','digest','claimant','kind','kind_request')
if operation=='begin' then
    local kind=ARGV[10] or 'assigned'
    if kind~='assigned' and kind~='next' then return {} end
    if kind=='next' and ARGV[4]==ARGV[7] then return {} end
    local fields=active()
    if not fields or fields[4]~=ARGV[4] or fields[5]~=ARGV[5] or state[1]==fence then return {} end
    local remaining=tonumber(ARGV[6])-millis()
    if remaining<1 or remaining>15000 then return {} end
    redis.call('HSET',KEYS[4],'fence',fence,'request',ARGV[3],'phase','pending',
        'stream',ARGV[4],'profile',ARGV[5],'deadline',ARGV[6],'target',ARGV[7],'digest',ARGV[8],'claimant','',
        'kind',kind,'kind_request',ARGV[3])
    redis.call('PEXPIRE',KEYS[4],remaining)
    redis.call('PUBLISH',KEYS[5],ARGV[9])
    return {'accepted'}
end
if state[1]~=fence or state[2]~=ARGV[3] then return {} end
if operation=='claim' then
    -- Legacy assigned workers did not write a kind. Scope the marker to this
    -- request so a superseded generation cannot leak a previous Next marker.
    local kind=state[11]==ARGV[3] and state[10] or 'assigned'
    if kind~=(ARGV[5] or 'assigned') then return {} end
    local fields=active()
    if tonumber(state[6])<=millis() or not fields or fields[4]~=state[4] or fields[5]~=state[5] then
        return {}
    end
    if state[3]=='pending' then
        redis.call('HSET',KEYS[4],'phase','claimed','claimant',ARGV[4])
        redis.call('PEXPIRE',KEYS[4],60000)
    elseif state[3]~='claimed' or state[9]~=ARGV[4] then
        return {}
    end
    return {fence,state[4],state[5],state[6],state[7],state[8]}
end
if operation=='cancel' and state[3]=='pending' then
    redis.call('DEL',KEYS[4])
    return {}
end
if operation=='finish' and state[9]==ARGV[4] and (ARGV[5]=='switched' or ARGV[5]=='failed') then
    if state[3]=='claimed' then
        redis.call('HSET',KEYS[4],'phase',ARGV[5])
        redis.call('PEXPIRE',KEYS[4],15000)
        return {'accepted'}
    end
    if state[3]==ARGV[5] then return {'accepted'} end
end
if state[3]=='switched' or state[3]=='failed' then
    if operation=='read' then return {state[3]} end
    if operation=='ack' then redis.call('DEL',KEYS[4]) end
end
return {}
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remote_wake_contains_only_canonical_channel_and_request_identity() {
        let channel = Uuid::parse_str("00000000-0000-4000-8000-000000000721").unwrap();
        let request = Uuid::parse_str("00000000-0000-4000-8000-000000000722").unwrap();
        let wake = RemoteSwitchWake {
            channel,
            request,
            kind: RemoteSwitchKind::Assigned,
        };
        let bytes = wake.payload();
        let mut value: Value = serde_json::from_slice(&bytes).unwrap();
        let topic = format!("live:events:{channel}");
        assert_eq!(
            RemoteSwitchWake::decode(topic.as_bytes(), &value).unwrap(),
            Some(wake)
        );
        assert!(RemoteSwitchWake::decode(b"live:events:foreign", &value).is_err());
        value["url"] = Value::from("http://owned-origin/other.ts");
        assert!(RemoteSwitchWake::decode(topic.as_bytes(), &value).is_err());
        value.as_object_mut().unwrap().remove("url");
        value["request_id"] = Value::from("00000000-0000-0000-0000-000000000000");
        assert!(RemoteSwitchWake::decode(topic.as_bytes(), &value).is_err());
    }

    #[test]
    fn remote_next_wake_is_distinct_without_shipping_order_or_url() {
        let wake = RemoteSwitchWake {
            channel: Uuid::from_u128(11),
            request: Uuid::from_u128(12),
            kind: RemoteSwitchKind::Next,
        };
        let payload: Value = serde_json::from_slice(&wake.payload()).unwrap();
        assert_eq!(payload["event"], NEXT_SWITCH_EVENT);
        assert_ne!(payload["event"], ASSIGNED_SWITCH_EVENT);
        assert_eq!(payload.as_object().unwrap().len(), 3);
        assert_eq!(
            RemoteSwitchWake::decode(format!("live:events:{}", wake.channel).as_bytes(), &payload)
                .unwrap(),
            Some(wake)
        );
    }

    #[test]
    fn remote_snapshot_requires_an_exact_rust_owner_and_positive_canonical_ids() {
        let mut fields = [
            "rust:00000000-0000-4000-8000-000000000721".to_owned(),
            "2".to_owned(),
            "5".to_owned(),
            "1700000015000".to_owned(),
        ];
        let source = RemoteSwitchSource::from_reply(&fields).unwrap();
        assert_eq!((source.stream, source.profile), (2, 5));
        assert_eq!(source.fence(), fields[0]);
        fields[1] = "02".to_owned();
        assert_eq!(RemoteSwitchSource::from_reply(&fields), Err(Error::Corrupt));
        fields[1] = "2".to_owned();
        fields[0] = "foreign-owner".to_owned();
        assert_eq!(RemoteSwitchSource::from_reply(&fields), Err(Error::Corrupt));
    }

    #[tokio::test]
    #[ignore = "requires the task-owned disposable Redis Unix socket"]
    async fn real_redis_remote_switch_keeps_requests_and_receipts_generation_bound() {
        use crate::{OutputConfig, OutputProfileOwner, OutputProfileRequest};
        use redis::IntoConnectionInfo;
        use std::{future::Future, pin::Pin, sync::Arc, time::Duration};
        use tokio::sync::mpsc;

        struct Observer(mpsc::Sender<RemoteSwitchWake>);
        impl OutputProfileOwner for Observer {
            fn ensure_owned(
                &self,
                _: OutputProfileRequest,
                _: Arc<dyn Send + Sync>,
            ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + '_>> {
                Box::pin(async { panic!("unexpected output profile request") })
            }

            fn switch_source_owned(
                &self,
                wake: RemoteSwitchWake,
                _: Arc<dyn Send + Sync>,
            ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + '_>> {
                Box::pin(async move { self.0.try_send(wake).map_err(|_| Error::Capacity) })
            }
        }

        let url = std::env::var("DISPATCHARR_REMOTE_SWITCH_TEST_REDIS_URL")
            .expect("private owner socket");
        let address = url.as_str().into_connection_info().unwrap();
        let redis::ConnectionAddr::Unix(path) = &address.addr else {
            panic!("only an owned Unix socket is permitted");
        };
        assert!(path.starts_with("/tmp") && path.file_name().unwrap() == "owned-redis.sock");
        let client = redis::Client::open(address).unwrap();
        let mut connection = client.get_multiplexed_async_connection().await.unwrap();
        let size: usize = redis::cmd("DBSIZE")
            .query_async(&mut connection)
            .await
            .unwrap();
        assert_eq!(size, 0, "a fresh instance is required");
        let store = OutputStore::connect(&url, OutputConfig::default())
            .await
            .unwrap();

        let channel = Uuid::from_u128(101);
        let owner = format!("rust:{}", Uuid::from_u128(102));
        let replacement = format!("rust:{}", Uuid::from_u128(103));
        let first_claim = Uuid::from_u128(104);
        let foreign_claim = Uuid::from_u128(105);
        let prefix = format!("live:channel:{channel}");
        let owner_key = format!("{prefix}:owner");
        let metadata_key = format!("{prefix}:metadata");
        let mailbox_key = format!("{prefix}:rust_assigned_switch");
        let set_owner = |fence: &str| {
            let mut command = redis::pipe();
            command
                .atomic()
                .cmd("SET")
                .arg(&owner_key)
                .arg(fence)
                .ignore()
                .cmd("HSET")
                .arg(&metadata_key)
                .arg("owner")
                .arg(fence)
                .arg("rust_token")
                .arg(fence)
                .arg("state")
                .arg("active")
                .arg("stream_id")
                .arg("11")
                .arg("m3u_profile")
                .arg("17")
                .ignore();
            command
        };
        set_owner(&owner)
            .query_async::<()>(&mut connection)
            .await
            .unwrap();
        let source = store.observe_remote_switch(channel).await.unwrap().unwrap();
        let intent = RemoteSwitchIntent {
            source,
            target: 12,
            url_sha256: "a".repeat(64),
        };
        let lost_wake = RemoteSwitchWake {
            channel,
            request: Uuid::from_u128(106),
            kind: RemoteSwitchKind::Assigned,
        };
        assert!(store
            .begin_remote_switch(&lost_wake, &intent)
            .await
            .unwrap());
        assert!(store
            .read_remote_switch(&lost_wake, &intent.source)
            .await
            .unwrap()
            .is_none());
        store
            .cancel_pending_remote_switch(&lost_wake, &intent.source)
            .await
            .unwrap();
        assert!(store
            .claim_remote_switch(&lost_wake, &owner, first_claim)
            .await
            .unwrap()
            .is_none());

        let events_a = store.profile_events().unwrap();
        let events_b = store.profile_events().unwrap();
        let (send_a, mut received_a) = mpsc::channel(4);
        let (send_b, mut received_b) = mpsc::channel(4);
        events_a
            .start(Arc::new(Observer(send_a)), Arc::new(()))
            .unwrap();
        events_b
            .start(Arc::new(Observer(send_b)), Arc::new(()))
            .unwrap();
        let event_budget = Duration::from_secs(2);
        assert!(events_a.wait_ready(event_budget).await && events_b.wait_ready(event_budget).await);

        let wake = RemoteSwitchWake {
            channel,
            request: Uuid::from_u128(107),
            kind: RemoteSwitchKind::Assigned,
        };
        let foreign_wake = RemoteSwitchWake {
            channel,
            request: Uuid::from_u128(108),
            kind: RemoteSwitchKind::Assigned,
        };
        assert!(store.begin_remote_switch(&wake, &intent).await.unwrap());
        assert!(!store
            .begin_remote_switch(&foreign_wake, &intent)
            .await
            .unwrap());
        assert_eq!(
            tokio::time::timeout(event_budget, received_a.recv())
                .await
                .unwrap(),
            Some(wake.clone())
        );
        assert_eq!(
            tokio::time::timeout(event_budget, received_b.recv())
                .await
                .unwrap(),
            Some(wake.clone())
        );
        assert!(store
            .claim_remote_switch(&wake, &replacement, foreign_claim)
            .await
            .unwrap()
            .is_none());

        // Discard each first receipt to model a reply lost after an actual
        // successful Redis command. Same claim/read can recover the result.
        assert!(store
            .claim_remote_switch(&wake, &owner, first_claim)
            .await
            .unwrap()
            .is_some());
        assert_eq!(
            store
                .claim_remote_switch(&wake, &owner, first_claim)
                .await
                .unwrap(),
            Some(intent.clone())
        );
        assert!(store
            .claim_remote_switch(&wake, &owner, foreign_claim)
            .await
            .unwrap()
            .is_none());
        store
            .cancel_pending_remote_switch(&wake, &intent.source)
            .await
            .unwrap();
        assert!(store
            .finish_remote_switch(
                &wake,
                &intent.source,
                foreign_claim,
                RemoteSwitchVerdict::Switched
            )
            .await
            .is_err());
        for _ in 0..2 {
            store
                .finish_remote_switch(
                    &wake,
                    &intent.source,
                    first_claim,
                    RemoteSwitchVerdict::Switched,
                )
                .await
                .unwrap();
            assert_eq!(
                store
                    .read_remote_switch(&wake, &intent.source)
                    .await
                    .unwrap(),
                Some(RemoteSwitchVerdict::Switched)
            );
        }
        store
            .acknowledge_remote_switch(&foreign_wake, &intent.source)
            .await
            .unwrap();
        assert_eq!(
            store
                .read_remote_switch(&wake, &intent.source)
                .await
                .unwrap(),
            Some(RemoteSwitchVerdict::Switched)
        );
        store
            .acknowledge_remote_switch(&wake, &intent.source)
            .await
            .unwrap();
        assert!(store
            .read_remote_switch(&wake, &intent.source)
            .await
            .unwrap()
            .is_none());

        assert!(store
            .begin_remote_switch(&foreign_wake, &intent)
            .await
            .unwrap());
        assert!(store
            .claim_remote_switch(&foreign_wake, &owner, first_claim)
            .await
            .unwrap()
            .is_some());
        set_owner(&replacement)
            .query_async::<()>(&mut connection)
            .await
            .unwrap();
        assert!(store
            .finish_remote_switch(
                &foreign_wake,
                &intent.source,
                first_claim,
                RemoteSwitchVerdict::Switched
            )
            .await
            .is_err());
        assert!(store
            .read_remote_switch(&foreign_wake, &intent.source)
            .await
            .unwrap()
            .is_none());
        let next_source = store.observe_remote_switch(channel).await.unwrap().unwrap();
        let next = RemoteSwitchIntent {
            source: next_source,
            ..intent
        };
        let next_wake = RemoteSwitchWake {
            channel,
            request: Uuid::from_u128(109),
            kind: RemoteSwitchKind::Assigned,
        };
        assert!(store.begin_remote_switch(&next_wake, &next).await.unwrap());
        store
            .cancel_pending_remote_switch(&next_wake, &next.source)
            .await
            .unwrap();
        let remaining: usize = redis::cmd("EXISTS")
            .arg(&mailbox_key)
            .query_async(&mut connection)
            .await
            .unwrap();
        assert_eq!(remaining, 0);
        assert!(events_a.shutdown(event_budget).await && events_b.shutdown(event_budget).await);
        let deleted: usize = redis::cmd("DEL")
            .arg(owner_key)
            .arg(metadata_key)
            .query_async(&mut connection)
            .await
            .unwrap();
        assert_eq!(deleted, 2);
        let size: usize = redis::cmd("DBSIZE")
            .query_async(&mut connection)
            .await
            .unwrap();
        assert_eq!(size, 0, "exact owned keys must retire");
    }
}
