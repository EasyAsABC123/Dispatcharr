//! Source stream-limit check/preemption without creating a redirect viewer.
//! Original Dispatcharr authors retain all admission algorithm credit.
use super::*;
impl ClientStore {
    pub async fn check_redirect_retained(
        &self,
        channel: &str,
        viewer: &PlaybackViewer,
        retained: Arc<dyn Send + Sync>,
    ) -> Result<(), Error> {
        if self.inner.stopped.load(Ordering::Acquire) {
            return Err(Error::Stopped);
        }
        if channel.is_empty()
            || channel.len() > 256
            || channel.contains(':')
            || viewer.user_agent.len() > 4096
        {
            return Err(Error::InvalidClient);
        }
        let client = format!("client_{}", uuid::Uuid::new_v4().simple());
        let (hash, set, stop) = keys(channel, &client);
        let data = json!({"channel_id":channel,"user_id":viewer.user_id.unwrap_or(0).to_string()});
        let policy = json!({"terminate_on_limit_exceeded":viewer.limit_policy.terminate_on_limit_exceeded,
            "prioritize_single_client_channels":viewer.limit_policy.prioritize_single_client_channels,
            "ignore_same_channel_connections":viewer.limit_policy.ignore_same_channel_connections,"terminate_oldest":viewer.limit_policy.terminate_oldest});
        let limit = viewer.stream_limit;
        let maximum = self.inner.config.admission_scan_limit;
        let channel = channel.to_owned();
        era::supervised(self.inner.clone(), move |mut connection| async move {
            let _retained = retained;
            let result: Vec<String> = Script::new(scripts::REGISTER)
                .key(hash)
                .key(set)
                .key(stop)
                .key(format!("live:channel:{channel}:stopping"))
                .arg(data.to_string())
                .arg(policy.to_string())
                .arg(limit)
                .arg(60000)
                .arg(maximum)
                .arg(client)
                .arg("redirect-check")
                .invoke_async(&mut connection)
                .await
                .map_err(|_| Error::RedisUnavailable)?;
            match result.first().map(String::as_str) {
                Some("OK") => Ok(()),
                Some("LIMIT") => Err(Error::UserLimit),
                Some("STOPPED") => Err(Error::Stopped),
                Some("SCAN_LIMIT") => Err(Error::ScanLimit),
                Some("CORRUPT") => Err(Error::CorruptMetadata),
                _ => Err(Error::RedisUnavailable),
            }
        })
        .await
    }
}
