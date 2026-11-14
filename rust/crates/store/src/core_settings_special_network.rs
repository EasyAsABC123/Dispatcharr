//! Actual network-save signal, after generic group invalidation.
//! Original Dispatcharr authors retain developer notification/signal credit.
use crate::{AuthService, StoreError};
use serde_json::json;
use sqlx::Connection;
use std::sync::Arc;

async fn finish(mut c: sqlx::pool::PoolConnection<sqlx::Postgres>) -> Result<(), StoreError> {
    if let Err(error) = Connection::ping(&mut *c).await {
        let _ = c.close().await;
        return Err(error.into());
    }
    Connection::shrink_buffers(&mut *c);
    c.return_to_pool().await;
    drop(c);
    Ok(())
}
async fn transaction(auth: &AuthService, remove: bool) -> Result<(), StoreError> {
    let mut c = auth.store.pool().acquire().await?;
    let begin = sqlx::query("BEGIN").execute(&mut *c).await;
    if let Err(error) = begin {
        finish(c).await?;
        return Err(error.into());
    }
    let operation = async {
        if remove {
            sqlx::query("DELETE FROM core_notificationdismissal WHERE notification_id IN (SELECT id FROM core_systemnotification WHERE source='developer' AND notification_key<>'network_security_m3u_epg')").execute(&mut *c).await?;
            sqlx::query("DELETE FROM core_systemnotification WHERE source='developer' AND notification_key<>'network_security_m3u_epg'").execute(&mut *c).await?;
        } else {
            // Same pinned fixture used by Source51. Updating an existing row
            // MUST NOT attempt an INSERT or advance its sequence.
            let action=json!({"action_url":"/settings#network-access","action_text":"Review Settings","condition":["m3u_epg_network_insecure"],"min_version":null,"max_version":null,"user_level":"admin"});
            let mut existing=sqlx::query_as::<_,(i64,chrono::DateTime<chrono::Utc>)>("SELECT id::bigint,created_at FROM core_systemnotification WHERE notification_key='network_security_m3u_epg' FOR UPDATE").fetch_optional(&mut *c).await?;
            if existing.is_none() {
                let created=chrono::Utc::now(); let updated=chrono::Utc::now();
                let inserted=sqlx::query("INSERT INTO core_systemnotification(notification_key,notification_type,priority,source,title,message,action_data,is_active,admin_only,expires_at,created_at,updated_at) VALUES('network_security_m3u_epg','warning','high','developer','Network Access Security Warning','Your EPG/M3U output is accessible from any network. Consider restricting access to improve security.',$1,true,true,NULL,$2,$3) ON CONFLICT(notification_key) DO NOTHING")
                    .bind(&action).bind(created).bind(updated).execute(&mut *c).await?;
                if inserted.rows_affected()==0 {
                    existing=Some(sqlx::query_as::<_,(i64,chrono::DateTime<chrono::Utc>)>("SELECT id::bigint,created_at FROM core_systemnotification WHERE notification_key='network_security_m3u_epg' FOR UPDATE").fetch_one(&mut *c).await?);
                }
            }
            if let Some((id,created))=existing {
                sqlx::query("UPDATE core_systemnotification SET notification_type='warning',priority='high',source='developer',title='Network Access Security Warning',message='Your EPG/M3U output is accessible from any network. Consider restricting access to improve security.',action_data=$1,is_active=true,admin_only=true,expires_at=NULL,created_at=$2,updated_at=$3 WHERE id=$4")
                    .bind(action).bind(created).bind(chrono::Utc::now()).bind(id).execute(&mut *c).await?;
            }
        }
        Ok::<(),sqlx::Error>(())
    }.await;
    if let Err(error) = operation {
        if let Err(rollback) = sqlx::query("ROLLBACK").execute(&mut *c).await {
            let _ = c.close().await;
            return Err(rollback.into());
        }
        finish(c).await?;
        return Err(error.into());
    }
    if let Err(error) = sqlx::query("COMMIT").execute(&mut *c).await {
        let _ = c.close().await;
        return Err(error.into());
    }
    finish(c).await
}
async fn clear_conditions(auth: &AuthService) -> Result<(), StoreError> {
    let cache = auth
        .notification_cache
        .as_ref()
        .ok_or(StoreError::Unsupported("Shared cache required"))?;
    // A bounded server-side pass never returns an unbounded key list to Rust.
    // Source delete_pattern also iterates SCAN; capacity is explicit rather
    // than silently declaring an incomplete invalidation successful.
    let mut cursor = 0u64;
    let mut rounds = 0usize;
    loop {
        rounds += 1;
        if rounds > 128 {
            return Err(StoreError::Unsupported("Condition invalidation capacity"));
        }
        let mut scan = redis::cmd("EVAL");
        scan.arg("local r=redis.call('SCAN',ARGV[1],'MATCH',':1:dev_notif_condition_*','COUNT',64); if #r[2]>4096 then return redis.error_reply('condition key capacity') end; for _,k in ipairs(r[2]) do redis.call('DEL',k) end; return r[1]").arg(0).arg(cursor);
        let next: String = cache.command_settled(scan).await?;
        cursor = next
            .parse()
            .map_err(|_| StoreError::Unsupported("Unexpected condition scan cursor"))?;
        if cursor == 0 {
            break;
        }
    }
    Ok(())
}
/// Pinned Source swallows condition-clear/sync failures. The actual Source51
/// captures qualify success only; exceptional cache.clear fallback and raced
/// developer fixture changes remain separate gates, not implied by this code.
pub(crate) async fn save(
    auth: &AuthService,
    scratch: &crate::core_settings_special_budget::Scratch,
) {
    let charge = match scratch.charge(16384) {
        Ok(charge) => charge,
        Err(error) => {
            tracing::warn!(%error,"Network notice scratch capacity unavailable");
            return;
        }
    };
    let auth = auth.clone().with_retained_scope(charge);
    let auth = &auth;
    if let Err(error) = clear_conditions(auth).await {
        tracing::warn!(%error,"Network notice condition invalidation failed");
        // Do not silently flush the whole application/broker DB: this Source
        // exception fallback remains explicitly unsupported pending a finite
        // fault contract and application namespace review.
    }
    let result = async {
        transaction(auth, false).await?;
        transaction(auth, true).await?;
        if let Some(events) = &auth.event_sink {
            let owner: Arc<dyn Send + Sync> = auth
                .request_writer_permit
                .clone()
                .ok_or(StoreError::Unsupported("Original writer required"))?;
            events
                .send_retained(
                    "updates",
                    json!({"type":"update","data":{"type":"notifications_cleared"}}),
                    owner,
                )
                .await
                .map_err(|_| StoreError::Unsupported("Notification publication failed"))?;
        } else {
            return Err(StoreError::Unsupported(
                "Actual Channels publisher required",
            ));
        }
        Ok::<(), StoreError>(())
    }
    .await;
    if let Err(error) = result {
        tracing::warn!(%error,"Network developer notification synchronization failed");
    }
}
