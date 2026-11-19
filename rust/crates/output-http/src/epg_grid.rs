//! Guide entity component from original Dispatcharr apps/epg/api_grid.py.
//! Original authors retain full application credit. HTTP attachment is separate.
use crate::epg_dummy;
use chrono::{DateTime, SecondsFormat, Utc};
use dispatcharr_store::{
    grid_channel_profile, grid_dummy_span, grid_encode_batch, grid_resolve_window, GridTimestamp,
    Store, StoreError, User,
};
use serde_json::{json, Value};
use std::collections::HashSet;
fn capacity() -> StoreError {
    StoreError::StatusBody(503, json!({"detail":"Guide read capacity unavailable."}))
}
fn unsupported() -> StoreError {
    StoreError::StatusBody(
        501,
        json!({"detail":"Custom dummy guide rules require a separate compatibility gate."}),
    )
}
fn utc(value: GridTimestamp) -> DateTime<Utc> {
    value.local.and_utc() - chrono::Duration::microseconds(value.offset_micros)
}
/// Produce the original complete JSON entity for stored and standard dummy
/// guides. Retained frames are capped at32MiB, metadata16MiB and each frame
///16MiB. No HTTP admission/body lease is supplied by this component.
pub async fn stored_standard_entity(
    store: &Store,
    user: &User,
    params: &[(String, String)],
    now: DateTime<Utc>,
    history: i64,
) -> Result<Vec<Vec<u8>>, StoreError> {
    stored_scoped_entity(store, user, params, now, history, None).await
}
pub async fn stored_scoped_entity(
    store: &Store,
    user: &User,
    params: &[(String, String)],
    now: DateTime<Utc>,
    history: i64,
    scope: Option<&std::sync::Arc<crate::epg_custom_scope::Scope>>,
) -> Result<Vec<Vec<u8>>, StoreError> {
    let stamp = GridTimestamp {
        local: now.naive_utc(),
        offset_micros: 0,
    };
    let (start, end) = grid_resolve_window(params, stamp, history).map_err(|e| {
        StoreError::StatusBody(
            if e.class == "GridWindowError" {
                400
            } else {
                500
            },
            json!({"error":e.message}),
        )
    })?;
    let (start, end) = (utc(start), utc(end));
    let profile = grid_channel_profile(params)
        .map_err(|message| StoreError::StatusBody(400, json!({"error":message})))?;
    let channels = store
        .grid_visible_channels(user, profile.as_deref())
        .await?;
    if channels.iter().any(|c| {
        c.source_type.as_deref() == Some("dummy")
            && epg_dummy::simple_config(Some(&c.source_properties)).is_err()
            && (scope.is_none() || !crate::epg_custom::supported(&c.source_properties))
    }) {
        return Err(unsupported());
    }
    let names = crate::epg_names::resolve(
        store,
        &channels
            .iter()
            .map(|c| {
                (
                    c.id,
                    (c.source_type.as_deref() == Some("dummy")).then_some(&c.source_properties),
                )
            })
            .collect::<Vec<_>>(),
        16 * 1024 * 1024,
    )
    .await?;
    let ids: HashSet<i64> = channels
        .iter()
        .filter(|c| c.source_type.as_deref() != Some("dummy"))
        .filter_map(|c| c.epg_id)
        .collect();
    let mut frames = store
        .grid_stored_frames(&ids.into_iter().collect::<Vec<_>>(), start, end)
        .await?;
    let mut retained: usize = frames.iter().map(Vec::len).sum();
    let mut first = frames.is_empty();
    let (base, days) = grid_dummy_span(now, start, end).map_err(|_| capacity())?;
    let days = u32::try_from(days).map_err(|_| capacity())?;
    let mut batch = Vec::new();
    let mut batch_bytes = 0usize;
    for custom in [true, false] {
        for (index, channel) in channels.iter().enumerate().filter(|(_, c)| {
            if custom {
                c.source_type.as_deref() == Some("dummy")
            } else {
                c.epg_id.is_none()
            }
        }) {
            let plan = crate::epg_custom::prepare(
                scope,
                &channel.uuid.to_string(),
                names[index].as_deref().unwrap_or(&channel.name),
                custom.then_some(&channel.source_properties),
                crate::epg_custom::Window {
                    start: base,
                    clock: now,
                    days,
                    lookback: Some(start),
                    cutoff: Some(end),
                },
                1024 * 1024,
            )
            .await?;
            let programs = plan.programmes;
            for p in programs {
                let p = p?;
                let prefix = if custom {
                    "dummy-custom"
                } else {
                    "dummy-standard"
                };
                let is_new = p.properties.get("new") == Some(&Value::Bool(true));
                let is_live = p.properties.get("live") == Some(&Value::Bool(true));
                let properties = if p.properties.as_object().is_some_and(|v| v.is_empty()) {
                    Value::Null
                } else {
                    p.properties
                };
                let format = |time: DateTime<Utc>| {
                    time.to_rfc3339_opts(
                        if time.timestamp_subsec_micros() == 0 {
                            SecondsFormat::Secs
                        } else {
                            SecondsFormat::Micros
                        },
                        false,
                    )
                };
                let row = json!({"id":format!("{prefix}-{}-{}",channel.id,p.start_time.format("%Y%m%dT%H%M%S")),"start_time":format(p.start_time),"end_time":format(p.end_time),"title":p.title,"description":p.description,"tvg_id":channel.uuid.to_string(),"sub_title":p.sub_title,"custom_properties":properties,"season":null,"episode":null,"is_new":is_new,"is_live":is_live,"is_premiere":false,"is_finale":false});
                let charge = crate::epg_custom::metadata_bytes(&row)?;
                if charge > 1024 * 1024 {
                    return Err(capacity());
                }
                if !batch.is_empty() && batch_bytes + charge > 1024 * 1024 {
                    let (frame, next) = grid_encode_batch(&batch, first, 16 * 1024 * 1024)
                        .map_err(|_| capacity())?;
                    retained = retained
                        .checked_add(frame.len())
                        .filter(|n| *n <= 32 * 1024 * 1024)
                        .ok_or_else(capacity)?;
                    frames.push(frame);
                    first = next;
                    batch.clear();
                    batch_bytes = 0;
                }
                batch.push(row);
                batch_bytes += charge;
                if batch.len() == 500 {
                    let (frame, next) = grid_encode_batch(&batch, first, 16 * 1024 * 1024)
                        .map_err(|_| capacity())?;
                    retained = retained
                        .checked_add(frame.len())
                        .filter(|n| *n <= 32 * 1024 * 1024)
                        .ok_or_else(capacity)?;
                    frames.push(frame);
                    first = next;
                    batch.clear();
                    batch_bytes = 0;
                }
            }
        }
    }
    if !batch.is_empty() {
        let (frame, _) =
            grid_encode_batch(&batch, first, 16 * 1024 * 1024).map_err(|_| capacity())?;
        retained = retained
            .checked_add(frame.len())
            .filter(|n| *n <= 32 * 1024 * 1024)
            .ok_or_else(capacity)?;
        frames.push(frame);
    }
    if retained > 32 * 1024 * 1024 {
        return Err(capacity());
    }
    frames.insert(0, b"{\"data\":[".to_vec());
    frames.push(b"]}".to_vec());
    Ok(frames)
}
