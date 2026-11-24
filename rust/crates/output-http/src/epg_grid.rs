//! Guide entity component from original Dispatcharr apps/epg/api_grid.py.
//! Original authors retain full application credit. HTTP attachment is separate.
use crate::{epg_custom::Window, epg_dummy};
use chrono::{DateTime, Duration, Timelike, Utc};
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
fn dummy_window(
    clock: DateTime<Utc>,
    lookback: DateTime<Utc>,
    cutoff: DateTime<Utc>,
) -> Result<Window, StoreError> {
    let (start, days) = grid_dummy_span(clock, lookback, cutoff).map_err(|_| capacity())?;
    Ok(Window {
        start,
        clock,
        days: u32::try_from(days).map_err(|_| capacity())?,
        lookback: Some(lookback),
        cutoff: Some(cutoff),
    })
}

fn legacy_dummy_window(
    now: DateTime<Utc>,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    requested_offset_micros: i64,
    utc_window: Window,
) -> Result<(Window, i64), StoreError> {
    let current_hour = now
        .with_minute(0)
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .ok_or_else(capacity)?;
    if requested_offset_micros == 0 || start < current_hour {
        return Ok((utc_window, 0));
    }

    // The Source future branch truncates the requested start in its own
    // timezone. Run the ordinary dummy generator in that fixed wall clock
    // so slot alignment, template hour, and overlap agree with the source.
    let shift = |time: DateTime<Utc>| {
        time.checked_add_signed(Duration::microseconds(requested_offset_micros))
            .ok_or_else(capacity)
    };
    Ok((
        dummy_window(shift(current_hour)?, shift(start)?, shift(end)?)?,
        requested_offset_micros,
    ))
}

// No-time custom generation uses absolute instants for timezone-derived
// programme metadata. Only its grid IDs and ISO fields use the request wall
// offset. The shared legacy helper represents those same times on a wall axis.
fn custom_future_window(
    wall: Window,
    now: DateTime<Utc>,
    offset: i64,
) -> Result<Window, StoreError> {
    let instant = |stamp: DateTime<Utc>| {
        stamp
            .checked_sub_signed(Duration::microseconds(offset))
            .ok_or_else(capacity)
    };
    Ok(Window {
        start: instant(wall.start)?,
        clock: now,
        days: wall.days,
        lookback: wall.lookback.map(instant).transpose()?,
        cutoff: wall.cutoff.map(instant).transpose()?,
    })
}

fn custom_wire_time(time: DateTime<Utc>, offset: i64) -> Result<DateTime<Utc>, StoreError> {
    time.checked_add_signed(Duration::microseconds(offset))
        .ok_or_else(capacity)
}

fn dummy_timestamp(time: DateTime<Utc>, wall_offset_micros: i64) -> String {
    GridTimestamp {
        local: time.naive_utc(),
        offset_micros: wall_offset_micros,
    }
    .source_isoformat()
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
    let requested_offset = start.offset_micros;
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
    let window = dummy_window(now, start, end)?;
    let (legacy_window, legacy_offset) =
        legacy_dummy_window(now, start, end, requested_offset, window)?;
    let custom_future = (legacy_offset != 0)
        .then(|| custom_future_window(legacy_window, now, legacy_offset))
        .transpose()?;
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
            let properties = custom.then_some(&channel.source_properties);
            let simple = epg_dummy::simple_config(properties).is_ok();
            let (window, mut offset) = if simple {
                (legacy_window, legacy_offset)
            } else {
                (window, 0)
            };
            let plan = crate::epg_custom::prepare(
                scope,
                &channel.uuid.to_string(),
                names[index].as_deref().unwrap_or(&channel.name),
                properties,
                window,
                1024 * 1024,
            )
            .await?;
            let mut programs = plan.programmes;
            let display_custom_wall =
                if let (crate::epg_custom::Programmes::Custom(programs), Some(window)) =
                    (&mut programs, custom_future)
                {
                    programs.align_future_grid_window(window)?;
                    offset = legacy_offset;
                    true
                } else {
                    false
                };
            for p in programs {
                let p = p?;
                let (began, ended) = if display_custom_wall {
                    (
                        custom_wire_time(p.start_time, offset)?,
                        custom_wire_time(p.end_time, offset)?,
                    )
                } else {
                    (p.start_time, p.end_time)
                };
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
                let row = json!({"id":format!("{prefix}-{}-{}",channel.id,began.format("%Y%m%dT%H%M%S")),"start_time":dummy_timestamp(began,offset),"end_time":dummy_timestamp(ended,offset),"title":p.title,"description":p.description,"tvg_id":channel.uuid.to_string(),"sub_title":p.sub_title,"custom_properties":properties,"season":null,"episode":null,"is_new":is_new,"is_live":is_live,"is_premiere":false,"is_finale":false});
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn custom_future_grid_aligns_true_instants_and_prints_request_wall_time() {
        let now = "2026-04-06T10:05:00Z".parse::<DateTime<Utc>>().unwrap();
        for (offset, start, end, first_base, first_wall) in [
            (
                19_800_000_000,
                "2026-04-07T07:07:19.123456Z",
                "2026-04-08T10:30:00Z",
                "2026-04-07T06:30:00Z",
                "2026-04-07T12:37:19.123456+05:30",
            ),
            (
                -12_600_000_000,
                "2026-04-07T16:07:19.123456Z",
                "2026-04-08T19:30:00Z",
                "2026-04-07T15:30:00Z",
                "2026-04-07T12:37:19.123456-03:30",
            ),
        ] {
            let start = start.parse::<DateTime<Utc>>().unwrap();
            let end = end.parse::<DateTime<Utc>>().unwrap();
            let normal = dummy_window(now, start, end).unwrap();
            let (wall, offset) = legacy_dummy_window(now, start, end, offset, normal).unwrap();
            let custom = custom_future_window(wall, now, offset).unwrap();
            assert_eq!(custom.start, first_base.parse::<DateTime<Utc>>().unwrap());
            assert_eq!(custom.lookback, Some(start));
            assert_eq!(custom.cutoff, Some(end));
            assert_eq!(custom.clock, now);
            assert_eq!(custom.days, 2);
            let display = custom_wire_time(start, offset).unwrap();
            assert_eq!(dummy_timestamp(display, offset), first_wall);
            assert_eq!(
                display.format("%Y%m%dT%H%M%S").to_string(),
                "20260407T123719"
            );
        }
    }

    #[test]
    fn future_dummy_grid_preserves_its_requested_offset_and_local_template_hour() {
        let now = "2026-04-06T10:05:00Z".parse::<DateTime<Utc>>().unwrap();
        for (offset, start, end, expected_start, expected_end) in [
            (
                19_800_000_000,
                "2026-04-07T07:07:00Z",
                "2026-04-07T08:00:00Z",
                "2026-04-07T12:00:00+05:30",
                "2026-04-07T16:00:00+05:30",
            ),
            (
                -14_400_000_000,
                "2026-04-07T16:37:00Z",
                "2026-04-07T17:00:00Z",
                "2026-04-07T12:00:00-04:00",
                "2026-04-07T16:00:00-04:00",
            ),
        ] {
            let start = start.parse::<DateTime<Utc>>().unwrap();
            let end = end.parse::<DateTime<Utc>>().unwrap();
            let old = dummy_window(now, start, end).unwrap();
            let (window, format_offset) =
                legacy_dummy_window(now, start, end, offset, old).unwrap();
            let standard: Vec<_> = epg_dummy::configured(
                "owned",
                "Owned guide",
                window.start,
                window.days,
                window.lookback,
                window.cutoff,
                None,
            )
            .unwrap()
            .collect();
            assert_eq!(standard.len(), 1);
            assert_eq!(
                standard[0].start_time.format("%Y%m%dT%H%M%S").to_string(),
                "20260407T120000"
            );
            assert_eq!(
                dummy_timestamp(standard[0].start_time, format_offset),
                expected_start
            );
            assert_eq!(
                dummy_timestamp(standard[0].end_time, format_offset),
                expected_end
            );
            let utc_noon = "2026-04-07T12:00:00Z".parse::<DateTime<Utc>>().unwrap();
            let expected = epg_dummy::standard("owned", "Owned guide", utc_noon, 1, None, None)
                .unwrap()
                .next()
                .unwrap();
            assert_eq!(standard[0].description, expected.description);
        }
        let earlier = "2026-04-06T09:55:00Z".parse::<DateTime<Utc>>().unwrap();
        let end = "2026-04-06T11:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let expected = dummy_window(now, earlier, end).unwrap();
        let (ordinary, offset) =
            legacy_dummy_window(now, earlier, end, 19_800_000_000, expected).unwrap();
        assert_eq!(offset, 0);
        assert_eq!(ordinary.start, expected.start);
        assert_eq!(ordinary.days, expected.days);
        assert_eq!(
            dummy_timestamp(ordinary.start, offset),
            "2026-04-06T10:00:00+00:00"
        );
    }
}
