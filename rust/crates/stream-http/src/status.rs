//! Compatibility projection of upstream ChannelStatus metadata (upstream credit retained).
use crate::StreamEngine;
use dispatcharr_contracts::PlaybackError;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    time::{SystemTime, UNIX_EPOCH},
};

impl StreamEngine {
    pub async fn legacy_status(
        &self,
        id: &str,
        detailed: bool,
    ) -> Result<Option<Value>, PlaybackError> {
        let Some(store) = &self.inner.clients else {
            return Ok(None);
        };
        let Some((meta, index)) = store
            .channel_snapshot(id)
            .await
            .map_err(|_| PlaybackError::new(503, "Channel metadata unavailable"))?
        else {
            return Ok(None);
        };
        let clients = store
            .channel_clients(id)
            .await
            .map_err(|_| PlaybackError::new(503, "Client metadata unavailable"))?;
        let mut status = project_status_at(
            id,
            &meta,
            index,
            clients,
            detailed,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64(),
        );
        if detailed {
            if let Some(inputs) = &self.inner.inputs {
                status["buffer_stats"] = inputs
                    .buffer_diagnostics(id, index)
                    .await
                    .map_err(|_| PlaybackError::new(503, "Buffer diagnostics unavailable"))?;
            }
        }
        Ok(Some(status))
    }
}
pub fn project_status_at(
    id: &str,
    meta: &BTreeMap<String, String>,
    index: u64,
    clients: Vec<Value>,
    detailed: bool,
    now: f64,
) -> Value {
    let created = meta
        .get("init_time")
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(0.0);
    let uptime = if created > 0.0 { now - created } else { 0.0 };
    let value = |key: &str, default: &str| meta.get(key).map_or(default.to_string(), Clone::clone);
    let mut info = json!({"channel_id":id,"state":meta.get("state"),"url":value("url",""),"stream_profile":value("stream_profile",""),"owner":meta.get("owner"),"buffer_index":index,"client_count":clients.len(),"uptime":uptime,"started_at":if created>0.0{Some(created)}else{None}});
    for key in ["channel_name", "stream_name"] {
        if let Some(v) = meta.get(key).filter(|v| !v.is_empty()) {
            info[key] = json!(v);
        }
    }
    for (target, key) in [
        ("stream_id", "stream_id"),
        ("logo_id", "logo_id"),
        ("m3u_profile_id", "m3u_profile"),
    ] {
        if detailed && target == "logo_id" {
            continue;
        }
        if let Some(n) = meta.get(key).and_then(|v| v.parse::<i64>().ok()) {
            info[target] = json!(n);
        }
    }
    if let Some(bytes) = meta.get("total_bytes").and_then(|v| v.parse::<u64>().ok()) {
        info["total_bytes"] = json!(bytes);
        if uptime > 0.0 {
            let bitrate = bytes as f64 * 8.0 / uptime / 1000.0;
            info["avg_bitrate_kbps"] = json!(bitrate);
            info["avg_bitrate"] = json!(if bitrate > 1000.0 {
                format!("{:.2} Mbps", bitrate / 1000.0)
            } else {
                format!("{bitrate:.2} Kbps")
            });
        }
        if detailed {
            info["total_data"] = json!(if bytes < 1024 {
                format!("{bytes} B")
            } else if bytes < 1024 * 1024 {
                format!("{:.2} KB", bytes as f64 / 1024.0)
            } else if bytes < 1024 * 1024 * 1024 {
                format!("{:.2} MB", bytes as f64 / (1024.0 * 1024.0))
            } else {
                format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
            });
        }
    }
    if detailed {
        if let Some(v) = meta.get("m3u_profile_name") {
            info["m3u_profile_name"] = json!(v);
        }
        if let Some(v) = meta
            .get("state_changed_at")
            .and_then(|v| v.parse::<f64>().ok())
        {
            info["state_changed_at"] = json!(v);
            info["state_duration"] = json!(now - v);
        }
        info["clients"] = json!(clients);
        // Fallback for engines without a shared input store. The application
        // replaces this with an actual bounded Redis sample above.
        info["buffer_stats"] = json!({"chunks":index,"diagnostics":if index>0{json!({"all_buffer_keys":[],"total_buffer_keys":0})}else{json!({})},"latest_chunk_ttl":-2});
    } else {
        let concise:Vec<Value>=clients.into_iter().take(10).map(|client|{
            let mut value=json!({"client_id":client["client_id"],"user_agent":client["user_agent"],"output_format":client["output_format"],"output_profile_id":client["output_profile_id"]});
            for key in ["ip_address","connected_at","user_id"]{if !client[key].is_null(){value[key]=client[key].clone();}}value
        }).collect();
        info["clients"] = json!(concise);
    }
    for key in [
        "video_codec",
        "resolution",
        "audio_codec",
        "audio_channels",
        "stream_type",
    ] {
        if let Some(v) = meta.get(key).filter(|v| !v.is_empty()) {
            info[key] = json!(v);
        }
    }
    for key in ["source_fps", "ffmpeg_speed"] {
        if let Some(v) = meta.get(key).filter(|v| !v.is_empty()) {
            if detailed {
                info[key] = json!(v);
            } else if let Ok(number) = v.parse::<f64>() {
                info[key] = json!(number);
            }
        }
    }
    if detailed {
        for key in [
            "pixel_format",
            "source_bitrate",
            "sample_rate",
            "audio_bitrate",
            "ffmpeg_fps",
            "actual_fps",
            "ffmpeg_bitrate",
        ] {
            if let Some(v) = meta.get(key).filter(|v| !v.is_empty()) {
                info[key] = json!(v);
            }
        }
    }
    info
}
