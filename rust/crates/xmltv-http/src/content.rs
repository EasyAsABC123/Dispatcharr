//! Streaming XMLTV generation from the original Dispatcharr epg_generator.
//! Original authors retain full credit. Custom regular-expression dummy guides
//! are explicitly unsupported until their separate Source engine gate passes.
use crate::{dummy, render, rows, window::Window};
use chrono::{DateTime, Duration, Timelike, Utc};
use dispatcharr_store::{Store, StoreError, User};
use futures_util::{Stream, TryStreamExt};
use serde_json::json;
use std::{collections::BTreeMap, pin::Pin};

pub const PREAMBLE:&str="<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<tv generator-info-name=\"Dispatcharr\" generator-info-url=\"https://github.com/Dispatcharr/Dispatcharr\">\n";
pub struct Context {
    pub origin: String,
    pub profile: Option<String>,
    pub user: Option<User>,
    pub window: Window,
    pub cached_logos: bool,
    pub tvg_id_source: String,
    pub now: DateTime<Utc>,
    pub max_frame_bytes: usize,
    pub numbering_bytes: usize,
}

fn failure(status: u16, detail: &str) -> StoreError {
    StoreError::StatusBody(status, json!({"detail":detail}))
}
fn render_error(_: render::RenderError) -> StoreError {
    failure(503, "XMLTV resource or metadata capacity unavailable.")
}
/// Each emitted String is bounded. HTTP must retain the build admission until
/// all SQL/cache operations complete, and split UTF8 cache members if needed.
pub fn generate(
    store: Store,
    ctx: Context,
    limits: rows::Limits,
) -> Pin<Box<dyn Stream<Item = Result<String, StoreError>> + Send>> {
    generate_counted(store, ctx, limits, None)
}
pub fn generate_counted(
    store: Store,
    ctx: Context,
    limits: rows::Limits,
    channel_count: Option<std::sync::Arc<std::sync::atomic::AtomicUsize>>,
) -> Pin<Box<dyn Stream<Item = Result<String, StoreError>> + Send>> {
    Box::pin(async_stream::try_stream! {
        limits.validate()?;
        if ctx.origin.len()>65536 || ctx.tvg_id_source.len()>65536 || ctx.max_frame_bytes<PREAMBLE.len() || ctx.max_frame_bytes>8*1024*1024 { Err(failure(503,"XMLTV resource capacity unavailable."))?; }
        yield PREAMBLE.to_owned();
        let rows::Channels::Rows(channels)=rows::channels(&store,ctx.user.as_ref(),ctx.profile.as_deref(),&limits).await? else {Err(StoreError::NotFound)?;unreachable!()};
        if let Some(count)=&channel_count{count.store(channels.len(),std::sync::atomic::Ordering::Release);}
        let numbers=if ctx.user.is_some(){Some(dispatcharr_xc_http::numbers::assign(&channels.iter().map(|c|(c.id,c.number)).collect::<Vec<_>>(),ctx.numbering_bytes).map_err(|_|failure(503,"XMLTV numbering capacity unavailable."))?)}else{None};
        let mut real=BTreeMap::<i64,Vec<String>>::new();
        let mut placeholders=Vec::new();
        let mut map_bytes=0usize;
        let target=ctx.max_frame_bytes.min(64*1024);
        let mut batch=String::new();
        let mut channel_rows=0usize;
        for channel in channels {
            // Unknown custom data is never silently treated as a standard guide.
            if rows::requires_custom_adapter(&channel) { Err(failure(501,"Custom XMLTV source properties require a verified regex adapter."))?; }
            let number=if let Some(numbers)=&numbers {numbers.get(&channel.id).expect("assigned channel number").clone()}else{dispatcharr_output_http::render::display_number(channel.number).map_err(|_|failure(503,"Invalid XMLTV channel number."))?};
            let id=match ctx.tvg_id_source.as_str(){"tvg_id" if channel.tvg_id.as_ref().is_some_and(|s|!s.is_empty())=>channel.tvg_id.clone().unwrap(),"gracenote" if channel.station.as_ref().is_some_and(|s|!s.is_empty())=>channel.station.clone().unwrap(),_ if !number.is_empty()=>number,_=>channel.id.to_string()};
            let logo=channel.logo.as_ref().map_or_else(String::new,|(id,url)|if !ctx.cached_logos && (url.starts_with("http://")||url.starts_with("https://")){url.clone()}else{format!("{}/api/channels/logos/{id}/cache/",ctx.origin)});
            let xml=render::channel(&id,&channel.name,&logo,ctx.max_frame_bytes).map_err(render_error)?;
            if !batch.is_empty() && batch.len()+xml.len()>target {yield std::mem::take(&mut batch);}
            batch.push_str(&xml);channel_rows+=1;
            if channel_rows.is_multiple_of(200) || batch.len()>=target {yield std::mem::take(&mut batch);}
            map_bytes=map_bytes.checked_add(256+id.capacity()+channel.name.capacity()).ok_or_else(||failure(503,"XMLTV mapping capacity unavailable."))?;
            if map_bytes>limits.max_metadata_bytes {Err(failure(503,"XMLTV mapping capacity unavailable."))?;}
            if channel.epg_id.is_none() || channel.source_type.as_deref()==Some("dummy") {placeholders.push((id,channel.name));} else {real.entry(channel.epg_id.unwrap()).or_default().push(id);}
        }
        if !batch.is_empty(){yield std::mem::take(&mut batch);}
        drop(numbers);
        let lookback=ctx.now-Duration::days(ctx.window.prev_days);
        let cutoff=(ctx.window.forward_days>0).then(||ctx.now+Duration::days(ctx.window.forward_days));
        let mut programmes=rows::programmes(&store,real.keys().copied().collect(),lookback,cutoff,&limits);
        while let Some(programme)=programmes.try_next().await? {
            for id in real.get(&programme.row.epg_id).expect("selected EPG map") {
                let mut xml=render::programme(render::Programme{id:programme.id,channel:id,origin:&ctx.origin,row:&programme.row},ctx.max_frame_bytes.saturating_sub(1)).map_err(render_error)?;
                xml.push('\n');
                if !batch.is_empty() && batch.len()+xml.len()>target {yield std::mem::take(&mut batch);}
                batch.push_str(&xml);
                if batch.len()>=target {yield std::mem::take(&mut batch);}
            }
        }
        if !batch.is_empty(){yield std::mem::take(&mut batch);}
        drop(programmes);drop(real);
        let start=ctx.now.with_minute(0).and_then(|v|v.with_second(0)).and_then(|v|v.with_nanosecond(0)).expect("valid UTC hour");
        for (id,name) in placeholders {
            let days=if ctx.window.forward_days>0{ctx.window.forward_days as u32}else{3};
            for programme in dummy::standard(&id,&name,start,days,Some(lookback),cutoff).map_err(render_error)? {
                let xml=render::standard_dummy(&programme,ctx.max_frame_bytes).map_err(render_error)?;
                if !batch.is_empty() && batch.len()+xml.len()>target {yield std::mem::take(&mut batch);}
                batch.push_str(&xml);
                if batch.len()>=target {yield std::mem::take(&mut batch);}
            }
            if !batch.is_empty(){yield std::mem::take(&mut batch);}
        }
        yield "</tv>\n".into();
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    #[ignore = "requires actual original-schema PostgreSQL fixture and captured generator corpus"]
    async fn complete_original_generator_bytes() {
        let store = Store::connect(
            &std::env::var("DATABASE_TEST_URL").expect("owned fixture URL"),
            2,
        )
        .await
        .unwrap();
        let corpus: serde_json::Value = serde_json::from_slice(
            &std::fs::read(std::env::var("XMLTV_CONTENT_SOURCE").expect("actual Source corpus"))
                .unwrap(),
        )
        .unwrap();
        let cases = corpus["cases"].as_array().unwrap();
        assert_eq!(cases.len(), 36);
        for (index, row) in cases.iter().enumerate() {
            let user = if let Some(id) = row["user_id"].as_i64() {
                store.user_by_id(id).await.unwrap()
            } else {
                None
            };
            let ctx = Context {
                origin: row["origin"].as_str().unwrap().into(),
                profile: None,
                user,
                window: Window {
                    forward_days: row["forward_days"].as_i64().unwrap(),
                    prev_days: row["prev_days"].as_i64().unwrap(),
                },
                cached_logos: row["cached_logos"].as_bool().unwrap(),
                tvg_id_source: row["tvg_id_source"].as_str().unwrap().into(),
                now: DateTime::parse_from_rfc3339(row["now"].as_str().unwrap())
                    .unwrap()
                    .with_timezone(&Utc),
                max_frame_bytes: 1024 * 1024,
                numbering_bytes: 8 * 1024 * 1024,
            };
            let limits = rows::Limits {
                max_channels: 1000,
                max_metadata_bytes: 16 * 1024 * 1024,
                max_programme_metadata_bytes: 1024 * 1024,
                max_programmes: 10000,
            };
            let mut generated = generate(store.clone(), ctx, limits);
            let mut xml = String::new();
            while let Some(frame) = generated.try_next().await.unwrap() {
                xml.push_str(&frame);
                assert!(xml.len() < 8 * 1024 * 1024);
            }
            assert_eq!(
                xml,
                row["xml"].as_str().unwrap(),
                "actual Source generator case {index}"
            );
        }
    }
}
