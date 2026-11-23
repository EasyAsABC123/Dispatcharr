//! Normal Source DVR supervision: real polls, finite outage, retained attempts.
//! Original Dispatcharr authors retain algorithm/application credit.
use crate::{
    recording_active, recording_dvr_normal::Error, recording_files, recording_normal::NormalPaths,
    recording_process, recording_stop_handoff,
};
use chrono::{DateTime, Utc};
use dispatcharr_store::recording_capture::{CaptureStore, NormalRecordingState};
use recording_process::{NormalControl, NormalStop};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime},
};

pub(crate) struct Context {
    pub store: Arc<CaptureStore>,
    pub active: recording_active::Operations,
    pub owner: Arc<dyn Send + Sync>,
    pub trace: Arc<Mutex<Vec<Value>>>,
    pub recording: i64,
    pub channel: uuid::Uuid,
    pub paths: NormalPaths,
    pub origin: String,
    pub output_profile_id: Option<String>,
    pub files: Arc<recording_files::Working>,
    pub end: DateTime<Utc>,
    pub shutdown: Arc<AtomicBool>,
    pub transcript: Arc<recording_process::Transcript>,
    pub confirmation: Option<recording_active::LiteralConfirmation>,
    pub stop_generation: Option<Arc<recording_stop_handoff::Generation>>,
}
pub(crate) struct Outcome {
    pub reason: NormalStop,
    pub interrupted_reason: Option<String>,
    pub stream_confirmed: bool,
    pub end: DateTime<Utc>,
}
fn trace(context: &Context, kind: &str, value: Value) -> Result<(), Error> {
    let mut rows = context.trace.lock().map_err(|_| Error::Unsettled)?;
    if rows.len() == 16 {
        rows.remove(0);
    }
    rows.push(json!({"kind":kind,"observation":value}));
    Ok(())
}
async fn state(context: &Context) -> Result<Option<NormalRecordingState>, Error> {
    context
        .store
        .clone()
        .normal_state(context.recording, context.owner.clone())
        .await
        .map_err(Error::Store)
}
async fn progress(context: &Context) -> Result<recording_files::Progress, Error> {
    let files = context.files.clone();
    let owner = context.owner.clone();
    tokio::task::spawn_blocking(move || {
        let _owner = owner;
        files.normal_progress()
    })
    .await
    .map_err(|_| Error::Supervisor)?
    .map_err(Error::Files)
}
fn interrupted(reason: NormalStop, last_error: &str, confirmed: bool) -> Option<String> {
    match reason {
        NormalStop::HlsRemoved => Some("hls_directory_removed".into()),
        NormalStop::NoSegments | NormalStop::Stall => Some(if confirmed {
            format!("ffmpeg_outage_window_exhausted: {last_error}")
        } else {
            format!("no_stream_data: {last_error}")
        }),
        _ => None,
    }
}

fn observe_attempt_settlement(
    context: &Context,
    attempt: &Result<recording_process::NormalAttempt, recording_process::Error>,
) {
    let Some(generation) = &context.stop_generation else {
        return;
    };
    let settled = match attempt {
        // Constructing a NormalAttempt requires the child wait and
        // stderr reader join. A reported read error does not leave a reader.
        Ok(_) => true,
        Err(recording_process::Error::Spawn(_)) => context.transcript.settled_spawn_failure(),
        Err(_) => false,
    };
    if settled {
        generation.attempt_settled();
    } else {
        generation.attempt_uncertain();
    }
}

pub(crate) async fn capture(mut context: Context) -> Result<Outcome, Error> {
    // TSConfig.STREAM_TIMEOUT20 + FAILOVER_GRACE_PERIOD20 in pinned Source.
    const OUTAGE: Duration = Duration::from_secs(40);
    let mut end = context.end;
    let mut outage: Option<tokio::time::Instant> = None;
    let mut retry = 0u64;
    let mut confirmed = false;
    let mut last_error = "ffmpeg_failed".to_string();
    let mut last_break = "unknown";
    loop {
        if context.shutdown.load(Ordering::Acquire) {
            return Ok(Outcome {
                reason: NormalStop::Shutdown,
                interrupted_reason: None,
                stream_confirmed: confirmed,
                end,
            });
        }
        if Utc::now() >= end {
            return Ok(Outcome {
                reason: NormalStop::End,
                interrupted_reason: None,
                stream_confirmed: confirmed,
                end,
            });
        }
        match state(&context).await? {
            None => {
                return Ok(Outcome {
                    reason: NormalStop::Deleted,
                    interrupted_reason: None,
                    stream_confirmed: confirmed,
                    end,
                })
            }
            Some(row) if row.stopped => {
                return Ok(Outcome {
                    reason: NormalStop::Stopped,
                    interrupted_reason: None,
                    stream_confirmed: confirmed,
                    end,
                })
            }
            Some(row) if row.property_object => {
                end = end.max(row.end);
            }
            _ => {}
        }
        if retry > 0 {
            let delay = Duration::from_secs_f64((retry as f64 * 0.25).min(3.0));
            let until = tokio::time::Instant::now() + delay;
            while tokio::time::Instant::now() < until {
                if context.shutdown.load(Ordering::Acquire) {
                    return Ok(Outcome {
                        reason: NormalStop::Shutdown,
                        interrupted_reason: None,
                        stream_confirmed: confirmed,
                        end,
                    });
                }
                if Utc::now() >= end {
                    break;
                }
                if outage.is_some_and(|since| since.elapsed() >= OUTAGE) {
                    return Ok(Outcome {
                        reason: NormalStop::Stall,
                        interrupted_reason: interrupted(
                            NormalStop::Stall,
                            if confirmed { last_break } else { &last_error },
                            confirmed,
                        ),
                        stream_confirmed: confirmed,
                        end,
                    });
                }
                match state(&context).await? {
                    None => {
                        return Ok(Outcome {
                            reason: NormalStop::Deleted,
                            interrupted_reason: None,
                            stream_confirmed: confirmed,
                            end,
                        })
                    }
                    Some(row) if row.stopped => {
                        return Ok(Outcome {
                            reason: NormalStop::Stopped,
                            interrupted_reason: None,
                            stream_confirmed: confirmed,
                            end,
                        })
                    }
                    _ => {}
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            if Utc::now() >= end {
                continue;
            }
        }
        let initial = progress(&context).await?;
        let start_number = match initial {
            recording_files::Progress::Missing => {
                return Ok(Outcome {
                    reason: NormalStop::HlsRemoved,
                    interrupted_reason: Some("hls_directory_removed".into()),
                    stream_confirmed: confirmed,
                    end,
                })
            }
            recording_files::Progress::Present {
                max_index,
                playlist_has_segments,
                ..
            } => {
                if playlist_has_segments {
                    0
                } else {
                    match max_index {
                        None => 0,
                        Some(v) => v
                            .checked_add(1)
                            .ok_or(Error::Unsupported("segment index overflow"))?,
                    }
                }
            }
        };
        let control = Arc::new(NormalControl::new(end));
        let process_control = control.clone();
        let owner = context.owner.clone();
        let transcript = context.transcript.clone();
        let shutdown = context.shutdown.clone();
        let paths = context.paths.clone();
        let recording = context.recording;
        let channel = context.channel;
        let origin = recording_process::ObservedLocalOrigin::from_normal_internal_profile(
            &context.origin,
            context.output_profile_id.as_deref(),
        )
        .map_err(Error::Media)?;
        let attempt_wall = SystemTime::now();
        let attempt_start = tokio::time::Instant::now();
        let mut last_new = attempt_start;
        let mut last_count = start_number;
        let mut attempt_confirmed = false;
        let generation = context.stop_generation.clone();
        let mut media = tokio::task::spawn_blocking(move || {
            if generation.is_some_and(|generation| !generation.begin_attempt()) {
                return None;
            }
            Some(recording_process::capture_normal_attempt(
                channel,
                recording,
                paths,
                origin,
                start_number,
                process_control,
                shutdown,
                owner,
                transcript,
            ))
        });
        let mut tick = tokio::time::interval(Duration::from_millis(500));
        tick.tick().await;
        let mut last_sql = tokio::time::Instant::now();
        let mut last_active = tokio::time::Instant::now() - Duration::from_secs(15);
        let attempt = loop {
            tokio::select! {
                actual=&mut media=>break actual.map_err(|_|Error::Supervisor)?,
                _=tick.tick()=>{
                    let monitoring:Result<(),Error>=async {
                        if context.shutdown.load(Ordering::Acquire){control.request(NormalStop::Shutdown);return Ok(());}
                        if last_active.elapsed()>=Duration::from_secs(15){
                            let prior=context.confirmation.take().ok_or(Error::Active(recording_active::Error::Uncertain))?;
                            let actual=context.active.refresh_normal(context.recording,prior,context.owner.clone()).await.map_err(Error::Active)?;
                            trace(&context,"renewed",actual.observation.receipt(json!(actual.observation.value)))?;context.confirmation=Some(actual.confirmation);last_active=tokio::time::Instant::now();
                        }
                        match progress(&context).await?{
                            recording_files::Progress::Missing=>{let reason=if state(&context).await?.is_none(){NormalStop::Deleted}else{NormalStop::HlsRemoved};control.request(reason);return Ok(());},
                            recording_files::Progress::Present{count,newest_mtime,..}=>{
                                let activity=newest_mtime.is_some_and(|time|time>=attempt_wall);
                                if !attempt_confirmed {
                                    if count>last_count||activity {attempt_confirmed=true;confirmed=true;retry=0;outage=None;last_count=count;last_new=tokio::time::Instant::now();}
                                    else if attempt_start.elapsed()>Duration::from_secs(15){control.request(NormalStop::NoSegments);}
                                }else if count>last_count||newest_mtime.is_some_and(|time|time.duration_since(attempt_wall).is_ok_and(|elapsed|elapsed>last_new.duration_since(attempt_start))) {last_count=count;last_new=tokio::time::Instant::now();}
                                else if last_new.elapsed()>Duration::from_secs(60){control.request(NormalStop::Stall);}
                            }
                        }
                        if Utc::now()>=end{control.request(NormalStop::End);}
                        if last_sql.elapsed()>=Duration::from_secs(2){
                            last_sql=tokio::time::Instant::now();
                            match state(&context).await {
                                Ok(None)=>control.request(NormalStop::Deleted),Ok(Some(row))if row.stopped=>control.request(NormalStop::Stopped),
                                Ok(Some(row))if row.property_object=>{end=end.max(row.end);control.extend(end);},Ok(_)=>{},
                                Err(Error::Store(dispatcharr_store::recording_capture::Error::Sql{stage:dispatcharr_store::recording_capture::Stage::Recording,settlement:None,..}))=>trace(&context,"caught-stop-poll-SQL",json!({"settled":true}))?,
                                Err(error)=>return Err(error),
                            }
                        }Ok(())
                    }.await;
                    if let Err(error) = monitoring {
                        control.request(NormalStop::Shutdown);
                        let stopped = media.await.map_err(|_| Error::Supervisor)?;
                        if let Some(stopped) = stopped {
                            observe_attempt_settlement(&context, &stopped);
                        }
                        return Err(error);
                    }
                }
            }
        };
        let Some(attempt) = attempt else {
            return Ok(Outcome {
                reason: NormalStop::Stopped,
                interrupted_reason: None,
                stream_confirmed: confirmed,
                end,
            });
        };
        observe_attempt_settlement(&context, &attempt);
        match attempt {
            Ok(actual) => {
                trace(
                    &context,
                    "capture-attempt-retired",
                    json!({"code":actual.receipt.code,"signal":actual.receipt.signal,"stopped":format!("{:?}",actual.stopped),"forced_kill":actual.receipt.forced_kill}),
                )?;
                match actual.stopped {
                    Some(
                        reason @ (NormalStop::End
                        | NormalStop::Stopped
                        | NormalStop::Deleted
                        | NormalStop::HlsRemoved
                        | NormalStop::Shutdown),
                    ) => {
                        return Ok(Outcome {
                            reason,
                            interrupted_reason: interrupted(reason, &last_error, confirmed),
                            stream_confirmed: confirmed,
                            end,
                        })
                    }
                    Some(NormalStop::NoSegments) => {
                        last_break = "no_segments";
                        last_error = format!("no_segments_in_15.0s_from_{}", context.origin);
                    }
                    Some(NormalStop::Stall) => {
                        last_break = "stall";
                    }
                    _ => {
                        last_break = "unexpected_exit";
                        if !confirmed {
                            last_error = format!(
                                "rc={} from {}",
                                actual
                                    .receipt
                                    .code
                                    .map_or_else(|| "None".into(), |v| v.to_string()),
                                context.origin
                            );
                        }
                    }
                }
            }
            Err(recording_process::Error::Spawn(error))
                if context.transcript.settled_spawn_failure() =>
            {
                last_break = "launch_failed";
                last_error = error.to_string().chars().take(2048).collect();
                trace(
                    &context,
                    "capture-launch-failed",
                    json!({"error":last_error}),
                )?;
            }
            Err(error) => return Err(Error::Media(error)),
        }
        if Utc::now() >= end {
            return Ok(Outcome {
                reason: NormalStop::End,
                interrupted_reason: if confirmed {
                    None
                } else {
                    Some(format!("no_stream_data: {last_error}"))
                },
                stream_confirmed: confirmed,
                end,
            });
        }
        let since = *outage.get_or_insert_with(tokio::time::Instant::now);
        if since.elapsed() >= OUTAGE {
            return Ok(Outcome {
                reason: NormalStop::Stall,
                interrupted_reason: interrupted(
                    NormalStop::Stall,
                    if confirmed { last_break } else { &last_error },
                    confirmed,
                ),
                stream_confirmed: confirmed,
                end,
            });
        }
        retry = retry.saturating_add(1);
    }
}
