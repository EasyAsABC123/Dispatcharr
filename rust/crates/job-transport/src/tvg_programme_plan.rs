//! Source per-channel atomic swap decision, with no SQL/ACK side effects.
//! Existing Source row-projection authority is reused, not a whole-task claim.
use chrono::{DateTime, Utc};
use dispatcharr_ingest_core::XmltvProgramme;
#[derive(Clone, Debug)]
pub struct Stored {
    pub id: i64,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}
#[derive(Debug)]
pub struct Plan {
    pub delete_ids: Vec<i64>,
    pub insert: Vec<XmltvProgramme>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refusal {
    Capacity,
    Identity,
}
/// Empty parse retains even expired rows, per pinned parse_programs_for_tvg_id.
/// Nonempty parse prunes cutoff, replaces overlap and preserves pre-cutoff
/// spanning starts without reinserting the duplicate pulled row.
pub fn plan(
    epg: i64,
    cutoff: DateTime<Utc>,
    stored: &[Stored],
    parsed: Vec<XmltvProgramme>,
) -> Result<Plan, Refusal> {
    if stored.len() > 4096 || parsed.len() > 4096 {
        return Err(Refusal::Capacity);
    }
    if epg <= 0 || parsed.iter().any(|r| r.epg_id != epg) {
        return Err(Refusal::Identity);
    }
    if parsed.is_empty() {
        return Ok(Plan {
            delete_ids: Vec::new(),
            insert: Vec::new(),
        });
    }
    let mut incoming: Vec<_> = parsed
        .into_iter()
        .filter(|p| p.end_time >= cutoff)
        .collect();
    let first = incoming
        .iter()
        .filter(|p| p.start_time >= cutoff)
        .map(|p| p.start_time)
        .min();
    let delete_ids: Vec<_> = stored
        .iter()
        .filter(|p| p.end < cutoff || first.is_some_and(|f| p.end > f))
        .map(|p| p.id)
        .collect();
    let kept: Vec<_> = stored
        .iter()
        .filter(|p| !delete_ids.contains(&p.id) && p.start < cutoff && p.end >= cutoff)
        .map(|p| p.start)
        .collect();
    incoming.retain(|p| p.start_time >= cutoff || !kept.contains(&p.start_time));
    Ok(Plan {
        delete_ids,
        insert: incoming,
    })
}
