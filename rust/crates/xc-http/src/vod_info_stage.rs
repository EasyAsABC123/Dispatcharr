//! Private cached-info state machine. No route, refresh or Main activation.
//! Source's synchronous refresh boundary stays explicit. Original Dispatcharr
//! authors retain view/model/algorithm credit. Caller retains the SAME existing
//! request, SQL and byte owners through every await and response-frame lifetime.
use crate::{
    project::JsonBytes,
    reader::{self, ReadLimit},
    vod, vod_episode_rows,
    vod_info_refresh::Decision,
    vod_info_row::{self, InfoRow},
    vod_lists, vod_movie_info,
    vod_priority::Family,
    vod_series_info,
};
use dispatcharr_store::{Store, StoreError, User};
use serde_json::Value;

#[derive(Debug)]
pub(crate) enum Error {
    NotFound,
    MovieInvalidId,
    UnsupportedIdRange,
    UnsupportedClock,
    UnsupportedContext,
    Projection(vod_lists::Error),
    Store(StoreError),
}
impl From<StoreError> for Error {
    fn from(value: StoreError) -> Self {
        Self::Store(value)
    }
}
impl From<vod_lists::Error> for Error {
    fn from(value: vod_lists::Error) -> Self {
        Self::Projection(value)
    }
}
impl Error {
    /// A later HTTP adapter must retain backend/capacity classification instead
    /// of changing it into a not-found or invalid-login result.
    pub fn separate_backend(self) -> Result<Self, StoreError> {
        match self {
            Self::Store(error) | Self::Projection(vod_lists::Error::Store(error)) => Err(error),
            other => Ok(other),
        }
    }
}

pub(crate) struct Context<'a> {
    pub store: &'a Store,
    pub user: &'a User,
    pub family: Family,
    pub id: Option<&'a str>,
    pub origin: &'a str,
    pub limits: &'a ReadLimit,
    pub available_work_bytes: usize,
}
/// The only constructor is eligibility preparation. A required or failed
/// refresh check cannot accidentally enter the cached serializer as success.
pub(crate) struct FreshInfo {
    row: InfoRow,
    family: Family,
}
pub(crate) enum Prepared {
    Fresh(FreshInfo),
    RefreshRequired(InfoRow),
    PropertyCheckFailed(InfoRow),
}
impl Prepared {
    pub fn retained_bytes(&self) -> usize {
        match self {
            Self::Fresh(row) => row.row.retained_bytes,
            Self::RefreshRequired(row) | Self::PropertyCheckFailed(row) => row.retained_bytes,
        }
    }
}
impl FreshInfo {
    /// Preserve the initially selected relation/account through any later
    /// bridge, rather than rerunning priority selection after a task.
    pub fn selected_ids(&self) -> (i64, i64, i64) {
        (
            self.row.object.content_id,
            self.row.relation_id,
            self.row.account_id,
        )
    }
}
fn lookup_id(props: &Value, family: Family, input: Option<&str>) -> Result<i64, Error> {
    if !vod::enabled(props, matches!(family, Family::Series))? {
        return Err(Error::NotFound);
    }
    let raw = input.filter(|s| !s.is_empty()).ok_or(Error::NotFound)?;
    let invalid = || {
        if matches!(family, Family::Series) {
            Error::NotFound
        } else {
            Error::MovieInvalidId
        }
    };
    let integer = dispatcharr_ingest_core::source_integer_literal(raw).ok_or_else(invalid)?;
    integer.parse().map_err(|_| Error::UnsupportedIdRange)
}
pub(crate) async fn prepare<N: FnMut() -> i64>(
    ctx: &Context<'_>,
    now: N,
) -> Result<Prepared, Error> {
    let id = lookup_id(&ctx.user.custom_properties, ctx.family, ctx.id)?;
    let row = vod_info_row::read(
        ctx.store,
        ctx.user,
        ctx.family,
        id,
        ctx.limits,
        ctx.available_work_bytes,
    )
    .await?
    .ok_or(Error::NotFound)?;
    let decision = row
        .refresh_decision(ctx.family, now)
        .map_err(vod_lists::Error::from)?;
    let prepared = match decision {
        Decision::Fresh => Prepared::Fresh(FreshInfo {
            row,
            family: ctx.family,
        }),
        Decision::RefreshRequired => Prepared::RefreshRequired(row),
        Decision::CheckFailed => Prepared::PropertyCheckFailed(row),
        Decision::UnsupportedClock => return Err(Error::UnsupportedClock),
    };
    if prepared.retained_bytes() > ctx.available_work_bytes {
        return Err(reader::capacity().into());
    }
    Ok(prepared)
}
pub(crate) async fn write_cached(
    ctx: &Context<'_>,
    fresh: &FreshInfo,
    out: &mut JsonBytes,
) -> Result<(), Error> {
    if !matches!(
        (ctx.family, fresh.family),
        (Family::Movie, Family::Movie) | (Family::Series, Family::Series)
    ) {
        return Err(Error::UnsupportedContext);
    }
    let (content, relation, account) = fresh.selected_ids();
    if content <= 0 || relation <= 0 || account <= 0 {
        return Err(Error::UnsupportedIdRange);
    }
    match ctx.family {
        Family::Movie => {
            vod_movie_info::write(&fresh.row, ctx.origin, ctx.available_work_bytes, out)?
        }
        Family::Series => {
            // Parent row persists across both independent episode statements.
            // Charge it and the existing output/origin scratch BEFORE driver
            // buffers/episode rows, then use the full same quote in projection.
            let child_work = ctx
                .available_work_bytes
                .checked_sub(fresh.row.retained_bytes)
                .and_then(|n| n.checked_sub(out.maximum().checked_mul(2)?))
                .and_then(|n| n.checked_sub(ctx.origin.len().checked_mul(6)?))
                .and_then(|n| n.checked_sub(4096))
                .ok_or_else(reader::capacity)?;
            let rows = vod_episode_rows::read(ctx.store, content, ctx.limits, child_work).await?;
            vod_series_info::write(&fresh.row, &rows, ctx.origin, ctx.available_work_bytes, out)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn disabled_and_missing_ids_precede_conversion_or_query() {
        assert!(matches!(
            lookup_id(
                &json!({"vod_movies_enabled":false}),
                Family::Movie,
                Some("bad")
            ),
            Err(Error::NotFound)
        ));
        assert!(matches!(
            lookup_id(
                &json!({"vod_series_enabled":false}),
                Family::Series,
                Some("bad")
            ),
            Err(Error::NotFound)
        ));
        for family in [Family::Movie, Family::Series] {
            assert!(matches!(
                lookup_id(&json!({}), family, None),
                Err(Error::NotFound)
            ));
            assert!(matches!(
                lookup_id(&json!({}), family, Some("")),
                Err(Error::NotFound)
            ));
        }
    }
    #[test]
    fn movie_series_invalid_id_branches_remain_distinct_and_range_is_unqualified() {
        assert!(matches!(
            lookup_id(&json!({}), Family::Movie, Some("bad")),
            Err(Error::MovieInvalidId)
        ));
        assert!(matches!(
            lookup_id(&json!({}), Family::Series, Some("bad")),
            Err(Error::NotFound)
        ));
        for family in [Family::Movie, Family::Series] {
            assert_eq!(lookup_id(&json!({}), family, Some("  +1_2  ")).unwrap(), 12);
            assert!(matches!(
                lookup_id(&json!({}), family, Some("9223372036854775808")),
                Err(Error::UnsupportedIdRange)
            ));
        }
        let _ = prepare::<fn() -> i64>;
        let _ = write_cached;
    }
    #[test]
    fn capacity_and_backend_errors_are_not_reclassified_as_authentication() {
        assert!(matches!(
            Error::Store(reader::capacity()).separate_backend(),
            Err(StoreError::StatusBody(503, _))
        ));
        assert!(matches!(
            Error::Projection(vod_lists::Error::Store(reader::capacity())).separate_backend(),
            Err(StoreError::StatusBody(503, _))
        ));
        assert!(matches!(
            Error::NotFound.separate_backend(),
            Ok(Error::NotFound)
        ));
    }
}
