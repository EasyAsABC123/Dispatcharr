//! Original Dispatcharr Channel Batch group, content and access flag updates.
//! Original authors and contributors retain full credit; see ATTRIBUTION.md.
use super::{boolean, err, integer, mark_group, primary_key, rename};
use crate::{channels, AuthService, StoreError};
use axum::{
    extract::{ConnectInfo, OriginalUri, Query, Request, State},
    http::header,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::{json, Map, Value};
use sqlx::{Postgres, QueryBuilder, Transaction};
use std::{
    collections::{BTreeSet, HashMap},
    net::SocketAddr,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum Reference {
    #[default]
    Absent,
    Clear,
    Set(i64),
}

impl Reference {
    fn parse(field: &str, value: Option<&Value>) -> Result<Self, StoreError> {
        match value {
            None => Ok(Self::Absent),
            Some(Value::Null) => Ok(Self::Clear),
            Some(value) => primary_key(value)
                .map_err(|error| rename(error, "channel", field))?
                .map(Self::Set)
                .ok_or_else(|| err(field, "Object does not exist.")),
        }
    }

    fn id(self) -> Option<i64> {
        if let Self::Set(id) = self {
            Some(id)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct References {
    profile: Reference,
    logo: Reference,
}

impl References {
    fn parse(fields: &Map<String, Value>) -> Result<Self, StoreError> {
        Ok(Self {
            profile: Reference::parse("stream_profile_id", fields.get("stream_profile_id"))?,
            logo: Reference::parse("logo_id", fields.get("logo_id"))?,
        })
    }

    fn present(self) -> bool {
        self.profile != Reference::Absent || self.logo != Reference::Absent
    }

    fn clears(self) -> bool {
        self.profile == Reference::Clear || self.logo == Reference::Clear
    }
}

#[derive(Default)]
struct ReferenceColumns {
    profiles_present: Vec<bool>,
    profiles: Vec<Option<i64>>,
    logos_present: Vec<bool>,
    logos: Vec<Option<i64>>,
}

impl FromIterator<References> for ReferenceColumns {
    fn from_iter<T: IntoIterator<Item = References>>(rows: T) -> Self {
        let mut columns = Self::default();
        for row in rows {
            columns
                .profiles_present
                .push(row.profile != Reference::Absent);
            columns.profiles.push(row.profile.id());
            columns.logos_present.push(row.logo != Reference::Absent);
            columns.logos.push(row.logo.id());
        }
        columns
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct Edit {
    id: i64,
    group: Option<i64>,
    override_group: Option<i64>,
    mature: Option<bool>,
    radio: Option<bool>,
    override_radio: Option<bool>,
    level: Option<i32>,
    references: References,
    override_references: References,
    clear_override: bool,
    hidden: Option<bool>,
}

impl Edit {
    fn has_override_values(&self) -> bool {
        self.override_group.is_some()
            || self.override_radio.is_some()
            || self.override_references.present()
    }

    fn changes_override(&self) -> bool {
        self.clear_override || self.has_override_values()
    }
}

fn unsupported() -> StoreError {
    StoreError::Unsupported("This Channel Batch field is not implemented in Native.")
}

fn group(value: &Value) -> Result<i64, StoreError> {
    value.as_i64().filter(|id| *id > 0).ok_or_else(unsupported)
}

fn user_level(value: &Value) -> Result<i32, StoreError> {
    let integer = integer(value).map_err(|error| rename(error, "channel_id", "user_level"))?;
    let number = integer.as_i64().unwrap_or(i64::MAX);
    i32::try_from(number).map_err(|_| {
        let message = if number < i64::from(i32::MIN) {
            "Ensure this value is greater than or equal to -2147483648."
        } else {
            "Ensure this value is less than or equal to 2147483647."
        };
        err("user_level", message)
    })
}

fn edits(value: Value) -> Result<Vec<Edit>, StoreError> {
    let Value::Array(items) = value else {
        return Err(StoreError::InvalidInput(
            json!({"error":"Expected a list of channel updates"}),
        ));
    };
    // Dict replacement keeps the first position while applying the last
    // item. Keep the response order separate from PostgreSQL lock order.
    let mut ordered = Vec::with_capacity(items.len());
    let mut positions = HashMap::with_capacity(items.len());
    let mut overrides = HashMap::new();
    let mut missing = Vec::new();
    for (index, item) in items.into_iter().enumerate() {
        let row = item.as_object().ok_or_else(unsupported)?;
        let Some(id) = row.get("id").and_then(Value::as_i64).filter(|id| *id != 0) else {
            missing.push(format!("Item {index}: Channel ID is required"));
            continue;
        };
        if let Some(value) = row.get("override") {
            overrides.insert(id, value.clone());
        }
        if let Some(&position) = positions.get(&id) {
            ordered[position] = (id, item);
        } else {
            positions.insert(id, ordered.len());
            ordered.push((id, item));
        }
    }
    if !missing.is_empty() {
        return Err(StoreError::InvalidInput(json!({"errors":missing})));
    }
    ordered
        .into_iter()
        .map(|(id, item)| {
            let row = item.as_object().expect("checked batch object");
            if row.keys().any(|key| {
                !matches!(
                    key.as_str(),
                    "id" | "channel_group_id"
                        | "is_adult"
                        | "is_radio"
                        | "user_level"
                        | "override"
                        | "stream_profile_id"
                        | "logo_id"
                        | "hidden_from_output"
                )
            }) {
                return Err(unsupported());
            }
            let override_intent = overrides.get(&id);
            let (override_group, override_radio, override_references) = match override_intent {
                None | Some(Value::Null) => (None, None, References::default()),
                Some(Value::Object(values))
                    if values.keys().all(|key| {
                        matches!(
                            key.as_str(),
                            "channel_group_id" | "is_radio" | "stream_profile_id" | "logo_id"
                        )
                    }) =>
                {
                    (
                        values.get("channel_group_id").map(group).transpose()?,
                        values
                            .get("is_radio")
                            .map(|value| boolean("is_radio", value))
                            .transpose()?,
                        References::parse(values)?,
                    )
                }
                _ => return Err(unsupported()),
            };
            Ok(Edit {
                id,
                group: row.get("channel_group_id").map(group).transpose()?,
                override_group,
                override_radio,
                override_references,
                clear_override: override_intent == Some(&Value::Null),
                hidden: row
                    .get("hidden_from_output")
                    .map(|value| {
                        value
                            .as_bool()
                            .ok_or_else(|| err("hidden_from_output", "Must be a valid boolean."))
                    })
                    .transpose()?,
                references: References::parse(row)?,
                radio: row
                    .get("is_radio")
                    .map(|value| boolean("is_radio", value))
                    .transpose()?,
                level: row.get("user_level").map(user_level).transpose()?,
                mature: row
                    .get("is_adult")
                    .map(|value| boolean("is_adult", value))
                    .transpose()?,
            })
        })
        .collect()
}

async fn lock_references(
    tx: &mut Transaction<'_, Postgres>,
    statement: &str,
    ids: impl Iterator<Item = i64>,
) -> Result<BTreeSet<i64>, StoreError> {
    let requested: Vec<_> = ids.collect::<BTreeSet<_>>().into_iter().collect();
    if requested.is_empty() {
        return Ok(BTreeSet::new());
    }
    Ok(sqlx::query_scalar(statement)
        .bind(&requested)
        .fetch_all(&mut **tx)
        .await?
        .into_iter()
        .collect())
}

fn reference_errors(
    group: Option<i64>,
    references: References,
    groups: &BTreeSet<i64>,
    profiles: &BTreeSet<i64>,
    logos: &BTreeSet<i64>,
) -> Map<String, Value> {
    [
        ("channel_group_id", group, groups),
        ("stream_profile_id", references.profile.id(), profiles),
        ("logo_id", references.logo.id(), logos),
    ]
    .into_iter()
    .filter_map(|(field, id, existing)| {
        id.filter(|id| !existing.contains(id)).map(|missing| {
            (
                field.to_owned(),
                json!([format!("Invalid pk \"{missing}\" - object does not exist.")]),
            )
        })
    })
    .collect()
}

struct LockedChannel {
    managed: bool,
    hidden: bool,
    has_raw_number: bool,
}

impl LockedChannel {
    fn can_change_compact_number(&self, hidden: bool) -> bool {
        self.managed && self.hidden != hidden && self.has_raw_number == hidden
    }
}

async fn validate_and_lock(
    tx: &mut Transaction<'_, Postgres>,
    edits: &[Edit],
    ids: &[i64],
) -> Result<HashMap<i64, LockedChannel>, StoreError> {
    // Group update/delete already starts with the group lock. Match
    // that order before taking the channel rows in canonical order.
    let groups = lock_references(
        tx,
        "SELECT id FROM dispatcharr_channels_channelgroup WHERE id = ANY($1) ORDER BY id FOR KEY SHARE",
        edits.iter().flat_map(|edit| [edit.group, edit.override_group].into_iter().flatten()),
    ).await?;
    let profiles = lock_references(
        tx,
        "SELECT id FROM core_streamprofile WHERE id = ANY($1) ORDER BY id FOR KEY SHARE",
        edits.iter().flat_map(|edit| {
            [
                edit.references.profile.id(),
                edit.override_references.profile.id(),
            ]
            .into_iter()
            .flatten()
        }),
    )
    .await?;
    let selected: HashMap<_, _> = sqlx::query_as::<_, (i64, bool, bool, bool)>(
        "SELECT id, auto_created, hidden_from_output, channel_number IS NOT NULL
         FROM dispatcharr_channels_channel
         WHERE id = ANY($1) ORDER BY id FOR UPDATE",
    )
    .bind(ids)
    .fetch_all(&mut **tx)
    .await?
    .into_iter()
    .map(|(id, managed, hidden, has_raw_number)| {
        (
            id,
            LockedChannel {
                managed,
                hidden,
                has_raw_number,
            },
        )
    })
    .collect();
    // Logo collectors clear channel and existing override links before
    // deleting logo rows. Lock the same children before the logo reference,
    // including overrides that have no corresponding raw logo link.
    let logo_ids: Vec<_> = edits
        .iter()
        .flat_map(|edit| {
            [
                edit.references.logo.id(),
                edit.override_references.logo.id(),
            ]
            .into_iter()
            .flatten()
        })
        .collect();
    if !logo_ids.is_empty() {
        let overlays: Vec<_> = edits
            .iter()
            .filter(|edit| edit.changes_override())
            .map(|edit| edit.id)
            .collect();
        if !overlays.is_empty() {
            sqlx::query(
                "SELECT channel_id FROM dispatcharr_channels_channeloverride
                 WHERE channel_id = ANY($1) ORDER BY channel_id FOR UPDATE",
            )
            .bind(overlays)
            .execute(&mut **tx)
            .await?;
        }
    }
    let logos = lock_references(
        tx,
        "SELECT id FROM dispatcharr_channels_logo WHERE id = ANY($1) ORDER BY id FOR KEY SHARE",
        logo_ids.into_iter(),
    )
    .await?;
    let mut errors = Vec::new();
    for edit in edits {
        let Some(channel) = selected.get(&edit.id) else {
            errors.push(json!({"channel_id":edit.id,"error":"Channel not found"}));
            continue;
        };
        if !channel.managed && edit.has_override_values() {
            return Err(unsupported());
        }
        let mut fields = reference_errors(edit.group, edit.references, &groups, &profiles, &logos);
        let nested = reference_errors(
            edit.override_group,
            edit.override_references,
            &groups,
            &profiles,
            &logos,
        );
        if !nested.is_empty() {
            fields.insert("override".into(), Value::Object(nested));
        }
        if !fields.is_empty() {
            errors.push(json!({"channel_id":edit.id,"errors":fields}));
        }
    }
    if errors.is_empty() {
        Ok(selected)
    } else {
        Err(StoreError::InvalidInput(
            json!({"updated_count":edits.len()-errors.len(),"errors":errors}),
        ))
    }
}

const CHANNEL_BATCH_SIZE: usize = 100;
const OVERRIDE_BATCH_SIZE: usize = 200;

async fn apply(
    auth: &AuthService,
    tx: &mut Transaction<'_, Postgres>,
    edits: &[Edit],
    selected: &HashMap<i64, LockedChannel>,
    owner: crate::compact_numbering::Owner,
) -> Result<(), StoreError> {
    let mut manual_groups = BTreeSet::new();
    let mut scalar_edits = Vec::new();
    let mut override_edits = Vec::new();
    let mut clear_ids = Vec::new();
    let mut unhide = Vec::new();
    let mut hide = Vec::new();
    for edit in edits {
        if let Some(hidden) = edit
            .hidden
            .filter(|hidden| selected[&edit.id].can_change_compact_number(*hidden))
        {
            if hidden {
                hide.push(edit.id);
            } else {
                unhide.push(edit.id);
            }
        }
        if edit.group.is_some()
            || edit.mature.is_some()
            || edit.level.is_some()
            || edit.radio.is_some()
            || edit.hidden.is_some()
            || edit.references.present()
        {
            scalar_edits.push(edit);
        }
        if !selected[&edit.id].managed {
            manual_groups.extend(edit.group);
        }
        if edit.clear_override {
            clear_ids.push(edit.id);
        } else if edit.has_override_values() {
            override_edits.push(edit);
        }
    }
    for chunk in scalar_edits.chunks(CHANNEL_BATCH_SIZE) {
        let ids: Vec<_> = chunk.iter().map(|edit| edit.id).collect();
        let groups: Vec<_> = chunk.iter().map(|edit| edit.group).collect();
        let mature: Vec<_> = chunk.iter().map(|edit| edit.mature).collect();
        let radio: Vec<_> = chunk.iter().map(|edit| edit.radio).collect();
        let levels: Vec<_> = chunk.iter().map(|edit| edit.level).collect();
        let hidden: Vec<_> = chunk.iter().map(|edit| edit.hidden).collect();
        let refs: ReferenceColumns = chunk.iter().map(|edit| edit.references).collect();
        // Stock bulk_update leaves channel row clocks unchanged.
        sqlx::query(
            "UPDATE dispatcharr_channels_channel AS channel
             SET channel_group_id = COALESCE(change.group_id, channel.channel_group_id),
                 is_adult = COALESCE(change.mature, channel.is_adult),
                 is_radio = COALESCE(change.radio, channel.is_radio),
                 user_level = COALESCE(change.level, channel.user_level),
                 stream_profile_id = CASE WHEN change.has_profile THEN change.profile ELSE channel.stream_profile_id END,
                 logo_id = CASE WHEN change.has_logo THEN change.logo ELSE channel.logo_id END,
                 hidden_from_output = COALESCE(change.hidden, channel.hidden_from_output)
             FROM unnest($1::bigint[], $2::bigint[], $3::boolean[], $4::boolean[], $5::integer[],
                         $6::boolean[], $7::bigint[], $8::boolean[], $9::bigint[], $10::boolean[])
                 AS change(id, group_id, mature, radio, level, has_profile, profile, has_logo, logo, hidden)
             WHERE channel.id = change.id",
        )
        .bind(ids)
        .bind(groups)
        .bind(mature)
        .bind(radio)
        .bind(levels)
        .bind(refs.profiles_present)
        .bind(refs.profiles)
        .bind(refs.logos_present)
        .bind(refs.logos)
        .bind(hidden)
        .execute(&mut **tx)
        .await?;
    }
    for group in manual_groups {
        mark_group(tx, &json!(group)).await?;
    }
    // The stock bulk action assigns unhide numbers while all old hide
    // numbers are still reserved. Overrides follow both number phases.
    crate::compact_unhide::bulk_visibility(auth, tx, &unhide, &hide, owner).await?;
    // Source applies whole-row clear to every selected ID, including
    // manual channels. A missing overlay is an ordinary no-op.
    for chunk in clear_ids.chunks(OVERRIDE_BATCH_SIZE) {
        sqlx::query("DELETE FROM dispatcharr_channels_channeloverride WHERE channel_id = ANY($1)")
            .bind(chunk)
            .execute(&mut **tx)
            .await?;
    }
    for chunk in override_edits.chunks(OVERRIDE_BATCH_SIZE) {
        let ids: Vec<_> = chunk.iter().map(|edit| edit.id).collect();
        let groups: Vec<_> = chunk.iter().map(|edit| edit.override_group).collect();
        let radio: Vec<_> = chunk.iter().map(|edit| edit.override_radio).collect();
        let refs: ReferenceColumns = chunk.iter().map(|edit| edit.override_references).collect();
        // A data-modifying CTE separates existing rows from newly inserted
        // overlays without spending an identity or updating clocks on a hit.
        sqlx::query(
            "WITH incoming AS (
                 SELECT * FROM unnest($1::bigint[], $2::bigint[], $3::boolean[], $4::boolean[],
                                      $5::bigint[], $6::boolean[], $7::bigint[])
                     AS change(id, group_id, radio, has_profile, profile, has_logo, logo)
             ), existing AS (
                 UPDATE dispatcharr_channels_channeloverride AS overlay
                 SET channel_group_id = COALESCE(incoming.group_id, overlay.channel_group_id),
                     is_radio = COALESCE(incoming.radio, overlay.is_radio),
                     stream_profile_id = CASE WHEN incoming.has_profile THEN incoming.profile ELSE overlay.stream_profile_id END,
                     logo_id = CASE WHEN incoming.has_logo THEN incoming.logo ELSE overlay.logo_id END
                 FROM incoming WHERE overlay.channel_id = incoming.id
                 RETURNING overlay.channel_id
             )
             INSERT INTO dispatcharr_channels_channeloverride
                 (channel_id, channel_group_id, is_radio, stream_profile_id, logo_id, created_at, updated_at)
             SELECT incoming.id, incoming.group_id, incoming.radio, incoming.profile, incoming.logo,
                    clock_timestamp(), clock_timestamp()
             FROM incoming
             WHERE NOT EXISTS (SELECT 1 FROM existing WHERE existing.channel_id = incoming.id)",
        )
        .bind(ids)
        .bind(groups)
        .bind(radio)
        .bind(refs.profiles_present)
        .bind(refs.profiles)
        .bind(refs.logos_present)
        .bind(refs.logos)
        .execute(&mut **tx)
        .await?;
        let may_clear: Vec<_> = chunk
            .iter()
            .filter(|edit| edit.override_references.clears())
            .map(|edit| edit.id)
            .collect();
        if !may_clear.is_empty() {
            sqlx::query(
                "DELETE FROM dispatcharr_channels_channeloverride
                 WHERE channel_id = ANY($1) AND num_nonnulls(
                    name, channel_number, channel_group_id, logo_id, tvg_id, tvc_guide_stationid,
                    epg_data_id, stream_profile_id, is_radio
                 ) = 0",
            )
            .bind(may_clear)
            .execute(&mut **tx)
            .await?;
        }
    }
    Ok(())
}

fn deadlock(error: &StoreError) -> bool {
    matches!(error, StoreError::Database(sqlx::Error::Database(error))
        if error.code().as_deref() == Some("40P01"))
}

async fn write(
    auth: &AuthService,
    edits: &[Edit],
    ids: &[i64],
    include: bool,
    base: &url::Url,
    owner: crate::compact_numbering::Owner,
) -> Result<Vec<Value>, StoreError> {
    for attempt in 0..3 {
        let mut tx = auth.store.pool().begin().await?;
        let result = update_and_read(auth, &mut tx, edits, ids, include, base, owner.clone()).await;
        match result {
            Ok(rows) => {
                // A commit error can have an uncertain outcome. Never retry it.
                tx.commit().await?;
                return Ok(rows);
            }
            Err(error) if attempt < 2 && deadlock(&error) => tx.rollback().await?,
            Err(error) => return Err(error),
        }
    }
    unreachable!("final transaction attempt returns its result")
}

async fn update_and_read(
    auth: &AuthService,
    tx: &mut Transaction<'_, Postgres>,
    edits: &[Edit],
    ids: &[i64],
    include: bool,
    base: &url::Url,
    owner: crate::compact_numbering::Owner,
) -> Result<Vec<Value>, StoreError> {
    let selected = validate_and_lock(tx, edits, ids).await?;
    apply(auth, tx, edits, &selected, owner).await?;
    let fetched = QueryBuilder::new(channels::SELECT)
        .push(" AND c.id = ANY(")
        .push_bind(ids)
        .push(")")
        .build()
        .fetch_all(&mut **tx)
        .await?;
    let mut rows = HashMap::with_capacity(fetched.len());
    for row in fetched {
        let serialized = channels::serialize(row, include, true, base)?;
        let id = serialized["id"]
            .as_i64()
            .ok_or(StoreError::UnhandledRestFault)?;
        rows.insert(id, serialized);
    }
    edits
        .iter()
        .map(|edit| {
            let mut row = rows
                .remove(&edit.id)
                .ok_or(StoreError::UnhandledRestFault)?;
            if edit.hidden == Some(false)
                && selected[&edit.id].can_change_compact_number(false)
                && !row["channel_number"].is_null()
            {
                project_unhide_response(&mut row);
            }
            Ok(row)
        })
        .collect()
}

fn project_unhide_response(row: &mut Value) {
    // Original assigns through fresh queryset objects and serializes its
    // previously fetched Channel objects. GET reads the committed raw slot.
    row["channel_number"] = Value::Null;
    row["effective_channel_number"] = row["override"]["channel_number"].clone();
}

pub(crate) async fn handle(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    request: Request,
) -> Result<Response, StoreError> {
    let permit = auth.task_sql_writer()?;
    let auth = auth.with_request_writer_permit(permit.clone())?;
    let owner = auth.core_publication_owner(permit);
    tokio::spawn(async move {
        let headers = request.headers().clone();
        auth.account_user(peer, headers.clone(), true).await?;
        let base = channels::base_url(&headers, &uri)?;
        let include = params.get("include_streams").is_some_and(|value| value == "true");
        let edits = edits(channels::mutation_value(request).await?)?;
        let ids: Vec<i64> = edits.iter().map(|edit| edit.id).collect();
        let result = write(&auth, &edits, &ids, include, &base, owner).await?;
        Ok((
            [(header::ALLOW, "PATCH, OPTIONS"), (header::VARY, "Accept, origin")],
            Json(json!({"message":format!("Successfully updated {} channels", result.len()),"channels":result})),
        ).into_response())
    })
    .await
    .map_err(|_| StoreError::UnhandledRestFault)?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mixed_react_group_and_mature_fields_keep_input_order() {
        assert_eq!(
            edits(json!([
                {"id":21,"override":{"channel_group_id":3},"is_adult":true},
                {"id":14,"channel_group_id":3,"is_adult":true}
            ]))
            .unwrap(),
            vec![
                Edit {
                    id: 21,
                    group: None,
                    override_group: Some(3),
                    mature: Some(true),
                    ..Edit::default()
                },
                Edit {
                    id: 14,
                    group: Some(3),
                    override_group: None,
                    mature: Some(true),
                    ..Edit::default()
                },
            ]
        );
    }

    #[test]
    fn duplicate_channel_uses_last_payload_at_its_first_position() {
        let result = edits(json!([
            {"id":21,"is_adult":true},
            {"id":14,"channel_group_id":3},
            {"id":21,"is_adult":false}
        ]))
        .unwrap();
        assert_eq!(result[0].id, 21);
        assert_eq!(result[0].mature, Some(false));
        assert_eq!(result[1].id, 14);
    }

    #[test]
    fn duplicate_channel_keeps_last_explicit_override_independently_of_scalars() {
        let result = edits(json!([
            {"id":21,"override":{"channel_group_id":3},"is_adult":true},
            {"id":14,"override":{"channel_group_id":3}},
            {"id":21,"override":{"channel_group_id":4}},
            {"id":14,"override":{}},
            {"id":21,"is_adult":false},
            {"id":14,"is_adult":true}
        ]))
        .unwrap();
        assert_eq!(
            result[0],
            Edit {
                id: 21,
                group: None,
                override_group: Some(4),
                mature: Some(false),
                ..Edit::default()
            }
        );
        assert_eq!(
            result[1],
            Edit {
                id: 14,
                group: None,
                override_group: None,
                mature: Some(true),
                ..Edit::default()
            }
        );
    }

    #[test]
    fn standard_access_and_false_radio_resolve_into_distinct_raw_and_override_writes() {
        let result = edits(json!([
            {"id":21,"user_level":"1","override":{"is_radio":false,"channel_group_id":3}},
            {"id":14,"user_level":"1","is_radio":false}
        ]))
        .unwrap();
        assert_eq!(
            result,
            vec![
                Edit {
                    id: 21,
                    level: Some(1),
                    override_group: Some(3),
                    override_radio: Some(false),
                    ..Edit::default()
                },
                Edit {
                    id: 14,
                    level: Some(1),
                    radio: Some(false),
                    ..Edit::default()
                },
            ]
        );
    }

    #[test]
    fn profile_and_logo_edits_keep_absent_set_and_clear_distinct_at_both_write_levels() {
        let rows = edits(json!([
            {"id":21,"override":{"stream_profile_id":"5","logo_id":null}},
            {"id":14,"stream_profile_id":null,"logo_id":6},
            {"id":12,"logo_id":null},
        ]))
        .unwrap();
        assert_eq!(
            rows[0].override_references,
            References {
                profile: Reference::Set(5),
                logo: Reference::Clear
            }
        );
        assert_eq!(
            rows[1].references,
            References {
                profile: Reference::Clear,
                logo: Reference::Set(6)
            }
        );
        assert_eq!(
            rows[2].references,
            References {
                profile: Reference::Absent,
                logo: Reference::Clear
            }
        );
        let cols: ReferenceColumns = rows.iter().map(|row| row.references).collect();
        assert_eq!(cols.profiles_present, [false, true, false]);
        assert_eq!(cols.profiles, [None, None, None]);
        assert_eq!(cols.logos_present, [false, true, true]);
        assert_eq!(cols.logos, [None, Some(6), None]);
    }

    #[test]
    fn every_overlay_write_participates_in_the_owned_logo_lock_order() {
        let rows = edits(json!([
            {"id":1,"logo_id":5},
            {"id":2,"override":{"logo_id":5}},
            {"id":3,"override":{"stream_profile_id":null}},
            {"id":4,"override":{"is_radio":false}},
            {"id":5,"override":{"channel_group_id":9}},
            {"id":6,"override":{}}
        ]))
        .unwrap();
        assert_eq!(
            rows.iter()
                .filter(|edit| edit.changes_override())
                .map(|edit| edit.id)
                .collect::<Vec<_>>(),
            [2, 3, 4, 5]
        );
    }

    #[test]
    fn whole_overlay_clear_is_not_an_upsert_and_stays_in_logo_child_lock_order() {
        let rows = edits(json!([
            {"id":1,"override":null},
            {"id":2,"logo_id":5},
            {"id":3,"override":{"logo_id":5}},
            {"id":4,"override":{}}
        ]))
        .unwrap();
        assert!(rows[0].clear_override && rows[0].changes_override());
        assert!(!rows[0].has_override_values());
        assert!(!rows[1].changes_override());
        assert!(rows[2].has_override_values() && rows[2].changes_override());
        assert!(!rows[3].changes_override());
    }

    #[test]
    fn repeated_rows_preserve_last_explicit_whole_overlay_intent() {
        let rows = edits(json!([
            {"id":1,"override":null},
            {"id":2,"override":{"logo_id":5}},
            {"id":3,"override":null},
            {"id":4,"override":null},
            {"id":1,"is_adult":true},
            {"id":2,"override":null},
            {"id":3,"override":{}},
            {"id":4,"override":{"logo_id":7}},
        ]))
        .unwrap();
        assert_eq!(
            rows.iter().map(|row| row.id).collect::<Vec<_>>(),
            [1, 2, 3, 4]
        );
        assert_eq!(
            rows.iter()
                .map(|row| row.clear_override)
                .collect::<Vec<_>>(),
            [true, true, false, false]
        );
        assert_eq!(rows[0].mature, Some(true));
        assert!(!rows[1].has_override_values());
        assert!(!rows[2].changes_override());
        assert_eq!(rows[3].override_references.logo, Reference::Set(7));
    }

    #[test]
    fn bulk_hidden_output_accepts_only_selected_raw_booleans() {
        let rows = edits(json!([
            {"id": 21, "hidden_from_output": true},
            {"id": 14, "hidden_from_output": false},
            {"id": 18}
        ]))
        .unwrap();
        assert_eq!(
            rows.iter().map(|edit| edit.hidden).collect::<Vec<_>>(),
            [Some(true), Some(false), None]
        );
        assert!(matches!(
            edits(json!([{"id": 1, "hidden_from_output": null}])),
            Err(StoreError::InvalidInput(_))
        ));
        assert!(matches!(
            edits(json!([{"id": 1, "override": {"hidden_from_output": true}}])),
            Err(StoreError::Unsupported(_))
        ));
        let mixed = edits(json!([
            {"id": 1, "hidden_from_output": true, "override": {"logo_id": 4}},
            {"id": 2, "hidden_from_output": false, "channel_group_id": 5, "is_radio": false}
        ]))
        .unwrap();
        assert_eq!(mixed[0].hidden, Some(true));
        assert_eq!(mixed[0].override_references.logo, Reference::Set(4));
        assert_eq!(mixed[1].hidden, Some(false));
        assert_eq!(mixed[1].group, Some(5));
        assert_eq!(mixed[1].radio, Some(false));
    }

    #[test]
    fn compact_unhide_patch_keeps_preassignment_raw_and_current_overlay() {
        for pin in [Value::Null, json!(0.0), json!(3003.0)] {
            let mut row = json!({
                "channel_number": 3002.0,
                "effective_channel_number": 3002.0,
                "hidden_from_output": false,
                "override": {"channel_number": pin},
            });
            project_unhide_response(&mut row);
            assert_eq!(row["channel_number"], Value::Null);
            assert_eq!(row["effective_channel_number"], pin);
            assert_eq!(row["hidden_from_output"], json!(false));
        }
    }

    #[test]
    fn only_actual_eligible_managed_transitions_enter_compact_numbering() {
        for managed in [false, true] {
            for hidden in [false, true] {
                for has_raw_number in [false, true] {
                    let row = LockedChannel {
                        managed,
                        hidden,
                        has_raw_number,
                    };
                    assert!(!row.can_change_compact_number(hidden));
                    assert_eq!(
                        row.can_change_compact_number(!hidden),
                        managed && ((!hidden && has_raw_number) || (hidden && !has_raw_number))
                    );
                }
            }
        }
    }

    #[test]
    fn unrelated_mutations_cannot_be_silently_accepted() {
        for request in [
            json!([{"id":1,"streams":[4]}]),
            json!([{"id":1,"override":{"channel_group_id":3,"epg_data_id":7}}]),
        ] {
            assert!(matches!(edits(request), Err(StoreError::Unsupported(_))));
        }
    }
}
