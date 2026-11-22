//! Direct, read-only ChannelGroup changelist for existing Django database sessions.
//! Original Dispatcharr and Django authors and contributors retain full credit.
use super::admin_session_read::{session_cookie, DatabaseSessions};
use super::AuthService;
use crate::StoreError;
use axum::{
    extract::{RawQuery, Request, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::get,
    Router,
};
use chrono::{DateTime, Utc};
use sqlx::PgConnection;
use std::{fmt::Write, net::SocketAddr, sync::Arc};

const GROUPS: &str = "/admin/dispatcharr_channels/channelgroup/";
const GROUPS_LOGIN: &str = "/admin/login/?next=/admin/dispatcharr_channels/channelgroup/";
const MODEL_GRANT: &str = "
    SELECT EXISTS (
      SELECT 1
      FROM auth_permission AS permission
      JOIN django_content_type AS model ON model.id=permission.content_type_id
      WHERE model.app_label='dispatcharr_channels' AND model.model='channelgroup'
        AND permission.codename IN ('view_channelgroup', 'change_channelgroup')
        AND (
          EXISTS (
            SELECT 1 FROM accounts_user_user_permissions AS member
            WHERE member.user_id=$1 AND member.permission_id=permission.id
          ) OR EXISTS (
            SELECT 1 FROM accounts_user_groups AS member
            JOIN auth_group_permissions AS grant_row ON grant_row.group_id=member.group_id
            WHERE member.user_id=$1 AND grant_row.permission_id=permission.id
          )
        )
    )";

type Group = (i64, String);

struct DirectAdmin {
    auth: AuthService,
    host: String,
}

enum GroupList {
    Anonymous,
    Forbidden,
    UnsupportedQuery,
    Ready(Vec<Group>),
}

impl AuthService {
    /// This router is only for an independently bound loopback listener.
    pub fn direct_channel_group_admin_router(self, address: SocketAddr) -> Option<Router> {
        if !address.ip().is_loopback() || address.port() == 0 {
            return None;
        }
        let state = Arc::new(DirectAdmin {
            auth: self,
            host: address.to_string(),
        });
        Some(
            Router::new()
                .route(GROUPS, get(changelist))
                .route_layer(middleware::from_fn_with_state(state.clone(), direct_host))
                .with_state(state),
        )
    }

    async fn read_channel_group_admin(
        &self,
        headers: &HeaderMap,
        query_supported: bool,
    ) -> Result<GroupList, StoreError> {
        let Some(key) = session_cookie(headers) else {
            return Ok(GroupList::Anonymous);
        };
        let mut tx = self.store.pool().begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .execute(&mut *tx)
            .await?;
        let outcome = self
            .channel_group_admin_snapshot(&mut tx, key, query_supported)
            .await?;
        tx.commit().await?;
        Ok(outcome)
    }

    async fn channel_group_admin_snapshot(
        &self,
        connection: &mut PgConnection,
        key: &str,
        query_supported: bool,
    ) -> Result<GroupList, StoreError> {
        let row: Option<(String, DateTime<Utc>, DateTime<Utc>)> = sqlx::query_as(
            "SELECT session_data, expire_date, clock_timestamp()
             FROM django_session WHERE session_key=$1 AND expire_date>clock_timestamp()",
        )
        .bind(key)
        .fetch_optional(&mut *connection)
        .await?;
        let Some((data, expiry, now)) = row else {
            return Ok(GroupList::Anonymous);
        };
        let sessions = DatabaseSessions::new(self.config.signing_key.as_bytes())
            .map_err(|_| StoreError::Unsupported("Dedicated administrator session unavailable"))?;
        let Ok(claims) = sessions.open(&data, expiry, now) else {
            return Ok(GroupList::Anonymous);
        };
        let actor: Option<(String, bool)> = sqlx::query_as(
            "SELECT password, is_superuser FROM accounts_user
             WHERE id=$1 AND is_active AND is_staff",
        )
        .bind(claims.user_id)
        .fetch_optional(&mut *connection)
        .await?;
        let Some((password, superuser)) = actor else {
            return Ok(GroupList::Anonymous);
        };
        if !sessions.password_matches(&claims, &password) {
            return Ok(GroupList::Anonymous);
        }
        let allowed = superuser
            || sqlx::query_scalar::<_, bool>(MODEL_GRANT)
                .bind(claims.user_id)
                .fetch_one(&mut *connection)
                .await?;
        if !allowed {
            return Ok(GroupList::Forbidden);
        }
        if !query_supported {
            return Ok(GroupList::UnsupportedQuery);
        }
        let rows = sqlx::query_as(
            "SELECT id::bigint, name
             FROM dispatcharr_channels_channelgroup ORDER BY id DESC LIMIT 100",
        )
        .fetch_all(connection)
        .await?;
        Ok(GroupList::Ready(rows))
    }
}

async fn direct_host(
    State(admin): State<Arc<DirectAdmin>>,
    request: Request,
    next: Next,
) -> Response {
    let mut hosts = request.headers().get_all(header::HOST).iter();
    if hosts.next().and_then(|value| value.to_str().ok()) != Some(admin.host.as_str())
        || hosts.next().is_some()
    {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let mut response = next.run(request).await;
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("max-age=0, no-cache, no-store, must-revalidate, private"),
    );
    response
        .headers_mut()
        .insert(header::VARY, HeaderValue::from_static("Cookie"));
    response
}

async fn changelist(
    State(admin): State<Arc<DirectAdmin>>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
) -> Response {
    let supported = query.as_deref().is_none_or(str::is_empty);
    match admin
        .auth
        .read_channel_group_admin(&headers, supported)
        .await
    {
        Ok(GroupList::Anonymous) => (
            StatusCode::FOUND,
            [
                (header::LOCATION, GROUPS_LOGIN),
                (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            ],
        )
            .into_response(),
        Ok(GroupList::Forbidden) => StatusCode::FORBIDDEN.into_response(),
        Ok(GroupList::UnsupportedQuery) => StatusCode::NOT_IMPLEMENTED.into_response(),
        Ok(GroupList::Ready(groups)) => Html(render_groups(groups)).into_response(),
        Err(_) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

fn render_groups(groups: Vec<Group>) -> String {
    let mut page = String::from(
        "<!doctype html><html lang=\"en\"><title>Select channel group to change</title>\
         <h1>Select channel group to change</h1><table id=\"result_list\">\
         <thead><tr><th>ID</th><th>Name</th></tr></thead><tbody>",
    );
    for (id, name) in groups {
        write!(page, "<tr><th>{id}</th><td>").expect("write to string");
        for ch in name.chars() {
            let entity = match ch {
                '&' => "&amp;",
                '<' => "&lt;",
                '>' => "&gt;",
                '"' => "&quot;",
                '\'' => "&#x27;",
                _ => {
                    page.push(ch);
                    continue;
                }
            };
            page.push_str(entity);
        }
        page.push_str("</td></tr>");
    }
    page.push_str("</tbody></table></html>");
    page
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_actual_ids_and_escapes_display_names() {
        let page = render_groups(vec![(31, String::from("Ordinary & <owned>"))]);
        assert!(page.contains("<tr><th>31</th><td>Ordinary &amp; &lt;owned&gt;</td></tr>"));
    }
}
