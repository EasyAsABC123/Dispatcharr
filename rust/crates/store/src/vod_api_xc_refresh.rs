//! Synchronous XC calls reuse the existing HTTP provider refresh owner.
//! Original Dispatcharr authors retain application and algorithm credit.
use super::{
    vod_api_owned_read::{ReadError, ReadRuntime},
    vod_api_provider_refresh_actor::{self as refresh, Clock},
    vod_api_provider_refresh_sql as sql,
};
use crate::{AuthService, StoreError};
use std::{mem::size_of, sync::Arc};

#[derive(Clone)]
pub struct XcVodRefresh {
    runtime: ReadRuntime,
    auth: AuthService,
}

#[derive(Clone, Copy)]
pub enum XcVodRefreshTarget {
    Movie,
    Series,
}

impl XcVodRefresh {
    pub(super) fn new(auth: AuthService, runtime: ReadRuntime) -> Self {
        Self { auth, runtime }
    }

    pub async fn run(
        &self,
        family: XcVodRefreshTarget,
        content: i64,
        relation: i64,
        account: i64,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<(), StoreError> {
        let auth = self.auth.clone();
        self.runtime
            .run(size_of::<[i64; 3]>(), move |work| async move {
                let _owner = owner;
                let mut document = work.snapshot_document();
                serde_json::to_writer(&mut document, &[content, relation, account])
                    .map_err(|_| ReadError::Capacity)?;
                let input = work.freeze_snapshot(document.finish())?;
                let input = work.admit_refresh(input).await?;
                let [content, relation, account] =
                    serde_json::from_slice(&work.consume_snapshot(input)?)
                        .map_err(|_| ReadError::WorkerFailed)?;
                let clock = Clock { fixed: None };
                match family {
                    XcVodRefreshTarget::Movie => {
                        refresh::selected_movie(&work, &auth, relation, content, account, clock)
                            .await?;
                    }
                    XcVodRefreshTarget::Series => {
                        let mut connection = work.connection(auth.store.pool()).await?;
                        let snapshot = sql::series_view(&work, &mut connection, relation).await;
                        connection.finish().await?;
                        let Some(snapshot) = snapshot? else {
                            return Err(ReadError::Unsupported("XC Series relation changed"));
                        };
                        if snapshot["series"]["id"].as_i64() != Some(content)
                            || snapshot["account"]["id"].as_i64() != Some(account)
                            || snapshot["account"]["account_type"].as_str() != Some("XC")
                            || snapshot["account"]["is_active"].as_bool() != Some(true)
                        {
                            return Err(ReadError::Unsupported("XC Series relation changed"));
                        }
                        refresh::series(&work, &auth, &snapshot, clock).await?;
                    }
                }
                Ok(Vec::new())
            })
            .await
            .map(|_| ())
            .map_err(|error| match error {
                ReadError::Source(error) => error,
                ReadError::Unsupported(reason) => StoreError::Unsupported(reason),
                _ => StoreError::StatusBody(
                    503,
                    serde_json::json!({"detail":"VOD read capacity unavailable."}),
                ),
            })
    }
}
