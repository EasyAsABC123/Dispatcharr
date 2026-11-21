//! PostgreSQL write transaction for the existing Channel Groups routes.
//! Original Dispatcharr authors and contributors retain all application credit.
use super::{missing, unique, write_error};
use crate::{channel_writes::mark_group, StoreError};
use serde_json::json;
use sqlx::{postgres::PgRow, PgPool, Postgres, Row, Transaction};

pub(super) struct GroupWrite {
    transaction: Transaction<'static, Postgres>,
}

pub(super) struct LockedGroup(PgRow);

impl LockedGroup {
    pub(super) fn has_provider(&self) -> Result<bool, StoreError> {
        Ok(self.0.try_get("has_provider")?)
    }

    pub(super) fn is_custom(&self) -> Result<bool, StoreError> {
        Ok(self.0.try_get("is_custom")?)
    }

    pub(super) fn name(&self) -> Result<String, StoreError> {
        Ok(self.0.try_get("name")?)
    }
}

impl GroupWrite {
    pub(super) async fn begin(pool: &PgPool) -> Result<Self, StoreError> {
        Ok(Self {
            transaction: pool.begin().await?,
        })
    }

    pub(super) async fn commit(self) -> Result<(), StoreError> {
        self.transaction.commit().await.map_err(Into::into)
    }

    pub(super) async fn check_unique(
        &mut self,
        name: &str,
        except: Option<i64>,
    ) -> Result<(), StoreError> {
        let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM dispatcharr_channels_channelgroup WHERE name=$1 AND ($2::bigint IS NULL OR id<>$2))").bind(name).bind(except).fetch_one(&mut *self.transaction).await?;
        if exists {
            Err(unique())
        } else {
            Ok(())
        }
    }

    pub(super) async fn lock_group(&mut self, id: i64) -> Result<LockedGroup, StoreError> {
        sqlx::query("SELECT g.name,EXISTS(SELECT 1 FROM dispatcharr_channels_channelgroupm3uaccount r JOIN m3u_m3uaccount a ON a.id=r.m3u_account_id WHERE r.channel_group_id=g.id AND NOT a.locked) AS has_provider,EXISTS(SELECT 1 FROM dispatcharr_channels_channelgroupm3uaccount r JOIN m3u_m3uaccount a ON a.id=r.m3u_account_id WHERE r.channel_group_id=g.id AND a.locked) AS is_custom FROM dispatcharr_channels_channelgroup g WHERE g.id=$1 FOR UPDATE OF g")
            .bind(id).fetch_optional(&mut *self.transaction).await?.map(LockedGroup).ok_or_else(missing)
    }

    pub(super) async fn insert(&mut self, name: String) -> Result<i64, StoreError> {
        sqlx::query_scalar(
            "INSERT INTO dispatcharr_channels_channelgroup(name) VALUES($1) RETURNING id",
        )
        .bind(name)
        .fetch_one(&mut *self.transaction)
        .await
        .map_err(write_error)
    }

    pub(super) async fn mark_custom(&mut self, id: i64) -> Result<(), StoreError> {
        mark_group(&mut self.transaction, &json!(id)).await
    }

    pub(super) async fn rename(&mut self, id: i64, name: String) -> Result<(), StoreError> {
        sqlx::query("UPDATE dispatcharr_channels_channelgroup SET name=$1 WHERE id=$2")
            .bind(name)
            .bind(id)
            .execute(&mut *self.transaction)
            .await
            .map_err(write_error)?;
        Ok(())
    }

    pub(super) async fn move_manual_channels(
        &mut self,
        id: i64,
        new: i64,
    ) -> Result<(), StoreError> {
        sqlx::query("UPDATE dispatcharr_channels_channel SET channel_group_id=$1 WHERE channel_group_id=$2 AND NOT auto_created").bind(new).bind(id).execute(&mut *self.transaction).await?;
        Ok(())
    }

    pub(super) async fn remove_custom_marker(&mut self, id: i64) -> Result<(), StoreError> {
        sqlx::query("DELETE FROM dispatcharr_channels_channelgroupm3uaccount r USING m3u_m3uaccount a WHERE r.m3u_account_id=a.id AND r.channel_group_id=$1 AND a.locked").bind(id).execute(&mut *self.transaction).await?;
        Ok(())
    }

    pub(super) async fn has_effective_channels(&mut self, id: i64) -> Result<bool, StoreError> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM dispatcharr_channels_channel c LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id WHERE COALESCE(o.channel_group_id,c.channel_group_id)=$1)").bind(id).fetch_one(&mut *self.transaction).await?)
    }
    pub(super) async fn remove(&mut self, id: i64) -> Result<(), StoreError> {
        for query in [
            "UPDATE dispatcharr_channels_channel SET channel_group_id=NULL WHERE channel_group_id=$1",
            "UPDATE dispatcharr_channels_channeloverride SET channel_group_id=NULL WHERE channel_group_id=$1",
            "UPDATE dispatcharr_channels_stream SET channel_group_id=NULL WHERE channel_group_id=$1",
            "DELETE FROM dispatcharr_channels_channelgroupm3uaccount WHERE channel_group_id=$1",
            "DELETE FROM dispatcharr_channels_channelgroup WHERE id=$1",
        ] {
            sqlx::query(query).bind(id).execute(&mut *self.transaction).await?;
        }
        Ok(())
    }

    pub(super) async fn remove_unused(&mut self, include: bool) -> Result<Vec<String>, StoreError> {
        let rows = sqlx::query("SELECT g.id,g.name FROM dispatcharr_channels_channelgroup g WHERE NOT EXISTS(SELECT 1 FROM dispatcharr_channels_channel c LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id WHERE COALESCE(o.channel_group_id,c.channel_group_id)=g.id) AND NOT EXISTS(SELECT 1 FROM dispatcharr_channels_channelgroupm3uaccount r JOIN m3u_m3uaccount a ON a.id=r.m3u_account_id WHERE r.channel_group_id=g.id AND NOT a.locked) AND ($1 OR NOT EXISTS(SELECT 1 FROM dispatcharr_channels_channelgroupm3uaccount r JOIN m3u_m3uaccount a ON a.id=r.m3u_account_id WHERE r.channel_group_id=g.id AND a.locked)) FOR UPDATE OF g")
            .bind(include).fetch_all(&mut *self.transaction).await?;
        let mut names = Vec::new();
        for row in rows {
            let id: i64 = row.try_get("id")?;
            names.push(row.try_get::<String, _>("name")?);
            self.remove(id).await?;
        }
        Ok(names)
    }
}
