//! UNREGISTERED/UNCOMPILED ordinary VOD read peer. No router, no app acceptance.
//! Original Dispatcharr authors retain all application/algorithm credit.
//! Raw DRF filters/ordering/search normalization, category list integration,
//! unified rawSQL and provider/action reads remain explicit separate gates.
use super::{vod_api_dto::LogoContext,vod_api_owned_read::{ReadError,ReadLimits,ReadRuntime},
    vod_api_pagination::{ordinary,page_url},vod_api_projection::visibility,
    vod_api_sql::{Resource,project_fresh},vod_api_query::{self,QueryError}};
use axum::http::HeaderMap;
use bytes::Bytes;
use dispatcharr_contracts::BoxFuture;
use serde_json::json;
use sqlx::{PgPool,Row};
use std::{io::Write,net::SocketAddr,sync::Arc,time::Duration};

/// Deliberate integration seam, not a fake authenticated-user implementation.
/// Before wiring, shared AuthService must expose a retained-scope adapter so
/// all physical authentication SQL/Redis/cache/publication work carries guard.
/// Simple `account_user(...).await` alone is insufficient if an internal
/// logical cache timeout returns before its owned Redis reply completes.
pub(crate) trait RetainedUiAuthorizer:Send+Sync {
    fn authorize<'a>(&'a self,peer:SocketAddr,headers:HeaderMap,guard:Arc<dyn Send+Sync>)
        ->BoxFuture<'a,Result<crate::User,crate::StoreError>>;
}
pub(crate) struct ReadRequest {
    pub peer:SocketAddr,pub headers:HeaderMap,pub resource:Resource,pub detail:Option<i64>,
    pub query:Vec<(String,String)>,pub origin:String,pub path:String,
}
type PageHook=Arc<dyn Fn()->BoxFuture<'static,Result<(),ReadError>>+Send+Sync>;
#[derive(Clone)]
pub(crate) struct SqlReads {pool:PgPool,auth:Arc<dyn RetainedUiAuthorizer>,runtime:ReadRuntime,
    after_page:Option<PageHook>}
fn missing(resource:Resource)->ReadError {
    let name=match resource {Resource::Movie=>"Movie",Resource::Series=>"Series",Resource::Episode=>"Episode"};
    ReadError::Source(crate::StoreError::StatusBody(404,json!({"detail":format!("No {name} matches the given query.")})))
}
fn invalid_page()->ReadError {ReadError::Source(crate::StoreError::StatusBody(404,json!({"detail":"Invalid page."})))}
fn output_error(_:impl std::fmt::Display)->ReadError {ReadError::Capacity}
impl SqlReads {
    pub fn new(pool:PgPool,auth:Arc<dyn RetainedUiAuthorizer>,limits:ReadLimits)->Result<Self,ReadError> {
        Ok(Self {pool,auth,runtime:ReadRuntime::new(limits)?,after_page:None})
    }
    pub fn with_page_hook(mut self,hook:PageHook)->Self {self.after_page=Some(hook);self}
    /// Ordinary Movie/Series/Episode component input, not a routed HTTP API.
    /// Filter normalization runs after authentication/permission/visibility,
    /// including for disabled-access empty querysets, before count/pagination.
    /// Unsupported grammar is explicit and must pass Source2 before expansion.
    pub async fn read(&self,request:ReadRequest)->Result<Bytes,ReadError> {
        let state=self.clone();
        let input=request.input_bytes()?;
        self.runtime.run(input,move |work|async move {
            work.check()?;
            let user=state.auth.authorize(request.peer,request.headers,work.physical_guard()).await?;
            work.check()?;
            if user.user_level<1{return Err(crate::StoreError::PermissionDenied.into());}
            let visible=visibility(&user.custom_properties,user.user_level)
                .map_err(|_|ReadError::Unsupported("Nonmapping VOD properties fault contract is not captured"))?;
            let selection=vod_api_query::ordinary(request.resource,request.detail,&request.query).map_err(|error|match error {
                QueryError::Source400(body)=>ReadError::Source(crate::StoreError::StatusBody(400,body)),
                QueryError::Capacity=>ReadError::Capacity,QueryError::Unsupported(message)=>ReadError::Unsupported(message),
            })?;
            let detail=selection.detail.is_some();
            let mut document=work.document();
            if !selection.resource.enabled(&visible) {
                if detail{return Err(missing(request.resource));}
                ordinary(&request.query,0).map_err(|_|invalid_page())?;
                serde_json::to_writer(&mut document,&json!({"count":0,"next":null,"previous":null,"results":[]})).map_err(output_error)?;
                return Ok(document.finish());
            }
            let mut connection=work.connection(&state.pool).await?;
            let result=async {
                let page=if detail {None}else {
                    connection.configure(&work).await?;
                    let count=selection.count(&visible,work.row_bytes())?.query().fetch_one(connection.raw()).await?.try_get::<i64,_>("count")?;
                    let count=u64::try_from(count).map_err(|_|ReadError::WorkerFailed)?;
                    let page=ordinary(&request.query,count).map_err(|_|invalid_page())?;
                    let next=page.has_next().then(||page_url(&request.origin,&request.path,&request.query,Some(page.number+1)));
                    let previous=page.has_previous().then(||page_url(&request.origin,&request.path,&request.query,(page.number>2).then_some(page.number-1)));
                    document.write_all(b"{\"count\":").map_err(output_error)?;
                    serde_json::to_writer(&mut document,&count).map_err(output_error)?;
                    document.write_all(b",\"next\":").map_err(output_error)?;
                    serde_json::to_writer(&mut document,&next).map_err(output_error)?;
                    document.write_all(b",\"previous\":").map_err(output_error)?;
                    serde_json::to_writer(&mut document,&previous).map_err(output_error)?;
                    document.write_all(b",\"results\":[").map_err(output_error)?;
                    Some(page)
                };
                work.check()?;connection.configure(&work).await?;
                let statement=selection.rows(&visible,work.row_bytes(),page.as_ref())?;
                let rows=statement.query().fetch_all(connection.raw()).await?;
                if rows.len()>100 {return Err(ReadError::Capacity);}
                // Preserve the Source page's emitted order in Rust. Metadata
                // starts only after all bounded raw PgRows are materialized.
                if let Some(hook)=&state.after_page {hook().await?;}
                let context=LogoContext {absolute_origin:Some(&request.origin)};
                let mut count=0;
                for row in rows {
                    work.check()?;
                    let value=project_fresh(&row,selection.resource,&context,&work,&mut connection).await?;
                    if count>0 {if detail{return Err(ReadError::WorkerFailed);}document.write_all(b",").map_err(output_error)?;}
                    serde_json::to_writer(&mut document,&value).map_err(output_error)?;
                    count+=1;
                }
                if detail&&count==0{return Err(missing(request.resource));}
                if !detail{document.write_all(b"]}").map_err(output_error)?;}
                Ok(document.finish())
            }.await;
            // Do not drop SQL futures at the HTTP deadline. Explicit cleanup
            // is acknowledged before releasing admission; panic/error Drop
            // transfers the connection+lease to a bounded rollback supervisor.
            let cleanup=connection.finish().await;
            match (result,cleanup) {(Err(error),_)=>Err(error),(Ok(_),Err(error))=>Err(error),(Ok(value),Ok(()))=>Ok(value)}
        }).await
    }
    pub async fn provider_info(&self)->Result<Bytes,ReadError> {
        // Never substitute a cached-only or empty success for Source on-demand
        // provider refresh. Fresh/stale/forced action gates remain separate.
        Err(ReadError::Unsupported("VOD provider-info refresh/actions not implemented by ordinary SQL component"))
    }
    pub async fn shutdown(&self,budget:Duration)->bool {self.runtime.shutdown(budget).await}
    pub fn physical_workers(&self)->usize {self.runtime.physical_workers()}
    pub fn retained_memory_bytes(&self)->usize {self.runtime.retained_memory_bytes()}
}
impl ReadRequest {
    fn input_bytes(&self)->Result<usize,ReadError> {
        let mut total=self.origin.len().checked_add(self.path.len()).ok_or(ReadError::Capacity)?;
        for (name,value) in &self.headers {total=total.checked_add(name.as_str().len()).and_then(|n|n.checked_add(value.as_bytes().len())).ok_or(ReadError::Capacity)?;}
        for (name,value) in &self.query {total=total.checked_add(name.len()).and_then(|n|n.checked_add(value.len())).ok_or(ReadError::Capacity)?;}
        Ok(total)
    }
}
