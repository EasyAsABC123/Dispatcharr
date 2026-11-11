//! Source-validated ordinary VOD read component with scoped cached auth.
//! Original Dispatcharr authors retain all application/algorithm credit.
//! Captured SQL/HTTP/physical ownership gates passed; canonical app acceptance
//! is separate. Category/unified/provider/action routes remain unwired.
use super::{vod_api_dto::LogoContext,vod_api_owned_read::{ReadError,ReadLimits,ReadRuntime},
    vod_api_pagination::{ordinary,page_url},vod_api_projection::visibility,
    vod_api_sql::{Resource,project_fresh},vod_api_query::{self,QueryError}};
use axum::http::{HeaderMap,Method,Uri};
use bytes::Bytes;
use dispatcharr_contracts::BoxFuture;
use serde_json::json;
use sqlx::{PgPool,Row};
use std::{io::Write,net::SocketAddr,sync::Arc,time::Duration};

/// Deliberate integration seam, not a fake authenticated-user implementation.
/// The shared AuthService adapter carries this guard through its physical
/// SQL/Redis/cache/publication work, including replies after logical timeout.
/// Simple `account_user(...).await` alone is insufficient if an internal
/// logical cache timeout returns before its owned Redis reply completes.
pub(crate) trait RetainedUiAuthorizer:Send+Sync {
    fn authorize<'a>(&'a self,peer:SocketAddr,headers:HeaderMap,guard:Arc<dyn Send+Sync>)
        ->BoxFuture<'a,Result<crate::User,crate::StoreError>>;
}
/// Shared authentication bridge uses existing policy/cache/sink on a scoped
/// request clone. Real paused-Redis/fallback/cancellation tests retain ownership
/// through actual replies; canonical app validation remains separate.
struct ScopedUiAuth(crate::AuthService);
impl RetainedUiAuthorizer for ScopedUiAuth {
    fn authorize<'a>(&'a self,peer:SocketAddr,headers:HeaderMap,guard:Arc<dyn Send+Sync>)
        ->BoxFuture<'a,Result<crate::User,crate::StoreError>> {
        let scoped=self.0.clone().with_retained_scope(guard);
        Box::pin(async move {scoped.account_user(peer,headers,false).await})
    }
}
pub(crate) struct ReadRequest {
    pub peer:SocketAddr,pub headers:HeaderMap,pub resource:Resource,pub detail:Option<i64>,
    pub query:Vec<(String,String)>,pub origin:String,pub path:String,
}
#[derive(Clone)]
pub(crate) struct HttpRead {
    pub method:Method,pub uri:Uri,pub raw_detail:Option<String>,pub scope:Option<super::vod_api_owned_read::HttpScope>,pub deadline:tokio::time::Instant,
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
    /// Uses real cached UI authentication with per-request physical ownership.
    pub fn from_auth(auth:crate::AuthService,limits:ReadLimits)->Result<Self,ReadError> {
        Self::new(auth.store.pool().clone(),Arc::new(ScopedUiAuth(auth)),limits)
    }
    pub fn new(pool:PgPool,auth:Arc<dyn RetainedUiAuthorizer>,limits:ReadLimits)->Result<Self,ReadError> {
        Ok(Self {pool,auth,runtime:ReadRuntime::new_fair(limits)?,after_page:None})
    }
    pub fn with_page_hook(mut self,hook:PageHook)->Self {self.after_page=Some(hook);self}
    /// Ordinary Movie/Series/Episode component input, not a routed HTTP API.
    /// Filter normalization runs after authentication/permission/visibility,
    /// including for disabled-access empty querysets, before count/pagination.
    /// Unsupported grammar is explicit and must pass Source2 before expansion.
    pub async fn read(&self,request:ReadRequest)->Result<Bytes,ReadError> {
        self.dispatch(request,None).await
    }
    /// Ordinary HTTP dispatch validated against captured original network
    /// representations and physical resource gates; app packaging is separate.
    pub async fn dispatch(&self,mut request:ReadRequest,http:Option<HttpRead>)->Result<Bytes,ReadError> {
        let state=self.clone();
        let input=request.input_bytes()?.checked_add(http.as_ref().and_then(|h|h.uri.query()).map_or(0,str::len)).ok_or(ReadError::Capacity)?;
        let deadline=http.as_ref().map(|h|h.deadline);
        self.runtime.run_scoped(input,deadline,http.as_ref().and_then(|h|h.scope.clone()),move |work|async move {
            let indent=http.as_ref().and_then(|_|super::vod_api_http::json_indent(&request.headers));
            let result=async {
            work.check()?;
            if let Some(http)=&http {
                super::vod_api_http::negotiate(&request.headers,&http.uri)?;
                request.query=url::form_urlencoded::parse(http.uri.query().unwrap_or("").as_bytes()).map(|(key,value)|(key.into_owned(),value.into_owned())).collect();
            }
            let guard=work.io_guard()?.erased();
            let guard:Arc<dyn Send+Sync>=match http.as_ref().and_then(|h|h.scope.as_ref()) {Some(scope)=>Arc::new((guard,scope.clone())),None=>guard};
            let user=state.auth.authorize(request.peer,request.headers,guard).await?;
            work.check()?;
            if let Some(http)=&http {
                if !matches!(http.method,Method::GET|Method::HEAD) {
                    if user.user_level<10 {return Err(crate::StoreError::PermissionDenied.into());}
                    if http.method==Method::OPTIONS {
                        let mut document=work.document();
                        serde_json::to_writer(&mut document,&super::vod_api_http::options(request.resource,http.raw_detail.is_some())).map_err(output_error)?;
                        return Ok(document.finish());
                    }
                    return Err(crate::StoreError::StatusBody(405,json!({"detail":format!("Method \"{}\" not allowed.",http.method)})).into());
                }
            }
            if user.user_level<1{return Err(crate::StoreError::PermissionDenied.into());}
            let visible=visibility(&user.custom_properties,user.user_level)
                .map_err(|_|ReadError::Unsupported("Nonmapping VOD properties fault contract is not captured"))?;
            let mut invalid_detail=None;
            if let Some(raw)=http.as_ref().and_then(|h|h.raw_detail.as_ref()) {
                request.detail=Some(match dispatcharr_ingest_core::source_integer_literal(raw) {
                    None=>{invalid_detail=Some(ReadError::Source(crate::StoreError::NotFound));0},
                    Some(number)=>match number.parse() {Ok(id)=>id,Err(_)=>{invalid_detail=Some(missing(request.resource));0}},
                });
            }
            let selection=vod_api_query::ordinary(request.resource,request.detail,&request.query).map_err(|error|match error {
                QueryError::Source400(body)=>ReadError::Source(crate::StoreError::StatusBody(400,body)),
                QueryError::Capacity=>ReadError::Capacity,QueryError::Unsupported(message)=>ReadError::Unsupported(message),
            })?;
            if let Some(error)=invalid_detail {return Err(error);}
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
            }.await;
            match (result,indent) {(Ok(data),Some(indent))=>super::vod_api_http::pretty_document(&data,indent,work.document()),(result,_)=>result}
        }).await
    }
    pub async fn provider_info(&self)->Result<Bytes,ReadError> {
        // Never substitute a cached-only or empty success for Source on-demand
        // provider refresh. Fresh/stale/forced action gates remain separate.
        Err(ReadError::Unsupported("VOD provider-info refresh/actions not implemented by ordinary SQL component"))
    }
    pub async fn shutdown(&self,budget:Duration)->bool {self.runtime.shutdown(budget).await}
    pub(super) fn shared_runtime(&self)->ReadRuntime {self.runtime.clone()}
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
