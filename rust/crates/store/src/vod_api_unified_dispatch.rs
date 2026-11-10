//! Scoped unified JSON dispatch using the shared retained AuthService/runtime.
//! Actual HTTP/ownership component gates passed; canonical app/React and open
//! browsable HTML/query/property grammar remain separate acceptance gates.
//! Original Dispatcharr authors/contributors retain credit and AGPL licensing.
use super::{
    vod_api_http::{json_indent, negotiate, pretty_document},
    vod_api_owned_read::{ReadError, ReadRuntime},
    vod_api_projection::visibility,
    vod_api_read_component::HttpRead,
    vod_api_unified_read::{self, UnifiedFault},
};
use axum::http::{HeaderMap, Method};
use bytes::Bytes;
use serde_json::json;
use std::{net::SocketAddr, sync::Arc};

pub(super) struct UnifiedRequest {
    pub peer: SocketAddr,
    pub headers: HeaderMap,
    pub origin: String,
    pub path: String,
}
#[derive(Clone)]
pub(super) struct UnifiedReads {
    pub runtime: ReadRuntime,
    auth: crate::AuthService,
}
impl UnifiedReads {
    pub fn from_runtime(auth: crate::AuthService, runtime: ReadRuntime) -> Self {
        Self { runtime, auth }
    }
    pub async fn dispatch(
        &self,
        request: UnifiedRequest,
        http: HttpRead,
    ) -> Result<Bytes, ReadError> {
        let input = request
            .headers
            .iter()
            .fold(
                request
                    .origin
                    .len()
                    .checked_add(request.path.len())
                    .and_then(|v| v.checked_add(http.uri.query().map_or(0, str::len))),
                |sum, (name, value)| {
                    sum.and_then(|v| v.checked_add(name.as_str().len()))
                        .and_then(|v| v.checked_add(value.as_bytes().len()))
                },
            )
            .ok_or(ReadError::Capacity)?;
        let state = self.clone();
        self.runtime.run_until(input,Some(http.deadline),move |work| async move {
            let indent=json_indent(&request.headers);
            let result=async {
                work.check()?;
                negotiate(&request.headers,&http.uri)?;
                let guard=work.physical_guard();
                let guard:Arc<dyn Send+Sync>=match &http.scope {Some(scope)=>Arc::new((guard,scope.clone())),None=>guard};
                let auth=state.auth.clone().with_retained_scope(guard);
                let user=auth.account_user(request.peer,request.headers,false).await?;
                work.check()?;
                if !matches!(http.method,Method::GET|Method::HEAD) {
                    if user.user_level<10 {return Err(crate::StoreError::PermissionDenied.into());}
                    if http.method==Method::OPTIONS {
                        let mut document=work.document();
                        let detail=http.raw_detail.is_some();
                        let value=json!({"name":if detail {"Unified Content Instance"}else{"Unified Content List"},"description":"ViewSet that combines Movies and Series for unified 'All' view","renders":["application/json","text/html"],"parses":["application/json","application/x-www-form-urlencoded","multipart/form-data"]});
                        serde_json::to_writer(&mut document,&value).map_err(|_|ReadError::Capacity)?;
                        return Ok(document.finish());
                    }
                    return Err(crate::StoreError::StatusBody(405,json!({"detail":format!("Method \"{}\" not allowed.",http.method)})).into());
                }
                if user.user_level<1 {return Err(crate::StoreError::PermissionDenied.into());}
                if let Some(raw)=&http.raw_detail {
                    return Err(match dispatcharr_ingest_core::source_integer_literal(raw) {
                        Some(_)=>crate::StoreError::StatusBody(404,json!({"detail":"No Movie matches the given query."})),
                        None=>crate::StoreError::NotFound,
                    }.into());
                }
                let visible=visibility(&user.custom_properties,user.user_level).map_err(|_|ReadError::Unsupported("Unified nonmapping user-properties fault needs Original HTTP capture"))?;
                let pairs=url::form_urlencoded::parse(http.uri.query().unwrap_or("").as_bytes()).map(|(a,b)|(a.into_owned(),b.into_owned())).collect::<Vec<_>>();
                match vod_api_unified_read::read(auth.store.pool(),&work,&visible,&pairs,&request.origin,&request.path).await {
                    Ok(bytes)=>Ok(bytes),
                    Err(UnifiedFault::Read(error))=>Err(error),
                    Err(UnifiedFault::View(message))=>Err(crate::StoreError::StatusBody(500,json!({"error":message})).into()),
                    Err(UnifiedFault::Renderer)=>Err(ReadError::SourceUnhandledFault("Unified JSONRenderer nonfinite float")),
                }
            }.await;
            match (result,indent) {
                (Ok(bytes),Some(indent))=>pretty_document(&bytes,indent,work.document()),
                (result,_)=>result,
            }
        }).await
    }
}
