//! Concrete pure-template/event bridge to the SAME scoped task-role actor.
//! Original Dispatcharr/Django/CPython authors retain all algorithm credit.
use super::*;
impl Templates for crate::PluginHookSession {
    fn begin_event(&self, event: String, payload: Value) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async move { crate::PluginHookSession::begin_event(self, event, payload).await })
    }
    fn render(&self, template: String) -> BoxFuture<'_, Result<TemplateResult, StoreError>> {
        Box::pin(async move {
            let outcome = self.render_connect_template(template).await?;
            match outcome.render {
                crate::PluginTemplateRender::Rendered(body) => Ok(TemplateResult::Rendered {
                    body,
                    source_json_loads_succeeded: outcome.source_json_loads_succeeded.ok_or(
                        StoreError::Unsupported("Source template JSON parse observation missing"),
                    )?,
                    payload: outcome.current_payload,
                }),
                crate::PluginTemplateRender::Caught {
                    actual_class,
                    message,
                } => Ok(TemplateResult::Caught {
                    actual_class,
                    message,
                    payload: outcome.current_payload,
                }),
            }
        })
    }
}
pub struct EventReport {
    pub connect: Report,
    pub plugins: Option<crate::PluginHookReport>,
}
/// Invoke after the actual SystemEvent INSERT and before max-events read/trim.
/// Preserve the caller's independent Source WS-before/after-audit ordering.
/// Per-event observations are NOT the final task trail; finish the real hook
/// session only after native publication flow closes its NativeTaskTrail.
pub async fn dispatch_scoped_event(
    event: String,
    payload: Value,
    session: TaskSqlSession,
    hooks: Arc<crate::PluginHookSession>,
    owner: Owner,
) -> Result<EventReport, Error> {
    tokio::spawn(async move {
        let templates: Arc<dyn Templates> = hooks.clone();
        let connect = dispatch(event, payload, session, templates, owner).await?;
        let plugins = if connect.flow == Flow::Complete {
            Some(hooks.dispatch_plugin_actions().await?)
        } else {
            None
        };
        hooks.end_event().await?;
        Ok(EventReport { connect, plugins })
    })
    .await
    .map_err(|_| Error::Supervisor)?
}
