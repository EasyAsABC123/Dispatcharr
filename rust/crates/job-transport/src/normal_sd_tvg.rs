//! Source parse_programs_for_tvg_id delegates SD before XML locks or imports.
//! Original Dispatcharr authors retain full application/algorithm credit.
use super::*;
pub(super) struct Bridge {
    sd: Prepared,
    xml: Arc<dyn Handler>,
}
impl Bridge {
    pub(super) fn new(sd: Prepared, xml: Arc<dyn Handler>) -> Self {
        Self { sd, xml }
    }
    async fn run(&self, raw: Arc<[u8]>, owner: Owner) -> Result<D, D> {
        let message = protocol::parse(&raw).map_err(|_| D::UnsupportedHead)?;
        if !message.delegated {
            return Err(D::UnsupportedHead);
        }
        let session = self
            .sd
            .inner
            .auth
            .borrow_task_sql_session(owner.clone())
            .await
            .map_err(|e| match e {
                dispatcharr_store::StoreError::StatusBody(503, _) => D::Capacity,
                _ => fault("TVG source observation admission"),
            })?;
        let epg = message.target;
        let observed=session.run(move|c|Box::pin(async move{sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM epg_epgdata e JOIN epg_epgsource s ON s.id=e.epg_source_id WHERE e.id=$1 AND s.source_type='schedules_direct')").bind(epg).fetch_one(c).await})).await;
        let closed = session.close().await;
        drop(session);
        closed.map_err(|_| fault("TVG source observation close"))?;
        let is_sd = observed
            .map_err(|_| fault("TVG source observation"))?
            .map_err(|_| fault("TVG source query"))?;
        if is_sd {
            self.sd.execute(Some(raw), false, owner).await
        } else {
            self.xml.run_selected(raw, owner).await
        }
    }
}
impl Handler for Bridge {
    fn supports_selected_head(&self) -> bool {
        true
    }
    fn maximum_concurrency(&self) -> usize {
        self.xml.maximum_concurrency()
    }
    fn head_advance_notifier(&self) -> Option<Arc<Notify>> {
        self.sd.head_advance_notifier()
    }
    fn due_head(&self) -> Option<Arc<[u8]>> {
        self.xml.due_head()
    }
    fn run_due(&self, raw: Arc<[u8]>, owner: Owner) -> BoxFuture<'_, Result<D, D>> {
        self.xml.run_due(raw, owner)
    }
    fn maintenance(&self, owner: Owner) -> BoxFuture<'_, Result<(), D>> {
        self.xml.maintenance(owner)
    }
    fn run_selected(&self, raw: Arc<[u8]>, owner: Owner) -> BoxFuture<'_, Result<D, D>> {
        Box::pin(self.run(raw, owner))
    }
    fn run_once(&self, owner: Owner) -> BoxFuture<'_, Result<D, D>> {
        Box::pin(async move {
            let raw = self
                .sd
                .inner
                .jobs
                .peek_sd_tvg_head(owner.clone())
                .await
                .map_err(wire)?;
            let Some(raw) = raw else { return Ok(D::Empty) };
            self.run(Arc::from(raw), owner).await
        })
    }
}
impl Lifecycle for Bridge {
    fn stop_admission(&self) {
        self.xml.stop_admission();
        self.sd.stop_admission();
    }
    fn begin_drain(&self, budget: Duration) {
        self.xml.begin_drain(budget);
        self.sd.begin_drain(budget);
    }
    fn shutdown(&self, budget: Duration) -> BoxFuture<'_, Result<(), D>> {
        self.xml.shutdown(budget)
    }
}
