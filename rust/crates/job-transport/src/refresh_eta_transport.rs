//! Refresh-only bounded reservation methods; Index paths remain unchanged.
//! Original Dispatcharr/Celery/Kombu authors retain algorithm/transport credit.
use super::*;
impl Jobs {
    pub(crate) fn owns_refresh_reservation(
        &self,
        reservation: &crate::refresh_eta::Reservation,
    ) -> Result<(), Error> {
        if self
            .inner
            .eta
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|pool| Arc::ptr_eq(pool, &reservation.pool))
        {
            Ok(())
        } else {
            Err(Error::Configuration)
        }
    }
    pub async fn peek_refresh_candidate(
        &self,
        caller: Arc<dyn Send + Sync>,
        limits: crate::eta::Limits,
    ) -> Result<Option<crate::refresh_eta::Candidate>, Error> {
        if !self.allows_refresh() {
            return Err(Error::UnsupportedTask);
        }
        let pool = self.eta_pool(limits)?;
        let budget = Arc::new(pool.acquire()?);
        let guard = Arc::new((caller, budget.clone()));
        let lease = self.lease(guard)?;
        let keys = vec!["celery".to_owned()];
        let (reply, _held) = self
            .inner
            .broker
            .command_classified::<(i64, Vec<u8>), _>(PEEK_CREATE, &keys, &[], lease)
            .await
            .map_err(map)?;
        match reply.0 {
            0 => Ok(None),
            1 => crate::refresh_eta::Receipt::prepare(&reply.1)
                .map(|receipt| {
                    Some(crate::refresh_eta::Candidate {
                        receipt: Arc::new(receipt),
                        budget,
                        pool,
                    })
                })
                .map_err(|_| Error::UnsupportedTask),
            _ => Err(Error::Corrupt),
        }
    }
    pub async fn reserve_refresh_candidate(
        &self,
        candidate: crate::refresh_eta::Candidate,
        caller: Arc<dyn Send + Sync>,
        limits: crate::eta::Limits,
    ) -> Result<crate::refresh_eta::Reservation, Error> {
        if !self.allows_refresh() {
            return Err(Error::UnsupportedTask);
        }
        let pool = self.eta_pool(limits)?;
        if !Arc::ptr_eq(&pool, &candidate.pool) {
            return Err(Error::Configuration);
        }
        let budget = candidate.budget.clone();
        self.reserve_refresh_prepared(candidate, caller, limits, budget)
            .await
    }
    pub async fn reserve_refresh_candidate_settled(
        &self,
        candidate: crate::refresh_eta::Candidate,
        caller: Arc<dyn Send + Sync>,
        limits: crate::eta::Limits,
    ) -> Result<crate::refresh_eta::Reservation, Error> {
        if !self.allows_refresh() {
            return Err(Error::UnsupportedTask);
        }
        let expected = crate::refresh_eta::Reservation::new_shared(
            candidate.receipt.clone(),
            candidate.budget.clone(),
            candidate.pool.clone(),
        );
        let (done, completed) = tokio::sync::oneshot::channel();
        let physical: Arc<dyn Send + Sync> =
            Arc::new((caller.clone(), PhysicalCompletion(Some(done))));
        match self
            .reserve_refresh_candidate(candidate, physical, limits)
            .await
        {
            Err(Error::AmbiguousReservation { .. }) => {
                // Completion closes only when the actual command drops its
                // unique guard, including after a detached timeout.
                let _ = completed.await;
                loop {
                    match self
                        .refresh_reservation_present(&expected, caller.clone())
                        .await
                    {
                        Ok(true) => return Ok(expected),
                        Ok(false) => return Err(Error::ClaimRace),
                        Err(Error::Backend | Error::Capacity) => {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
            result => result,
        }
    }
    async fn refresh_reservation_present(
        &self,
        reservation: &crate::refresh_eta::Reservation,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        self.owns_refresh_reservation(reservation)?;
        let lease = self.cleanup_lease(Arc::new((caller, reservation.clone())))?;
        let keys = vec!["unacked".to_owned(), "unacked_index".to_owned()];
        let tag = reservation.delivery_tag().to_string();
        let (reply, _held) = self
            .inner
            .broker
            .command_classified::<(i64, Vec<u8>), _>(READ_ETA, &keys, &[tag.as_bytes()], lease)
            .await
            .map_err(map)?;
        match reply.0 {
            0 => Ok(false),
            1 if reply.1 == reservation.tuple_bytes() => Ok(true),
            1 => Err(Error::LostReservation),
            _ => Err(Error::Corrupt),
        }
    }
    async fn reserve_refresh_prepared(
        &self,
        candidate: crate::refresh_eta::Candidate,
        caller: Arc<dyn Send + Sync>,
        limits: crate::eta::Limits,
        budget: Arc<crate::eta::Budget>,
    ) -> Result<crate::refresh_eta::Reservation, Error> {
        let reservation =
            crate::refresh_eta::Reservation::new_shared(candidate.receipt, budget, candidate.pool);
        let guard = Arc::new((caller, reservation.clone()));
        let lease = self.lease(guard)?;
        let keys = vec![
            "celery".to_owned(),
            "unacked".to_owned(),
            "unacked_index".to_owned(),
        ];
        let tag = reservation.delivery_tag().to_string();
        let score = format!(
            "{:.6}",
            chrono::Utc::now().timestamp_micros() as f64 / 1_000_000.0
        );
        let count = limits.max_pending.to_string();
        let bytes = limits.max_pending_bytes.to_string();
        let r = &reservation.receipt;
        match self
            .inner
            .broker
            .command_classified::<i64, _>(
                crate::eta::RESERVE,
                &keys,
                &[
                    &r.queued_bytes,
                    tag.as_bytes(),
                    &r.tuple_bytes,
                    score.as_bytes(),
                    count.as_bytes(),
                    bytes.as_bytes(),
                ],
                lease,
            )
            .await
        {
            Ok((1, _)) => Ok(reservation),
            Ok((0, _)) => Err(Error::ClaimRace),
            Ok((-4, _)) => Err(Error::Capacity),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousReservation {
                task_id: reservation.message().task_id,
                delivery_tag: reservation.delivery_tag(),
            }),
            Err(e) => Err(map(e)),
        }
    }
    pub async fn reserve_refresh_eta(
        &self,
        caller: Arc<dyn Send + Sync>,
        limits: crate::eta::Limits,
    ) -> Result<Option<crate::refresh_eta::Reservation>, Error> {
        let Some(candidate) = self.peek_refresh_candidate(caller.clone(), limits).await? else {
            return Ok(None);
        };
        if candidate.message().eta.is_none() {
            return Err(Error::UnsupportedTask);
        }
        self.reserve_refresh_candidate(candidate, caller, limits)
            .await
            .map(Some)
    }
    pub async fn read_refresh_eta(
        &self,
        tag: Uuid,
        caller: Arc<dyn Send + Sync>,
        limits: crate::eta::Limits,
    ) -> Result<Option<crate::refresh_eta::Reservation>, Error> {
        if !self.allows_refresh() {
            return Err(Error::UnsupportedTask);
        }
        let pool = self.eta_pool(limits)?;
        let budget = Arc::new(pool.acquire()?);
        let guard = Arc::new((caller, budget.clone()));
        let lease = self.lease(guard)?;
        let keys = vec!["unacked".to_owned(), "unacked_index".to_owned()];
        let field = tag.to_string();
        let (reply, _held) = self
            .inner
            .broker
            .command_classified::<(i64, Vec<u8>), _>(READ_ETA, &keys, &[field.as_bytes()], lease)
            .await
            .map_err(map)?;
        match reply.0 {
            0 => Ok(None),
            1 => crate::refresh_eta::Receipt::from_stored(&reply.1, tag)
                .map(|r| Some(crate::refresh_eta::Reservation::new(r, budget, pool)))
                .map_err(|_| Error::Corrupt),
            _ => Err(Error::Corrupt),
        }
    }
    pub async fn ack_refresh_eta(
        &self,
        reservation: &crate::refresh_eta::Reservation,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<(), Error> {
        if !self.allows_refresh() {
            return Err(Error::UnsupportedTask);
        }
        self.owns_refresh_reservation(reservation)?;
        let lease = self.cleanup_lease(Arc::new((caller, reservation.clone())))?;
        let tag = reservation.delivery_tag().to_string();
        let keys = vec![
            "celery".to_owned(),
            "unacked".to_owned(),
            "unacked_index".to_owned(),
        ];
        match self
            .inner
            .broker
            .command_classified::<i64, _>(
                crate::eta::ACK,
                &keys,
                &[tag.as_bytes(), reservation.tuple_bytes()],
                lease,
            )
            .await
        {
            Ok((1, _)) => Ok(()),
            Ok((0, _)) => Err(Error::LostReservation),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => {
                Err(Error::AmbiguousAck(reservation.message().task_id))
            }
            Err(e) => Err(map(e)),
        }
    }
    pub async fn restore_refresh_eta(
        &self,
        reservation: &crate::refresh_eta::Reservation,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<(), Error> {
        if !self.allows_refresh() {
            return Err(Error::UnsupportedTask);
        }
        self.owns_refresh_reservation(reservation)?;
        let lease = self.cleanup_lease(Arc::new((caller, reservation.clone())))?;
        let tag = reservation.delivery_tag().to_string();
        let keys = vec![
            "celery".to_owned(),
            "unacked".to_owned(),
            "unacked_index".to_owned(),
        ];
        match self
            .inner
            .broker
            .command_classified::<i64, _>(
                crate::eta::RESTORE,
                &keys,
                &[
                    tag.as_bytes(),
                    reservation.tuple_bytes(),
                    &reservation.receipt.restored_bytes,
                ],
                lease,
            )
            .await
        {
            Ok((1, _)) => Ok(()),
            Ok((0, _)) => Err(Error::LostReservation),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => {
                Err(Error::AmbiguousRestore(reservation.message().task_id))
            }
            Err(e) => Err(map(e)),
        }
    }
    pub(crate) async fn write_refresh_scheduled_success(
        &self,
        message: &crate::refresh_protocol::RefreshMessage,
        result: crate::refresh_worker::NormalReturn,
        children: Vec<Uuid>,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        if !self.allows_refresh() {
            return Err(Error::UnsupportedTask);
        }
        if message.source_id <= 0
            || children.len() > 64
            || message.parent.is_none() && message.root != message.task_id
        {
            return Err(Error::Protocol);
        }
        let value = match result {
            crate::refresh_worker::NormalReturn::Null => serde_json::Value::Null,
            crate::refresh_worker::NormalReturn::MissingSource => serde_json::json!(format!(
                "EPG source {} does not exist, task cleaned up",
                message.source_id
            )),
        };
        self.write_result(message.task_id, value, message.parent, children, caller)
            .await
    }
}
