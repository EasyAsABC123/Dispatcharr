//! Default plugin-only bounded reservation methods; Index paths remain unchanged.
//! Original Dispatcharr/Celery/Kombu authors retain algorithm/transport credit.
use super::*;
impl Jobs {
    pub(crate) fn owns_plugin_reservation(
        &self,
        reservation: &crate::plugin_eta::Reservation,
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
    pub(crate) async fn peek_plugin_candidate(
        &self,
        expected: Option<&[u8]>,
        caller: Arc<dyn Send + Sync>,
        limits: crate::eta::Limits,
    ) -> Result<Option<crate::plugin_eta::Candidate>, Error> {
        if !self.allows_defaults() {
            return Err(Error::UnsupportedTask);
        }
        let pool = self.plugin_eta_pool(limits)?;
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
        if expected.is_some_and(|raw| reply.0 != 1 || reply.1 != raw) {
            return Err(Error::ClaimRace);
        }
        match reply.0 {
            0 => Ok(None),
            1 => crate::plugin_eta::Receipt::prepare(&reply.1)
                .map(|receipt| {
                    Some(crate::plugin_eta::Candidate {
                        receipt: Arc::new(receipt),
                        budget,
                        pool,
                    })
                })
                .map_err(|_| Error::UnsupportedTask),
            _ => Err(Error::Corrupt),
        }
    }
    pub(crate) async fn reserve_plugin_candidate(
        &self,
        candidate: crate::plugin_eta::Candidate,
        caller: Arc<dyn Send + Sync>,
        limits: crate::eta::Limits,
    ) -> Result<crate::plugin_eta::Reservation, Error> {
        if !self.allows_defaults() {
            return Err(Error::UnsupportedTask);
        }
        let pool = self.plugin_eta_pool(limits)?;
        if !Arc::ptr_eq(&pool, &candidate.pool) {
            return Err(Error::Configuration);
        }
        let budget = candidate.budget.clone();
        self.reserve_plugin_prepared(candidate, caller, limits, budget)
            .await
    }
    pub(crate) async fn reserve_plugin_candidate_settled(
        &self,
        candidate: crate::plugin_eta::Candidate,
        caller: Arc<dyn Send + Sync>,
        limits: crate::eta::Limits,
    ) -> Result<crate::plugin_eta::Reservation, Error> {
        if !self.allows_defaults() {
            return Err(Error::UnsupportedTask);
        }
        let expected = crate::plugin_eta::Reservation::new_shared(
            candidate.receipt.clone(),
            candidate.budget.clone(),
            candidate.pool.clone(),
        );
        let (done, completed) = tokio::sync::oneshot::channel();
        let physical: Arc<dyn Send + Sync> =
            Arc::new((caller.clone(), PhysicalCompletion(Some(done))));
        match self
            .reserve_plugin_candidate(candidate, physical, limits)
            .await
        {
            Err(Error::AmbiguousReservation { .. }) => {
                // Completion closes only when the actual command drops its
                // unique guard, including after a detached timeout.
                let _ = completed.await;
                loop {
                    match self
                        .plugin_reservation_present(&expected, caller.clone())
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
    async fn plugin_reservation_present(
        &self,
        reservation: &crate::plugin_eta::Reservation,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        self.owns_plugin_reservation(reservation)?;
        let lease = self.lease(Arc::new((caller, reservation.clone())))?;
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
    async fn reserve_plugin_prepared(
        &self,
        candidate: crate::plugin_eta::Candidate,
        caller: Arc<dyn Send + Sync>,
        limits: crate::eta::Limits,
        budget: Arc<crate::eta::Budget>,
    ) -> Result<crate::plugin_eta::Reservation, Error> {
        let reservation =
            crate::plugin_eta::Reservation::new_shared(candidate.receipt, budget, candidate.pool);
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
                task_id: reservation.message().id,
                delivery_tag: reservation.delivery_tag(),
            }),
            Err(e) => Err(map(e)),
        }
    }
    pub(crate) async fn read_plugin_eta(
        &self,
        tag: Uuid,
        caller: Arc<dyn Send + Sync>,
        limits: crate::eta::Limits,
    ) -> Result<Option<crate::plugin_eta::Reservation>, Error> {
        if !self.allows_defaults() {
            return Err(Error::UnsupportedTask);
        }
        let pool = self.plugin_eta_pool(limits)?;
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
            1 => crate::plugin_eta::Receipt::from_stored(&reply.1, tag)
                .map(|r| Some(crate::plugin_eta::Reservation::new(r, budget, pool)))
                .map_err(|_| Error::Corrupt),
            _ => Err(Error::Corrupt),
        }
    }
    pub(crate) async fn ack_plugin_eta(
        &self,
        reservation: &crate::plugin_eta::Reservation,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<(), Error> {
        if !self.allows_defaults() {
            return Err(Error::UnsupportedTask);
        }
        self.owns_plugin_reservation(reservation)?;
        let guard: Arc<dyn Send + Sync> = Arc::new((caller, reservation.clone()));
        let tag = reservation.delivery_tag().to_string();
        let keys = vec![
            "celery".to_owned(),
            "unacked".to_owned(),
            "unacked_index".to_owned(),
        ];
        match self
            .plugin_broker_settled::<i64>(
                crate::eta::ACK,
                &keys,
                &[tag.as_bytes(), reservation.tuple_bytes()],
                guard,
            )
            .await
        {
            Ok(1) => Ok(()),
            Ok(0) => Err(Error::LostReservation),
            Ok(_) => Err(Error::Corrupt),
            Err(Error::Backend) => Err(Error::AmbiguousAck(reservation.message().id)),
            Err(e) => Err(e),
        }
    }
    pub(crate) async fn restore_plugin_eta(
        &self,
        reservation: &crate::plugin_eta::Reservation,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<(), Error> {
        if !self.allows_defaults() {
            return Err(Error::UnsupportedTask);
        }
        self.owns_plugin_reservation(reservation)?;
        let guard: Arc<dyn Send + Sync> = Arc::new((caller, reservation.clone()));
        let tag = reservation.delivery_tag().to_string();
        let keys = vec![
            "celery".to_owned(),
            "unacked".to_owned(),
            "unacked_index".to_owned(),
        ];
        match self
            .plugin_broker_settled::<i64>(
                crate::eta::RESTORE,
                &keys,
                &[
                    tag.as_bytes(),
                    reservation.tuple_bytes(),
                    &reservation.receipt.restored_bytes,
                ],
                guard,
            )
            .await
        {
            Ok(1) => Ok(()),
            Ok(0) => Err(Error::LostReservation),
            Ok(_) => Err(Error::Corrupt),
            Err(Error::Backend) => Err(Error::AmbiguousRestore(reservation.message().id)),
            Err(e) => Err(e),
        }
    }
}
