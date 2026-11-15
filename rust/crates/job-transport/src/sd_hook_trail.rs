//! Actual normal SD native publication trail; metadata, never child authority.
//! Original Dispatcharr/Celery authors retain algorithm credit.
use dispatcharr_store::{
    NativeTaskTrail, NativeTaskTrailDelta, NativeTaskTrailEntry, NativeTaskTrailState, StoreError,
};
use std::sync::Mutex;
pub(super) struct Trail {
    id: String,
    state: Mutex<State>,
}
struct State {
    children: Vec<uuid::Uuid>,
    closed: bool,
    unsettled: bool,
}
impl Trail {
    pub(super) fn new(id: uuid::Uuid) -> Self {
        Self {
            id: id.to_string(),
            state: Mutex::new(State {
                children: Vec::new(),
                closed: false,
                unsettled: false,
            }),
        }
    }
    /// Invoke only for the real children returned by execute_body AFTER its
    /// actual Jobs publication replies. Never populate from plugin JSON trails.
    pub(super) fn close(&self, children: &[uuid::Uuid]) -> Result<(), StoreError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| StoreError::Unsupported("SD native trail lock failed"))?;
        if state.closed || state.unsettled {
            return Err(StoreError::Unsupported("SD native trail state invalid"));
        }
        state.children.extend_from_slice(children);
        state.closed = true;
        Ok(())
    }
    pub(super) fn fault(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.unsettled = true;
        }
    }
}
impl NativeTaskTrail for Trail {
    fn claimed_task_id(&self) -> &str {
        &self.id
    }
    fn settled_after(&self, cursor: u64) -> Result<NativeTaskTrailDelta, StoreError> {
        let state = self
            .state
            .lock()
            .map_err(|_| StoreError::Unsupported("SD native trail lock failed"))?;
        let after = usize::try_from(cursor)
            .map_err(|_| StoreError::Unsupported("SD native trail cursor invalid"))?;
        if after > state.children.len() {
            return Err(StoreError::Unsupported(
                "SD native trail cursor beyond settled publications",
            ));
        }
        let entries = state
            .children
            .iter()
            .enumerate()
            .skip(after)
            .map(|(i, id)| NativeTaskTrailEntry {
                ordinal: i as u64 + 1,
                source_result_tuple: serde_json::json!([[id.to_string(), null], null]),
            })
            .collect();
        Ok(NativeTaskTrailDelta {
            next_cursor: state.children.len() as u64,
            entries,
            state: if state.unsettled {
                NativeTaskTrailState::Unsettled
            } else if state.closed {
                NativeTaskTrailState::Closed
            } else {
                NativeTaskTrailState::Open
            },
        })
    }
}
