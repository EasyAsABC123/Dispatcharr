//! Native catch-up sessions ported from Dispatcharr apps/timeshift/sessions.py
//! and api_views.py. All original application credit: upstream authors and
//! contributors; retained AGPL license/attribution at the repository root.
mod playback_source;
mod resolver;
mod routes;
mod sessions;
pub use resolver::{SessionChannel, SessionResolver, SqlSessionResolver};
pub use sessions::{
    CatchupSession, CatchupSessions, SessionBinding, SessionConfig, HANDSHAKE_TTL_SECONDS,
    SESSION_IDLE_TTL_SECONDS,
};

pub use playback_source::{CatchupCandidate, CatchupSource, SqlCatchupResolver};

mod upstream;
pub use upstream::{ArchiveClient, ArchiveFailure, ArchiveMedia, ArchiveOpen, ArchiveRequest};

mod media_pool;
pub use media_pool::{
    ArchivePool, ArchivePoolConfig, ArchivePoolRequest, ArchiveProbe, ArchiveTicket,
};

mod direct;
mod media;
mod xc;
pub use media::{ArchiveResolver, CatchupMedia, CatchupMediaConfig, SqlArchiveResolver};
pub use xc::XcIntent;

mod management;
mod management_data;
mod management_programmes;
pub use management::CatchupManagement;

mod events;
