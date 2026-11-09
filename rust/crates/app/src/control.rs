//! Connect channel-delete control to the shared live stop path.
use dispatcharr_contracts::{BoxFuture, ChannelControl, PlaybackError};
use dispatcharr_stream_http::StreamEngine;
pub struct EngineControl(pub StreamEngine);
impl ChannelControl for EngineControl {
    fn stop_channel(&self, channel_id: String) -> BoxFuture<'_, Result<(), PlaybackError>> {
        Box::pin(async move { self.0.stop_global(&channel_id).await.map(|_| ()) })
    }
}
