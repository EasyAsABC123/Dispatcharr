# VOD replacement admission on c1ff4005

At 32 and 128 KiB/s, both packaged backends returned HTTP 503 to the early fresh-ID request without opening another provider connection. After the old provider reservation retired, the same fresh ID returned HTTP 200. The fresh and unrelated readers supplied useful MPEG-TS data with audio and video decoding. Shared-session and same-ID reconnect checks also passed with exact owned teardown. [Exact paired evidence](../scripts/rust-integration/m4-c1ff4005-vod-replacement.json)

The Rust pump can finish only a provider read that was already pending when the last ordinary viewer closed. The existing read deadline still applies. Shutdown and a new admission to the same session can cancel the read. The departing read discards its result and opens no new provider request. This follows the measured Original iterator behavior. [Original iterator trace](rust-m4-21da6d0a-vod-passive-iterator.md)

The first pair stopped before making Native's early request at either rate. Its shared observer retained only the first 80 writes across both backends and missed later writes. The same-image retry kept separate bounded traces for each provider request. The receipt preserves the first run's exit 1 and its cleanup evidence.

Formatting, strict Clippy and 1,505 enabled Cargo tests passed. Recovery and broader VOD acceptance remain open, as does comparative performance. Original Dispatcharr and dependency contributors retain full credit. [Original attribution](../ATTRIBUTION.md)
