# ARM64 HDHomeRun configured playback attempt

The ARM64 Compose attempt pinned Original `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` and Native `8a547cf00cd33a94fe4ccb982272be1861d52e73`. The preflight exited `0`, Original Compose startup exited `0` and the strict Original probe exited `1`. The named inactive-profile discovery, advertised lineup and scan status assertions completed before the probe followed the selected lineup URL. The request reached the wait for the first HTTP response headers and timed out. Media decode and the Native arm did not start. This attempt leaves configured ARM64 playback qualification open.

This attempt used the older metadata fixture’s direct Daphne listener. Stock Dispatcharr routes `/hdhr` and `/proxy/` through uWSGI. An independent read-only audit confirmed the transport difference. The cause of the timeout remains unconfirmed. A replay uses the stock WSGI application with the repository’s existing private uWSGI playback configuration and keeps the original media and teardown checks.

The automatic Compose down exited `124`. A separate project-scoped recovery exited `0` and proved absence of the exact seven containers, the network ID and name, and both named volumes. The scoped inventories were empty. The original failed run remains recorded. The raw pair digest is `9922ee7f84648d740dad9e49e6cf020e31109457e15b7b412ba5e42d8153462d` and the separately scoped recovery digest is `24cbce4473d4a91a2d7368a38e0bdff128810933ad1c725f6f305e64ddfda085`. The fixture used an owned private Compose network. Media, credentials and raw logs stayed on the verification host. This scope covers configured HTTP. Hardware and UDP discovery remain outside this attempt.

Application and React credit belongs to the original Dispatcharr authors and contributors.
