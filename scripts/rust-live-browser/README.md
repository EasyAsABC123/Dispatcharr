# Unchanged React live playback and statistics

Original Dispatcharr authors and contributors retain all application/algorithm
credit and original licenses. This gate executes their existing React bundle
without frontend changes, mocked API data, injected authentication state or
direct authenticated browser fetches.

[The first image47 comparison](verified-image47-live-ui-1.json) is **failed**.
The Original passes normal login, channel preview, actual decoded video advancing
over two seconds, navigation to Stats, an actual combined-stats response naming
the active channel, and closing the floating player. Image47 also plays video
(the retained failure observation shows 578 decoded frames), but its Stats page
receives combined-stats404 and channel UUID-action405. Those are implementation
gaps, not a browser pass or M2 completion. Implementations and a fresh packaged
replay are required; image47's separate live CPU/memory results retain their
original narrower scope.

```sh
python3 scripts/rust-live-browser/run.py \
  --build-manifest artifacts/rust/YOUR_BUILD/build.json \
  --output artifacts/rust/live-browser-NEW --source-only
python3 scripts/rust-live-browser/run.py \
  --build-manifest artifacts/rust/YOUR_BUILD/build.json \
  --output artifacts/rust/live-browser-paired-NEW
```

Use Python3, Docker Compose, Node22+ and local Chromium (or
`RUST_BROWSER_CHROMIUM`). Use a clean checkout: support scripts are copied and
hashed before execution. The application is never built implicitly. A passing
immutable build manifest/image is required even for Source-only execution,
because the exact packaged React assets are extracted and served to both sides.
The Original backend uses its pinned, unchanged archive. Candidate image ID and
source labels are checked before native browser execution.

The fixture creates a synthetic administrator on an owned database. Passwords
and tokens remain in memory or browser state; credentials enter Chromium through
stdin and normal login. The temporary browser profile is removed and its process
group is stopped. Project containers, networks and volumes are destroyed and
checked afterward. No real providers are loaded. Outputs must be new directories.

The observer attaches to dedicated workers before resuming them, since mpegts.js
loads media there. The first page-only observer failed to see the stream HTTP
response; that failure is retained separately. Source qualification and the
paired run use identical corrected observer bytes. Actual media time, decoded
frame count, dimensions, media error, HTTP status, displayed channel identity
and uncaught exceptions are recorded. Header credentials are never recorded.

This fixture runs the Source uWSGI web service without Daphne; its visible
WebSocket-reconnect message is preserved. It does not accept realtime behavior,
all statistics fields, resource gains, or full application parity. Source2 has
no unexpected API failures beyond the normal unauthenticated proxy-login/logout
401 responses. A final package review must also reject unexpected API failures,
including the currently observed UUID405, rather than accepting video alone.
