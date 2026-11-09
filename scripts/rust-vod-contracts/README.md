# Native VOD contract evidence (M4)

Original application authorship belongs entirely to upstream Dispatcharr's
authors and contributors. See LICENSE and ATTRIBUTION.md.

`run.py --output artifacts/rust/<new-directory>` uses the pinned Python image and
immutable original source commit, creates an isolated Compose database, seeds
actual Movie/M3UMovieRelation/XC account/profile objects through Django ORM, and
runs `probe.py` against the wired `/proxy/vod/movie/{uuid}` route. Its separate
origin aliases a native XC movie URL to the deterministic 30-second seekable
H264/AAC MP4 fixture. It always attempts Compose cleanup and retains source
mounts if cleanup fails. Docker must be available through the configured
`DOCKER_HOST` and `DOCKER_CONFIG`.

The probe records HEAD, full-file, first/middle/suffix byte ranges, unsatisfiable
range and mismatched If-Range behavior. Bodies are bounded to 8 MiB and recorded
as hashes/lengths; opaque session IDs and provider redirect URLs are normalized
or redacted. Tokens, if needed, are read from a private temporary `--token-file`
and never stored. A successful characterization means all seven requests were
recorded; it does not mean the original implementation meets RFC range rules or
that Rust matches it.

`probe.py --base-url http://127.0.0.1:<port> --movie-id <uuid> --output <file>` is
reusable against a candidate with the same seeded database and origin. Actual
Rust application wiring, original/candidate comparison, decoded end-to-end
media, authenticated session handoff, midstream resume/provider failover,
user-cap admission, cross-worker behavior and performance remain separate gates.
The default Rust slice preserves the recorded Python suffix/If-Range/416 quirks
for the requested 1:1 migration. No M4 completion or performance gain is asserted
by these helpers.

Add `--rust-resolver-test` to run the ignored Rust actual-schema contract against
the disposable published PostgreSQL port before cleanup. This requires Cargo
and the configured local toolchain. `compare.py` compares normalized captured
wire responses and body hashes, including compatibility quirks; it never marks
full application parity or performance verified.

For a candidate application, pass `--candidate-overlay docker/compose.rust-candidate.yml`
and `--candidate-build-manifest <build.json>`. The runner verifies the running
image/source binding, stops baseline web while retaining fixture PostgreSQL and
Redis, clears only that isolated Redis, then compares the same seven wire cases.
It saves bounded full-file captures in ignored evidence and strictly decodes
video plus audio using the pinned baseline FFmpeg. Candidate mismatch exits
nonzero. This is functional comparison, never a performance measurement or full
VOD acceptance. The ordinary run also decodes the actual Python native response.

The frozen candidate comparison in `verified-compose.json` passed all seven
wire cases with zero differences. Actual native Python and Rust MP4 responses
both passed strict audio/video decoding (75 video frames), with verified image
source binding and Compose cleanup. This is a measured functional milestone,
not full M4 acceptance or a performance claim.

`--profile-aliases` captures a separate source routing corner. Its durable
`profile-alias-characterization.json` explicitly rejects accepting the observed
follow500 as a source contract: the immutable archive fixture lacked the built
React template. The source's profile-duplicating redirect misses media routing;
repeat with built original React before deciding exact fallback parity.

A subsequent source capture with the unchanged built React mounted into the
fixture resolved the alias confound: the duplicated-profile 301 falls through
to React200 (858-byte index hash recorded), not media. The working-tree Rust
helper now preserves positive-profile and zero-profile Location quirks, covered
by a unit regression; its frozen-image alias comparison remains pending. The
original seven-case repeat differed only in nondeterministic `Allow` method
ordering across Python processes. `compare.py` now compares this header as a
set, with an executable regression that still rejects redirect/body changes;
raw captures stay unchanged and the separate normalized comparison passes.
