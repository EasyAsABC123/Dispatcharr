# Native catch-up session API prototype (M4, in progress)

Original application authorship and credit belong entirely to the original
Dispatcharr contributors. See the retained AGPL LICENSE and ATTRIBUTION.md.

This additive crate begins the native authenticated catch-up handshake,
ownership/revocation, bound playback identity resolution and position metadata
contracts. It reuses Store/Auth channel visibility and the bounded supervised
Redis transport. Session handshake TTL is 60 seconds; authorized playback/position
extends the source's 600-second sliding idle window. Bound playback loads the
stored channel without repeating mint-time profile filtering, while refreshing
active user state and checking access if a provider pool names another user.

This is not a Timeshift streaming implementation and is not app-wired. HTTP,
actual-schema/source differential evidence, arbitrary serializer corner cases,
Redis cancellation/failure cases, complete position WebSocket effects, native
and XC playback, provider reservations/format fallback/range generation/grace,
Docker decoding and performance remain open. No M4 completion, application
parity or performance gain is claimed.

Verification so far: four real Redis/loopback HTTP contracts pass, covering 22
session API wire cases from a 24-case original Python Docker capture, session
schema/ownership/revocation and bound-visibility/active-user/position TTL behavior.
The two remaining captured cases are native playback error endpoints. The host
wire tests use a fake authorization resolver. A separate actual Django
PostgreSQL/Auth/Redis integration test passes using a real JWT in Rust HTTP:
handshake, mint-time visibility versus bound playback, current active-user
checks without unauthorized TTL refresh, and revocation. Strict Clippy for this
crate and all targets passes. None of these checks exercises a media body.

The additive `ArchiveClient` provider transport now has four passing real
loopback HTTP contracts: seven-shape/cached-format ordering, cached CDN
redirect suppression and fallback, PHP/JSON rejection, verbatim partial byte
ranges, terminal 416, decisive provider denials versus 5xx cascade, request
capacity held through body ownership, and initial network failure. It keeps
only a prefix/network frame while handing off an incremental response; it does
not buffer the archive file. It is not yet connected to a Timeshift media route,
provider lease, user admission or statistics/grace supervisor.

Original media behavior is now captured separately under Compose:
`scripts/rust-timeshift-contracts/python-media-goldens.json`. Three paced
H264/AAC Range responses returned identical bytes and passed strict audio/video
decoding (74 video frames). PATH-to-QUERY fallback and PHP200 rejection were
observed. Successful responses released provider capacity then removed native
API/pool/stats records after grace. Provider failures released capacity and
retained their API session for retry. These are source goldens, not candidate
media parity evidence.
