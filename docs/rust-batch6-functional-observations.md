# Candidate6 functional observations and remaining failures

Original Dispatcharr and dependency authors retain all application/algorithm
credit and licenses. Git `b7dafc1a586c6f1c5a466349ca5afa0f54639b14`, image
`sha256:4a6b7ffab1bb9cbd316ae54f4ca18fcd5d476e5b7971286c49ffc82fd1446afe`,
was tested through actual production Main in owned Docker fixtures. These are
functional observations, not performance gains or completed migration milestones.

The grouped Native TLS13 counterpart reused existing Source13 authorities with
zero Source API/driver replay. All13 Main starts settled. Eleven of12 PostgreSQL
cells matched Source. Verify-ca hostname mismatch and both Allow branches now
pass: the TLS-required server produced encrypted client-DN sessions, while the
plaintext-capable server retained plaintext preference. Wrong CA, missing CA,
missing client certificate, verify-full hostname mismatch and require+wrong-CA
still refused. Explicit Unix sockets used no TLS. Explicit IPv6 verify-ca
remained a real startup1/no-PG-session failure; the later proposed normalization
is not claimed as accepted here.

SQLite remains a separate required implementation gap. Main exited1 without PG
fallback and left the SQLite file unchanged. The raw controller was2 with11/13
expected checks because its observer inspected only Docker logs stdout. Actual
captured stderr contains the explicit unsupported-SQLite message. Read-only
qualification yields12/13 corrected expected-behavior observations while keeping
the raw false guard and controller2. This is neither SQLite parity nor a new
controller0. All14 actual CIDs, three project inventories and141 host CLI
generations were absent after cleanup. Defaults62 tables/59 sequences, RedisDB7
and SQLite bytes were unchanged. Private certificate keys never left their
owned Docker volume; those volumes were removed.

Selected-normal M6 attempt1 failed before business: Main reported selected-DVR
initialization failure. Its newly read fixture plugin directory was0700 under
umask077 despite a requested0755 mkdir; Native UID65532 could not read it.
Startup capture stayed partial with zero root frames and no business capture.
The failure and all ten actual cleanup/default-isolation results remain. A fresh
attempt used explicit0755.

Attempt2 completed both startup roots and all12 business cases' scoped status
checks. Startup recovery/version frames and terminal receipts were measured
before business, with their actual SQL/cache/backend effects retained. Business
included configured-rule API202, Core PATCH200, no-rules API202, retained Native
consumers and P0001 failure/repeat. The pre-stop snapshot had20 backends:19 SUCCESS
and1 FAILURE, including both startup roots. No post-stop business snapshot or
whole-cohort/FIFO qualification exists for this failed attempt.

Its controller1 is a real evidence failure: the per-line-fsync observer persisted
6,737 rows/1,113,072 bytes but covered only1.261seconds of Redis history, ending
1.120seconds before the final business timestamp. The terminal ECHO was absent
after five seconds. The prefix also exposed application NotDue spin:1,201 Celery
PEEK RPCs, peaking at191 in a10ms bucket. Its captured71.6ms future-head interval
contains1,164 PEEKs versus a finite20-read diagnostic bound from100ms polling and
named completion/boundary allowances. The incomplete journal is diagnostic-only.

The next observer batches writes and fsyncs the actual terminal ECHO before its
durable event, retaining the20,000-row/16MiB caps. Jobs prepared actual head
backoff. Both require one full coherent-image rerun with business/startup checks
and measured future-head counts. No third image6 rerun is represented. Both
failed attempts completed all ten normal cleanup/default-isolation steps.

The compact packet excludes Original runtime trees, actual JWTs, private keys,
raw SQL/cache/auth captures and raw journals; their hashes/scoped observations
remain reviewable. Failures remain separate from proposals and upcoming tests.
Source47 coverage, broader recovery/artwork/scheduler/restart/held-wire behavior
and full M6 are unfinished. No CPU/RSS or paired gain follows from RPC counts.
