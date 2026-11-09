# Original native TS initialization and HTTP method evidence

Full original application credit belongs to the Dispatcharr authors. This is a
characterization of unchanged application source at the baseline commit recorded
in each artifact, not a completed migration or a performance claim.

`cold_ts.py` uses a bounded origin with 503, headers-with-no-body, and empty200
responses. `cold_lifecycle.py` adds provider/user admission observations,
initializing and already-active viewers, and HEAD/authentication checks. The
optional `--ghost-seconds` fixture changes class configuration through a uWSGI
import; it does not patch application source. Default runs keep source defaults.

Measured evidence:

| Artifact under `artifacts/rust` | Observation |
| --- | --- |
| `m2-cold-wire-2` | Original all three cold modes return200/video/mp2t after about50s, exactly188 bytes containing `Error: Client disconnected`. Frozen Rust image `m2-replica-1` instead returns502/504 JSON after2/30s: explicit open parity gap. |
| `m2-cold-lifecycle-default-1` | Valid partial characterization: fresh and existing-initializing errors at51.498/54.957s. Metadata stays connecting, inputindex0, provider slots1 through56.50s, even after clients drop to0. This run's later warm-fixture check failed because only3 chunks were generated; its overall passed flag remains false. |
| `m2-cold-fail-transitions-1` | Default503-only origins, full lifecycle run passed: fresh/existing initialization errors51.405/54.890s; metadata connecting/index0, provider1 throughout, including clients0 at56.51s. |
| `m2-cold-lifecycle-short-1` | Fixture-only ghost6 variant: errors6.120/6.998s. Provider still1 immediately after both clients disappear. User-cap429, provider-cap503 and invalidJWT401 remain separate pre-stream JSON failures. Overall run failed only the insufficient warm fixture. |
| `m2-cold-warm-transitions-1` | Correct12sec H264 fixture: connecting/index3 at4.667s; index4 at5.672s; active/index4 at6.007s; first sampled viewer media at7.010s. New head-position viewer during active/no-data receives an empty null188 packet after13.536s, not the cold error packet. |
| `m2-playback-head-source-1` |18 original requests,0 upstream connections: anonymous/valid HEAD405; presented invalidBearer401 even on AllowAny HLS routes. HLS anonymous GET missingtoken410. Adminstatus anonymous HEAD401/validadmin405. |

The source `_set_waiting_for_clients` uses `INITIAL_BEHIND_CHUNKS` default4.
Publishing the first input chunk alone does not satisfy its readiness condition.
`StreamGenerator._wait_for_initialization` emits no initialization keepalives.
It checks client disappearance first, then stalled-start conditions, then metadata
states: explicit error/stopped/stopping produces the corresponding188 error
packet immediately. The separately measured503/retry exhaustion did not set metadata error;
it remained connecting while the ghost-client timer expired. HTTP headers were
buffered until the first emitted packet in the pinned uWSGI fixture.

Do not translate authentication, user/provider admission or registration errors
into200 TS packets. Do not emulate original resource lifecycle by delaying only
an already-released HTTP error response. A future immutable Rust image must be
retested against these wire and Redis transitions before accepting parity.
