# Source6 observed timeshift behavior

The Source-only batch consumed its real controller exit **1** and independently verified cleanup of **3 exact containers and 2 original host process generations**. It retained six scenario observation sets containing **12 attempted media requests and 12 responses**. **Zero scenarios are fully qualified**: the helper required an `X-Worker-ID` response header that was absent from all twelve responses. These results do not close a migration milestone or establish Native parity or performance gains.

The original Dispatcharr and dependency authors retain all application, API and algorithm credit.

The public [typed observations and provenance](rust-source6-observed.json) contain no JWTs, credentials, private request headers, response bodies or copied wire files. Hashes identify retained local evidence; the local raw artifacts are not part of this public summary.

| Scenario | Actual responses and retained prefixes | Profile counter sample after bodies close for five seconds | Sample after independent web/peer retirement |
| --- | --- | --- | --- |
| Idle stored profile after default change | 200 / 8,192 bytes; 200 / 8,192 bytes | A: `"0"`; B: absent | A: `"0"`; B: absent |
| Same ID live programme hop | 200 / 8,192 bytes; 200 / 8,192 bytes | A: `"0"`; B: absent | A: `"0"`; B: absent |
| Cross-worker idle same ID | Port 5656: 200 / 8,192 bytes; port 5657: 200 / 8,192 bytes | A: `"0"`; B: absent | A: `"0"`; B: absent |
| Cross-worker busy fingerprint without ID | Port 5656: 200 / 8,192 bytes; port 5657: 200 / 8,192 bytes | A: `"0"`; B: absent | A: `"0"`; B: absent |
| Same ID after actual public pool loss | 200 / 8,192 bytes; 200 / 8,192 bytes | A: `"1"`; B: absent | A: `"1"`; B: absent |
| Simultaneous first open with same ID | Port 5657: 200 / 8,192 bytes; port 5656: 503 / 16 bytes | A: `"0"`; B: absent | A: `"0"`; B: absent |

Counters are literal Redis string samples; “absent” means the captured value was null. In the public-pool-loss scenario, profile A was `"1"` after the first body and `"2"` after the second body. The retained `"1"` samples after body closure and worker retirement do not prove a permanent leak or a future automatic release. The concurrent scenario's client barrier and 200/503 outcome do not prove which internal scheduler race occurred.

The bounded observer successfully read both independently launched uWSGI master/child families as UID 65532, retaining executable, PID/starttime, process group and disjoint-family evidence before and after requests. The SQL observer retained UID 0 and private file custody. Request destinations and pinned configurations identify the declared 5656/5657 routing. Those facts do not establish the exact process that served each request or ownership of each listening socket inode. A subsequent authorized read-only socket inspection found that the Source container had already retired; no socket proof was invented, and no application calls were replayed.

Every scenario retains all 61 SQL tables and 59 literal sequence states at its boundaries, full bounded cache snapshots, provider/archive observations, response prefixes, Channels observations and complete journal custody. The original controller status and failed header condition remain unchanged. These captures can support a narrower offline behavior review; any claim about exact serving workers or listener ownership remains open. No Source6 requests should be replayed merely to turn the existing helper verdict green.

Evidence anchors:

- Controller and independent cleanup: `artifacts/rust/m4-timeshift-six-durable-Root-execution-1/`.
- Scoped qualification: `artifacts/rust/m4-timeshift-six-source-qualification-1/qualification.json`, SHA-256 `ac0b8b0edcfa86397be9c7c4c8afa249249536c93c40c282a2a0727770d0c7af`.
- Immutable Source driver closure: `14ce19c7f88f23e842e0b3a2bca4f565e5351d264dbfbec99c618350c9a01829`.
- No earlier SD4 or SD21 business calls were replayed; this batch selected zero Native contexts.
