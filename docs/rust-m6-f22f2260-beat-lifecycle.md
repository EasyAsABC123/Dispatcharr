# Beat startup and shutdown on the packaged Rust application

Exact Rust application `f22f2260dae5ae67293c785a54e558ea24103918` and image `sha256:4dcadb57bdca7aec9f5fa110ce91664ac41a5a4ac283d0c80abf3436fb9ca48b` passed the two
isolated Compose campaigns below. Original Dispatcharr and the Django,
Celery, django-celery-beat and dependency authors retain full credit.

- Normal startup and stop used one ordinary scheduled Series task. Original
  Beat and Native Main both exited 0 on normal stop. A separate probe built
  from the same Rust application verified that already-admitted default and
  plugin initializers drain. PostgreSQL, the provider, the tested processes
  and their owned containers retired. Proof SHA-256 `da882b2669165a0779051730666c9f6caad234d83eb3caca9f0f4218de41482a`.
- A separate Native Main run held a copied-state schedule row while the
  real default initializer waited in PostgreSQL. Main and the same SQL
  session remained owned after SIGTERM. The fixture released its
  transaction and Main exited 0 after six default schedule writes. HTTP
  stayed unavailable throughout the measured interval, and both owned
  SQL sessions retired. The queues and result store remained empty.
  Proof SHA-256 `9d76ab0d71162d3fa0762aeb1212c8e0f752e5da01eb2162b01224c4e072a26c`.

The first preready fixture exited 1 when its host observer could not write
to the copied state's container-owned lock marker. Independent retirement
verified three containers, one network, and thirteen process generations.
The fresh run used a guest-side marker write and is the qualification
reported above. The first ordinary Beat campaign exited 1 after both
scheduler processes stopped cleanly. Its separate probe had a host-side
permission error when installing the executable. The fresh run reused the
same verified executable and installed it through the owned guest.
Both first-run project inventories were independently empty.

The campaigns used Selected Jobs and Beat with copied test state. The
complete default worker graph, rollout, and comparative performance remain
open. [Redacted receipt](../scripts/rust-integration/m6-f22f2260-beat-lifecycle.json)
