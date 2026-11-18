# Functional leases

Original Dispatcharr and dependency authors retain application and algorithm
credit. `functional_lease.py` provides one public lock helper for existing batch
owners. It starts no work, creates no worker pool and grants no performance
acceptance. The global coordinator continues to use an exclusive lease; its
three-slot scheduler, resource upgrades, dependency order, output bounds and
physical cleanup rules are unchanged.

Default behavior is exclusive. A missing or literal-false `functional_parallel`
field returns no backend lane. The optional value `source-native-pair-v1` requires
exactly one nonempty `source_contexts` or `native_contexts` list and
`base_binding.performance` literally false. It returns `Original` or `Native`.
The coordinator CLI does not opt into this mode, even if its binding document
contains that field. No existing owner is switched by this change.

After actual two-fixture namespace/resource qualification, a future owner may
call the public helpers exposed by the coordinator:

```python
backend = coordinator.functional_backend(plan)
with coordinator.fixture_lease(primary_path, backend) as admission:
    # Keep the existing real launch, actual wait/EOF/group retirement and
    # independent exact-fixture cleanup inside this block.
    # An admission is None for default-exclusive mode.
    if admission is not None:
        admission["cleanup_verified"] = physical_cleanup_verified
```

`physical_cleanup_verified` must come from the owner's actual consumed child,
process-generation and exact fixture cleanup receipts. It is not the business
pass flag. Failed business work with confirmed physical cleanup can release a
clean lane; a passed controller with missing EOF, descendants or unknown cleanup
cannot. The helper requires the literal True. The durable service/tool terminal
still must be consumed by the existing owner before any replacement dispatch.
Do not invent a successful receipt or clear uncertainty merely because a PID is
absent.

An opt-in owner holds a shared lock on the same canonical PRIMARY and an
exclusive `Original` or `Native` slot beside it. At most one of each backend can
be active. Exclusive owners still exclude both lanes. Use the same canonical
PRIMARY across independent owners. Nested Root and support processes need
separate Root/support lock paths; attempting to acquire the same backend slot
twice through distinct file descriptions is a real conflict, not reentrancy.
Keep contexts within one fixture serial when they share Redis FLUSHALL, database
templates, media symlinks, providers, startup roots or mutable application state.

A paired owner writes and fsyncs a pending marker before its body can start work.
It records PID/starttime/boot identity, a unique lease ID and the held lane's
file identity. The other backend may join only when that pending owner still
matches a live process generation and its lane is actually held. Same-backend
entry still fails at the kernel lock. Dead, stale, malformed or explicitly
uncertain records block later shared and exclusive helper admissions, including
the global coordinator's quiet entry. A live PID without its old lane is not
joinable. The earlier single-marker name also remains an interlock.

Literal-True physical cleanup removes only the exact own pending record, then
fsyncs the directory before releasing either lock. A changed lease identity is
preserved and refused. Otherwise the record becomes explicitly uncertain, which
blocks new owners even while its process is still alive. Original and Native
have separate records so simultaneous failures cannot overwrite either owner.
A SIGKILL or os._exit skips Python finalizers but cannot skip the already fsynced
pending marker; kernel lock release does not permit helper or coordinator reuse.
There is no timeout or automatic stale-marker removal. Root must independently
establish cleanup, persist and consume that receipt, and remove only the scoped
record. Both records need resolution if both owners failed cleanup. The existing
durable service/tool interruption audit remains required after hard kill/reboot.

Before enabling a pair, update every participating functional and quiet owner to
use this helper; a historical direct flock owner ignores marker safety after the
lock releases. Verify actual nonoverlapping Docker networks/subnets, published
ports, PostgreSQL/Redis endpoints, mounts, project names, database names, provider
and media state. A unique Compose name alone is insufficient. Real startup and
per-context reset/teardown evidence must remain in the existing affected matrix.
Do not replay qualified Source business calls just to test concurrency.

The opt-in guard currently requires12GiB MemAvailable and8 allowed CPUs. Those
checks are conservative admission checks, not reservations or cgroup quota
proof. Account for actual container caps, controllers, Docker, ancestor CPU,
memory/PID limits and resource peaks in the paired fixture smoke. No compilation
or unrelated heavy work may overlap that functional pair. Performance/quiet
phases stay exclusive, after both owners and their cleanup have actually settled;
functional overlap supplies no CPU/memory/latency/throughput comparison.

Run the pure tests without applications:

```sh
python3 -B -m unittest discover -s scripts/rust-milestone-batches -p test_functional_lease.py
python3 -B -m unittest discover -s scripts/rust-milestone-batches -p test_coordinator.py
```

The new tests use temporary directories and actual parent/child kernel locks.
They preserve the original eight lock/admission checks, add direct coordinator
legacy/interlock coverage and simultaneous-owner marker preservation. Resource
sizing is mocked only inside temporary kernel tests, then separately checked at
its actual production thresholds. No test opens canonical PRIMARY or starts an
application, Docker fixture, Source request, Cargo build or benchmark. Existing
coordinator tests continue to cover scheduling, held descendants, bounded output
and process cleanup. Passing these tests does not qualify real fixture isolation
or authorize enabling the opt-in mode.
