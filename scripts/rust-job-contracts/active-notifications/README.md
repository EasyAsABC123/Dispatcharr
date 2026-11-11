# Bounded active-account expiration task subset

Original Dispatcharr, Django, Celery, Channels, redis-py and related dependency authors retain code, algorithm and license credit. The pinned Original task is `apps.m3u.tasks.check_account_expirations` at revision `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`.

This adds a normal inert library constructor, `KnownDomains::with_active_notifications(pool, existing_update_sink)`, and an injectable per-evaluation `NotificationClock`. It starts no consumer. Existing `KnownDomains::with_notifications` keeps its inactive-only authority and behavior. The existing task enum spelling is preserved for API compatibility. No Main, default intake, Cargo, lockfile, transport or worker changes are selected.

The bounded domain admits zero or one eligible stored profile, profile name at most 237 characters, account name at most 255, UTC dates in years 1970–9999, 128 collector notices and 1024 global dismissals, with four shared retained domain admissions. Multiple eligible profiles, out-of-domain title/date/row bounds and unrelated task families refuse before ACK. This is not full task-family or M6 acceptance.

Opposite-key deletion, upsert and final stale deletion retain the Original's separate commit boundaries. Django's application-level child-before-parent cascade is explicit. Native retains caller ownership through SQL replies, transaction rollback/close and the existing UpdateEventSink's actual drop receipt. Normal caught Channels Unavailable returns null after physical settlement; unproved capacity, invalid sink, SQL and uncertain work stay operational. The Source's process-local pre_delete receiver is captured with empty worker-local manager maps; no effect in a separate HTTP process is inferred.

Source-only: 219 checks/17 actual prefork worker cases. Paired production path: 314 checks/17 cases plus seven physical controls. Thirteen normal cases compare exact stored rows and Channels payload/order, including new-notice defaults, Unicode, updated content and preserved creation/source/expiry/dismissals. Three SQL fault cases compare exact committed row/event prefixes but intentionally retain native PENDING/operational instead of inventing Source Python FAILURE metadata. Source title overflow is explicitly unsupported before native ACK. Normal result UUID/date/TTL fields are validated in their own clock/identity domain, not forced equal across executions. Row snapshots use predeclared ID ordering; events are compared in actual delivery order without post-diff sorting. Profile IDs, notice/dismissal IDs and sequence state are restored for exact comparisons.

The dependency clock is fixed only for explicitly owned Original tasks; no task/helper/result implementation is replaced. Actual Celery result timestamps use the real worker clock. Actual active profiles are seeded through owned copied-data bulk writes, so profile save-time parsing/signals are a different, unqualified boundary. Source worker code runs in the pinned Original image; no external collector/network/version work is performed.

Reproduce after the selected immutable Git blobs are published:

```sh
. artifacts/rust/tooling/environment.sh
python3 scripts/rust-job-contracts/active-notifications/test_reconstruct.py
python3 scripts/rust-job-contracts/active-notifications/reconstruct.py --repo . --output /tmp/active-notification-source
python3 scripts/rust-job-contracts/active-notifications/build_peer.py --source /tmp/active-notification-source --target /tmp/active-notification-target --output /tmp/active-notification-build
python3 scripts/rust-job-contracts/active-notifications/source/run_pair.py --repo . --output artifacts/rust/active-notification-source-rerun --support-root scripts/rust-core-perf/support
python3 scripts/rust-job-contracts/active-notifications/native/run_native.py --repo . --output artifacts/rust/active-notification-paired-rerun --native-binary /tmp/active-notification-build/native-default-peer --support-root scripts/rust-core-perf/support --resources
```

The exact reconstruction is base `2fbeabcd69802ef82c7dcb2be277d622e6eba7df` plus twenty immutable blobs, yielding 713 Rust/Cargo/dataset inputs: the earlier qualified expiry711 source plus five owned active-module overlays (two new paths). It deliberately excludes the separate Root UUID union. No dirty source or private fallback is accepted. The public builder records compiler JSON, compiler versions, full source map and an outside-target executable. The committed support tree is explicit.

The actual aggregate-exit negative control uses `--resources --resource-only --force-resource-exit 27`; after the seven physical tests pass the controller must exit 1 and retain `passed:false`, `owned_forced_resource_exit:27`, audits and cleanup. Do not interpret this expected failing run as a positive task result.

Failed attempts are retained: native4 completed 314 functional checks but timed out in a new physical fixture; its aggregate stayed false and cleanup/audit passed. Resource5 revealed fixture profile-name uniqueness and accidental `active_resources` substring matching `inactive_resources`; native6 corrected only fixture/test selection and passed six physical tests. The final suite adds the seventh four-admission cancellation case. No production runtime patch was needed for those fixture failures. Early partial logs from timeout4 were not streamed; resource5 onward streams diagnostics before subprocess completion.

Remaining: multiple-profile ordering/failure prefixes; concurrent row/key changes; profile-save input parsing; larger name/date/budget domains; same-process nonempty proxy managers; exact Source failure-result rendering; active recording and other default families; scheduler/application/Main/default intake integration. No resource-performance result or gain is claimed.
