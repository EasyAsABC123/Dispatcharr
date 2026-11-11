# Six default families: Source-first inventory

Original Dispatcharr/Celery authors retain application, algorithm and protocol credit and licenses. No default consumer, scheduler intake, Main wiring or new Cargo dependency is introduced by this component.

| Family | Captured baseline | Remaining execution scope |
|---|---|---|
| `celery.backend_cleanup` | Actual Redis result backend cleanup is a no-op; existing owned result bytes/TTL survive; task returns null | Other backend types are unsupported |
| `apps.proxy.tasks.fetch_channel_stats` | Six real worker cases: empty, live client, ghost cleanup, wrong-type metadata, invalid buffer index and failed group send | Dynamic clocks/other malformed metadata and broader capacity/fault domains need separate gates |
| `core.tasks.scan_and_process_files` | Empty owned `/data/{m3us,epgs,logos}` with copied sources inactive: null, no child, event or recording effect | Actual M3U/EPG/logo imports, first-scan rebuild descendants, filesystem faults and skip/mtime rules |
| `apps.channels.tasks.maintain_recurring_recordings` | Disabled copied recurring rules: exact integer0 and no recording effects | Enabled rule synchronization/deletion/recording behavior; this is not Series evaluation |
| `core.tasks.check_for_version_update` | Actual body with explicit HTTP dependency route to an owned loopback503: null, one request, no event | Real upstream availability, update comparisons and notification mutation branches |
| `apps.m3u.tasks.check_account_expirations` | Copied inactive profiles plus one stale warning: null, stale row deletion and actual dismissal event | Active profile warning/expiration/date/notification updates |

`m6-original-default-stats-worker-2` passes65 checks. `m6-original-default-effects-worker-1` passes93 checks, including those65 and five additional actual task executions. Worker observation, typed results, actual Redis TTL and queues/reservations are captured. All11 tasks executed are explicitly owned. The version route adapter changes only the external HTTP dependency, preserves the task body and never contacts an external server. These baseline branches do not establish complete parity for all six families.

The first stats capture used the wrong singular client-metadata key and therefore characterized another ghost instead of a live client. It remains retained as a weaker64-check historical capture. The second capture uses Original's exact plural `clients:<id>` key and explicitly asserts the live hash/client survives with count1.

The pinned settings explicitly disable the old `fetch-channel-statuses` task. No Python caller queues this task when viewing. Current live/statistics events use direct producers already covered by the M7 reports. `core.tasks.fetch_channel_stats` is a separate legacy helper called by `beat_periodic_task`; it must not be confused with the configured `apps.proxy.tasks.fetch_channel_stats` task. This work adds a narrowly known task compatibility lane, not a reason to enable periodic polling.

The native fixture worker reuses `StreamEngine.global_live_statistics`, the bounded `ClientStore` catalog and the existing `RealtimeBus`. An additive retained scan API transfers a task guard into each actual catalog Lua future; legacy methods and scanner semantics are unchanged. The event wrapper reconstructs Original's dictionary insertion order and no-positive-init integer0 branch before Python-style spaced/ASCII JSON rendering. Raw `stats` strings and complete event envelopes are compared exactly, rather than comparing normalized parsed JSON.

Task completion waits for a drop receipt from the retained event attempt. A logical sink error may precede its physical Redis reply; the worker cannot publish a task result before that owner settles. Source's actual failed group-send path returns normal null. Native admission/representation failures remain operational. Build errors do not become fabricated SUCCESS: the shared scanner has native capacity/error categories that have not all been matched to Source's broad caught-read behavior.

Publication, bounded FIFO peek/CAS and null result writing are test-only, Fixture-authorized helpers. Index/Refresh/Series authorities are not widened; unsupported queue heads remain unchanged. The fixture dependency bridge runs the actual native implementations in a private process and waits through their retained physical operations. It is a test bridge, not an application architecture or a second scanner service. Production constructor, driver and application attachment remain later gates.
