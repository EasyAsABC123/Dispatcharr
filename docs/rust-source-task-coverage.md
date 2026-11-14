# Source task coverage beyond the selected default graph

Original Dispatcharr and Celery authors retain application and protocol credit.
The pinned Source revision is
`1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`.

The [reproducible static inventory](../scripts/rust-job-contracts/source-task-declarations.json)
finds47 top-level task-decorated functions outside test files. It records exact
Source paths, lines, file hashes and literal decorator options. Root reproduced
the JSON byte for byte from Git without importing Source or running a service.
This is not the runtime registry, an implementation count or a passing coverage
gate: aliases, dynamic registration, nested definitions and required framework
workflows still need an executable crosswalk.

The existing default-dispatch proposal selects14 families, including framework
backend cleanup. It includes13 of those47 application declarations. Completing
that selected graph alone cannot establish full application coverage. Additional
declarations include Core rehash/offset rescheduling, backup/restore, channel
matching/bulk actions, Schedules Direct guide fetches, M3U account/profile actions,
plugin repository refresh and VOD batch/metadata cleanup.

For each declaration, map its actual natural producer or callback to the Rust
production entrypoint, required effects, existing Source authority and acceptance
cohort. A missing registration is distinct from a missing algorithm; some native
HTTP/service components already implement portions of these workflows. A
decorated helper such as Source's `_safe_name` is not proof of a natural queued
workflow. Preserve behavior without requiring Rust to reproduce an unused Celery
internal interface. Record unused/direct-only cases with their actual callsites,
and include dynamically registered or framework tasks when application workflows
depend on them.

Core99 already demonstrates two required natural producers outside the selected
graph: `core.tasks.rehash_streams` and
`apps.channels.tasks.reschedule_upcoming_recordings_for_offset_change`.
Their actual envelopes are observed but downstream execution is unqualified.
These heads must not disappear from acceptance merely because the selected
dispatcher enum does not contain them. Keep M0 executable coverage, M6 and final
parity open until this crosswalk and the required workflows pass.

```sh
python3 -B scripts/rust-job-contracts/source_task_inventory.py \
  --output /absolute/path/to/fresh-inventory.json
```
