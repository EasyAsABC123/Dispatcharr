# Proxy health settings consumer

Original Dispatcharr authors retain full credit for the application and its
behavior. React is unchanged. This component does not close M2, M3 or M5.

The new Source cohort used the pinned Original application, one network-isolated
Docker Compose bootstrap, one owned database clone and one persisted-key PATCH.
It did not replay the previously captured Core settings or OutputProfile cases.

| Actual Source call | Startup / connection / initialization seconds |
| --- | --- |
| Cold TSConfig read | 8 / 10 / 60 |
| BaseConfig read | 8 / 10 / 60 |
| BaseConfig after PATCH 200 | 3 / 4 / 5 |
| TSConfig immediately after PATCH | 8 / 10 / 60 |
| TSConfig after actual cache age 10.275289 s | 3 / 4 / 5 |

The first TS read owned a distinct subclass cache. Its Redis GET reply was held
for 2.400140 s; the Source timestamp preceded that load. The final call expired
that timestamp while still being less than ten seconds after the initial call
completed. This distinguishes Source's pre-load timestamp from a completion-time
TTL. The earlier Base-first cohort remains authority for inherited Base hits.

The same bootstrap captured 18 StreamProfile argument cases and six owner-lock
operations. These establish model/Redis behavior, not FFmpeg/VLC execution or
shared profile delivery. All eight cleanup stages passed, including default SQL
and cache isolation, owned backend absence, normal process stop, empty Compose
inventories and retirement of 26 CLI processes and the baseline container.

The Rust implementation copies three health durations from the shared Core
context, drops the raw settings read, and reads the values at real engine health
decisions. Its TS snapshot follows the measured independent/inherited cache
semantics. It uses the existing admission tokens and pools. Main constructs the
Core context before attaching EngineControl to avoid a reference cycle, then
drains that context after stopping the engine.

`RUST_DYNAMIC_PROXY_HEALTH_ENABLED=1` exercises this migration capability. It is
temporarily off by default because negative durations, nonnumeric settings and
physical read failures are not qualified by this numeric cohort. Those values
are explicit errors; they are never silently converted into default health
settings. An error stops the opted-in session. That failure policy is not claimed
as Source parity. Removing this switch requires the exceptional domains and live
Main settings-to-stream workflow to pass on the common candidate.

Input/output retention freshness, public Core write integration, actual media
health transitions and whole-service performance remain required milestone work.
No performance gain is claimed by this change.

Evidence: `artifacts/rust/m235-new-source-cohort-2/application/` contains the raw
capture, `Source-domain-qualification.json` and `execution-private.json` with
actual controller exit 0. The earlier preflight failure is retained separately
under `m235-new-source-cohort-1`; no application started in that attempt.
