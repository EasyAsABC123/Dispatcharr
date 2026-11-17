# Candidate59: root-artwork publisher socket readiness

Original Dispatcharr, Django, Celery, DRF, redis-py and redis-rs authors retain all
application, model, task, protocol and algorithm/code credit. React and the
original license remain unchanged.

[SDK97](../scripts/rust-integration/milestone-batch59/SDK/index.json) passes all
four actual gates: formatting, workspace check, strict Clippy and workspace
tests. Results are **1,214 passed, zero failed, 792 ignored**, across 180 result
blocks. All 1,461 inputs, including 1,455 Rust files, remained unchanged during
testing. Five Rust paths differ from SDK96. The tested repair is published as
`d6bf6787ad1ad504f9da5ce4fb30e53ee95ebcce`. Its Docker build and all six package families pass; the rebuilt application
workflow remains pending. Workspace-ignored integration controls are qualified
separately by the actual physical gate below.

Candidate57's natural automatic MARK recording now passes, including both task
terminal results. Its next natural CUT booking returned 503 after recording,
clocked schedule, periodic task and task_id SQL writes had committed. No artwork
publication appeared in its complete scoped DB1 journal. The prior artwork
producer client had been idle at least 540.654 seconds; pinned Source config uses
Redis idle timeout 300, also independently observed in the same fixture family.
The physical error kind behind that original 503 was not captured. A post-commit
SQL settlement error remains another possible origin; the original failure is
preserved rather than reclassified as a proven disconnect.
Pinned Source's recording post_save signal catches/logs scheduling and artwork
publication exceptions. Rust still reports503 for an unsettled publication.
This repair targets normal socket readiness; caught-fault HTTP behavior and
ambiguous-publication/SQL-fault parity remain explicitly unqualified.

The bounded repair gives the root-artwork producer a fixed-response read-only
PING before its unchanged single PUBLISH Lua invocation. It reuses the qualified
result preflight's owned physical task, caller guard, admission permit and
existing deadline. Only a settled dropped-socket PING may repeat once using the
manager's replacement connection. The mutation is never retried. Other publisher
paths, generic commands, Lua bytes, task envelopes and Redis settings remain
unchanged. Existing capacity-only backpressure remains intact.

The existing diagnostics opt-in adds a fixed artwork_publication scope with
stage, kind, elapsed time, physical pending state and typed task UUID. It shares
the existing 64-record cap; no keys, payloads, URLs, credentials or error text are
logged. Physical returned means a Redis future observed a reply/error, not proof
of remote mutation or all-owner settlement. PING and publication are traced as
one operation, without subcommand-stage identity.

Three added real-Redis controls cover an exact owned socket closure, loss of the
publication reply without replay, and a held PONG beyond the logical deadline
while owner/admission stay retained. Each checks exactly one LPUSH frame after
settlement. The [actual SDK97 Redis gate](../scripts/rust-integration/milestone-batch59/physical9/index.json)
passes all nine controls with zero failures: **19.057 seconds for the complete
Root wrapper**, **18.943 seconds for the inner gate**. Root consumed actual exit 0;
Cargo output confirms 9 passed. All bound and source inputs remain unchanged,
and independent verification finds the exact container, original host generation
and project containers/network/volumes absent. This proves the component's closed
socket, no-replay and ownership behavior; it does not establish the original
CUT503 cause or a successful rebuilt Main recording workflow. No new Cargo target,
application fixture hierarchy or full source clone was needed.

The [Candidate58 Native12 management proof](../scripts/rust-integration/milestone-batch58/management-positive/index.json)
is retained as actual image 58 evidence, not attributed to Candidate59. Its
positive workflow passes, while full Source row/cache/clock and broader domain
comparisons remain open. A new built-image 16-context batch must cover the header
observer repairs, ten-root provider lifecycle and both natural recording modes without
replaying Source business cases. All milestones M0–M9 remain open. No application
CPU or memory improvement, full transport identity or full backend parity is
accepted by these compile and component results.

The [Candidate59 build](../scripts/rust-integration/milestone-batch59/build/index.json)
passes in 322.523 Docker seconds / 322.963 seconds for the complete wrapper.
Its image is `sha256:c110fe52e5a30e72e820b665339b416779f07bd425a1f236c6bf0fcfc8bb6e01`.
All [six package checks](../scripts/rust-integration/milestone-batch59/package/inventory.json)
pass in 27.032 seconds, with 55 exact containers independently absent.

The prepared application batch remains unrun. Static ownership review then found
a separate due-recording producer manager that can also idle during the long
recording; immediate named recording publication has the same readiness debt.
These are predicted risks, not observed Candidate59 failures. The next combined
candidate adds their bounded preflight and the separately characterized channel/
guide writes before running the expanded affected application batch. Existing
broker/notification/cache manager gaps remain in the
[readiness audit](rust-redis-client-readiness-audit.md). No old performance gain
is transferred to this image.
