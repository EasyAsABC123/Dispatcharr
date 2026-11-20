# 8a821e27 Compose coverage

Original Dispatcharr authors and contributors retain full application credit.

Fifteen of 127 required coordinator nodes passed on the exact `8a821e27`
application package. The Compose jobs covered fresh install, the shipped
operator, stored credential upgrades, proxy authentication, current programme
and Grid reads, provider actions, cached provider information, OutputProfile,
the provider browser workflow, the Main integration suite, the live React
Stats workflow and three shared MPEG-TS workflows. Each counted check used a
fresh execution with independent evaluation and cleanup.

Main passed 1,024 checks in 18 suites and decoded 75 MPEG-TS frames. Both
React browser arms showed advancing playback and HTTP 200 Stats with all
112 shipped assets verified. The provider scopes passed 150 action checks
and 90 cached-info checks. OutputProfile passed 130 packaged checks.

The first six-check launch stopped before Docker because the controller's
interpreter path differed from the one sealed in its arguments. The first
provider/profile attempt reached workload exit 0 in three checks but both
its evaluators and cleanup callbacks failed in an isolated Git context. A
separate inventory found no remaining fixture containers, networks, or
volumes. Only the clean, corrected attempts count here.

Full parity, release rehearsal, and current performance remain open. The
15 results apply to `8a821e27`. [Exact Compose receipt](../scripts/rust-integration/milestone-batch113/8a821e27-coordinator-coverage.json)
