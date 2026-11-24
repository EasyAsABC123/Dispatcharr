# Current AMD64 retention and native Core checks

The canonical dispatcher ran three ordered stream checks on the same `2d3ffe53` ordinary package. Shared TS and worker-outage recovery passed before the 100-reader retention check. Across ten channels, 100 late readers each received 1,500,000 bytes after 45 seconds of history. Ten captured prefixes matched their expected bytes and decoded audio and video. The test enforced 4 MiB local and 16 MiB authoritative history limits per channel and opened no additional upstream connection. The dispatcher verified teardown for all three rows. [Redacted retention and Core proof](../scripts/rust-integration/2d3ffe53-amd64-retention-and-core.json)

The separate current Native Core gate passed 51 cases and 290 controls across system settings, network access, reverse proxy auth, proxy settings, stream settings, and DVR settings. The compiled component gate consumed six pinned Original reference cohorts and preserved the exact SQL, Redis and cleanup checks. Ordinary Main and React settings workflows remain separate release rows.
The independently reconciled matrix now records **13 current executions, 4 retained Original authorities, and 110 open rows** for this exact package. [Current 127-row snapshot](../scripts/rust-integration/2d3ffe53-amd64-release-matrix-retention-and-core.json)

Dispatcharr’s original authors and the authors of its dependencies retain full credit. [Attribution](../ATTRIBUTION.md)
