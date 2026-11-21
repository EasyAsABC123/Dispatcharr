# d608 six-minute owned soak memory attribution

The same packaged Native ran six minutes with ten viewers, one shared
encoder and the +300-second XMLTV refresh. The unchanged idle gate
passed. Working memory grew by 26,927,104 B and
met the 67,108,864 B memory limit. The controller
exited 0, and the tracked runner exited
0. All 20 scoped clips passed decoding. All five owned container roles retired.

The [earlier failed run](rust-m9-d608-six-minute-soak-prefix.md) gained 74,117,120 B in
working memory from +90 to +95 seconds while `memory.current` fell
5,545,984 B and `inactive_file` fell 79,663,104 B. Across those same offsets
in this replay, working memory changed by −1,028,096 B,
total charge by −1,028,096 B, anonymous charge by
+0 B, file charge by +0 B, and
kernel charge by −4,096 B. Active file changed by
+0 B and inactive file by +0 B.
The receipt records the full sampled categories and the replay's largest
upward step separately.

Native's own idle working-memory median changed by +8,548,352 B.
Its median anonymous charge changed by
+1,531,904 B, file charge by +54,652,928 B,
inactive-file charge by +48,377,856 B, and
kernel charge by +430,080 B. The same
Native Main was the only persistent Native process at those sampled idle
points. All three observations per idle window completed. PID1 median anonymous PSS changed by +1,536,000 B, file PSS by +4,853,760 B, and shared-memory PSS by +0 B.

Cgroup charge categories and proportional process mappings have different
accounting. This replay does not identify live heap ownership, prove an
allocator leak, or close comparative performance or full M9.
[Linux cgroup memory accounting](https://docs.kernel.org/admin-guide/cgroup-v2.html)
[Linux procfs mapping accounting](https://docs.kernel.org/7.3/filesystems/proc.html)
[Redacted receipt](../scripts/rust-integration/m9-d608-six-minute-soak-memory-attribution.json)

The original Dispatcharr and dependency authors and contributors retain
full credit. [Attribution](../ATTRIBUTION.md)
