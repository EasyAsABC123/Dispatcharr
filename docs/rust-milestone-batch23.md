# Profile demand keeps a remote follower alive

Original Dispatcharr and dependency authors retain full application, media,
algorithm and licensing credit. React remains unchanged.

[Candidate22](rust-milestone-batch22.md) fails its real two-replica profile test:
the remote stream ends before its first profile frame. When temporary raw
subscription setup ends, the follower has no ordinary raw viewers. Its idle
cleanup then stops local clients, including the newly admitted profile client.

This correction retains bounded internal profile demand for the existing reader
and delivered frames. It reuses the reader permit and adds no ordinary client,
user slot, provider reservation, media queue or extra process. Idle retirement
checks ordinary raw viewers and internal profile demand. Final release uses the
existing fenced cleanup. Diagnostics and regression assertions require exactly
three actual clients for one ordinary viewer plus two profile viewers, one
provider/upstream/process, held-frame lifetime and selective follower retirement.

All four SDK gates pass: formatting, compilation, strict Clippy and1,034 tests,
zero failures,764 ignored across176 result blocks. The
[evidence](../scripts/rust-integration/milestone-batch23/inventory.json) binds
1,302 inputs and three corrected source paths. Actual Redis regression execution,
Docker package checks, combined Main acceptance and performance remain pending.
No full milestone or application performance gain is accepted.
