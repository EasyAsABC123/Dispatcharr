# Media paths, compact numbering and input profiles

Original Dispatcharr and dependency authors retain full application, algorithm
and license credit. The existing React frontend is preserved.

[Candidate20](rust-milestone-batch20.md) starts through actual Main, but its
metadata spool rejects the supported `/app/media -> /data` deployment path.
This candidate resolves the configured existing parent once before creating
private spool files. Existing private permissions, no-follow file creation and
inode-guarded cleanup remain. A real filesystem regression retargets the media
alias after admission and verifies cleanup stays at the admitted physical path.
The earlier runtime timeout's precise failing stage was not captured; bounded,
opt-in auto-sync stage diagnostics support the next application run without
printing provider configuration, request bodies or credentials.

Normal auto-sync now performs compact numbering through its existing owned SQL
session. The manual group-repack endpoint and single-channel hide/unhide hooks
share that implementation and its task ownership. Nested savepoints preserve
Source rollback behavior; busy work defers rather than borrowing another pool.
The wider API surface and unsupported numeric/sort domains remain open.

Input profiles follow Source selection for HLS, RTSP, RTP and UDP inputs: the
Proxy profile promotes to the actual configured FFmpeg profile when required.
Custom profiles retain Source command construction and user-agent precedence.
Captured Source vectors cover selection; real multi-viewer playback, failover
and retirement still require the combined Compose cohort. Shared output-profile
work and wider redirect behavior are separate, uninstalled work.

All four SDK gates pass: formatting, workspace compilation, strict Clippy and
1,029 passing tests, zero failures, 761 ignored tests across175 result blocks.
The [evidence](../scripts/rust-integration/milestone-batch21/inventory.json)
binds1,298 inputs and17 changed source paths. The preceding attempt failed to
compile because a SQL row stream borrowed a temporary formatted query string;
retaining the string through stream drain corrects that compiler error.

Docker package verification and real Main acceptance remain pending. The next
cohort uses corrected Source policy expectations, an explicitly bound real
Source task producer, compact/manual cells and input-profile playback. Ignored
physical controls, full M0–M9 milestones and application performance gains remain
unaccepted.
