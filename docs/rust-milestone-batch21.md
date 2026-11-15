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

Docker build and package verification now pass for `ac9d704943e69b9c343f31063cda1e3df5008791`,
image `sha256:421540af94b7efa8e403a982efd73fe1afee2d33677c86b2f87885d6b97440ae`.
The1,789-file source archive matches Git, including495 React inputs. All1,298
SDK inputs match committed blobs. Package checks cover original credit/asset
retention, crypto, PostgreSQL,648 actual Python/regex files/notices and the
paired VLC useful audio/video core.
[Package evidence](../scripts/rust-integration/milestone-batch21/package/)
retains the exact scope; full finite tails and real Main acceptance remain open. The next
cohort uses corrected Source policy expectations, an explicitly bound real
Source task producer, compact/manual cells and input-profile playback. Ignored
physical controls, full M0–M9 milestones and application performance gains remain
unaccepted.

Three explicitly scheduled [physical regex controls](../scripts/rust-integration/milestone-batch21/regex-physical/scope.json)
pass inside the exact candidate image: grammar/error recovery and normal wait/EOF,
actual CPU-active held-search ownership through forced retirement, and an
injected pre-ready stop. The host-compiled test controller and its read-only ELF
dependencies are identified separately from the packaged Python actor. The first
attempt exits127 before tests because its controller mode0700 denies UID65532;
a byte-identical0555 controller copy corrects that fixture issue. Both attempts
clean up exactly. Expected Drain/error outcomes remain errors. These controls
do not establish real Main held-search cancellation or full business parity.
