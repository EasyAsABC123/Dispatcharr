# Current programme reads without an EPG file

Original Dispatcharr and its contributors retain full credit for the React workflow, API, models, and guide selection behavior. This Rust change preserves the existing React request.

Candidate117 matched all six real-clock administrator POST requests to `/api/epg/current-programs/` in owned Docker Compose fixtures against Original `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`. The Source-only predecessor, candidate package, and paired test each exited 0. Both implementations returned HTTP 200 with the same JSON media type, content length, and body bytes in every case.

| Request | Matched result |
| --- | --- |
| Source-less EPG entry | Empty array |
| Schedules Direct entry without a current airing | Empty array |
| XMLTV entry with a null TVG ID | Empty array |
| XMLTV entry with an empty TVG ID | Empty array |
| All four entries in one request | Empty array |
| One currently stored programme and all four misses | The stored programme only |

The ordinary Rust Main deployment ran with guide refresh and selected jobs disabled. Source startup broker frames stayed byte-for-byte in order after each request. The candidate shut down with exit 0 and independent cleanup found zero remaining project containers, networks, or volumes. The exact Rust snapshot also passed formatting, application binary checks, six Store current-programme tests, and strict Store Clippy.

The tracked `scripts/rust-epg-contracts/control_reads.py` driver enables this six-case scope with `--candidate-current --current-proven-empty`, an exact candidate image and build manifest, and a fresh output directory in the owned validation environment. The [redacted evidence](rust-m7-current-programme-no-file-evidence.json) records image and build identity, exact checked source hashes, cleanup and each response digest.

File-backed misses with a real TVG ID still require separate byte-offset lookup coverage. The six cases qualify the administrator scope only. They establish no performance result or complete M7 verdict.
