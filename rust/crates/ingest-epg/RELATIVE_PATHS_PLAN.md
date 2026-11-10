# Current-file paths and Source working directory: component and migration gates

Original Dispatcharr authors and contributors retain all application and algorithm credit. This plan is based on the pinned Source files; no new fixtures or runtime changes were made while resource preflight was pending. Frozen100/106/107 remain component proofs with an explicit relative-path gap.

## Observed Source rules

- `apps/epg/serializers.py:EPGSourceSerializer` exposes `file_path`; it is not listed as read-only. Relative paths are therefore a supported input surface, not merely malformed database state.
- `apps/epg/tasks.py:_resolve_source_file` chooses the first truthy extracted path, then file path, then `source.get_cache_file()`. A selected but missing extracted path does **not** fall back to an existing ordinary file.
- `find_current_program_for_tvg_id` uses ordinary `os.path.exists` and binary `open`. Relative paths use the web process's working directory; symlinks and dot components use filesystem semantics. It does not treat `file_path` as a URL, expand `~`, substitute environment variables, or restrict it to a media subtree.
- Source's `docker/Dockerfile` sets `WORKDIR /app`; uWSGI configuration also sets `chdir=/app`, its web entrypoint changes to `/app`, and `entrypoint.celery.sh` changes to `/app`. Both web and worker relative-path behavior need verification.
- `dispatcharr/settings.py` sets `MEDIA_ROOT=BASE_DIR/media`, which is `/app/media` in the original image. `docker/init/03-init-dispatcharr.sh` creates `/app/media` as an image-layer application directory; it does **not** establish a symlink to `/data`. Do not assume the original fallback cache lives under `/data`.
- For the actual fallback branch, both configured paths are false; `get_cache_file()` creates `MEDIA_ROOT/cached_epg` and returns `<source_id>.tmp`. Its extension-sniffing branch requires an existing truthy `file_path`, so that branch is not used by `_resolve_source_file` when falling back.

The final native runtime currently declares no WORKDIR, so its inherited default is `/`. CurrentLookup explicitly rejects relative selected paths. Its caller supplies an absolute cache path; choosing `/data/cached_epg` automatically would not establish original `/app/media/cached_epg` compatibility.

## Proposed narrow implementation

Use an explicit source path context containing the original source working directory and media root, both absolute. Resolve a selected relative path by joining it to that working directory. Preserve the original stored text in database/API responses. Let the operating system resolve symlinks and `..` during existence/open calls: lexical normalization of `link/../guide.xml` can change its target. Do not canonicalize once and reuse an inode across the two helpers; Source reopens the selected path for the interleaved forward scan.

Keep the existing first-truthy selection, existence-before-clock, fresh-index and independent-forward-open order. Preserve a preferred missing extracted path as missing. Do not interpret a `file://` string in `file_path` as a URI; URL/download handling is a different field/pipeline. No extra path containment policy follows from this migration.

For original Docker installations, validate `/app` and `/app/media` compatibility explicitly. A native compatibility directory or volume mapping may preserve both absolute legacy paths and relative `media/...` paths, but copying/regenerating original image-layer caches and permissions requires a separate migration rehearsal. A symlink to a managed native volume is a proposal, not evidence that Source used that mapping. Non-Docker deployments need their actual original CWD and BASE_DIR/media captured; do not silently pin them to `/app`.

Do not change the native image's global WORKDIR solely for this helper before auditing other existing relative paths/subprocesses. An explicit lookup context can avoid changing unrelated execution. The future HTTP callback/worker builder must receive one consistent path context. No new environment variable or default is implemented by this plan.

## Required owned-fixture gates after preflight

1. Capture actual Source process CWD, BASE_DIR and MEDIA_ROOT in web and worker fixtures; use unique owned files only. Record exact stored path strings and directory effects.
2. Exercise absolute file, relative filename, `./`, `../`, Unicode/space/percent characters, literal `~` and dollar signs, truthy whitespace, extracted-path precedence, missing extracted plus existing file, and empty/null cache fallback. Include serializer trimming separately from direct model strings.
3. Use owned symlink targets and a `symlink/../` case to distinguish real filesystem traversal from lexical simplification. Broken symlinks must follow Source missing-file guards. Relative and absolute source strings pointing to the same file must return the same original DTO without rewriting stored values.
4. Repeat the atomic replacement/deletion between the first helper and forward open using relative paths. Preserve Source open counts and exception/fresh-clock/index timing.
5. Compare the real Store-to-CurrentLookup path under a one-connection pool and configured source context. Prove cancellation retains file/SQL/caller resources, final byte clones retain allocations, and path-length/regular-file/response limits remain explicit.
6. Run packaged Compose with original-compatible `/app` CWD/media mapping, read-only root, actual runtime UID and dedicated volumes. Test permission-denied and directory paths as separate error-family gates; a native resource response is not automatically the original exception/HTML behavior.
7. Rehearse existing-data restart/cutover with absolute `/app/media/...`, relative `media/...` and `/data/...` stored paths. Capture original image-layer cache handling and rollback. No production files may be changed as part of these fixtures.

Application attachment and durable missing-index task publication remain separate requirements. Passing these path cases would not accept the complete operational milestone or a performance gain.

## Scoped Source111 / native114 completion

The pinned original helper and unchanged real Celery worker now confirm `/app` CWD and `/app/media` in the disposable fixture. `/app/media` is an ordinary directory, not a symlink. Twenty owned path observations cover first-truthy precedence, missing preferred files, null/empty fallback, literal Unicode/space/percent/tilde/dollar names, filesystem symlink-parent traversal, fresh clock/index ordering and relative two-open replacement/deletion. Source UID0 can read an owned mode000 file; the same original helper under UID65534 raises `PermissionError`. This fixture does not establish production UID policy or a migrated media volume.

`SourcePathContext::new(source_cwd, media_root)` is an optional immutable `CurrentLookupConfig.path_context`. Its roots must be absolute and bounded. It resolves relative selected paths against the supplied CWD without canonicalization or process-wide `chdir`; configured fallback uses `media_root/cached_epg/<source-id>.tmp`. Stored database strings remain unchanged. Default `None` preserves the existing absolute-only component API and supplied cache path. No environment default, application builder, Dockerfile WORKDIR, mount or Store business query was changed.

Frozen114 passes 18 literal SQL/file cases under a one-connection Store and two controlled relative two-open cases through the same public physical file helper. Final Bytes clones retain allocation credits; clock sampling and fresh-index observation match the original corpus. The native executable runs inside the original isolated container as UID0, on the internal network against copied PostgreSQL. Source111's unprivileged denial is recorded separately; outer HTTP exception/body and deployment-UID parity are not accepted. Initial112/113 fixture setup failures are retained. See `scripts/rust-epg-parity/verified-index-paths-source-native-114.json`.

Packaged volume compatibility, a real Store callback with configured relative paths, existing-data cache cutover/rollback, Source serializer trimming, directory error classes, production missing-index publication and broader malformed/large-index limits remain open. The default107 physical bridge replay also passes as frozen116: nineteen literal outcomes, six explicit unsupported shapes, real Store/lookup pool-one execution and retained SQL/file/byte ownership. See `scripts/rust-epg-parity/verified-current-bridge-path-default-116.json`; no functional proof here establishes a performance gain.
