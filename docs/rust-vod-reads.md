# Ordinary VOD reads: image31 checkpoint

All application and algorithm credit belongs to the original Dispatcharr authors
and contributors. The original React application, branding and AGPL license are
preserved.

Committed `db532d02` attaches ordinary movie, series and episode list/detail
reads to the Rust application. [The packaged report](../scripts/rust-vod-api-contracts/verified-vod-rest-packaged-image31-1.json)
records 393 exact original/Rust HTTP comparisons and ten simultaneous successful
requests. The other 199 captured original cases are outside this adapter.
Selected headers include the original security headers, content length, allowed
methods, origin variation and bearer challenge. HEAD performs the same read and
reports its representation length while suppressing the body.

The default admission budget has two active reads, 192 MiB of configured ownership
credits and at most 256 waiting requests. Waiting input uses the remaining memory
credits, capped at 16 MiB in aggregate. The five-second arrival deadline includes
waiting; canceled or expired queued work cannot start authentication or SQL.
Physical Redis/SQL work and response-frame clones retain their owners through
completion. These are ownership bounds; they do not measure total allocator or
container memory.

Categories, unified reads, nested provider actions, provider refresh, artwork and
the complete React VOD workflow still need separate gates. Browsable HTML remains
an explicit unsupported capability. Functional checks alone establish no VOD API
performance gain or complete migration milestone. A later independent
[complete ordinary-list experiment](rust-vod-performance.md) now passes its
six-pair performance gates on this exact image. The separately measured
[image30 guide gains](rust-guide-performance.md) keep their original image and
workload binding.

## Reproduce the packaged checks

Use a full repository clone, Docker with Compose, Python 3, and the pinned Rust
toolchain for host checks. Browser checks also require Node.js 22+ and Chromium
on PATH; `RUST_BROWSER_NODE` and `RUST_BROWSER_CHROMIUM` can select installed
executables explicitly. The original baseline Git object and pinned container
images must be available. Choose new empty output directories for every attempt.
These commands use generated disposable fixtures and remove their own Compose
projects.

```sh
python3 scripts/rust-integration/build.py \
  --git-revision db532d02 \
  --tag dispatcharr-rust-candidate:vod-reads-replay \
  --output artifacts/rust/vod-reads-build-replay

python3 scripts/rust-vod-api-contracts/run.py --mode http \
  --candidate-image dispatcharr-rust-candidate:vod-reads-replay \
  --build-manifest artifacts/rust/vod-reads-build-replay/build.json \
  --output artifacts/rust/vod-reads-http-replay

python3 scripts/rust-integration/run.py \
  --candidate-image dispatcharr-rust-candidate:vod-reads-replay \
  --candidate-build-manifest artifacts/rust/vod-reads-build-replay/build.json \
  --output artifacts/rust/vod-reads-api-replay

python3 scripts/rust-integration/run_clean_install.py --logo-workflow \
  --candidate-build-manifest artifacts/rust/vod-reads-build-replay/build.json \
  --output artifacts/rust/vod-reads-browser-replay
```

[The complete image31 checkpoint](../scripts/rust-integration/verified-checkpoint-image31-1.json)
binds the build, strict workspace checks, API/authentication regressions, browser
workflow, guide replay and media preflight to their retained evidence hashes.
Future application changes require fresh verification; these commands rebuild
the named historical application source while using the checked-out test tools.

The later [category source extension](../scripts/rust-vod-api-contracts/CATEGORY-ROUTE-PLAN.md)
adds list/detail routes through the same VOD admission, memory and shutdown
resources. Its [source checkpoint](../scripts/rust-integration/verified-vod-category-source-1.json)
records component comparisons, ordinary-read regressions and the compiled main
merge. Canonical packaging, category browser workflows, performance, four query/
format differences and seven default-error branches remain open. Image31's
ordinary VOD measurements do not establish category performance gains.
