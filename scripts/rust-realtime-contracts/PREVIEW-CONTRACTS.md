# Admin regex preview and URL transformation

Original Dispatcharr authors retain full application credit; see
[ATTRIBUTION.md](../../ATTRIBUTION.md).

The pinned original runtime uses Python 3.13.18 and third-party
`regex` **2026.9.29**, default version **V0**, with native extension hash
`b9289d43031fd883e8f81c97e802ffb7d82d719f99d86efec09288a81eaa1ce5`.
The 267-case corpus executes unchanged AST bodies from
`dispatcharr/consumers.py:MyWebSocketConsumer.receive`, `user_is_admin` and
`apps/proxy/live_proxy/url_utils.py:transform_url`. Only authentication/ORM
imports are fixture shims; live authorization has separate original/native
wire proofs. Inputs, outputs, source hashes and extension identity are retained
in [datasets/source-preview.json](datasets/source-preview.json), bound by
[verified-preview-source-1.json](verified-preview-source-1.json).

Observed behavior includes:

- Standard/viewer requests are ignored. Admin replies contain
  `data.type=m3u_profile_test`, `search_preview` and `result`.
- Falsy fields become empty strings before validation. Truthy nonstrings,
  URLs over 8,192 code points, or patterns/replacements over 255 code points
  return the safe truncated URL for both outputs without regex work.
- Preview wraps every match in literal `<mark>` tags. An empty search still
  inserts empty marks between characters; transformation independently returns
  the original URL when either search or replacement is empty.
- No match or a transformation error/timeout returns `result:null`. Preview
  errors independently fall back to the original URL.
- JavaScript-style `$<name>` and `$1` references are converted before replacement.
  The original `$0` conversion produces an octal NUL escape, not a whole-match
  reference. Explicit `\g<0>` has different behavior.
- Actual cases include variable lookbehind, named groups, branch reset, V1 set
  operations/full folding, reverse matching, fuzzy matching, recursion,
  grapheme matching, Unicode and unpaired surrogate code points.
- User searches have a 100ms timeout in each of the two regex operations.
  Timing is recorded as an observation, not a deterministic golden.

The packaged Native gateway registers the account profile handler on the normal
`/ws/` route. The existing authenticated role check remains in Rust. Native
retains the private reply envelope, shared bounded actor admission and physical
child drain. The dependency operation uses the pinned Python `regex` package
for preview and transformation. ASCII-escaped JSON scalars preserve the
original wire behavior for lone surrogate code points. The actor caps the
encoded transformed scalar at 2 MiB and the gateway applies its existing
private frame limit. Those capacity limits can close an exceptionally large
preview with 1011.

The predecessor package closed valid admin preview requests with 1011 due to
its unregistered handler. Use the grouped fixture to qualify a successor
package against the pinned Original Django/Daphne app. It checks all 267
cases, prevents replies from reaching other roles and verifies that the
sockets remain usable. The receipt also records actual Native shutdown,
Original Daphne process exit and Compose project cleanup.

```bash
python3 scripts/rust-realtime-contracts/gateway_image.py \
  --profile-preview --output "$preview_evidence_dir" \
  --candidate-image "$native_image" \
  --candidate-build-manifest "$native_build_manifest"
```

This command covers the WebSocket profile editor. Full browser workflows,
other update producers and M7 performance have separate acceptance gates.
