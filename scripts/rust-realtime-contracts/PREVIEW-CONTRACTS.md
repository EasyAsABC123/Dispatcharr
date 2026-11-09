# Admin regex preview and URL transformation

Original Dispatcharr authors retain full application credit; see
[ATTRIBUTION.md](../../ATTRIBUTION.md). Native preview remains incomplete.

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

The existing builtin-Python-`re` ingestion oracle does not establish equivalence
for this runtime. No separate native library has passed this corpus, and no
backend is selected. Two possible engineering paths are to extend a native
interpreter with an explicitly separate third-party-regex mode, or to evaluate
another native engine with an adapter. Either path must pass the corpus before
being enabled and must reproduce replacement parsing, Unicode/code-point
behavior, match ordering and bounded execution. A shared parser name or a
successful simple URL replacement is insufficient evidence.

Current packaged behavior remains explicitly incomplete: an admin preview
request reaches the unimplemented handler and closes with 1011. Do not describe
that as original preview parity or complete React workflow support. Keeping
this gate open avoids shipping an unproved replacement under an equivalence
claim.
