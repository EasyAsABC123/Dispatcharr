# Guide grid components

All application credit remains with the original Dispatcharr authors. The date parser port also retains the original Django BSD license in [third-party/DJANGO-LICENSE](third-party/DJANGO-LICENSE).

The frozen component proof `scripts/rust-epg-contracts/verified-grid-window-components-17.json` compares 861 actual original helper cases: 557 windows, 34 profile selectors, 117 dummy spans, 48 stored-row projections, 98 batch encodings and seven chunk-count boundaries. Three Rust tests and strict Store all-target Clippy pass. This subset is independent of the packaged image23 checkpoint.

Window handling preserves absolute/relative precedence, last repeated query parameter, Unicode decimal integers, bounded day selection, timezone offsets including fractional seconds, and captured original exception classes/messages. Original local offset representation is retained separately from UTC comparisons. Golden cases cover the captured grammar; exhaustive Python datetime acceptance remains unclaimed.

Grid rows use the original Django millisecond datetime representation, separate from programme-detail microseconds. Batch encoding is capped at 500 rows and 16MiB per frame. Those bounds do not establish HTTP backpressure or query bounds: SQL visibility/overrides, dummy programmes, cancellation, database cursor lifetime and wire streaming must pass their own gates before application attachment. No grid HTTP parity or performance gain is claimed by this component proof.
