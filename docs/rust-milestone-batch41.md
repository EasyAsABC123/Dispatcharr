# Candidate41: existing VOD catalogues and manual commercial detection

Original Dispatcharr and dependency authors retain all application, algorithm,
protocol and configuration credit. React and licensing remain unchanged.

Candidate41 adds bounded plain Movie repeat/update ingestion through the existing
HTTP refresh root and shared selected worker. It admits at most 128 existing
Movies, 256 Movie relations, 128 categories and 256 category relations, with
untagged identities, empty descriptive/external-ID/logo metadata and bounded
relation properties. Ordinary repeats preserve global Movie and relation
identity, timestamps and unrelated relation properties; actual provider changes
can reassign a stream, deduplicate multiple streams into one Movie, reset its
detail flag when reassigned, replace categories, prune stale account relations
and delete only globally orphaned Movies. Empty provider categories preserve
existing selections; an empty Movie document performs ordinary scoped cleanup.
An inactive other account's relation can preserve the global Movie while the
public catalogue remains empty. Source repeat observations retain their original
failed controller and the separately qualified corrected catalogue observer.
Native comparison and application performance remain pending until actual tests.
Full M6 metadata, external identities, language reconciliation, nonempty Series,
Episodes and wider catalogue/concurrency domains remain open. All application
and algorithm credit belongs to the original Dispatcharr and dependency authors.


Manual commercial detection joins the normal selected Celery worker and actual
admin recording action. The supported workflow uses a completed local regular
MKV, mark configuration, real pinned Comskip executable, short SQL sessions,
physical media-process ownership and ordinary backend/ordered update delivery.
The [Source reference](rust-comskip-manual-source1.md) qualifies no-file,
actual exit-one/no-commercials, and already-processed cases. The Native test
must reuse the exact captured MKV. Nonempty commercial marks/FFprobe, automatic
recording handoff, cut/remux, broader permissions and plugin hooks remain open.
The original mutable Comskip build does not identify its corresponding source
revision; the exact binary digest, configuration, license and original build
recipe are retained, and release-ready source closure remains open.

The [VOD reference](rust-vod-repeat-source1.md) preserves its actual failed
controller and separately qualified complete observations. The observer now
expects zero public movies when only an inactive other account retains a
relation; no original task replay or Rust algorithm change was needed.

SDK57 passes workspace check, strict Clippy and all workspace tests; formatting
failed only on two module-order differences. That attempt remains preserved.
Those differences are corrected. [SDK58](../scripts/rust-integration/milestone-batch41/checks.json)
passes all four gates with unchanged inputs: 1,145 passed tests, zero failures
and 784 ignored fixture tests across 180 result blocks. Its 1448 inputs
include 1442 Rust inputs. Physical SQL, packaging, the combined Native
batch and performance are pending. Ignored tests are not physical acceptance.
Compilation and passing subsets do not close full milestones.
