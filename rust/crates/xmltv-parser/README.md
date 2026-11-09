# Native streaming XMLTV parser component

All original application and algorithm credit belongs to Dispatcharr's authors.
This is a Rust callback interface around the same libxml2 **2.14.6** release used
by the pinned original lxml runtime. There is no Python interpreter or application
code in the parser. It preserves the original streaming recover/NOBLANKS/entity
and element cleanup behavior; full EPG refresh jobs are a separate gate.

The unchanged upstream archive is vendored for offline/reproducible builds:
[official archive and checksums](https://download.gnome.org/sources/libxml2/2.14/).
SHA256: `7ce458a0affeb83f0b55f1f4f9e0e55735dbfc1a9de124ee86fb4a66b597203a`.
Daniel Veillard and libxml2 contributors retain their copyrights and license in
`vendor/Copyright` and the original archive's individual source files.

Initial packaging supports native Linux builds with a C compiler, make and tar/xz.
The library is linked statically; build steps never fetch from the network.
Cross compilation and other platforms are not accepted by this component.

The private static library installs its allocator hooks before initialization.
Thread-local allocation attribution plus reference-counted budget headers keep
the budget alive until actual frees, including entity templates/copies and
reallocation's temporary old/new buffers. Charged live native allocations are
capped at16MiB/context; immutable library globals and opaque iconv internals are
separate. Logical event-token, prologue and dictionary guards remain explicit.
Rust extraction additionally caps one event tree before passing ownership to a
synchronous sink; callers must bound their retained records/queues and job count.
No caller cancellation or failed sink is treated as successful job completion.

The original XML suffix completeness check and doctype injection come from
`ingest-core`. Parser recovery success alone is not a guide-completeness check.
No ingestion route, whole-job resource bound or performance gain is implied by
this library's component tests.
