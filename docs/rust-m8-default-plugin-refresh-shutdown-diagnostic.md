# Default PluginRefresh shutdown on a diagnostic build

The private image built successfully from revision 21337 with two files
of lifecycle instrumentation. The copied trusted-taskless restoration
completed with default startup plugin refresh enabled. Rust Main and
the paired controller each exited 1 during shutdown.

The fixed phase markers show that the delayed refresh was reserved,
started, returned through the other-domain branch, and retained its
family permit. The instrumented transport exception-class branch
was absent. The other-domain branch has multiple possible causes
in repository I/O, response decoding, SQL and manifest handling.
The exact cause still needs isolation.

Independent Docker event and inventory checks confirmed retirement
of all four Native fixture container generations and empty project
inventories. The literal shutdown failure remains open alongside
full M8. The previously published opt-in restore result used the
supported startup refresh skip setting.

The original Dispatcharr authors and dependency contributors retain full credit.
[Redacted diagnostic receipt](../scripts/rust-integration/m8-default-plugin-refresh-shutdown-diagnostic.json)

## Refined platform DNS result

A fresh diagnostic image reran the same copied restore with startup
plugin refresh enabled. Restore completed, and the controller and
Rust Main each retained exit 1. The refined fixed markers place the
error in the platform DNS branch of claimed PluginRefresh execution.
The family permit remained held. This trace establishes the observed
branch. The stock failure mapping and safe graceful-shutdown behavior
still need verification.

Independent Docker event and inventory checks verified retirement of
all four tracked Native fixture roles. Full M8 and performance stay open.
[Redacted refined receipt](../scripts/rust-integration/m8-default-plugin-refresh-platform-dns.json)
