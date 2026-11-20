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
