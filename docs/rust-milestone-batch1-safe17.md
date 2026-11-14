# Current candidate: packaged safe CLI

The current `8e6a871b…27315d` image passes17 packaged safe-operator cases and694
actual check predicates. The [raw proof](../scripts/rust-integration/milestone-batch1-safe17/proof.json)
binds the exact image, source tree, build manifest, tool hashes and retained
Original17 corpus. That corpus is reused without rerunning Original commands.
The Original container supplies read/fixture/Channels observation only.

The [dispatcher result](../scripts/rust-integration/milestone-batch1-safe17/result.json)
records the `production-CLI` entrypoint. Execution, cleanup and evaluation all
exit0 with both pipe EOFs, process-group settlement and empty owned project
inventories. Full Django CLI behavior, exact native help/stderr and Main API
acceptance are outside this safe-command scope. The global dispatcher exit2 is
retained because other required cells remain blocked.

This is a functional CLI result, with no new performance claim. Original
Dispatcharr authors and dependency contributors retain all credit and licenses.
