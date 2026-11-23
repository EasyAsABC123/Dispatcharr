# AMD64 restore and scheduler drain on b8d39793

The ordinary Rust package `b8d3979366981f0372794f8e04b01763a233bae7` ran the 16 signed backup and restore calls against a disposable Compose database. The typed HTTP responses, six state checks and five worker tasks joined to their actual broker and result records. The last three queue checks were empty, and the archived account, database, files and archives passed the persistence checks after shutdown.

Main exited 1 on scheduler shutdown. The readiness probes immediately before stopping all observed a ready scheduler. Before restore, the scheduler table had eight rows. The restored table contained one archived row and the previous schedule marker was absent. The observed post-restore ticks reported no reload. The evidence leaves the final cache state and the immediate source of the drain failure unresolved.

The independent audit verified exact retirement of the application generation and all owned resources, including removal of the exact cleanup marker. The actual preflight, run, closure, evaluation and campaign exits were `0/1/0/1/1`. M8 remains open on this gate. [Aggregate evidence](../scripts/rust-integration/m8-b8d39793-scheduler-drain-recovery.json)

The original Dispatcharr authors and contributors retain full application and React credit.
