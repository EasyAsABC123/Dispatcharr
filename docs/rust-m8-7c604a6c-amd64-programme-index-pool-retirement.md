# AMD64 ProgrammeIndex and restore pool admission

On Linux AMD64, the exact Rust `7c604a6c` host regression verified that an
active ProgrammeIndex writer occupies its Main PostgreSQL pool slot until
the write settles. The real restore pool admission returned `Capacity`
while the writer was blocked. The same admission succeeded before the
blocker was installed and after the writer retired.

The test used a separate, unpooled PostgreSQL observer to identify one
actual blocked ProgrammeIndex INSERT. PostgreSQL’s `pg_blocking_pids`
identified the fixture’s lock holder, and the observer verified the same
waiter across the refusal. Cancelling the index operation and releasing the
blocker preserved the index data and update time. The test verified
retirement of the exact writer and blocker generations, the Redis file
lease and the caller’s retained job owner. PostgreSQL’s numeric process
identities stayed inside the test.

The isolated host build matched all 2,179 files in the verified production
source map and added one test file. The runner selected that one test
against disposable stock PostgreSQL and Redis. The test and all controller
phases exited 0. Compose retired the exact stock container, host process
generation and network. The project had zero remaining containers,
networks or volumes.

The first attempt stopped before the test started. Docker could not create
the nested `/app/media` tmpfs mountpoint under the read-only source bind.
That owner also cleaned up. The retry supplied the empty mountpoint and
used the same compiled test. The test exercises live pool admission and
rollback. An application restore, archive execution, timing comparison and
full M8 acceptance are outside its scope.
[Scoped physical evidence](../scripts/rust-integration/m8-7c604a6c-amd64-programme-index-pool-retirement.json)

Original Dispatcharr authors and contributors retain full application and
protocol credit.
