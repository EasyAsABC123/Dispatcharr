# Safe native network-reset operator

Original Dispatcharr authors and contributors retain full application and algorithm credit. The native implementation is derived from the pinned Original `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`; existing AGPL and third-party notices remain included.

The additive `dispatcharr-operator` binary supports these two safe commands:

```sh
dispatcharr-operator reset_network_access
dispatcharr-operator reset_user_network
dispatcharr-operator reset_user_network --user USERNAME
```

It requires `DATABASE_URL` and `REDIS_URL` from the native operator's environment. `--user` is bounded to 1,024 UTF-8 bytes. An empty `--user ''` selects all users, as Original does. Per-command `--help` and native top-level `--help` describe the supported native grammar. This is a native operator entry point: Django global flags, argument abbreviation/duplicate-option handling, `--user=VALUE` forms and option-like username parsing, Python tracebacks, Django system-check warnings, and exact Django help formatting are not emulated. Unknown arguments exit 2; execution failures exit 1. There are no destructive management-command adapters.

`reset_network_access` resets the existing `network_access` row to `{}` and fails if it is missing. Its save preserves Original's cached key/name snapshot: a concurrent rename is overwritten, and deletion after the initial get triggers same-PK insertion. Initial absence still fails. It invalidates the shared group cache, bumps its persistent integer generation, clears matching notification-condition cache entries, and synchronizes the pinned developer notification definition. `reset_user_network` removes only `allowed_networks` from selected user properties and prints the exact successful count line. User saves do not add timestamp updates or change unrelated properties. A single-target deletion after its snapshot produces a typed zero-row-save failure with no count output and no user resurrection; Original raises `User.NotUpdated` (a `DatabaseError` subclass), whereas a network-setting full save can reinsert its cached PK. Iteration is deliberately not made into a new all-user transaction.

Existing developer-notice updates lock/read the existing row and update it without consuming another sequence value. An insert is attempted only when the row is absent. Runtime code never rewinds sequences.

The actual Original subprocess oracle found an important startup effect: these command names are absent from `dispatcharr.app_initialization`'s skip list. Each captured invocation, including command help and unknown flags, synchronizes developer notifications and sends one `notifications_cleared` Channels envelope before argument handling. A successful network reset then sends a second envelope through its post-save signal. The native supported invocations preserve this ordering. Conditions are evaluated at notification-read time, rather than suppressing notification creation during synchronization.

## Reproduce the captured scope

These commands require Docker and the pinned Original/official PostgreSQL/Redis images. Each runner creates its own isolated project and disposable database, then removes the project. Original source and operator commands only use owned fixture data; internal Docker networks prevent remote provider work.

Capture the 17 Original subprocess cases and their actual SQL/cache/notification/Channels effects:

```sh
python3 scripts/rust-operator-contracts/run_source.py --output /tmp/operator-source
```

Build the native application image from a reviewed source snapshot using the existing application image builder. The application package should retain `default-run = "dispatcharr-rs"` so adding an extra bin does not make the existing bare Cargo run command ambiguous. The runtime Dockerfile must include `/usr/local/bin/dispatcharr-operator`; compiling an autodiscovered extra binary alone does not ship it. Verify that actual shipped binary against official, owned PostgreSQL/Redis:

```sh
python3 scripts/rust-operator-contracts/run_packaged.py \
  --output /tmp/operator-package \
  --build-manifest /path/to/build.json \
  --source-cases /tmp/operator-source/operator-cli-source.json
```

The Original image in this packaged gate is a fixture/SQL/cache/Channels observer only. It does not run the command under test. The operator executes through Compose as UID 65532 with a read-only root and dropped capabilities. Captured generated notification IDs are reproduced by restoring the owned fixture sequence; changed timestamps must fall within the recorded actual native execution window, while unchanged creation/user dates must remain exact. Group generations are persistent integers, rather than expected to equal a different subprocess's clock sample.

For the six resource controls, compile `dispatcharr-store`'s `operator_commands_sql` test target with `cargo test --no-run --message-format=json`, preserve its reported executable outside the Cargo target, then run:

```sh
python3 scripts/rust-operator-contracts/run_resources.py \
  --output /tmp/operator-resources --native-binary /path/to/operator-resource-tests
```

The controls cover blocked SQL cancellation, an actually applied DEL with its reply held over two seconds, row/aggregate/count refusal before user writes, oversized cache keys without a cache-wide clear, and retained-publication scope after logical timeout. The publication composition test uses an explicitly controlled retained sink; it is not a claim that this test physically pauses a real Channels PUBLISH. The existing realtime transport's retained-publication contract is reused, and the packaged cases separately check actual Channels envelopes.

The actual locked-row/sequence boundary oracle is independently runnable with the preserved native CLI executable (copied out of the built image):

```sh
python3 scripts/rust-operator-contracts/run_sql_boundaries.py \
  --output /tmp/operator-sql --native-binary /path/to/dispatcharr-operator
```

It compares existing-notice sequence state and the next actual newly-created notice ID, then blocks the real Original/native setting UPDATE while another owned transaction renames or deletes its row. These three SQL families include eight Source/native observations: notice sequence/next-ID, network rename/delete, and single-user `update_fields` deletion. They distinguish initial absence, same-PK setting resurrection after a successful read, and the separate non-resurrecting user save failure. The earlier sequence, full-save, user false-count, and retained-publication admission negatives are retained; the original 17 selected-field checks were not complete SQL-state verification.

## Bounds and remaining scope

One operation per service is admitted without an unbounded waiter queue. A supervised operation retains its caller and admission through physical SQL/Redis replies; the same composite scope crosses retained event publication. Aborting an observer cannot admit another operation or let shutdown report settlement while that scope is held. Commands are not replayed after uncertain replies.

User snapshots are limited to 4,096 rows, 1 MiB per SQL JSON text value, and 4 MiB total SQL JSON text. These are representation budgets, not a claim that parsed JSON heap allocations equal 4 MiB. Notification-condition scanning returns at most 256 keys/128 KiB per reply, with a 1 KiB key limit, 4,096 matched keys overall, and 8,192 iterations. The native bounded scan uses EVAL; arbitrary Redis ACL configurations have not been accepted. SQL/network waits retain physical ownership until they settle; a CLI process kill is not a durability guarantee.

Original's condition-cache signal can fall back to clearing the entire cache after a deletion error. This safe component explicitly rejects that unsupported fallback instead of exposing `FLUSHDB`. A network reset can already have committed before that rejection. User-limit rejection prevents user-property writes, but startup notification synchronization can already have run. Neither command is advertised as an all-effect atomic operation.

Pure verifier controls are portable without private artifacts:

```sh
python3 -m unittest discover -s scripts/rust-operator-contracts -p 'test_*.py' -v
```

They reject boolean/integer/float substitution, reordered arrays, missing-versus-null properties, and missing/persistent/expiring cache TTL substitution. Packaged comparisons also require exact notification sequence state and network PK identity; Python's `1 == True` is not used to attest typed JSON preservation.

Developer definitions are pinned to the captured built-in JSON definition. Other Django app-ready work (including log-collector setup and unrelated cache warming), alternate notification files, version/expiry combinations, backend failures/ACLs, settings races beyond the specifically locked rename/delete schedules, malformed all-user iteration ordering, and broad Django management-command compatibility remain separate gates. The seventeen operational cases and bounded resource controls do not close all M8 milestones.
