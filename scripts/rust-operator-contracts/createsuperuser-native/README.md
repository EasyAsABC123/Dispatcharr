# Bounded native createsuperuser

Original Dispatcharr and Django authors retain all application, command and
algorithm credit. Their licenses and attribution remain unchanged.

The native operator now supports a bounded noninteractive superuser workflow:

```sh
dispatcharr-operator createsuperuser --noinput --username NAME --email ADDRESS
```

`DATABASE_URL` and `REDIS_URL` select the native operator's configured database
and cache. `DJANGO_SUPERUSER_USERNAME`, `DJANGO_SUPERUSER_EMAIL` and
`DJANGO_SUPERUSER_PASSWORD` provide the captured Django environment inputs.
Password input comes only from the environment. No password command-line flag is
added. Missing and explicitly empty passwords retain their different Original
behaviors. Successful output is `Superuser created successfully.`; `--verbosity 0`
suppresses it. `--no-input`, `--username=VALUE`, `--email=VALUE` and verbosity0–3
are supported. Interactive prompts and Django global settings/database flags
remain unsupported. Separate option values beginning with a dash are outside
this grammar; use an equals form or environment input. Original accepts some
negative-number and solitary-dash forms that remain unported here.

The accepted input domain is ASCII usernames plus fullwidth Latin letters/digits,
which the existing Source-compatible manager normalizer converts with NFKC.
Other Unicode username categories remain unsupported. Email supports blank values
and ordinary ASCII unquoted local parts/DNS domains; Unicode, quoted local parts
and address literals remain unsupported. Username length is limited to150
characters, email to254 ASCII bytes, and each argument/environment value/password
to4,096 UTF-8 bytes. These bounds do not establish full Django validator parity.

The raw natural-key uniqueness lookup precedes normalization, matching Original.
A normalized collision reaches the actual database uniqueness error; no upsert,
retry or single-admin restriction is introduced. New users retain admin flags and
user level10, empty names/properties, null API key/last login, no memberships, and
the normal database-generated ID. Existing users remain unchanged. Startup
notification synchronization reuses the already bounded operator implementation.

One supervised operation holds admission through password hashing, SQL and
retained startup publication. A real locked-table control cancelled its observer:
another operation remained refused, the caller stayed owned, and shutdown stayed
unsettled until the lock released and exactly one insertion completed. A second
actual uniqueness-error control holds trailing ReadyForQuery: the operation
retains its acquired connection through a Sync barrier and explicit close before
releasing ownership. This does
not promise durability after killing the whole CLI process.

## Accepted evidence

The frozen750-input source passes strict whole-workspace/all-target Clippy and
464 default tests across168 suites;648 fixture-dependent tests are ignored in
that default run. Actual Original and native CLI runs each pass22 cases/95
checks. There are154 strict typed comparisons of arguments/exit codes, user
fields, sequence state and actual startup events. Only ten actual creation-date
slots differ, each bounded by its own command window and immutable thereafter.
Both final-source SQL ownership controls pass. Default databases are unchanged
and owned resources are removed.

Help output and most stderr differ. Native diagnostics do not fabricate Python
tracebacks or system-check output. This is a scoped source component, not full
CLI byte parity, Django admin compatibility, image48 packaging, performance
acceptance or complete M8. The extended Original oracle and retained negatives
are recorded alongside it. Synthetic passwords are generated in memory; public
observations omit plaintext passwords and encoded password bytes.

Verify public observations without Docker or private artifacts:

```sh
python3 scripts/rust-operator-contracts/createsuperuser-native/verify.py \
  --source scripts/rust-operator-contracts/createsuperuser-native/source-createsuperuser-4.json \
  --native scripts/rust-operator-contracts/createsuperuser-native/native-createsuperuser-3.json
python3 -m unittest discover \
  -s scripts/rust-operator-contracts/createsuperuser-native -p test_verify.py -v
```

All12 corruption controls pass. Earlier actual failures remain documented:
the first Original oracle confused missing and empty passwords; a physical-test
setup attempted to copy its ELF before compilation finished. Fresh corrected
runs passed. A whole-workspace formatting check encountered existing formatting
differences; no blanket reformat was applied.

## Repeat the owned Compose gates

Use Rust/Cargo1.90, Python3, Docker Compose, and a checkout containing the pinned
Original Git revision. Each output must be new. Generated native executables
must be Linux-compatible with the pinned Original fixture container.

```sh
cargo build --manifest-path rust/Cargo.toml --locked --release \
  -p dispatcharr-rs --bin dispatcharr-operator --target-dir /tmp/dispatcharr-cli-build-NEW
python3 scripts/rust-operator-contracts/createsuperuser-extended/run_source.py \
  --output /tmp/dispatcharr-cli-original-NEW
python3 scripts/rust-operator-contracts/createsuperuser-native/run_source.py \
  --native-binary /tmp/dispatcharr-cli-build-NEW/release/dispatcharr-operator \
  --output /tmp/dispatcharr-cli-native-NEW
```

The runners freeze their tools, archive the pinned Original with its verified
hash, create a disposable copied database on an internal network, observe actual
SQL/password/event effects through Django, audit the default database, and remove
their project. A fresh build has its own executable identity; the recorded ELF
hash and source map identify the executed qualification rather than promising a
byte-identical rebuild. No Git-only rebuild claim is made here.

For the physical control, compile `dispatcharr-store`'s `operator_create_sql`
test with `cargo test --no-run --message-format=json`, copy the reported executable
outside its target directory, and pass it to
`createsuperuser-resources/run_source.py --native-binary ELF --output NEW`.
That runner exercises the real PostgreSQL lock and cancellation control, rather
than running the CLI examples as a substitute for physical ownership evidence.
