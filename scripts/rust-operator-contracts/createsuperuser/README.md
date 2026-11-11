# Original createsuperuser CLI contract

All application, Django command, and algorithm credit remains with the original
Dispatcharr and dependency authors. Existing licenses remain unchanged.

The pinned Original command passes **12 subprocess cases / 55 checks** in an
isolated copied database. This is an executable Source reference for the missing
native management command; it does not establish native CLI or Django admin parity.

The cases cover help, unknown flags, missing username, invalid username/email,
successful creation, duplicate username, environment inputs, missing versus empty
password, Unicode NFKC normalization, and quiet output. New accounts retain admin
flags/user level10, empty names/properties, null API key/last login, and no group or
permission memberships. Creation dates fall inside each actual command window.
Existing users are preserved. Every invocation emits one actual startup Channels
envelope, including help and argument errors.

The Original distinguishes an absent password environment variable, producing an
unusable password, from an explicitly empty value, producing a usable empty
password. The first oracle incorrectly expected these to behave alike and failed.
That run is retained; corrected fresh runs2/3 pass without modifying Original.
Native behavior must be compared against the observed command contract.

Passwords are generated in memory. Evidence contains verification booleans, not
plaintext passwords or stored password bytes. All24 retained subprocess stream
hashes were independently verified. The default database stayed unchanged, and
fresh container/network/volume queries confirm owned cleanup.

With Python3, Docker Compose, Git and the pinned Original image available:

```sh
python3 scripts/rust-operator-contracts/createsuperuser/run_source.py \
  --output /tmp/dispatcharr-createsuperuser-source-NEW
```

Run from a checkout containing pinned Original revision
`1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`. `DISPATCHARR_REPO` optionally selects
another repository root. The runner freezes its support/tools before execution,
uses synthetic local data and an internal network, audits the default database,
and removes its owned Compose project. Each output directory must be new.

Native implementation and packaged login tests remain open. Interactive TTY
prompts/defaults/password validation, wider global flags/settings/database options,
SQL/cache failures and cancellation require separate acceptance. These12 cases
do not close M8 or imply a full management-command replacement.
