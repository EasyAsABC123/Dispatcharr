# Copied SQLite preview

Rust `e9d143b8f803053e876ca435332d26d0fa15e2d6` can serve a detached, schema-compatible SQLite
copy through the opt-in copied-data runtime. The Docker Compose replay used
Redis and the app on a private loopback network. It verified that a real
listener stopped and restarted. The same bearer still worked, saved App
Data persisted, a token refresh could read a profile, and profile and group
reads and useful writes succeeded. The independent reducer verified
retirement of all five owned containers and all live observed generations.

Build the preview with `DISPATCHARR_BUILD_COPIED_SQLITE_RUNTIME=1` and start
the app with `--sqlite-copied-data-runtime`. Set `DB_ENGINE=sqlite`,
`RUST_SQLITE_COPIED_DATA_ROOT` to a private detached copy directory,
`RUST_BIND` to a loopback address, `REDIS_URL` to the owned loopback
cache, and supply the signing key from the existing secret store. The
ordinary build argument is `0`. Its actual package refused
the copied runtime and retired cleanly in the feature-off check.

The pinned Original replay used an adapted historical copy with 184
recorded migrations. It verified the 25-entry App Data limit, HTTP 409 at
the cap, and restart readback. The preview replay's first reducer read
the wrong phase filename. The corrected independent reducer reproduced
the successful physical receipt without replacing the first result.
Full SQLite application compatibility, additional provider and backup
workflows, and comparative performance remain open.
[Exact receipts](../scripts/rust-integration/m8-copied-sqlite-preview-compose.json)

The original Dispatcharr, Django, SQLite, SQLx, and other dependency
authors retain full credit.
