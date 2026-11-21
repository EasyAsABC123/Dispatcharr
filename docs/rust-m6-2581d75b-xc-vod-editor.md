# M6 XC VOD enable through the provider editor

The stock and packaged Rust backends accepted the same 14-field JSON settings save for an existing active XC account with VOD off. This fixture used the ordinary no-file React request shape and preserved the account's existing values. User agent, server group, and hash override were unset, and the existing interval-zero schedule stayed disabled. The Rust package ran with Selected workers enabled and Beat off. The stock arm used ordinary Celery workers with Beat off.

Each backend returned HTTP 200 JSON with the same `Allow` header for GET, PATCH, GET, and repeated PATCH. The false-to-true edit published one real `apps.vod.tasks.refresh_vod_content` root for the fixture account on the default queue. Both workers completed that root successfully. Each authenticated to the owned provider once before fetching the movie categories, series categories, movie list and series list in that order. Both populated one movie relation and zero series relations. The two new Uncategorized links remained disabled, the existing provider category stayed enabled, and the second account and profiles were unchanged. The repeated true save advanced the same schedule row's change timestamp and produced zero additional roots. Both queues and reservations cleared. Native Main exited 0 and both projects retired their exact owned resources.

The first paired run exited 1 in both arms at the final provider trace assertion. The expected four-action list omitted the ordinary authentication request, even though the scoped settings, SQL and worker assertions had passed. Both original projects retired. The fresh retry required the captured five-request sequence exactly and retained the first failure separately.

The earlier stock-only four-field capture and the separate generated React request capture each verified publication without a worker. The generated request's reduced receipt has SHA-256 `0032d250a58e44176fd8b7d3a8be6c577bc54c5b43fc2d926adb83adf1a6472e`. The new pair independently verified worker settlement. [Earlier stock producer evidence](rust-m6-original-xc-vod-enable.md)

The Rust application revision is `2581d75bc4c0ed7f522b08288f335a6db58d141a` and its image is `sha256:6fd49d3cd188b931dc4a28ca5525e297002584f9a5073baa2cb2f14da4b1abd7`. The package manifest has SHA-256 `7c5721fc0b5a12c796c0cc373248d59a8907dbc53372e9a23e78041c46ab6054`. The paired receipt has SHA-256 `6c529888c25834564f012aa977d2f97d49509c4d3465a172dfd5c7712bd1d9cd`. [Paired receipt](../scripts/rust-integration/m6-2581d75b-xc-vod-editor.json)

Flags-off worker parity, other editor field changes, and full M6 acceptance remain open. Performance was outside this fixture.

The original Dispatcharr, Django, Celery, and Kombu authors retain full credit.
