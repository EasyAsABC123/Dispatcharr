# Bounded P0001 error arguments

Original Dispatcharr authors retain full application credit and AGPL licensing.
PostgreSQL, Django, psycopg and SQLx authors retain framework credit and licenses.

[verified-pg-error-format-source-native-1.json](verified-pg-error-format-source-native-1.json)
binds the test-only formatter's exact module/dataset/tools. The actual pinned
Original PostgreSQL17.11/Django6.0.9/psycopg3.3.6 and native SQLx invoke the same
owned functions in a copied database. All35 cases/140 field and rendered-argument
checks pass.16 library tests and strict all-target Clippy also pass. Source-only
captures preceded implementation; both default database manifests remain unchanged
and all fixtures were removed.

The formatter borrows `PgDatabaseError.code/message/detail/hint/r#where/position`
from the existing owned error; it does not change IndexService or classify a
whole database error family. It requires SQLSTATEP0001 with no cursor/internal
position and bounds each input field and final output to8KiB. It preserves DETAIL,
HINT and CONTEXT labels with their two-space separators, including present empty
values, multiline/nested context and interior whitespace. Final stripping matches
the35 observed vectors, including22 control/Unicode boundaries and retained
U+180E/U+FEFF. No severity/sqlx Display prefix or invented QUERY/caret text is added.

The module is cfgtest-only and has no worker or application attachment. Other
SQLSTATEs, cursor-position/caret layouts, oversized errors and other server/client
versions remain unproved. Native failure classification must use accepted detailed
stage/commit/physical-release observations. Matching an error argument does not
claim a literal Python traceback or recreate executed Python frames.
