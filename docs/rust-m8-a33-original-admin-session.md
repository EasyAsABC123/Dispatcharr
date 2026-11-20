# Original administrator login and session lifecycle

The owned direct Original fixture passed four same-origin HTML requests
using actual passwords, form CSRF fields and cookies. Login rotated the
CSRF value and created a database session linked to the administrator.
The read-only settings form preserved the scoped settings and admin logs.
Logout expired the session cookie and removed the database row. The
administrator's last-login value was the only persistent change. Both
owned roles retired. The receipt retains status and state observations.

The dedicated Rust admin login and session writer remain open. The current
public Rust admin gateway follows the existing ingress redirect. This
Original-only comparison qualifies the positive form and session lifecycle.
Broader M8 and the complete admin session matrix remain open. The original
Dispatcharr and Django authors and contributors retain full credit.
[Redacted receipt](../scripts/rust-integration/m8-a33-admin-session-original.json)
