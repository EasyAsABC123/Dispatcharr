# Ordinary React settings readback across restart

Pinned Original and packaged Rust `6dbd0796` each accepted one normal
admin JWT login through the unchanged React API. Both saved the existing
system settings row with `max_system_events` set to 110. After the owned web
restart, the same issued bearer token read back the saved row. All unrelated
settings rows, the PostgreSQL and Redis service generations and the Django
session count stayed identical. Both Native Main instances retired with exit 0.

Each backend returned 301 to the public login path with no Set-Cookie header
for `/admin`, `/admin/`, `/admin/login/` and
`/admin/core/coresettings/add/`. The first attempt exited 1 before Original
login because the fixture expected the same redirect at
`/admin/core/coresettings/`. That path has the shallow shape exempted by
Original's nginx rule. The first Original project retired cleanly and never
started Native. The corrected isolated controller and independent reducer
both exited 0. The fresh Original and Native projects retired two and four
owned Compose role generations respectively.
[Redacted receipt](../scripts/rust-integration/m8-6dbd0796-react-settings-jwt-restart.json)

The workflow used the React HTTP contract and ordinary JWT, with separate
evidence needed for browser rendering or a private Django signed-cookie
restart. This check establishes the stated settings readback and lifecycle
scope. Full M8 and performance acceptance remain open.

Original Dispatcharr, Django, nginx and dependency contributors retain full
credit. [Attribution](../ATTRIBUTION.md)
