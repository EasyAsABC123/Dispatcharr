# Native ARM64 startup prerequisites for the ten-viewer comparison

Lappy qualified Original `1241b3f7` and ordinary Rust `e7dda7f` in separate disposable Compose projects. Both established signed access and refresh tokens after first-admin setup, read a protected endpoint, returned HTTP 401 without a bearer token, reached PostgreSQL and Redis, and retired the exact owned resources. Startup and teardown each exited 0 on both arms.

The stock Original ARM64 runtime image had no `/app` payload in its 17 verified image layers. The fixture copied all 1,211 files from the pinned Original source archive into its disposable volume and ran the stock `/app/docker/entrypoint.sh` unchanged. The source bytes matched. The successful run observed PostgreSQL and uWSGI sockets plus Nginx, Daphne, Redis, Celery workers and Beat. Compiled React assets were outside that startup fixture.

Earlier Original starts failed during permissions setup or database initialization and cleaned up. An owned PostgreSQL diagnostic reached the socket with the expected login role and found a SQL_ASCII template before the stock UTF8 database creation failed. The successful recipe mounted a read-only `C.UTF-8` locale setting for the login shell in the owned container. It observed a UTF8 template before database creation and a UTF8 application database before the stock encoding check.

Rust used its admitted native ARM64 index and ordinary features-off modular profile. The startup inventory verified Main, PostgreSQL, Redis, data initialization and the observer. The [aggregate receipt](../scripts/rust-integration/m2-e7dda7f-arm64-startup-prerequisites.json) identifies the exact running index, platform child and configuration seen by the fixture.

These startup observations qualify the two runtime recipes for the paired workload. The ten-viewer timing, natural XMLTV job results during playback, media decode and resource comparison remain open.

Original Dispatcharr and dependency contributors retain full credit.
