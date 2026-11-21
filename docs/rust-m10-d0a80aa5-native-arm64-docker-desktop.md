# Native ARM64 Docker Desktop on lappy

An owned local Docker Desktop build on lappy qualified the ARM64 package
for Rust `d0a80aa585516ac81d583e6c5c80a0b50a6bd5be`. The same build produced nine component exports,
verified the pinned Rust OCI index and native manifest, and passed the
non-root wrapper and exact checks for 27 DRF assets.

On the same retained ARM64 image, Docker Compose served ready and React
index responses with HTTP 200. A non-root `aarch64` probe verified the
served index matched the installed bundle. The initialized PostgreSQL
database contained 62 application tables, 184 migration records and zero
users. All five owned containers retired with empty inventories.

The build used a private anonymous Docker client configuration for the
public image inputs. The disposable recipes made the exact directories
needed for non-root reads searchable. The first Compose wrapper stopped
before starting containers because it used an unsupported Python file-open
argument. The corrected run used the same qualified image.

The retained run covers native packaging, readiness, empty PostgreSQL
schema initialization and React index delivery. Full ARM64 media, browser
and copied-data qualification and the release decision remain open.
[Exact receipts](../scripts/rust-integration/m10-d0a80aa5-native-arm64-docker-desktop.json)

Original Dispatcharr and the framework, PostgreSQL, Comskip, FFmpeg, VLC,
Rust and other dependency contributors retain full credit.
