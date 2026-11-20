FROM node:24@sha256:8fa96264f302f58bbb13aa7339c541547b56975766ce956c875da149452a3e40 AS frontend-dependencies
WORKDIR /dispatcharr-frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY frontend/ ./

FROM frontend-dependencies AS frontend-tests
RUN mkdir -p /verification-results && npm test -- --maxWorkers=2 --no-file-parallelism --reporter=json --outputFile=/verification-results/frontend-tests.json

FROM frontend-dependencies AS frontend
RUN npm run build

FROM rust:1.90.0-bookworm@sha256:3914072ca0c3b8aad871db9169a651ccfce30cf58303e5d6f2db16d1d8a7e58f AS taskless-restore-helper
COPY rust/runtime/taskless-guard/ /taskless-inputs/
RUN cd /taskless-inputs/debian-generators && sha256sum --strict -c SHA256SUMS \
    && set -- \
    && { command -v m4 >/dev/null || set -- "$@" m4_1.4.19-3_amd64.deb; } \
    && { command -v bison >/dev/null || set -- "$@" bison_3.8.2+dfsg-1+b1_amd64.deb; } \
    && { command -v flex >/dev/null || set -- "$@" flex_2.6.4-8.2_amd64.deb; } \
    && { [ "$#" -eq 0 ] || DEBIAN_FRONTEND=noninteractive dpkg -i "$@"; } \
    && m4 --version && bison --version && flex --version
RUN cd /taskless-inputs/debian-builder && sha256sum --strict -c SHA256SUMS \
    && { command -v bison || echo 'builder dependency bison unavailable'; } \
    && { command -v flex || echo 'builder dependency flex unavailable'; } \
    && { pkg-config --modversion libzstd || echo 'builder dependency libzstd unavailable'; } \
    && state="$(dpkg-query -W -f='${db:Status-Status} ${Version}' liblz4-1:amd64 2>/dev/null || true)" \
    && case "$state" in \
         'installed 1.9.4-1') dpkg -i liblz4-dev_1.9.4-1_amd64.deb ;; \
         '') dpkg -i liblz4-1_1.9.4-1_amd64.deb liblz4-dev_1.9.4-1_amd64.deb ;; \
         *) echo 'pinned LZ4 runtime package does not match builder' >&2; exit 1 ;; \
       esac \
    && pkg-config --modversion liblz4
RUN python3 /taskless-inputs/build_helper.py \
    --tarball /taskless-inputs/postgresql-17.11.tar.bz2 \
    --checksum /taskless-inputs/postgresql-17.11.tar.bz2.sha256 \
    --patch /taskless-inputs/postgresql-helper.patch --output /taskless-out

FROM rust:1.90.0-bookworm@sha256:3914072ca0c3b8aad871db9169a651ccfce30cf58303e5d6f2db16d1d8a7e58f AS build
COPY --from=taskless-restore-helper /taskless-out/taskless-helper.digest /tmp/taskless-helper.digest
WORKDIR /workspace
ENV CARGO_TARGET_DIR=/tmp/dispatcharr-target
COPY rust/Cargo.toml rust/Cargo.lock rust/rust-toolchain.toml ./rust/
COPY rust/crates/ ./rust/crates/
COPY rust/vendor/ ./rust/vendor/
# This one runtime JSON is a compile-time include in app/static_assets.rs.
COPY rust/runtime/drf-static-source-1.json ./rust/runtime/drf-static-source-1.json
# One coordinated target cache; compiled products must leave the mount before
# another image stage can COPY them. The first build seeds this cache.
# Deterministic source tar mtimes are zero; refresh ALL local compile-input
# mtimes inside every uncached Cargo RUN so changed bytes cannot look fresh.
RUN --mount=type=cache,id=dispatcharr-release-1.90.0-bookworm-amd64,target=/tmp/dispatcharr-target,sharing=locked \
    --mount=type=cache,id=dispatcharr-registry-1.90.0-bookworm-amd64,target=/usr/local/cargo/registry,sharing=locked \
    mkdir -p "$CARGO_TARGET_DIR" /out && \
    find rust/crates rust/vendor -type f -exec touch {} + && \
    touch rust/Cargo.toml rust/Cargo.lock rust/rust-toolchain.toml rust/runtime/drf-static-source-1.json && \
    DISPATCHARR_PACKAGED_TASKLESS_HELPER_SHA256="$(cat /tmp/taskless-helper.digest)" \
    cargo build --manifest-path rust/Cargo.toml --locked --release --jobs 2 -p dispatcharr-rs && \
    cp "$CARGO_TARGET_DIR/release/dispatcharr-rs" /out/dispatcharr-rs && \
    cp "$CARGO_TARGET_DIR/release/dispatcharr-operator" /out/dispatcharr-operator

FROM build AS xmltv-parser-tests
RUN --mount=type=cache,id=dispatcharr-release-1.90.0-bookworm-amd64,target=/tmp/dispatcharr-target,sharing=locked \
    --mount=type=cache,id=dispatcharr-registry-1.90.0-bookworm-amd64,target=/usr/local/cargo/registry,sharing=locked \
    find rust/crates rust/vendor -type f -exec touch {} + && \
    touch rust/Cargo.toml rust/Cargo.lock rust/rust-toolchain.toml rust/runtime/drf-static-source-1.json && \
    cargo test --manifest-path rust/Cargo.toml --locked --jobs 2 -p dispatcharr-xmltv-parser --all-targets && \
    cargo build --manifest-path rust/Cargo.toml --locked --release --jobs 2 -p dispatcharr-xmltv-parser --examples && \
    cp "$CARGO_TARGET_DIR/release/examples/xmltv_stream_parity" /usr/local/bin/dispatcharr-xmltv-parity
COPY LICENSE ATTRIBUTION.md /usr/share/doc/dispatcharr/
COPY rust/crates/xmltv-parser/vendor/Copyright /usr/share/doc/dispatcharr/libxml2-Copyright
COPY rust/crates/xmltv-parser/vendor/NOTICES /usr/share/doc/dispatcharr/libxml2-NOTICES
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/dispatcharr-xmltv-parity"]

FROM ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f AS media
COPY rust/runtime/collect_media.py /tmp/collect_media.py
RUN /dispatcharrpy/bin/python /tmp/collect_media.py
COPY rust/runtime/collect_comskip.py rust/runtime/comskip-source.ini rust/runtime/comskip-LICENSE rust/runtime/comskip-NOTICE rust/runtime/comskip-Original-DispatcharrBase /tmp/
RUN /dispatcharrpy/bin/python /tmp/collect_comskip.py
COPY rust/runtime/collect_crypto.py /tmp/collect_crypto.py
RUN /dispatcharrpy/bin/python /tmp/collect_crypto.py
COPY rust/runtime/collect_postgres.py /tmp/collect_postgres.py
RUN /dispatcharrpy/bin/python /tmp/collect_postgres.py
COPY rust/runtime/collect_vlc.py /tmp/collect_vlc.py
RUN /dispatcharrpy/bin/python /tmp/collect_vlc.py
COPY rust/runtime/collect_static.py rust/runtime/drf-static-source-1.json /tmp/
RUN /dispatcharrpy/bin/python /tmp/collect_static.py

# Lazy regex dependency only: no Source application backend is packaged.
COPY rust/runtime/collect_regex_host.py /tmp/collect_regex_host.py
RUN /dispatcharrpy/bin/python /tmp/collect_regex_host.py
COPY --chmod=0555 rust/runtime/regex_bridge/ /media-runtime/opt/dispatcharr-python/bridge/
COPY rust/runtime/plugin_compat_source/ /tmp/plugin_compat_source/
COPY rust/runtime/python_host_licenses/ /tmp/python_host_licenses/
COPY rust/runtime/python_host_exclusions/ /tmp/python_host_exclusions/
COPY rust/runtime/collect_python_host.py /tmp/collect_python_host.py
RUN /dispatcharrpy/bin/python /tmp/collect_python_host.py
COPY --chmod=0555 rust/runtime/plugin_bridge/ /media-runtime/opt/dispatcharr-python/plugin-bridge/

FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc
LABEL org.opencontainers.image.title="Dispatcharr derivative Rust backend port" \
      org.opencontainers.image.licenses="AGPL-3.0-only" \
      org.opencontainers.image.description="Original Dispatcharr credit belongs to its original authors and contributors; see /usr/share/doc/dispatcharr/ATTRIBUTION.md. Migration parity is incomplete."
COPY --from=media /media-runtime/ /
COPY --from=taskless-restore-helper /taskless-out/opt/dispatcharr-pg-guarded-restore/ /opt/dispatcharr-pg-guarded-restore/
COPY --from=taskless-restore-helper /taskless-out/usr/share/doc/dispatcharr/pg-guarded-restore/ /usr/share/doc/dispatcharr/pg-guarded-restore/
COPY --from=build /out/dispatcharr-rs /usr/local/bin/dispatcharr-rs
COPY --from=build /out/dispatcharr-operator /usr/local/bin/dispatcharr-operator
COPY --from=frontend /dispatcharr-frontend/dist/ /opt/dispatcharr-frontend/
COPY LICENSE ATTRIBUTION.md /usr/share/doc/dispatcharr/
COPY rust/crates/output-http/data/Django-LICENSE /usr/share/doc/dispatcharr/Django-LICENSE
COPY rust/crates/store/third-party/PYTHON-COPYRIGHT /usr/share/doc/dispatcharr/Python-COPYRIGHT
COPY rust/crates/store/third-party/REQUESTS-LICENSE /usr/share/doc/dispatcharr/Requests-LICENSE
COPY rust/crates/store/datasets/Celery-LICENSE /usr/share/doc/dispatcharr/Celery-LICENSE
COPY rust/crates/store/datasets/Kombu-LICENSE /usr/share/doc/dispatcharr/Kombu-LICENSE
COPY rust/vendor/sqlx-core-0.8.6/LICENSE-MIT rust/vendor/sqlx-core-0.8.6/LICENSE-APACHE /usr/share/doc/dispatcharr/sqlx-core/
COPY rust/vendor/sqlx-postgres-0.8.6/LICENSE-MIT rust/vendor/sqlx-postgres-0.8.6/LICENSE-APACHE /usr/share/doc/dispatcharr/sqlx-postgres/
COPY rust/crates/xmltv-parser/vendor/Copyright /usr/share/doc/dispatcharr/libxml2-Copyright
COPY rust/crates/xmltv-parser/vendor/NOTICES /usr/share/doc/dispatcharr/libxml2-NOTICES
RUN mkdir -p /app /data/logos && ln -s /data /app/media && chown 65532:65532 /data /data/logos
WORKDIR /app
USER 65532:65532
RUN /opt/dispatcharr-pg-guarded-restore/bin/dispatcharr-pg-guarded-restore --version
ENV RUST_BIND=0.0.0.0:9192 RUST_FRONTEND_DIR=/opt/dispatcharr-frontend
EXPOSE 9192
ENTRYPOINT ["/usr/local/bin/dispatcharr-rs"]
