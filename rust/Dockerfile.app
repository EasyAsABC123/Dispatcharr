FROM node:24@sha256:8fa96264f302f58bbb13aa7339c541547b56975766ce956c875da149452a3e40 AS frontend-dependencies
WORKDIR /dispatcharr-frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY frontend/ ./

FROM frontend-dependencies AS frontend-tests
RUN mkdir -p /verification-results && npm test -- --maxWorkers=2 --no-file-parallelism --reporter=json --outputFile=/verification-results/frontend-tests.json

FROM frontend-dependencies AS frontend
RUN npm run build

FROM rust:1.90.0-bookworm@sha256:3914072ca0c3b8aad871db9169a651ccfce30cf58303e5d6f2db16d1d8a7e58f AS build
WORKDIR /workspace
ENV CARGO_TARGET_DIR=/tmp/dispatcharr-target
COPY rust/ ./rust/
RUN mkdir -p "$CARGO_TARGET_DIR" && cargo build --manifest-path rust/Cargo.toml --locked --release -p dispatcharr-rs

FROM build AS xmltv-parser-tests
RUN cargo test --manifest-path rust/Cargo.toml --locked -p dispatcharr-xmltv-parser --all-targets && cargo build --manifest-path rust/Cargo.toml --locked --release -p dispatcharr-xmltv-parser --examples && cp /tmp/dispatcharr-target/release/examples/xmltv_stream_parity /usr/local/bin/dispatcharr-xmltv-parity
COPY LICENSE ATTRIBUTION.md /usr/share/doc/dispatcharr/
COPY rust/crates/xmltv-parser/vendor/Copyright /usr/share/doc/dispatcharr/libxml2-Copyright
COPY rust/crates/xmltv-parser/vendor/NOTICES /usr/share/doc/dispatcharr/libxml2-NOTICES
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/dispatcharr-xmltv-parity"]

FROM ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f AS media
COPY rust/runtime/collect_media.py /tmp/collect_media.py
RUN /dispatcharrpy/bin/python /tmp/collect_media.py
COPY rust/runtime/collect_crypto.py /tmp/collect_crypto.py
RUN /dispatcharrpy/bin/python /tmp/collect_crypto.py
COPY rust/runtime/collect_static.py rust/runtime/drf-static-source-1.json /tmp/
RUN /dispatcharrpy/bin/python /tmp/collect_static.py

FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc
LABEL org.opencontainers.image.title="Dispatcharr derivative Rust backend port" \
      org.opencontainers.image.licenses="AGPL-3.0-only" \
      org.opencontainers.image.description="Original Dispatcharr credit belongs to its original authors and contributors; see /usr/share/doc/dispatcharr/ATTRIBUTION.md. Migration parity is incomplete."
COPY --from=media /media-runtime/ /
COPY --from=build /tmp/dispatcharr-target/release/dispatcharr-rs /usr/local/bin/dispatcharr-rs
COPY --from=build /tmp/dispatcharr-target/release/dispatcharr-operator /usr/local/bin/dispatcharr-operator
COPY --from=frontend /dispatcharr-frontend/dist/ /opt/dispatcharr-frontend/
COPY LICENSE ATTRIBUTION.md /usr/share/doc/dispatcharr/
COPY rust/crates/output-http/data/Django-LICENSE /usr/share/doc/dispatcharr/Django-LICENSE
COPY rust/crates/store/third-party/PYTHON-COPYRIGHT /usr/share/doc/dispatcharr/Python-COPYRIGHT
COPY rust/crates/xmltv-parser/vendor/Copyright /usr/share/doc/dispatcharr/libxml2-Copyright
COPY rust/crates/xmltv-parser/vendor/NOTICES /usr/share/doc/dispatcharr/libxml2-NOTICES
RUN mkdir -p /app /data/logos && ln -s /data /app/media && chown 65532:65532 /data /data/logos
USER 65532:65532
ENV RUST_BIND=0.0.0.0:9192 RUST_FRONTEND_DIR=/opt/dispatcharr-frontend
EXPOSE 9192
ENTRYPOINT ["/usr/local/bin/dispatcharr-rs"]
