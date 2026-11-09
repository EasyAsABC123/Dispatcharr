FROM rust:1.90.0-bookworm@sha256:3914072ca0c3b8aad871db9169a651ccfce30cf58303e5d6f2db16d1d8a7e58f AS build
WORKDIR /workspace
ENV CARGO_TARGET_DIR=/tmp/dispatcharr-target
COPY rust/ ./rust/
RUN mkdir -p "$CARGO_TARGET_DIR" && cargo build --manifest-path rust/Cargo.toml --locked --release -p dispatcharr-rs

FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc
LABEL org.opencontainers.image.title="Dispatcharr derivative Rust backend port" \
      org.opencontainers.image.licenses="AGPL-3.0-only" \
      org.opencontainers.image.description="Original Dispatcharr credit belongs to its original authors and contributors; see /usr/share/doc/dispatcharr/ATTRIBUTION.md. Migration parity is incomplete."
COPY --from=build /tmp/dispatcharr-target/release/dispatcharr-rs /usr/local/bin/dispatcharr-rs
COPY LICENSE ATTRIBUTION.md /usr/share/doc/dispatcharr/
USER 65532:65532
ENV RUST_BIND=0.0.0.0:9192
EXPOSE 9192
ENTRYPOINT ["/usr/local/bin/dispatcharr-rs"]
