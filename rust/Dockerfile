FROM rust:1.90.0-bookworm@sha256:3914072ca0c3b8aad871db9169a651ccfce30cf58303e5d6f2db16d1d8a7e58f AS verify
RUN rustup component add rustfmt clippy
WORKDIR /workspace
ENV CARGO_TARGET_DIR=/tmp/dispatcharr-target
COPY rust/ ./rust/
RUN mkdir -p "$CARGO_TARGET_DIR" && cargo fetch --manifest-path rust/Cargo.toml --locked
COPY scripts/rust-perf/ ./scripts/rust-perf/
COPY scripts/rust-parity/ ./scripts/rust-parity/
COPY scripts/rust-contracts/ ./scripts/rust-contracts/
COPY apps/proxy/live_proxy/input/ts_discontinuity.py ./apps/proxy/live_proxy/input/ts_discontinuity.py
CMD ["sh", "scripts/rust-verify.sh"]
COPY scripts/rust-verify.sh ./scripts/rust-verify.sh
