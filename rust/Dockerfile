FROM rust:1.90.0-bookworm@sha256:3914072ca0c3b8aad871db9169a651ccfce30cf58303e5d6f2db16d1d8a7e58f AS verify
RUN rustup component add rustfmt clippy
WORKDIR /workspace
ENV CARGO_TARGET_DIR=/tmp/dispatcharr-target
COPY rust/ ./rust/
RUN mkdir -p "$CARGO_TARGET_DIR" && cargo fetch --manifest-path rust/Cargo.toml --locked
COPY scripts/rust-perf/ ./scripts/rust-perf/
COPY scripts/rust-parity/ ./scripts/rust-parity/
COPY scripts/rust-contracts/ ./scripts/rust-contracts/
# Source-bound workspace goldens and portable register-test authorities.
COPY scripts/rust-timeshift-contracts/python-session-goldens.json ./scripts/rust-timeshift-contracts/python-session-goldens.json
COPY scripts/rust-timeshift-contracts/python-busy-seek-goldens.json ./scripts/rust-timeshift-contracts/python-busy-seek-goldens.json
COPY scripts/rust-timeshift-contracts/python-busy-control-vectors.json ./scripts/rust-timeshift-contracts/python-busy-control-vectors.json
COPY scripts/rust-timeshift-contracts/python-helper-goldens.json ./scripts/rust-timeshift-contracts/python-helper-goldens.json
COPY scripts/rust-timeshift-contracts/python-range-goldens.json ./scripts/rust-timeshift-contracts/python-range-goldens.json
COPY docs/rust-contract-register.json ./docs/rust-contract-register.json
COPY docs/rust-runtime-contracts.json ./docs/rust-runtime-contracts.json
COPY docs/rust-runtime-methods.json ./docs/rust-runtime-methods.json
COPY docs/rust-contract-inventory.json ./docs/rust-contract-inventory.json
COPY scripts/rust-plugin-contracts/source-plugin-http-4.json ./scripts/rust-plugin-contracts/source-plugin-http-4.json
COPY scripts/rust-plugin-contracts/verified-plugin-http-4.json ./scripts/rust-plugin-contracts/verified-plugin-http-4.json
COPY scripts/rust-integration/verified-checkpoint-image44-1.json ./scripts/rust-integration/verified-checkpoint-image44-1.json
COPY scripts/rust-vod-api-contracts/verified-unified-image36-1.json ./scripts/rust-vod-api-contracts/verified-unified-image36-1.json
COPY apps/proxy/live_proxy/input/ts_discontinuity.py ./apps/proxy/live_proxy/input/ts_discontinuity.py
CMD ["sh", "scripts/rust-verify.sh"]
COPY scripts/rust-verify.sh ./scripts/rust-verify.sh
