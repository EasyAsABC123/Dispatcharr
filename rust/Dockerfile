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

# Exact Original task-fixture and retained-capture authorities for portable assertions.
COPY scripts/rust-job-contracts/scanner-logo/children/source-children-4.json ./scripts/rust-job-contracts/scanner-logo/children/source-children-4.json
COPY scripts/rust-job-contracts/scanner-logo/children/verified-children-4.json ./scripts/rust-job-contracts/scanner-logo/children/verified-children-4.json
COPY scripts/rust-job-contracts/scanner-logo/children/run_pair.py ./scripts/rust-job-contracts/scanner-logo/children/run_pair.py
COPY scripts/rust-job-contracts/scanner-logo/children/source_pair.py ./scripts/rust-job-contracts/scanner-logo/children/source_pair.py
COPY scripts/rust-job-contracts/scanner-logo/children/periodic_worker_observer.py ./scripts/rust-job-contracts/scanner-logo/children/periodic_worker_observer.py
COPY scripts/rust-job-contracts/scanner-logo/m3u-reference/source-m3u-4.json ./scripts/rust-job-contracts/scanner-logo/m3u-reference/source-m3u-4.json
COPY scripts/rust-job-contracts/scanner-logo/m3u-reference/verified-m3u-4.json ./scripts/rust-job-contracts/scanner-logo/m3u-reference/verified-m3u-4.json
COPY scripts/rust-job-contracts/scanner-logo/m3u-reference/run_pair.py ./scripts/rust-job-contracts/scanner-logo/m3u-reference/run_pair.py
COPY scripts/rust-job-contracts/scanner-logo/m3u-reference/source_pair.py ./scripts/rust-job-contracts/scanner-logo/m3u-reference/source_pair.py
COPY scripts/rust-job-contracts/scanner-logo/m3u-reference/periodic_worker_observer.py ./scripts/rust-job-contracts/scanner-logo/m3u-reference/periodic_worker_observer.py
COPY scripts/rust-job-contracts/scanner-logo/file-branches/source-file-branches-2.json ./scripts/rust-job-contracts/scanner-logo/file-branches/source-file-branches-2.json
COPY scripts/rust-job-contracts/scanner-logo/file-branches/verified-file-branches-2.json ./scripts/rust-job-contracts/scanner-logo/file-branches/verified-file-branches-2.json
COPY scripts/rust-job-contracts/scanner-logo/file-branches/run_pair.py ./scripts/rust-job-contracts/scanner-logo/file-branches/run_pair.py
COPY scripts/rust-job-contracts/scanner-logo/file-branches/source_pair.py ./scripts/rust-job-contracts/scanner-logo/file-branches/source_pair.py
COPY scripts/rust-job-contracts/scanner-logo/file-branches/periodic_worker_observer.py ./scripts/rust-job-contracts/scanner-logo/file-branches/periodic_worker_observer.py

# Public Source51/53/55 inputs read by portable register/source-contract tests.
COPY scripts/rust-integration/channel-matching-source51/Source51/qualification.json ./scripts/rust-integration/channel-matching-source51/Source51/qualification.json
COPY scripts/rust-integration/channel-matching-source51/Source51/cases.json ./scripts/rust-integration/channel-matching-source51/Source51/cases.json
COPY scripts/rust-integration/channel-matching-source51/Source51/availability.json ./scripts/rust-integration/channel-matching-source51/Source51/availability.json
COPY scripts/rust-integration/channel-matching-source51/Source51/cleanup.json ./scripts/rust-integration/channel-matching-source51/Source51/cleanup.json
COPY scripts/rust-integration/channel-matching-source51/Source51/execution.json ./scripts/rust-integration/channel-matching-source51/Source51/execution.json
COPY scripts/rust-integration/channel-stations-source53/metadata.json ./scripts/rust-integration/channel-stations-source53/metadata.json
COPY scripts/rust-integration/channel-stations-source53/execution.json ./scripts/rust-integration/channel-stations-source53/execution.json
COPY scripts/rust-integration/channel-stations-source53/report.json ./scripts/rust-integration/channel-stations-source53/report.json
COPY scripts/rust-integration/channel-stations-source53/independent-cleanup.json ./scripts/rust-integration/channel-stations-source53/independent-cleanup.json
COPY scripts/rust-integration/channel-stations-source53/transport-trust.json ./scripts/rust-integration/channel-stations-source53/transport-trust.json
COPY scripts/rust-integration/milestone-batch38/Source55/qualification.json ./scripts/rust-integration/milestone-batch38/Source55/qualification.json
COPY scripts/rust-integration/milestone-batch38/Source55/cases.json ./scripts/rust-integration/milestone-batch38/Source55/cases.json
COPY scripts/rust-integration/milestone-batch38/Source55/execution.json ./scripts/rust-integration/milestone-batch38/Source55/execution.json
COPY scripts/rust-integration/milestone-batch38/Source55/cleanup.json ./scripts/rust-integration/milestone-batch38/Source55/cleanup.json
COPY scripts/rust-integration/milestone-batch38/Source55/default-custom-contract.json ./scripts/rust-integration/milestone-batch38/Source55/default-custom-contract.json
COPY scripts/rust-integration/channel-stations-source53/creation-actual-frame.json ./scripts/rust-integration/channel-stations-source53/creation-actual-frame.json
COPY scripts/rust-integration/channel-stations-source53/creation-backend.json ./scripts/rust-integration/channel-stations-source53/creation-backend.json
COPY scripts/rust-integration/channel-stations-source53/lineup-add-actual-frame.json ./scripts/rust-integration/channel-stations-source53/lineup-add-actual-frame.json
COPY scripts/rust-integration/channel-stations-source53/lineup-add-backend.json ./scripts/rust-integration/channel-stations-source53/lineup-add-backend.json
COPY scripts/rust-integration/channel-stations-source53/partial-actual-frame.json ./scripts/rust-integration/channel-stations-source53/partial-actual-frame.json
COPY scripts/rust-integration/channel-stations-source53/partial-backend.json ./scripts/rust-integration/channel-stations-source53/partial-backend.json
COPY scripts/rust-integration/channel-stations-source53/empty4102-actual-frame.json ./scripts/rust-integration/channel-stations-source53/empty4102-actual-frame.json
COPY scripts/rust-integration/channel-stations-source53/empty4102-backend.json ./scripts/rust-integration/channel-stations-source53/empty4102-backend.json

# Public Original Comskip task authorities for offline register verification.
COPY scripts/rust-integration/comskip-positive-lifecycle-source1/ ./scripts/rust-integration/comskip-positive-lifecycle-source1/
COPY scripts/rust-integration/comskip-positive-journal-fences1/ ./scripts/rust-integration/comskip-positive-journal-fences1/
