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

# Exact public authorities used by the saved Connect webhook Test register check.
COPY scripts/rust-integration/milestone-batch84/packaged.json ./scripts/rust-integration/milestone-batch84/packaged.json
COPY scripts/rust-integration/milestone-batch89/packaged.json ./scripts/rust-integration/milestone-batch89/packaged.json

# Exact public authorities for the selected and batch Set EPG register checks.
COPY scripts/rust-integration/milestone-batch85/selected-guide-compose.json ./scripts/rust-integration/milestone-batch85/selected-guide-compose.json
COPY scripts/rust-integration/milestone-batch91/acceptance.json ./scripts/rust-integration/milestone-batch91/acceptance.json

# Exact public System Logs and stream regex preview register authorities.
COPY scripts/rust-integration/milestone-batch92/acceptance.json ./scripts/rust-integration/milestone-batch92/acceptance.json
COPY scripts/rust-integration/milestone-batch94/acceptance.json ./scripts/rust-integration/milestone-batch94/acceptance.json

# Exact public Original live TS channel-stop authorities.
COPY scripts/rust-milestone-batches/m235/profile-source-media/publication-summary.json ./scripts/rust-milestone-batches/m235/profile-source-media/publication-summary.json
COPY scripts/rust-milestone-batches/m235/profile-source-media/fresh-cleanup.json ./scripts/rust-milestone-batches/m235/profile-source-media/fresh-cleanup.json
COPY scripts/rust-milestone-batches/m235/profile-source-media/shared_ts.py ./scripts/rust-milestone-batches/m235/profile-source-media/shared_ts.py
COPY scripts/rust-milestone-batches/m235/profile-source-media/manifest.json ./scripts/rust-milestone-batches/m235/profile-source-media/manifest.json
COPY scripts/rust-milestone-batches/m235/ts-controls/Original.json ./scripts/rust-milestone-batches/m235/ts-controls/Original.json
COPY scripts/rust-milestone-batches/m235/ts-controls/request-driver.py ./scripts/rust-milestone-batches/m235/ts-controls/request-driver.py
COPY scripts/rust-integration/canonical-management-original/receipt.json ./scripts/rust-integration/canonical-management-original/receipt.json
COPY scripts/rust-integration/exact-vod-original-c98/receipt.json ./scripts/rust-integration/exact-vod-original-c98/receipt.json
COPY scripts/rust-integration/exact-vod-original-c98/request-driver.py ./scripts/rust-integration/exact-vod-original-c98/request-driver.py
COPY scripts/rust-integration/image57-checkpoint/selected-accounts-Original.json ./scripts/rust-integration/image57-checkpoint/selected-accounts-Original.json
COPY scripts/rust-integration/image57-checkpoint/selected-m3u-Original.json ./scripts/rust-integration/image57-checkpoint/selected-m3u-Original.json
COPY scripts/rust-integration/retained-provider-admin/receipt.json ./scripts/rust-integration/retained-provider-admin/receipt.json
COPY scripts/rust-integration/retained-c98-hdhr/receipt.json ./scripts/rust-integration/retained-c98-hdhr/receipt.json
COPY scripts/rust-integration/retained-core-profile-json/receipt.json ./scripts/rust-integration/retained-core-profile-json/receipt.json
COPY scripts/rust-integration/retained-hdhr-literal/receipt.json ./scripts/rust-integration/retained-hdhr-literal/receipt.json
COPY scripts/rust-integration/milestone-batch106/connect-replay-register.json ./scripts/rust-integration/milestone-batch106/connect-replay-register.json
COPY scripts/rust-integration/retained-logo-m3u/receipt.json ./scripts/rust-integration/retained-logo-m3u/receipt.json
COPY scripts/rust-integration/retained-epg-vod-logo/receipt.json ./scripts/rust-integration/retained-epg-vod-logo/receipt.json
COPY scripts/rust-integration/retained-c105-guide-startup/receipt.json ./scripts/rust-integration/retained-c105-guide-startup/receipt.json
COPY scripts/rust-pluginrepo-contracts/metadata-component/authority/Original/apps/plugins/tasks.py ./scripts/rust-pluginrepo-contracts/metadata-component/authority/Original/apps/plugins/tasks.py
COPY scripts/rust-integration/milestone-batch98/acceptance.json ./scripts/rust-integration/milestone-batch98/acceptance.json
COPY scripts/rust-integration/image57-checkpoint/evidence/matrix/reports/user-agent/0-integration.json.json ./scripts/rust-integration/image57-checkpoint/evidence/matrix/reports/user-agent/0-integration.json.json
COPY scripts/rust-integration/image57-checkpoint/evidence/matrix/reports/user-agent/1-packaged-checks.json.json ./scripts/rust-integration/image57-checkpoint/evidence/matrix/reports/user-agent/1-packaged-checks.json.json
COPY scripts/rust-integration/image57-checkpoint/evidence/matrix/controllers/user-agent.json ./scripts/rust-integration/image57-checkpoint/evidence/matrix/controllers/user-agent.json
COPY scripts/rust-integration/image57-checkpoint/evidence/matrix/cleanup/user-agent.json ./scripts/rust-integration/image57-checkpoint/evidence/matrix/cleanup/user-agent.json
COPY scripts/rust-integration/m4-credential-pool-source1/identity.json ./scripts/rust-integration/m4-credential-pool-source1/identity.json
COPY scripts/rust-integration/m4-credential-pool-source1/terminal.json ./scripts/rust-integration/m4-credential-pool-source1/terminal.json
COPY scripts/rust-integration/m4-credential-pool-source1/Root-independent-cleanup.json ./scripts/rust-integration/m4-credential-pool-source1/Root-independent-cleanup.json
COPY scripts/rust-integration/m4-credential-pool-source1/control/A-profile-save.json ./scripts/rust-integration/m4-credential-pool-source1/control/A-profile-save.json
COPY scripts/rust-integration/m4-credential-pool-source1/control/B-profile-save.json ./scripts/rust-integration/m4-credential-pool-source1/control/B-profile-save.json
COPY scripts/rust-integration/m4-credential-pool-source1/control/C-profile-save.json ./scripts/rust-integration/m4-credential-pool-source1/control/C-profile-save.json
COPY scripts/rust-integration/m4-credential-pool-source1/control/D-profile-save.json ./scripts/rust-integration/m4-credential-pool-source1/control/D-profile-save.json
COPY scripts/rust-integration/image57-checkpoint/evidence/matrix/reports/baseline-all/0-integration.json.json ./scripts/rust-integration/image57-checkpoint/evidence/matrix/reports/baseline-all/0-integration.json.json
COPY scripts/rust-integration/image57-checkpoint/evidence/matrix/controllers/baseline-all.json ./scripts/rust-integration/image57-checkpoint/evidence/matrix/controllers/baseline-all.json
COPY scripts/rust-integration/image57-checkpoint/evidence/matrix/cleanup/baseline-all.json ./scripts/rust-integration/image57-checkpoint/evidence/matrix/cleanup/baseline-all.json

# Public Original Comskip task authorities for offline register verification.
COPY scripts/rust-integration/comskip-positive-lifecycle-source1/ ./scripts/rust-integration/comskip-positive-lifecycle-source1/
COPY scripts/rust-integration/comskip-positive-journal-fences1/ ./scripts/rust-integration/comskip-positive-journal-fences1/

# Exact public Original clocked-recording task observations.
COPY scripts/rust-job-contracts/clocked-recording-source/evidence/integration.json.gz ./scripts/rust-job-contracts/clocked-recording-source/evidence/integration.json.gz
COPY scripts/rust-job-contracts/clocked-recording-source/evidence/source-clocked-recording-private.json.gz ./scripts/rust-job-contracts/clocked-recording-source/evidence/source-clocked-recording-private.json.gz
COPY scripts/rust-job-contracts/clocked-recording-source/evidence/independent-cleanup.json.gz ./scripts/rust-job-contracts/clocked-recording-source/evidence/independent-cleanup.json.gz
COPY scripts/rust-job-contracts/clocked-recording-source/evidence/clocked-prerun.jsonl.gz ./scripts/rust-job-contracts/clocked-recording-source/evidence/clocked-prerun.jsonl.gz
COPY scripts/rust-job-contracts/clocked-recording-source/evidence/clocked-postrun.jsonl.gz ./scripts/rust-job-contracts/clocked-recording-source/evidence/clocked-postrun.jsonl.gz

# Public authorities read by the portable register verification.
COPY scripts/rust-epg-contracts/verified-program-http-8.json ./scripts/rust-epg-contracts/verified-program-http-8.json
COPY scripts/rust-hdhr-contracts/probe.py ./scripts/rust-hdhr-contracts/probe.py
COPY scripts/rust-hdhr-contracts/verified-hdhr-image27-1.json ./scripts/rust-hdhr-contracts/verified-hdhr-image27-1.json
COPY scripts/rust-integration/auth-route-session-source1/README.md ./scripts/rust-integration/auth-route-session-source1/README.md
COPY scripts/rust-integration/auth-route-session-source1/Root-independent-cleanup.json ./scripts/rust-integration/auth-route-session-source1/Root-independent-cleanup.json
COPY scripts/rust-integration/auth-route-session-source1/Root-terminal.json ./scripts/rust-integration/auth-route-session-source1/Root-terminal.json
COPY scripts/rust-integration/auth-route-session-source1/captured-report.json ./scripts/rust-integration/auth-route-session-source1/captured-report.json
COPY scripts/rust-integration/auth-route-session-source1/identity.json ./scripts/rust-integration/auth-route-session-source1/identity.json
COPY scripts/rust-integration/auth-route-session-source1/index.json ./scripts/rust-integration/auth-route-session-source1/index.json
COPY scripts/rust-integration/auth-route-session-source1/private-raw-hashes.json ./scripts/rust-integration/auth-route-session-source1/private-raw-hashes.json
COPY scripts/rust-integration/auth-route-session-source1/qualification.json ./scripts/rust-integration/auth-route-session-source1/qualification.json
COPY scripts/rust-integration/auth-route-session-source1/throttle-analysis.json ./scripts/rust-integration/auth-route-session-source1/throttle-analysis.json
COPY scripts/rust-integration/auth-route-session-source2/README.md ./scripts/rust-integration/auth-route-session-source2/README.md
COPY scripts/rust-integration/auth-route-session-source2/Root-independent-cleanup.json ./scripts/rust-integration/auth-route-session-source2/Root-independent-cleanup.json
COPY scripts/rust-integration/auth-route-session-source2/Root-terminal.json ./scripts/rust-integration/auth-route-session-source2/Root-terminal.json
COPY scripts/rust-integration/auth-route-session-source2/captured-report.json ./scripts/rust-integration/auth-route-session-source2/captured-report.json
COPY scripts/rust-integration/auth-route-session-source2/identity.json ./scripts/rust-integration/auth-route-session-source2/identity.json
COPY scripts/rust-integration/auth-route-session-source2/index.json ./scripts/rust-integration/auth-route-session-source2/index.json
COPY scripts/rust-integration/auth-route-session-source2/login-budget-after.json ./scripts/rust-integration/auth-route-session-source2/login-budget-after.json
COPY scripts/rust-integration/auth-route-session-source2/login-budget-before.json ./scripts/rust-integration/auth-route-session-source2/login-budget-before.json
COPY scripts/rust-integration/auth-route-session-source2/private-raw-hashes.json ./scripts/rust-integration/auth-route-session-source2/private-raw-hashes.json
COPY scripts/rust-integration/auth-route-session-source2/qualification.json ./scripts/rust-integration/auth-route-session-source2/qualification.json
COPY scripts/rust-integration/verified-integration-m5-auth-image22-1.json ./scripts/rust-integration/verified-integration-m5-auth-image22-1.json
COPY scripts/rust-milestone-batches/m6/channel-http-source-v1/actual/source-report.json ./scripts/rust-milestone-batches/m6/channel-http-source-v1/actual/source-report.json
COPY scripts/rust-milestone-batches/m6/channel-http-source-v1/driver/tools/cases.py ./scripts/rust-milestone-batches/m6/channel-http-source-v1/driver/tools/cases.py
COPY scripts/rust-milestone-batches/m6/channel-http-source-v1/driver/tools/guest.py ./scripts/rust-milestone-batches/m6/channel-http-source-v1/driver/tools/guest.py
COPY scripts/rust-milestone-batches/m6/channel-http-source-v1/summary.json ./scripts/rust-milestone-batches/m6/channel-http-source-v1/summary.json
COPY scripts/rust-vod-api-contracts/source-vod-http-wire-2.json ./scripts/rust-vod-api-contracts/source-vod-http-wire-2.json
