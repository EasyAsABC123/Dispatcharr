FROM ghcr.io/dispatcharr/dispatcharr@sha256:6cbebd8fcd7dfeb32349ae75cad7022af15eef06d46f227cfb9373ea14c12ace AS donor
COPY rust/runtime/collect_regex_host.py /tmp/collect_regex_host.py
RUN /dispatcharrpy/bin/python -I -B /tmp/collect_regex_host.py
COPY --chmod=0555 rust/runtime/regex_bridge/ /media-runtime/opt/dispatcharr-python/bridge/
COPY rust/runtime/plugin_compat_source/ /tmp/plugin_compat_source/
COPY rust/runtime/python_host_licenses/ /tmp/python_host_licenses/
COPY rust/runtime/python_host_exclusions/ /tmp/python_host_exclusions/
COPY rust/runtime/collect_python_host.py /tmp/collect_python_host.py
RUN /dispatcharrpy/bin/python -I -B /tmp/collect_python_host.py
COPY --chmod=0555 rust/runtime/plugin_bridge/ /media-runtime/opt/dispatcharr-python/plugin-bridge/
COPY LICENSE ATTRIBUTION.md /media-runtime/usr/share/doc/dispatcharr/

FROM docker.io/library/debian:bookworm-slim@sha256:ec8534cda2cb30f29f44f23e135297af7a1496f2990fd8e4e84aa36b081a757a AS verify
COPY --from=donor /media-runtime/ /
USER 65532:65532
