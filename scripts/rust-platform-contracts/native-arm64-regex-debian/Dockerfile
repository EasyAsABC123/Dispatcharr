FROM ghcr.io/dispatcharr/dispatcharr@sha256:6cbebd8fcd7dfeb32349ae75cad7022af15eef06d46f227cfb9373ea14c12ace AS donor
COPY collect_regex_host.py /tmp/collect_regex_host.py
RUN /dispatcharrpy/bin/python -I -B /tmp/collect_regex_host.py
COPY --chmod=0555 regex_bridge/ /media-runtime/opt/dispatcharr-python/bridge/
COPY LICENSE ATTRIBUTION.md /media-runtime/usr/share/doc/dispatcharr/

FROM docker.io/library/debian:bookworm-slim@sha256:ec8534cda2cb30f29f44f23e135297af7a1496f2990fd8e4e84aa36b081a757a AS verify
COPY --from=donor /media-runtime/ /
USER 65532:65532
