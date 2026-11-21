FROM ghcr.io/dispatcharr/dispatcharr@sha256:6cbebd8fcd7dfeb32349ae75cad7022af15eef06d46f227cfb9373ea14c12ace AS collect
USER 0:0
COPY --chmod=0444 ffmpeg/collect_media_arm64.py ffmpeg/assemble.py ffmpeg/inputs.json ATTRIBUTION.md /verified-ffmpeg/
COPY --chmod=0444 assemble_vlc.py inputs.json ATTRIBUTION.md /verified-vlc/
RUN --network=none mkdir /closure /vlc-closure && chown 65532:65532 /closure /vlc-closure
USER 65532:65532
RUN --network=none /dispatcharrpy/bin/python -I -B /verified-ffmpeg/assemble.py
RUN --network=none /dispatcharrpy/bin/python -I -B /verified-vlc/assemble_vlc.py

FROM docker.io/library/debian:bookworm-slim@sha256:ec8534cda2cb30f29f44f23e135297af7a1496f2990fd8e4e84aa36b081a757a
COPY --from=collect /closure/ /
COPY --from=collect /vlc-closure/ /
USER 65532:65532
