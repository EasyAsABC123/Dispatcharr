FROM ghcr.io/dispatcharr/dispatcharr@sha256:6cbebd8fcd7dfeb32349ae75cad7022af15eef06d46f227cfb9373ea14c12ace AS collect
USER 0:0
COPY --chmod=0444 inventory.py assemble.py inputs.json ATTRIBUTION.md /verified/
RUN --network=none mkdir /closure && chown 65532:65532 /closure
USER 65532:65532
RUN --network=none /dispatcharrpy/bin/python -P -B /verified/assemble.py

FROM docker.io/library/debian:bookworm-slim@sha256:ec8534cda2cb30f29f44f23e135297af7a1496f2990fd8e4e84aa36b081a757a AS verify
COPY --from=collect /closure/ /
USER 65532:65532
