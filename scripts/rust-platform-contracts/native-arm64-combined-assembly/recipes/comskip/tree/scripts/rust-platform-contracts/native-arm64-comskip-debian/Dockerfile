FROM ghcr.io/dispatcharr/dispatcharr@sha256:6cbebd8fcd7dfeb32349ae75cad7022af15eef06d46f227cfb9373ea14c12ace AS build
USER 0:0
COPY --chmod=0444 stage.py inputs.json /inputs/
COPY packages/ /inputs/packages/
RUN --network=none mkdir -p /out /work/source && chown -R 65532:65532 /out /work && \
    /dispatcharrpy/bin/python -I -B /inputs/stage.py install
COPY --chown=65532:65532 source/ /work/source/
USER 65532:65532
RUN --network=none /dispatcharrpy/bin/python -I -B /inputs/stage.py build

FROM ghcr.io/dispatcharr/dispatcharr@sha256:6cbebd8fcd7dfeb32349ae75cad7022af15eef06d46f227cfb9373ea14c12ace AS collect
USER 0:0
COPY --chmod=0444 collect_comskip_arm64.py comskip-arm64-abi-inputs.json assemble_comskip.py /validation/
COPY --chmod=0444 ffmpeg/collect_media_arm64.py ffmpeg/assemble.py ffmpeg/inputs.json ATTRIBUTION.md /validated-ffmpeg/
COPY --from=build /out/comskip /materials/comskip
COPY --chmod=0444 Comskip-source.tar.gz Comskip-LICENSE Original-DispatcharrBase comskip.ini Dispatcharr-ATTRIBUTION.md /materials/
RUN --network=none mkdir /comskip-closure /closure && chown 65532:65532 /comskip-closure /closure
USER 65532:65532
RUN --network=none /dispatcharrpy/bin/python -I -B -c "import runpy,sys; sys.path.insert(0,'/validation'); runpy.run_path('/validation/assemble_comskip.py',run_name='__main__')"
RUN --network=none /dispatcharrpy/bin/python -I -B /validated-ffmpeg/assemble.py

FROM docker.io/library/debian:bookworm-slim@sha256:ec8534cda2cb30f29f44f23e135297af7a1496f2990fd8e4e84aa36b081a757a AS verify
COPY --from=collect /closure/ /
COPY --from=collect /comskip-closure/ /
USER 65532:65532
