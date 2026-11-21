FROM ghcr.io/dispatcharr/dispatcharr@sha256:6cbebd8fcd7dfeb32349ae75cad7022af15eef06d46f227cfb9373ea14c12ace AS build
USER 0:0
COPY --chmod=0444 stage.py inputs.json /inputs/
COPY packages/ /inputs/packages/
RUN --network=none mkdir -p /out /work/source && chown -R 65532:65532 /out /work && \
    /dispatcharrpy/bin/python -I -B /inputs/stage.py install
COPY --chown=65532:65532 source/ /work/source/
USER 65532:65532
RUN --network=none /dispatcharrpy/bin/python -I -B /inputs/stage.py build

FROM ghcr.io/dispatcharr/dispatcharr@sha256:6cbebd8fcd7dfeb32349ae75cad7022af15eef06d46f227cfb9373ea14c12ace AS smoke
COPY --from=build /out/comskip /out/build.json /opt/dispatcharr-comskip-abi/
COPY --chmod=0444 stage.py inputs.json comskip.ini /opt/dispatcharr-comskip-abi/
COPY --chmod=0444 Comskip-LICENSE Original-DispatcharrBase Dispatcharr-ATTRIBUTION.md /opt/dispatcharr-comskip-abi/notices/
USER 65532:65532
