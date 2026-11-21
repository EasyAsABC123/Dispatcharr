"""Render the reviewed offline-only amendment to the exact upstream Dockerfile."""
import re
UPSTREAM="fce79836385ee64bc3c91360fb9cfb40818b80c7"

def replace_one(text, before, after):
    assert text.count(before) == 1, before
    return text.replace(before,after)

def render(recipe, base):
    assert re.fullmatch(r"docker.io/library/ubuntu@sha256:[0-9a-f]{64}",base)
    docker = recipe["Dockerfile"][0].decode()
    docker = replace_one(docker,"# syntax=docker/dockerfile:1.7\n","")
    docker = replace_one(docker,"ARG UBUNTU_BASE=ubuntu:26.04",f"ARG UBUNTU_BASE={base}")
    docker = replace_one(docker,"ARG TARGETARCH\n\n# Toolchain","ARG TARGETARCH\nCOPY offline/source/ /offline/source/\n\n# Toolchain")
    apt = ('apt-get -o Dir::Etc::sourcelist=/offline/apt/sources.list '
           '-o Dir::Etc::sourceparts=-')
    assert docker.count("apt-get") == 7
    docker = docker.replace("apt-get",apt)
    # Insert a read-only local archive mirror at the only two Ubuntu package RUN steps.
    anchored = f"RUN set -eux; \\\n    {apt} update"
    assert docker.count(anchored) == 2
    docker = docker.replace(anchored,
        f"RUN --mount=type=bind,source=offline/apt,target=/offline/apt,readonly set -eux; \\\n    {apt} update")
    for name in ("NV_CODEC_HEADERS","FDK_AAC_STRIPPED","FFMPEG"):
        ext = ".tar.xz" if name == "FFMPEG" else ".tar.gz"
        docker = replace_one(docker,f'curl -fsSL "${name}_URL"',f'cat /offline/source/{name}{ext}')
    docker = replace_one(docker,"ARG SOURCE_REVISION=unknown",f"ARG SOURCE_REVISION={UPSTREAM}")
    static = recipe["scripts/build-static-codecs.sh"][0].decode()
    static = replace_one(static,'  curl -fsSL "$url" | tar -xz --strip-components=1 -C "$dest"',
                        '  cat "$url" | tar -xz --strip-components=1 -C "$dest"')
    for name in ("X264","X265","LAME","OPUS","DAV1D","LIBVPX"):
        static = replace_one(static,f'fetch "${name}_URL"',f'fetch "/offline/source/{name}.tar.gz"')
    assert "curl -fsSL" not in docker and "curl -fsSL" not in static
    return {"Dockerfile":docker.encode(),"scripts/build-static-codecs.sh":static.encode()}
