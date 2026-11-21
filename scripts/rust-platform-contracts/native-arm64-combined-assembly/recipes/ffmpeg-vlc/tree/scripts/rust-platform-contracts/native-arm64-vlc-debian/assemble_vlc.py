"""Copy only the qualified native donor inventory, with exact notice-byte binding."""
from hashlib import file_digest
import json
import os
from pathlib import Path
import shlex
import shutil

HERE = Path(__file__).resolve().parent
ROOT = Path("/vlc-closure")
PREFIX = Path("/opt/dispatcharr-vlc")
PLUGINS = Path("/usr/lib/aarch64-linux-gnu/vlc/plugins")
DATA = Path("/usr/share/vlc")
DOCS = Path("/usr/share/doc/dispatcharr-standalone-vlc")
LOADER = "/lib/ld-linux-aarch64.so.1"
INPUT_SHA = "736781db78aa402f7da88f88b9d2afabe47b333e68f4d608f538bbc66114f877"
ATTRIBUTION_SHA = "1202a089caf31b613a39ac819c8b362cd939cf828328a3880d1ffa16a45ffea7"
CREDIT = "Original Dispatcharr, VideoLAN, FFmpeg, Ubuntu, Debian and dependency authors retain full credit."


def stamp(path):
    with path.open("rb") as stream:
        return file_digest(stream, "sha256").hexdigest()


def absolute(value):
    path = Path(value)
    assert path.is_absolute() and ".." not in path.parts
    return path


def assemble(i):
    files = {}

    def copy(source, dest, expected, mode):
        target = ROOT / dest.relative_to("/")
        assert not target.exists() and source.is_file() and stamp(source) == expected
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        target.chmod(mode)
        assert stamp(target) == expected
        files[str(dest)] = expected

    assert os.uname().machine == "aarch64" and os.getuid() == os.getgid() == 65532
    assert i["vlc"]["interpreter"] == LOADER and i["vlc"]["ELF_machine"] == 183
    assert i["missing_notice_origins"] == [LOADER] and i["dpkg_ownership_gaps"] == {LOADER:["diversion-from"]}
    doc = i["interpreter_notice_correlation"]
    assert doc["owner"] == "libc6:arm64" and doc["path"] == "/usr/share/doc/libc6/copyright"
    assert doc["elf_sha256"] == i["resolved_libraries"][LOADER]["sha256"]
    assert doc["sha256"] == i["notice_files"][doc["owner"]]["sha256"]
    assert stamp(Path(doc["path"])) == doc["sha256"]
    copy(Path("/usr/bin/vlc"), PREFIX / "bin/vlc", i["vlc"]["sha256"], 0o555)
    assert stamp(Path("/usr/bin/cvlc")) == i["cvlc"]["sha256"]
    assert i["cvlc"]["literal_prefix_arguments"] == ["-I", "dummy"]
    libs = i["resolved_libraries"]
    assert len(libs) == len({Path(name).name for name in libs}) == 201
    for name, meta in libs.items():
        source = absolute(name)
        assert any(source.is_relative_to(root) for root in ("/lib", "/usr/lib", "/usr/local/lib"))
        copy(source, PREFIX / "lib" / source.name, meta["sha256"], 0o555)
    plugins = i["plugins"]
    assert len(plugins) == 340
    for name, meta in plugins.items():
        source = absolute(name)
        assert source.is_relative_to(PLUGINS)
        if meta.get("generated_plugin_cache"):
            assert source == PLUGINS / "plugins.dat" and stamp(source) == meta["sha256"]
        else:
            assert meta["ELF_machine"] == 183
            copy(source, source, meta["sha256"], 0o555)
    assert len(i["public_vlc_data"]) == 63
    for name, meta in i["public_vlc_data"].items():
        source = absolute(name)
        assert source.is_relative_to(DATA)
        copy(source, source, meta["sha256"], 0o444)
    assert len(i["notice_files"]) == 185
    for package, meta in i["notice_files"].items():
        source = absolute(meta["path"])
        assert source.is_relative_to("/usr/share/doc") or source.is_relative_to("/usr/local/share/doc/dispatcharr-ffmpeg")
        assert Path(package).name == package and package not in {".", ".."}
        copy(source, DOCS / "notices" / package / "copyright", meta["sha256"], 0o444)
    copy(HERE / "ATTRIBUTION.md", DOCS / "ATTRIBUTION.md", ATTRIBUTION_SHA, 0o444)
    for name, args in (("vlc", []), ("cvlc", i["cvlc"]["literal_prefix_arguments"])):
        output = Path("/usr/local/bin") / name
        target = ROOT / output.relative_to("/")
        command = [str(PREFIX / "lib" / Path(LOADER).name), "--inhibit-cache", "--library-path",
                   str(PREFIX / "lib"), str(PREFIX / "bin/vlc"), *args]
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(f"#!/bin/sh\nVLC_PLUGIN_PATH={PLUGINS}\nexport VLC_PLUGIN_PATH\nexec {shlex.join(command)} \"$@\"\n")
        target.chmod(0o555)
        files[str(output)] = stamp(target)
    assert len(files) == 792
    (ROOT / DOCS.relative_to("/") / "unit-inputs.json").write_text(json.dumps({
        "inputs_sha256": INPUT_SHA, "collected_files": files, "original_notice_gap": [LOADER],
        "independent_same_donor_interpreter_notice": doc, "distribution_qualified": False, "credit": CREDIT,
    }, sort_keys=True) + "\n")


if __name__ == "__main__":
    source = HERE / "inputs.json"
    assert stamp(source) == INPUT_SHA
    assemble(json.loads(source.read_bytes()))
