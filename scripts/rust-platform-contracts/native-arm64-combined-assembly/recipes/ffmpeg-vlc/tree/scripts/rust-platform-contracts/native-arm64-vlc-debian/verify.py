"""Validate bounded runtime output from the standalone VLC package smoke."""
from collections import Counter
from hashlib import sha256
import json
from pathlib import Path
import re
import shlex
import sys

from assemble_vlc import ATTRIBUTION_SHA, DOCS, INPUT_SHA, LOADER, PLUGINS, PREFIX


def verify(state, stdout, source):
    assert state["Status"] == "exited" and state["ExitCode"] == 0 and state["OOMKilled"] is False
    assert 0 < len(stdout) <= 512 * 1024 and sha256(source).hexdigest() == INPUT_SHA
    inputs = json.loads(source)
    fields = dict(re.findall(r"^BEGIN ([a-z]+)\n(.*?)^END \1\n", stdout.decode(), re.MULTILINE | re.DOTALL))
    assert set(fields) == {"bytes", "files", "inventory", "media", "versions"}
    manifest = json.loads(fields["inventory"])
    assert manifest["inputs_sha256"] == INPUT_SHA and manifest["distribution_qualified"] is False
    assert manifest["original_notice_gap"] == [LOADER]
    assert manifest["independent_same_donor_interpreter_notice"] == inputs["interpreter_notice_correlation"]
    expected = {str(PREFIX / "bin/vlc"): inputs["vlc"]["sha256"], str(DOCS / "ATTRIBUTION.md"): ATTRIBUTION_SHA}
    expected.update({str(PREFIX / "lib" / Path(k).name): v["sha256"] for k, v in inputs["resolved_libraries"].items()})
    expected.update({k:v["sha256"] for k, v in inputs["plugins"].items() if not v.get("generated_plugin_cache")})
    expected.update({k:v["sha256"] for k, v in inputs["public_vlc_data"].items()})
    expected.update({str(DOCS / "notices" / k / "copyright"): v["sha256"] for k, v in inputs["notice_files"].items()})
    for name, args in (("vlc", []), ("cvlc", inputs["cvlc"]["literal_prefix_arguments"])):
        command = [str(PREFIX / "lib" / Path(LOADER).name), "--inhibit-cache", "--library-path",
                   str(PREFIX / "lib"), str(PREFIX / "bin/vlc"), *args]
        text = f"#!/bin/sh\nVLC_PLUGIN_PATH={PLUGINS}\nexport VLC_PLUGIN_PATH\nexec {shlex.join(command)} \"$@\"\n"
        expected["/usr/local/bin/" + name] = sha256(text.encode()).hexdigest()
    hashes = [row.split("  ", 1) for row in fields["files"].splitlines()]
    assert len(hashes) == len(expected) == 792
    assert {path:digest for digest, path in hashes} == expected == manifest["collected_files"]
    ffmpeg = json.loads((Path(__file__).resolve().parent / "ffmpeg/inputs.json").read_bytes())
    vlc, *versions = fields["versions"].splitlines()
    assert re.fullmatch(r"VLC version 3\.0\.[0-9]+[\x20-\x7e]{0,120} \([\x20-\x7e]{1,40}\)", vlc)
    assert versions == [ffmpeg["binaries"][tool]["version"] for tool in ("ffmpeg", "ffprobe")]
    media = json.loads(fields["media"])
    assert Counter((stream["codec_type"],stream["codec_name"]) for stream in media["streams"]) == {
        ("video","mpeg2video"):1, ("audio","mp2"):1,
    }
    [video] = [stream for stream in media["streams"] if stream["codec_type"] == "video"]
    [audio] = [stream for stream in media["streams"] if stream["codec_type"] == "audio"]
    assert (video["width"],video["height"]) == (320,240)
    video_frames, audio_frames = int(video["nb_read_frames"]), int(audio["nb_read_frames"])
    assert 150 <= video_frames <= 225 and 240 <= audio_frames <= 360
    duration = float(media["format"]["duration"])
    size, checksum = fields["bytes"].splitlines()
    output_hash, name = checksum.split("  ", 1)
    assert 7 <= duration <= 9 and 0 < int(size) <= 8388608
    assert name == "remux.ts" and re.fullmatch(r"[0-9a-f]{64}", output_hash)
    assert output_hash != "562fa93972763130c3079330b5e028a1e248ff98d59202129164d20a8c67d0b1"
    return {"scope":"native Debian VLC and cvlc wrapper smoke", "uid":65532, "exact_donor_vlc_files":792,
            "vlc_version":vlc, "ffmpeg_wrapper_versions":versions, "video_codec":"mpeg2video", "audio_codec":"mp2",
            "width":320, "height":240, "duration_seconds":duration,
            "video_frames":video_frames, "audio_frames":audio_frames,
            "output_bytes":int(size), "output_sha256":output_hash, "input_unchanged":True,
            "video_and_audio_decode_completed":True, "distribution_qualified":False, "full_arm64_package_verified":False,
            "preserved_donor_notice_gap":[LOADER], "same_donor_interpreter_notice_sha256":manifest["independent_same_donor_interpreter_notice"]["sha256"],
            "credit":manifest["credit"]}


if __name__ == "__main__":
    state, stdout, inputs = map(Path, sys.argv[1:])
    print(json.dumps(verify(json.loads(state.read_bytes()), stdout.read_bytes(), inputs.read_bytes()), indent=2, sort_keys=True))
