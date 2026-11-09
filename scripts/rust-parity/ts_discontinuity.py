#!/usr/bin/env python3
"""Byte-exact Rust/Python differential check using the actual legacy Python helper.

No Django install needed. Only its relative constants import is shimmed; helper
logic is imported from the repository, never duplicated. Run from any directory:
    python3 scripts/rust-parity/ts_discontinuity.py
Requires cargo/rustc on PATH; all cases are deterministic.
"""
import argparse
import hashlib
import importlib.util
import random
import subprocess
import sys
import types
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
BASELINE_HELPER_SHA256 = '861f96ec1111fbc3daff5f80d3fe0c634d2a532e9ca0cdb4b4d04281ee546855'


def load_legacy():
    namespace = "_dispatcharr_ts_parity"
    for name in (namespace, namespace + ".input"):
        package = types.ModuleType(name)
        package.__path__ = []
        sys.modules[name] = package
    constants = types.ModuleType(namespace + ".constants")
    constants.TS_PACKET_SIZE = 188
    sys.modules[constants.__name__] = constants
    source = ROOT / "apps/proxy/live_proxy/input/ts_discontinuity.py"
    if hashlib.sha256(source.read_bytes()).hexdigest() != BASELINE_HELPER_SHA256:
        raise RuntimeError('Python helper differs from the pinned compatibility baseline')
    spec = importlib.util.spec_from_file_location(namespace + ".input.ts_discontinuity", source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def corpus(seed):
    rng = random.Random(seed)
    commands = []
    for length in (0, 1, 5, 6, 187, 189, 376):
        commands.append(("packet", rng.randbytes(length)))
    packets = []
    # Every adaptation-control layout, empty/max/invalid adaptation lengths,
    # existing indicator bits, null PID, scrambled headers, and invalid sync.
    for afc in range(4):
        for adaptation_length in (0, 1, 2, 182, 183, 184, 255):
            for pid in (0, 1, 63, 64, 4095, 8190, 8191):
                for indicator in (0, 0x80):
                    packet = bytearray(rng.randbytes(188))
                    packet[0] = 0x47
                    packet[1] = (packet[1] & 0xE0) | (pid >> 8)
                    packet[2] = pid & 255
                    packet[3] = (packet[3] & 0xCF) | (afc << 4)
                    packet[4] = adaptation_length
                    packet[5] = (packet[5] & 0x7F) | indicator
                    packets.append(bytes(packet))
                    commands.append(("packet", bytes(packet)))
                    invalid = bytearray(packet)
                    invalid[0] = 0
                    commands.append(("packet", bytes(invalid)))
    for _ in range(1000):
        packet = bytearray(rng.randbytes(188))
        packet[0] = rng.choice((0x47, 0x47, 0))
        commands.append(("packet", bytes(packet)))
        packets.append(bytes(packet))
    commands.append(("reset", b""))
    # Cross-call PID state must match Python's already_stamped set. Repeat the
    # exact same packets, including impossible-to-stamp packets, across batches.
    for _ in range(5):
        selected = [rng.choice(packets) for _ in range(40)]
        commands.append(("batch", b"".join(selected)))
        commands.append(("batch", b"".join(selected)))
    commands.extend((("batch", b"bad"), ("batch", b""), ("reset", b"")))
    commands.append(("batch", b"".join(packets[:100])))
    return commands


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=20261109)
    args = parser.parse_args()
    legacy = load_legacy()
    commands = corpus(args.seed)
    expected = []
    seen = set()
    for command, payload in commands:
        if command == "reset":
            seen.clear()
            expected.append("RESET")
        elif command == "packet":
            expected.append(legacy.set_discontinuity_indicator(payload).hex())
        else:
            try:
                data, _ = legacy.stamp_first_packet_per_pid(payload, seen)
                expected.append(data.hex())
            except ValueError:
                expected.append("ERR_ALIGNMENT")
    protocol = "\n".join(command if command == "reset" else f"{command} {payload.hex()}"
                         for command, payload in commands) + "\n"
    result = subprocess.run(
        ["cargo", "run", "--quiet", "--manifest-path", str(ROOT / "rust/Cargo.toml"),
         "-p", "dispatcharr-stream-core", "--example", "ts_parity"],
        input=protocol, text=True, capture_output=True, check=True,
    )
    actual = result.stdout.splitlines()
    if len(actual) != len(expected):
        raise SystemExit(f"response count differs: {len(actual)} != {len(expected)}")
    for index, (got, want) in enumerate(zip(actual, expected)):
        if got != want:
            raise SystemExit(f"case {index} ({commands[index][0]}) differs; seed={args.seed}\n"
                             f"Rust: {got}\nPython: {want}")
    print(f"PASS: {len(commands)} byte-exact Python/Rust cases; seed={args.seed}")


if __name__ == "__main__":
    main()
