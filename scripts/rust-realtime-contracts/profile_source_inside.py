"""Run the pinned Original ASGI entrypoint in its disposable Compose service."""
import hashlib
import json
from pathlib import Path
import socket
import subprocess
import sys
import time

from gateway_control import command
from profile_cases import DATASET, exercise
from profile_group_retirement import members, wait_for_members


def main():
    accounts = json.load(sys.stdin)
    oracle = json.loads(DATASET.read_text())
    assert all(
        hashlib.sha256((Path("/app") / path).read_bytes()).hexdigest() == expected
        for path, expected in oracle["source_sha256"].items()
    )
    def observe():
        return command({"op": "group-members"})

    boundary = {"before_open": observe()}

    def observe_open():
        boundary["open"] = observe()

    port = 8001
    process = subprocess.Popen(
        ["/dispatcharrpy/bin/daphne", "-b", "127.0.0.1", "-p", str(port), "dispatcharr.asgi:application"],
        cwd="/app",
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    killed = False
    try:
        deadline = time.monotonic() + 20
        while True:
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                    break
            except OSError:
                if process.poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError("Source Daphne startup failed")
                time.sleep(0.1)
        report = exercise("127.0.0.1", port, accounts, observe_open=observe_open)
        boundary["immediate_after_close"] = observe()
        # Client close-frame acknowledgements precede the async Django
        # disconnect receiver's group_discard. Keep Daphne alive until
        # the owned members have actually retired or the bound expires.
        boundary["before_daphne_termination"] = wait_for_members(
            observe, members(boundary["before_open"])
        )
    finally:
        process.terminate()
        try:
            exit_code = process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            killed = True
            process.kill()
            exit_code = process.wait(timeout=3)
    report["source_group_boundary"] = boundary
    report["source_daphne_exit"] = exit_code
    report["source_daphne_killed"] = killed
    assert exit_code in (0, -15) and not killed
    print(json.dumps(report))


if __name__ == "__main__":
    main()
