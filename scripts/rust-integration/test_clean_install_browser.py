"""Engine-free browser ownership controls using inert subprocesses."""
from contextlib import contextmanager
from pathlib import Path
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import textwrap
import unittest
from unittest.mock import patch

from run_clean_install import Browser

HERE = Path(__file__).resolve().parent
PEER = r'''
import json, os, signal, subprocess, sys, time
from pathlib import Path

mode = Path(__file__).stem
if mode == "child_then_failure":
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    def retire(*_):
        child.terminate()
        child.wait()
        raise SystemExit(1)
    signal.signal(signal.SIGTERM, retire)
    Path(os.environ["RUST_BROWSER_PROFILE"], "child.pid").write_text(str(child.pid))
    print(json.dumps({"ready": False}), flush=True)
    time.sleep(60)
if mode in {"stubborn", "partial"}:
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    if mode == "stubborn":
        print(json.dumps({"ready": True}), flush=True)
    else:
        print('{"unfinished":', end="", flush=True)
    time.sleep(60)
if mode == "normal":
    print(json.dumps({"ready": True}), flush=True)
    assert json.loads(sys.stdin.readline()) == {"op": "close"}
    print(json.dumps({"closed": True}), flush=True)
'''


class BrowserLifecycle(unittest.TestCase):
    @contextmanager
    def owner(self, node=sys.executable, chrome=sys.executable):
        owner = Browser()
        owner.startup_seconds = 0.5
        owner.close_seconds = 0.15
        owner.signal_seconds = 0.5
        with tempfile.TemporaryDirectory(prefix="clean-install-browser-control-") as tmp:
            with patch("run_clean_install.browser_tools", return_value=(node, chrome, "fixture")):
                try:
                    yield owner, Path(tmp)
                finally:
                    if not owner.closed:
                        owner.close()

    def stage(self, owner, directory, mode):
        script = directory / (mode + ".py")
        script.write_text(PEER)
        owner.start(script, directory / "browser.log")

    def check_settled(self, owner, proof):
        self.assertTrue(proof["group_inactive"])
        self.assertIsNotNone(owner.process.returncode)
        self.assertTrue(owner.process.stdin.closed and owner.process.stdout.closed and owner.log.closed)
        self.assertFalse(owner.profile.exists())

    def test_normal_close_acknowledges_exits_and_removes_profile(self):
        with self.owner() as (owner, directory):
            self.stage(owner, directory, "normal")
            proof = owner.close()
            self.check_settled(owner, proof)
            self.assertTrue(proof["close_acknowledged"])
            self.assertFalse(proof["sent_term"] or proof["sent_kill"])
            self.assertEqual(proof["launcher_exit"], 0)

    def test_startup_rejection_retires_owned_child_and_reaps_launcher(self):
        with self.owner() as (owner, directory):
            with self.assertRaisesRegex(RuntimeError, "startup failed"):
                self.stage(owner, directory, "child_then_failure")
            child_pid = int((owner.profile / "child.pid").read_text())
            self.assertEqual(os.getpgid(child_pid), owner.process.pid)
            proof = owner.close()
            self.check_settled(owner, proof)
            self.assertTrue(proof["sent_term"])
            self.assertFalse(proof["sent_kill"])

    def test_terminated_launcher_ignoring_term_is_killed_and_reaped(self):
        with self.owner() as (owner, directory):
            self.stage(owner, directory, "stubborn")
            proof = owner.close()
            self.check_settled(owner, proof)
            self.assertTrue(proof["sent_term"] and proof["sent_kill"])
            self.assertEqual(proof["launcher_exit"], -signal.SIGKILL)

    def test_partial_startup_frame_is_bounded_and_launcher_reaped(self):
        with self.owner() as (owner, directory):
            with self.assertRaisesRegex(TimeoutError, "response deadline"):
                self.stage(owner, directory, "partial")
            proof = owner.close()
            self.check_settled(owner, proof)
            self.assertTrue(proof["sent_kill"])

    def test_fallback_preserves_a_different_process_group(self):
        peer = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"], start_new_session=True)
        try:
            with self.owner() as (owner, directory):
                self.stage(owner, directory, "stubborn")
                self.assertNotEqual(os.getpgid(peer.pid), owner.process.pid)
                self.assertTrue(owner.close()["sent_kill"])
                self.assertIsNone(peer.poll())
        finally:
            peer.terminate()
            peer.wait(timeout=3)

    def test_popen_failure_retires_owned_log_and_profile(self):
        with self.owner() as (owner, directory):
            with patch("run_clean_install.subprocess.Popen", side_effect=OSError("controlled spawn failure")):
                with self.assertRaisesRegex(OSError, "controlled spawn failure"):
                    self.stage(owner, directory, "normal")
            proof = owner.close()
            self.assertTrue(proof["group_inactive"])
            self.assertIsNone(proof["launcher_exit"])
            self.assertTrue(owner.log.closed)
            self.assertFalse(owner.profile.exists())

    @unittest.skipUnless(shutil.which("node"), "requires local Node for the actual JS bootstrap")
    def test_actual_js_browser_child_inherits_owned_group_before_ready(self):
        with tempfile.TemporaryDirectory(prefix="clean-install-inert-chrome-") as tmp:
            fake = Path(tmp) / "inert-chrome"
            fake.write_text(f"#!{sys.executable}\n" + textwrap.dedent("""
                import os, signal, time
                from pathlib import Path
                signal.signal(signal.SIGTERM, signal.SIG_IGN)
                Path(os.environ["RUST_BROWSER_PROFILE"], "group").write_text(str(os.getpgrp()))
                time.sleep(60)
            """))
            fake.chmod(0o755)
            with self.owner(shutil.which("node"), str(fake)) as (owner, directory):
                owner.startup_seconds = 1
                with self.assertRaisesRegex(TimeoutError, "response deadline"):
                    owner.start(HERE / "clean_browser.js", directory / "browser.log")
                self.assertEqual(int((owner.profile / "group").read_text()), owner.process.pid)
                proof = owner.close()
                self.check_settled(owner, proof)
                self.assertTrue(proof["sent_term"] and proof["sent_kill"])


if __name__ == "__main__":
    unittest.main()
