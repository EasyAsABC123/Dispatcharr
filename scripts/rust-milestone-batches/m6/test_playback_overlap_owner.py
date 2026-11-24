"""Tests for the reviewed partial-only held guide playback dispatcher."""
from hashlib import sha256
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import playback_overlap_owner as owner


class PlaybackOverlapAdmission(unittest.TestCase):
    def test_explicit_sealed_stage_requires_reviewed_bytes_and_approved_paths(self):
        with TemporaryDirectory() as folder:
            repository = Path(folder).resolve()
            artifacts = repository / "artifacts/rust"
            receiving = artifacts / "alternate-local-checkout"
            stage = artifacts / "reviewed-real-layout"
            receiving.mkdir(parents=True)
            stage.mkdir()
            code = stage / "run.py"
            code.write_bytes(b"reviewed fixture\n")
            sums = stage / "SHA256SUMS"
            expected = sha256(code.read_bytes()).hexdigest()
            sums.write_text(f"{expected}  run.py\n")
            seal = sha256(sums.read_bytes()).hexdigest()
            manifest = artifacts / "build.json"
            manifest.write_text(json.dumps({"git_revision": "a"*40, "image_id": "sha256:"+"b"*64,
                "passed": True, "exit_code": 0, "platform": "linux/amd64"}))
            candidate = {"git": "a"*40, "image_id": "sha256:"+"b"*64,
                         "build_manifest_file": str(manifest),
                         "build_manifest_sha256": sha256(manifest.read_bytes()).hexdigest()}
            self.addCleanup(lambda: stage.chmod(0o755) if stage.exists() else None)
            with patch.object(owner, "WORKFLOW_PINS", {"run.py": expected}):
                with self.assertRaisesRegex(ValueError, "read-only reviewed"):
                    owner.inputs(stage, seal, candidate, repository, receiving)
                code.chmod(0o444)
                stage.chmod(0o555)
                inputs = owner.inputs(stage, seal, candidate, repository, receiving)
                self.assertEqual(inputs[str(code)], expected)
                with self.assertRaisesRegex(ValueError, "exact declared"):
                    owner.inputs(stage, "d"*64, candidate, repository, receiving)
                alias = artifacts / "symlink"
                alias.symlink_to(stage, target_is_directory=True)
                with self.assertRaisesRegex(ValueError, "explicit approved"):
                    owner.inputs(alias, seal, candidate, repository, receiving)
                code.chmod(0o644)
                code.write_text("unapproved fixture")
                code.chmod(0o444)
                sums.write_text(f"{sha256(code.read_bytes()).hexdigest()}  run.py\n")
                changed = sha256(sums.read_bytes()).hexdigest()
                with self.assertRaisesRegex(ValueError, "reviewed held overlap"):
                    owner.inputs(stage, changed, candidate, repository, receiving)
                stage.chmod(0o755)

    def test_success_remains_partial_and_raw_replay_must_match(self):
        with TemporaryDirectory() as folder:
            root = Path(folder).resolve()
            ledger = root / "owner-1"
            ledger.mkdir()
            close = {"exact_cleanup": True, "arms": []}
            candidate = {"git": "a"*40, "build_manifest_sha256": "c"*64}
            proof = {"scope": "shared-context-held-live-two-guide-imports-and-recording61",
                     "same_long_lived_context": True, "held_refresh_same_viewers_decodes": 4,
                     "full_strict_eof_matched": True, "m6_formal_credit": False,
                     "physical_cleanup": close, "candidate": candidate["git"],
                     "manifest_sha256": candidate["build_manifest_sha256"]}
            for name in (*owner.PHASES, "campaign"):
                (ledger / f"{name}.actual-exit").write_bytes(b"0\n")
            (ledger / "proof.stdout").write_text(json.dumps(proof))
            journal = root / "execution.json"
            journal.write_text(json.dumps({"cleanup_verified": True, "execution": {
                    "actual_exit": 0, "failure": None, "stdout_EOF": True,
                    "stderr_EOF": True, "process_group_settled": True}}))

            def verb(argv):
                self.assertEqual(argv, ["proof", "same-input"])
                print(json.dumps(proof))
                return 0

            driver = SimpleNamespace(main=verb)
            with patch.object(owner, "teardown", return_value={"physical_cleanup": close}):
                result = owner.partial_evaluation(root, candidate, journal, driver, ["same-input"])
                self.assertTrue(result["partial_result_observed"])
                self.assertFalse(result["full_required_workflow"])
                (ledger / "proof.stdout").write_text(json.dumps(proof | {"held_refresh_same_viewers_decodes": 5}))
                with self.assertRaisesRegex(ValueError, "reverified"):
                    owner.partial_evaluation(root, candidate, journal, driver, ["same-input"])
                (ledger / "run.actual-exit").write_bytes(b"1\n")
                with self.assertRaisesRegex(ValueError, "actual successful"):
                    owner.partial_evaluation(root, candidate, journal, driver, ["same-input"])


if __name__ == "__main__":
    unittest.main()
