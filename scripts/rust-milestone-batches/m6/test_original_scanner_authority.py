"""Cold admission tests for sealed Original scanner evidence."""
from copy import deepcopy
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import original_scanner_authority as scanner


def witness():
    return {"scope": scanner.SCOPE, "immutable_Original_image": scanner.IMAGE,
            "Original_archive_sha256": scanner.ARCHIVE, "actual_source_arms": 1,
            "actual_Native_arms": 0, "exact_cleanup": True, "formal_m6_credit": False,
            "retained_content_predecessor_sha256": scanner.CONTENT,
            "actual": {"actual_results_sha256": [letter * 64 for letter in "abc"],
                       "empty_journal_sha256": "d" * 64, "content_journal_sha256": "e" * 64,
                       "saved_row_count": 2}}


class OriginalScannerAuthority(unittest.TestCase):
    def setUp(self):
        sandbox = TemporaryDirectory()
        self.addCleanup(sandbox.cleanup)
        self.repo = Path(sandbox.name)
        self.stage = self.repo / scanner.STAGE
        self.stage.mkdir(parents=True)
        files = {name: (f"cold sealed input {name}\n").encode() for name in scanner.FILES}
        stamps = {name: scanner.digest(body) for name, body in files.items()}
        sums = "".join(f"{stamp}  {name}\n" for name, stamp in stamps.items()).encode()
        for name, body in (files | {"SHA256SUMS": sums}).items():
            (self.stage / name).write_bytes(body)
        exact = patch.multiple(scanner, FILES=stamps, STAGE_SEAL=scanner.digest(sums))
        exact.start()
        self.addCleanup(exact.stop)
        self.owner = self.stage / "owner-original-1"
        self.owner.mkdir()
        self.actual = self.stage / "actual-original-1"
        self.actual.mkdir()
        for phase in scanner.PHASES:
            (self.owner / phase).write_bytes(b"0\n")
        (self.actual / "actual-exit").write_bytes(b"0\n")
        self.proof = (json.dumps(witness(), sort_keys=True) + "\n").encode()
        (self.owner / "proof.stdout").write_bytes(self.proof)
        self.context = {"stage": str(self.stage), "seal": scanner.STAGE_SEAL,
                        "proof_sha256": scanner.digest(self.proof)}
        self.receipt = SimpleNamespace(returncode=0, stdout=self.proof, stderr=b"")

    def test_sealed_evidence_uses_only_proof_and_cannot_grant_candidate_credit(self):
        with patch.object(scanner.subprocess, "run", return_value=self.receipt) as verifier:
            output = scanner.resolve(self.context, self.repo, {})
        argv, = verifier.call_args.args
        self.assertEqual(argv, ["/usr/bin/python3", "-E", "-s", "-B", str(self.stage / "run.py"), "--proof"])
        self.assertEqual(verifier.call_args.kwargs["cwd"], self.repo)
        self.assertFalse(output["canonical_row_passed"])
        self.assertFalse(output["native_executed"])
        self.assertTrue(output["exact_cleanup"])
        self.assertEqual(output["evidence_cases"], ["empty", "content"])

    def test_missing_cleanup_or_changed_scripts_never_dispatch_anything(self):
        actual_exit = self.actual / "actual-exit"
        actual_exit.write_bytes(b"1\n")
        with patch.object(scanner.subprocess, "run") as verifier:
            with self.assertRaises(ValueError):
                scanner.resolve(self.context, self.repo, {})
            actual_exit.write_bytes(b"0\n")
            source = self.stage / "run.py"
            source.write_bytes(source.read_bytes() + b"\n")
            with self.assertRaises(ValueError):
                scanner.resolve(self.context, self.repo, {})
        verifier.assert_not_called()

    def test_wrong_path_literal_phase_or_mismatched_replay_are_refused(self):
        with patch.object(scanner.subprocess, "run", return_value=self.receipt) as verifier:
            wrong = self.context | {"stage": str(self.repo / "alternate")}
            with self.assertRaises(ValueError):
                scanner.resolve(wrong, self.repo, {})
            (self.owner / "proof.actual-exit").write_bytes(b"00\n")
            with self.assertRaises(ValueError):
                scanner.resolve(self.context, self.repo, {})
            verifier.assert_not_called()
            (self.owner / "proof.actual-exit").write_bytes(b"0\n")
            self.receipt.stdout = b"{}\n"
            with self.assertRaises(ValueError):
                scanner.resolve(self.context, self.repo, {})

    def test_alternate_repo_must_own_exact_common_package(self):
        with TemporaryDirectory() as scratch:
            other = Path(scratch)
            manifest = self.repo / "artifacts/rust/exact-package.json"
            manifest.write_bytes(b"{}\n")
            context = self.context | {"repository": str(self.repo)}
            candidate = {"build_manifest_file": str(manifest), "build_manifest_sha256": "0" * 64}
            with patch.object(scanner.subprocess, "run", return_value=self.receipt) as verifier:
                with self.assertRaises(ValueError):
                    scanner.resolve(context, other, candidate)
                verifier.assert_not_called()
                candidate["build_manifest_sha256"] = scanner.digest(manifest.read_bytes())
                self.assertEqual(scanner.resolve(context, other, candidate)["scope"], scanner.NODE)

    def test_candidate_marker_scope_and_reused_journal_rejected(self):
        for field, value in (("actual_Native_arms", 1), ("actual_source_arms", True),
                             ("formal_m6_credit", True), ("exact_cleanup", False)):
            with self.assertRaises(ValueError):
                scanner.checked_reduction(witness() | {field: value})
        altered = deepcopy(witness())
        altered["actual"]["content_journal_sha256"] = altered["actual"]["empty_journal_sha256"]
        with self.assertRaises(ValueError):
            scanner.checked_reduction(altered)


if __name__ == "__main__":
    unittest.main()
