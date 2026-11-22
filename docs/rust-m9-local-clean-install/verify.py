"""Verify a clean-install replay on the exact published Dispatcharr application inputs."""
from argparse import ArgumentParser
from hashlib import sha256
from importlib.util import module_from_spec, spec_from_file_location
from io import BytesIO
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tarfile
from tempfile import TemporaryDirectory

APPLICATION = "9cf640701dacea07ea5f394ce7b18a6fa7c8c088"
TOOLS = "a9a7e10f4ec636f9438836d046dba3f95b9e61ac"
SOURCE_TREE = "d882ac35a940175173a8293595de8b91a00a0372442c12230f3c279e8c603e6c"
PUBLISHED_IMAGE = "sha256:fd1a839e1fce4835039b7b079cd0929cd3c658261374cea2c7bbebf813fae3a9"
SCRIPTS = "scripts/rust-integration/"
BUILD = SCRIPTS + "build.py"
CLEAN = SCRIPTS + "run_clean_install.py"
BROWSER = SCRIPTS + "clean_browser.js"
EVIDENCE = SCRIPTS + "m9-9cf6407-eleven-workflows.json"
COMPOSE = "docker/compose.rust-clean-install.yml"
FILES = (BUILD, CLEAN, BROWSER, EVIDENCE, COMPOSE, *(SCRIPTS + name for name in (
    "run.py", "fixture_ports.py", "tool_snapshot.py",
)), "docker/compose.rust-candidate.yml", "docker/compose.rust-playback.yml")
CHECKS = {
    "fresh-empty-database", "initialization-default-off-no-ddl", "native-only-service-set",
    "rust-initialized-original-schema", "candidate-has-no-python-runtime", "first-admin-form-status",
    "actual-react-first-admin-login-init", "browser-created-real-admin-row",
    "restart-react-token-persistence-init", "restart-preserves-existing-schema-and-admin",
    "setup-closed-after-browser-create",
}


def digest(value):
    return sha256(value).hexdigest()


def git(repo, *args):
    return subprocess.check_output(["git", "-C", str(repo), *args], timeout=60)


def checkout_tool(repo, name):
    return git(repo, "show", f"{TOOLS}:{name}")


def source(repo):
    data = git(repo, "archive", APPLICATION, "--", "ATTRIBUTION.md", "LICENSE", "frontend", "rust")
    with tarfile.open(fileobj=BytesIO(data), mode="r:") as archive:
        files = {item.name: digest(archive.extractfile(item).read()) for item in archive if item.isfile()}
    assert len(files) == 2157
    assert digest(json.dumps(files, sort_keys=True, separators=(",", ":")).encode()) == SOURCE_TREE
    return files


def preflight(repo):
    git(repo, "merge-base", "--is-ancestor", APPLICATION, TOOLS)
    files = source(repo)
    data = git(repo, "archive", TOOLS, "--", *FILES)
    with TemporaryDirectory(prefix="dispatcharr-m9-published-tools-") as directory:
        scratch = Path(directory)
        with tarfile.open(fileobj=BytesIO(data), mode="r:") as archive:
            archive.extractall(scratch, filter="data")
        assert {(entry.relative_to(scratch).as_posix()) for entry in scratch.rglob("*") if entry.is_file()} == set(FILES)
        evidence = json.loads((scratch / EVIDENCE).read_bytes())
        assert evidence["application_revision"] == APPLICATION and evidence["image"] == PUBLISHED_IMAGE
        assert "package/clean-install" in evidence["fresh_results"]
        for script, options in ((BUILD, ("--git-revision", "--output", "--tag")), (CLEAN, ("--output", "--candidate-build-manifest"))):
            help_text = subprocess.check_output([sys.executable, "-B", script, "--help"], cwd=scratch, timeout=20)
            assert all(option.encode() in help_text for option in options)
        node = shutil.which("node")
        if node:
            subprocess.run([node, "--check", BROWSER], cwd=scratch, timeout=20, check=True)
    return {"published_tool_commit": TOOLS, "application_revision": APPLICATION, "source_tree_sha256": SOURCE_TREE,
            "archived_application_inputs": len(files), "browser_syntax_checked": node is not None,
            "engine_calls": 0}


def checked_build(repo, output):
    assert (output / "build.actual-exit").read_bytes() == b"0\n"
    assert git(repo, "rev-parse", "HEAD").strip().decode() == TOOLS
    for name in FILES:
        assert (repo / name).read_bytes() == checkout_tool(repo, name)
    directory = output / "build"
    raw = (directory / "build.json").read_bytes()
    build_spec = spec_from_file_location("published_dispatcharr_builder", repo / BUILD)
    builder = module_from_spec(build_spec)
    build_spec.loader.exec_module(builder)
    saved = builder.validate_replay_archive(directory / "source.tar", raw)
    assert saved["git_revision"] == APPLICATION and saved["source_tree_sha256"] == SOURCE_TREE
    assert saved["source_sha256"] == source(repo) and saved["source_input_mode"] == "committed-revision-snapshot"
    assert saved["platform"] == "linux/amd64" and "--build-arg" not in saved["command"]
    return saved, digest(raw)


def verify(repo, output):
    build, manifest_sha = checked_build(repo, output)
    assert (output / "clean-install.actual-exit").read_bytes() == b"0\n"
    receipt = (output / "clean-install/integration.json").read_bytes()
    result = json.loads(receipt)
    assert result["passed"] is result["no_django_service"] is True
    assert result["build_manifest_sha256"] == manifest_sha
    assert result["candidate_image_binding"] == {
        "image_id": build["image_id"], "source_tree_sha256": SOURCE_TREE, "manifest_verified": True,
    }
    assert result["cleanup"] == "project removed" and "cleanup_error" not in result
    checks = result["checks"]
    assert {row["name"] for row in checks} == CHECKS and len(checks) == len(CHECKS)
    assert all(row["passed"] is True for row in checks)
    project = result["project"]
    assert re.fullmatch(r"dispatcharr-native-install-[0-9a-f]{10}", project)
    for kind in ("container", "network", "volume"):
        options = ["-aq"] if kind == "container" else ["-q"]
        left = subprocess.check_output(
            ["docker", kind, "ls", *options, "--filter", f"label=com.docker.compose.project={project}"],
            timeout=25,
        )
        assert not left.strip()
    return {"application_revision": APPLICATION, "build_manifest_sha256": manifest_sha,
            "local_image_id": build["image_id"], "receipt_sha256": digest(receipt),
            "verified_checks": len(checks), "remaining_owned_resources": 0}


def main():
    assert __debug__
    cli = ArgumentParser(description=__doc__)
    cli.add_argument("mode", choices=("preflight", "build", "verify"))
    cli.add_argument("--repository", required=True, type=Path)
    cli.add_argument("--output", type=Path)
    args = cli.parse_args()
    repo = args.repository.resolve()
    if args.mode == "preflight":
        assert args.output is None
        result = preflight(repo)
    else:
        assert args.output is not None
        if args.mode == "build":
            saved, manifest = checked_build(repo, args.output)
            result = {"application_revision": APPLICATION, "build_manifest_sha256": manifest, "local_image_id": saved["image_id"]}
        else:
            result = verify(repo, args.output)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
