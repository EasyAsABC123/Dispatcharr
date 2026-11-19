#!/usr/bin/env python3
"""Exercise blank PostgreSQL defaults through the shipped Dispatcharr operator.

Original Dispatcharr authors and contributors retain full application credit.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
from secrets import token_hex
import subprocess
import tarfile
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[2]
BASE = ROOT / "docker/compose.rust-clean-install.yml"
CONFIG = "rust/crates/app/src/database_config.rs"
SCHEMA = "rust/crates/store/schema/original-1241b3-schema.sql"
CONFIG_HASHES = (
    "cbafc5899e2309af22eca3d99354950317c4827e7d3467dcdf38dc8c10d7e68c",
    "63d409bbc65acc8c3697136c9fd5db3645e8ffbc9a30c0237a49dd1a43705440",
)
IMAGE_SHAPE = '{{json .Id}}\n{{json (index .Config.Labels "org.opencontainers.image.source-tree-sha256")}}'
OPERATOR_SHAPE = (
    "[{{json .Id}},{{json .Image}},{{json .State.Status}},{{json .State.ExitCode}},"
    '{{json (index .Config.Labels "com.docker.compose.project")}},{{json .Path}},{{json .Config.User}},'
    '[{{range $value := .Config.Env}}{{$key := index (split $value "=") 0}}'
    '{{if or (eq $key "POSTGRES_DB") (eq $key "POSTGRES_USER") (eq $key "POSTGRES_PORT") '
    '(eq $key "PGDATABASE") (eq $key "PGUSER") (eq $key "PGPORT")}}{{json $value}},{{end}}{{end}}null]]'
)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def write(path, data):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(data, indent=2) + "\n")
    temporary.replace(path)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def python_support(path, name):
    loader = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(loader)
    loader.loader.exec_module(module)
    return module


def package(path, revision, expected):
    raw = path.read_bytes()
    manifest = json.loads(raw)
    path_part = manifest["source_archive"]
    require(Path(path_part).name == path_part, "source archive must be a sibling of the build manifest")
    archive = path.parent / path_part
    require(re.fullmatch(r"[0-9a-f]{40}", revision) and manifest["git_revision"] == revision
            and manifest["source_input_mode"] == "committed-revision-snapshot"
            and manifest["runtime_packages_frozen"] is True
            and type(manifest["exit_code"]) is int and manifest["exit_code"] == 0
            and re.fullmatch(r"sha256:[0-9a-f]{64}", manifest["image_id"])
            and manifest["source_sha256"][CONFIG] == expected, "exact passing Candidate package required")
    python_support(ROOT / "scripts/rust-integration/build.py", "build_validator").validate_replay_archive(archive, raw)
    with tarfile.open(archive) as source:
        schema = source.extractfile(SCHEMA).read()
    require(digest(schema) == manifest["source_sha256"][SCHEMA], "packaged schema changed during intake")
    return {"revision": revision, "image": manifest["image_id"], "archive": manifest["source_archive_sha256"],
            "source_tree": manifest["source_tree_sha256"], "manifest": digest(raw)}, schema


CLIENT_NAMES = ("POSTGRES_USER", "POSTGRES_DB", "POSTGRES_PORT")


def scenarios(explicit_url):
    empty = {key: {"FIXTURE_CLIENT_" + name: ""} for key, name in zip(("user", "database", "port"), CLIENT_NAMES, strict=True)}
    all_empty = {key: "" for entry in empty.values() for key in entry}
    return (
        ("before-configured", "before", True, {}),
        *((f"before-blank-{key}", "before", False, changes) for key, changes in empty.items()),
        *((f"after-blank-{key}", "after", True, changes) for key, changes in empty.items()),
        ("after-all-blank", "after", True, all_empty),
        ("after-explicit-wins", "after", True, {
            "FIXTURE_CLIENT_POSTGRES_USER": "wrong_fixture_user",
            "FIXTURE_CLIENT_POSTGRES_DB": "wrong_fixture_database",
            "FIXTURE_CLIENT_POSTGRES_PORT": "1",
            "DATABASE_URL": explicit_url,
        }),
    )


def composition(before, after):
    pg_password = "${OPERATOR_FIXTURE_POSTGRES_PASSWORD:?}"
    redis_password = "${OPERATOR_FIXTURE_REDIS_PASSWORD:?}"

    def operator(image):
        client = {name: "${FIXTURE_CLIENT_" + name + "?}" for name in CLIENT_NAMES}
        return {
            "image": image, "platform": "linux/amd64", "pull_policy": "never",
            "entrypoint": ["/usr/local/bin/dispatcharr-operator"],
            "read_only": True, "cap_drop": ["ALL"], "security_opt": ["no-new-privileges:true"],
            "environment": {
                **client, "POSTGRES_HOST": "database", "POSTGRES_PASSWORD": pg_password,
                "PGUSER": "ambient_witness_user", "PGDATABASE": "ambient_witness_db", "PGPORT": "6699",
                "POSTGRES_SSL": "false", "REDIS_URL": "${OPERATOR_FIXTURE_REDIS_URL:?}",
            },
        }

    return {"services": {
        "database": {
            "pull_policy": "never", "environment": {
                "POSTGRES_PASSWORD": pg_password, "PGPASSWORD": pg_password,
                "PGUSER": "dispatch", "PGDATABASE": "dispatcharr",
                "POSTGRES_HOST_AUTH_METHOD": "scram-sha-256",
            },
            "healthcheck": {"test": ["CMD", "pg_isready", "-h", "127.0.0.1", "-U", "dispatch", "-d", "dispatcharr"]},
        },
        "cache": {
            "pull_policy": "never", "environment": {"REDISCLI_AUTH": redis_password},
            "command": ["redis-server", "--save", "", "--appendonly", "no", "--requirepass", redis_password],
            "healthcheck": {"test": ["CMD", "redis-cli", "ping"]},
        },
        "before": operator(before["image"]), "after": operator(after["image"]),
    }, "networks": {"default": {"internal": True}}}


def run(args):
    before, before_schema = package(args.before, args.before_revision, CONFIG_HASHES[0])
    after, schema = package(args.after, args.after_revision, CONFIG_HASHES[1])
    require(before["image"] != after["image"] and before_schema == schema,
            "different exact application images and a common packaged schema required")
    lease_root = args.lease_root.resolve()
    primary = lease_root / "scripts/rust-milestone-batches/.dispatcher.lock"
    require(primary.parent.is_dir(), "existing migration lease directory required")
    socket_path = args.docker_host.removeprefix("unix://")
    require(args.docker_host.startswith("unix:///") and Path(socket_path).is_socket(),
            "explicit local validation Docker socket required")
    environment = os.environ.copy()
    for name in ("DATABASE_URL", "DOCKER_CONTEXT", "COMPOSE_PROJECT_NAME"):
        environment.pop(name, None)
    environment["DOCKER_HOST"] = args.docker_host
    user, password, database = "dispatch", token_hex(24), "dispatcharr"
    redis_password = token_hex(24)
    environment.update(
        OPERATOR_FIXTURE_POSTGRES_PASSWORD=password,
        OPERATOR_FIXTURE_REDIS_PASSWORD=redis_password,
        OPERATOR_FIXTURE_REDIS_URL=f"redis://:{redis_password}@cache:6379/0",
        FIXTURE_CLIENT_POSTGRES_USER=user, FIXTURE_CLIENT_POSTGRES_DB=database,
        FIXTURE_CLIENT_POSTGRES_PORT="5432", RUST_CANDIDATE_IMAGE=after["image"],
    )
    url = f"postgresql://{quote(user, safe='')}:{quote(password, safe='')}@database:5432/{quote(database, safe='')}"
    project = "dispatcharr-pg-defaults-" + token_hex(8)
    scope = "label=com.docker.compose.project=" + project
    output = args.output.resolve()
    output.mkdir(mode=0o700)
    compose = output / "compose.json"
    write(compose, composition(before, after))
    command = ("compose", "--env-file", "/dev/null", "--project-directory", str(BASE.parent),
               "-p", project, "-f", str(BASE), "-f", str(compose))
    report = {"project": project, "before": before, "after": after, "schema_sha256": digest(schema),
              "storage_compose_sha256": digest(BASE.read_bytes()), "cases": [], "passed": False}

    def call(*options, timeout=20, overlay=None, stdin=None):
        return subprocess.run([args.docker, *options], env={**environment, **(overlay or {})},
                              input=stdin, capture_output=True, timeout=timeout)

    def inventory():
        found = {}
        for kind, flags in (("container", ("-aq", "--no-trunc")), ("network", ("-q", "--no-trunc")), ("volume", ("-q",))):
            output = call(kind, "ls", *flags, "--filter", scope)
            require(output.returncode == 0, "project inventory command failed")
            found[kind] = output.stdout.decode().splitlines()
            if kind == "volume":
                require(all(re.fullmatch(r"[a-f0-9]{64}", value) or value.startswith(project + "_")
                            for value in found[kind]), "unexpected owned volume identity")
            else:
                require(all(re.fullmatch(r"[a-f0-9]{64}", value) for value in found[kind]), "unexpected inventory identity")
        return found

    cases = scenarios(url)
    gate = lease_root / ".dispatcharr-recording-cohort-cleanup-required.json"
    leases = python_support(ROOT / "scripts/rust-milestone-batches/functional_lease.py", "owned_fixture_leases")
    with leases.fixture_lease(primary):
        require(not gate.exists() and not any(inventory().values()), "a prior fixture needs cleanup")
        write(gate, {"owned_run": str(output), "compose_project": project, "compose_path": str(compose)})
        phase = "image-inspection"
        try:
            for build in (before, after):
                result = call("image", "inspect", "--format", IMAGE_SHAPE, build["image"])
                actual = [json.loads(value) for value in result.stdout.splitlines()]
                require(result.returncode == 0 and actual == [build["image"], build["source_tree"]],
                        "local image id or source tree label differs")
            phase = "dependencies"
            result = call(*command, "up", "--detach", "--wait", "--wait-timeout", "60", "--no-build", "--pull", "never",
                          "database", "cache", timeout=75)
            report["dependencies_exit"] = result.returncode
            require(result.returncode == 0, "owned PostgreSQL/Redis did not become healthy")
            phase = "schema-tcp-authentication"
            result = call(*command, "exec", "-T", "database", "psql", "--host=127.0.0.1", "--set=ON_ERROR_STOP=1",
                          timeout=45, stdin=schema)
            report["schema_exit"] = result.returncode
            require(result.returncode == 0, "the credential-authenticated schema install failed")
            for name, service, success, overlay in cases:
                phase = name
                container = project + "-" + name
                override = ("-e", "DATABASE_URL") if "DATABASE_URL" in overlay else ()
                result = call(*command, "run", "--no-deps", "--detach", "--pull", "never", "-T", "--name", container, *override,
                              service, "reset_user_network", "--user", "fixture-user-absent", overlay=overlay)
                require(result.returncode == 0, "packaged operator failed to launch")
                waited = call("container", "wait", container, timeout=35)
                inspected = call("container", "inspect", "--format", OPERATOR_SHAPE, container)
                require(inspected.returncode == 0, "packaged operator container was not available for observation")
                identifier, image, status, exit_code, owner, binary, uid, actual_environment = json.loads(inspected.stdout)
                require(re.fullmatch(r"[a-f0-9]{64}", identifier), "unexpected container identity")
                expected_environment = {"PGDATABASE=ambient_witness_db", "PGUSER=ambient_witness_user", "PGPORT=6699"}
                for key in CLIENT_NAMES:
                    prefixed = "FIXTURE_CLIENT_" + key
                    expected_environment.add(f"{key}={overlay.get(prefixed, environment[prefixed])}")
                logs = call("container", "logs", identifier)
                stdout, stderr = ((b"Reset allowed_networks for 0 user(s).\n", b"") if success
                                  else (b"", b"Operator unavailable\n"))
                matches = {"output": logs.returncode == 0 and (logs.stdout, logs.stderr) == (stdout, stderr),
                           "image_and_owner": image == report[service]["image"] and owner == project,
                           "shipped_operator": binary == "/usr/local/bin/dispatcharr-operator" and uid == "65532:65532",
                           "exact_live_environment": len(actual_environment) == 7 and actual_environment[-1] is None
                           and set(actual_environment[:-1]) == expected_environment,
                           "exit": waited.returncode == 0 and waited.stdout == f"{exit_code}\n".encode()
                           and status == "exited" and exit_code == (0 if success else 1)}
                report["cases"].append({"case": name, "container": identifier, "actual_exit": exit_code,
                                        "compose_exit": result.returncode, "matches": matches, "passed": all(matches.values())})
                write(output / "result.json", report)
            report["passed"] = len(report["cases"]) == len(cases) and all(row["passed"] for row in report["cases"])
        except (OSError, ValueError, subprocess.SubprocessError) as error:
            # No raw command output, arguments, exception text or Docker env enters evidence.
            report["failure_type"] = type(error).__name__
            report["failure_phase"] = phase
        finally:
            try:
                try:
                    report["owned_before_cleanup"] = inventory()
                except (OSError, ValueError, subprocess.SubprocessError) as error:
                    report["cleanup_inventory_failure_type"] = type(error).__name__
                result = call(*command, "down", "--volumes", "--remove-orphans", timeout=40)
                report["down_exit"] = result.returncode
                remaining = inventory()
                if remaining["container"]:
                    removed = call("container", "rm", "--force", *remaining["container"], timeout=40)
                    report["owned_container_cleanup_exit"] = removed.returncode
                    result = call(*command, "down", "--volumes", "--remove-orphans", timeout=40)
                    report["retry_down_exit"] = result.returncode
                    remaining = inventory()
                report["owned_after_cleanup"] = remaining
                known = {row["container"] for row in report["cases"]}
                known.update(report.get("owned_before_cleanup", {}).get("container", ()))
                report["exact_container_absence"] = {}
                for identifier in sorted(known):
                    check = call("container", "ls", "-aq", "--no-trunc", "--filter", "id=" + identifier)
                    report["exact_container_absence"][identifier] = check.returncode == 0 and not check.stdout.strip()
                report["cleanup_passed"] = (
                    result.returncode == 0 and "cleanup_inventory_failure_type" not in report
                    and not any(remaining.values()) and all(report["exact_container_absence"].values())
                )
                if report["cleanup_passed"]:
                    gate.unlink()
            finally:
                report["passed"] = report["passed"] and report.get("cleanup_passed", False)
                write(output / "result.json", report)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    for side in ("before", "after"):
        parser.add_argument("--" + side, required=True, type=Path)
        parser.add_argument("--" + side + "-revision", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--docker", default="docker")
    parser.add_argument("--docker-host", required=True, help="Explicit unix:/// path to the local validation Docker engine")
    parser.add_argument("--lease-root", default=ROOT, type=Path,
                        help="Checkout whose exclusive migration lease this fixture shares")
    raise SystemExit(run(parser.parse_args()))
