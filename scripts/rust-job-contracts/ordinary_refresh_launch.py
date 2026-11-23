"""Normal packaged AIO launch boundary for the existing complete-refresh owner."""
from datetime import datetime
from hashlib import sha256
import json
import re
import subprocess
import time

VETO = {
    "RUST_EXPERIMENTAL_SELECTED_JOBS", "RUST_EXPERIMENTAL_SELECTED_BEAT", "RUST_EXPERIMENTAL_SELECTED_CORE_BEAT",
    "RUST_CORE_SCHEDULE_OWNER", "RUST_BACKUP_TASKLESS_RESTORE_OWNER", "RUST_BACKUP_CREATE_OWNER",
    "RUST_BACKUP_RESTORE_OWNER", "RUST_VERSION_STARTUP_OWNER", "RUST_VERSION_TASKS_ENABLED",
    "RUST_EPG_REFRESH_ENABLED", "RUST_M3U_REFRESH_ALL_OWNER",
}


def candidate_overlay(image, name, guide_root):
    assert re.fullmatch(r"sha256:[a-f0-9]{64}", image)
    assert re.fullmatch(r"rust_probe_backup_jobs_[a-f0-9]{10}", name)
    assert guide_root.is_absolute() and guide_root.name == "workflow-files"
    return 'services:\n  data-init:\n    image: debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc\n    user: "0:0"\n    entrypoint: [sh, -c, "mkdir -p /data/logos /data/cached_epg /data/plugins /data/recordings /data/epgs /data/cached_programs /data/cached_m3u && chown -R 65532:65532 /data"]\n    volumes: [workflow-data:/data]\n    network_mode: none\n  candidate:\n    image: '+image+'\n    read_only: true\n    user: "65532:65532"\n    cap_drop: [ALL]\n    security_opt: [no-new-privileges:true]\n    depends_on:\n      data-init: {condition: service_completed_successfully}\n      baseline: {condition: service_started}\n    environment:\n      DATABASE_URL: postgres://dispatch:playback-fixture-only@baseline:5432/'+name+'\n      DJANGO_SECRET_KEY: playback-fixture-only\n      REDIS_URL: redis://:playback-fixture-only@baseline:6379/0\n      CELERY_BROKER_URL: redis://:playback-fixture-only@baseline:6379/8\n      CELERY_RESULT_BACKEND: redis://:playback-fixture-only@baseline:6379/9\n      DISPATCHARR_ENV: aio\n      RUST_EPG_SOURCE_CWD: /app\n      RUST_EPG_MEDIA_ROOT: /app/media\n      RUST_BIND: 0.0.0.0:9192\n      TMPDIR: /data\n      TOKIO_WORKER_THREADS: "2"\n    volumes:\n      - workflow-data:/data\n      - '+str(guide_root)+':/evidence/workflow-files:ro\n    stop_grace_period: 20s\nvolumes:\n  workflow-data:\n'


def verify_candidate(info, image, guide_root):
    config = info["Config"]
    env = dict(item.split("=", 1) for item in config["Env"])
    assert info["Image"] == image and config["User"] == "65532:65532"
    assert env["DISPATCHARR_ENV"] == "aio" and not VETO.intersection(env)
    assert info["HostConfig"]["ReadonlyRootfs"] is True and info["State"]["Running"] is True
    [mount] = [item for item in info["Mounts"] if item["Destination"] == "/evidence/workflow-files"]
    assert mount["Source"] == str(guide_root) and mount["RW"] is False


def read_handoff(path, guide):
    assert path.is_file() and not path.is_symlink() and path.stat().st_size < 1024
    value = json.loads(path.read_bytes())
    assert set(value) == {"fixture_clock", "source_id", "file_sha256", "owned_broker_ready", "owned_broker_reserved"}
    assert datetime.fromisoformat(value["fixture_clock"]).utcoffset() is not None
    assert type(value["source_id"]) is int and value["source_id"] > 0
    assert type(value["owned_broker_ready"]) is type(value["owned_broker_reserved"]) is int
    assert value["owned_broker_ready"] == value["owned_broker_reserved"] == 0
    assert guide.is_file() and not guide.is_symlink() and guide.stat().st_size < 4096
    assert value["file_sha256"] == sha256(guide.read_bytes()).hexdigest()
    return value


def run_prepared_candidate(command, compose, output, argv, env, image):
    marker = output / "native-workflow-prepared.json"
    assert not marker.exists() and not marker.is_symlink()
    guide_root = output / "workflow-files"
    deadline = time.monotonic() + 480
    log = output / "source-controller-private.log"
    with log.open("x") as stream:
        process = subprocess.Popen(argv, env=env, text=True, stdout=stream, stderr=subprocess.STDOUT)
        try:
            prepare_deadline = min(deadline, time.monotonic() + 60)
            while not marker.exists():
                if process.poll() is not None:
                    raise subprocess.CalledProcessError(process.returncode or 1, argv)
                if time.monotonic() >= prepare_deadline:
                    raise TimeoutError("owned Native fixture preparation was not received")
                time.sleep(.05)
            handoff = read_handoff(marker, guide_root / "guide.xml")
            command(compose + ["up", "-d", "--pull", "never", "--no-build", "candidate"], timeout=90)
            candidate = command(compose + ["ps", "-q", "candidate"]).strip()
            assert re.fullmatch(r"[a-f0-9]{12,64}", candidate)
            [info] = json.loads(command(["docker", "inspect", candidate]))
            verify_candidate(info, image, guide_root)
            audit = command(compose + ["exec", "-T", "candidate", "sh", "-c",
                "id; pwd; ls -ld /app /app/media /data /data/cached_epg /evidence /evidence/workflow-files; "
                "test -x /evidence/workflow-files; test -w /data; test /app/media -ef /data"])
            (output / "candidate-path-audit.txt").write_text(audit)
            code = process.wait(timeout=max(0, deadline - time.monotonic()))
            if code != 0:
                raise subprocess.CalledProcessError(code, argv)
            return {"ordinary_aio": True, "no_selected_or_eager_overrides": True,
                    "prepared_before_candidate_intake": True, "source_id": handoff["source_id"],
                    "input_file_sha256": handoff["file_sha256"]}
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
