"""Run a copied PostgreSQL startup after the same-job native playback passes."""
from copy import deepcopy
from decimal import Decimal
import json
import os
from pathlib import Path
import re
from urllib.parse import urlsplit

HERE = Path(__file__).resolve().parent
COPY = "dispatcharr_arm64_copy"
COPY_VOLUME = "candidate-copy-data"
COPY_SERVICES = ("copied-cache", "copied-data-init", "copied-candidate")
SOURCE_SERVICES = {"cache", "data-init", "candidate", "database"}
CHANNEL_ID_SQL = (
    "SELECT id FROM dispatcharr_channels_channel "
    "WHERE uuid='3b81d384-d6a1-4d64-938e-3e91075cec0a'"
)
LOGIN_TIME_SQL = "SELECT extract(epoch from last_login) FROM accounts_user WHERE username='owned-arm64-admin'"
SCHEMA_SQL = "SELECT count(*) FROM pg_tables WHERE schemaname='public'"


def distinct_data_volume(service):
    [volume] = service["volumes"]
    assert (volume["source"], volume["target"], volume["type"]) == ("candidate-data", "/data", "volume")
    volume["source"] = COPY_VOLUME


def overlay(config):
    assert set(config["services"]) == SOURCE_SERVICES
    assert config["networks"]["default"]["internal"] is True
    cache, init, app = (deepcopy(config["services"][name]) for name in ("cache", "data-init", "candidate"))
    assert "container_name" not in cache and "container_name" not in init and "container_name" not in app
    assert not any(role.get("ports") for role in (cache, init, app))
    distinct_data_volume(init)
    distinct_data_volume(app)
    environment = app["environment"]
    database = urlsplit(environment["DATABASE_URL"])
    assert database.scheme == "postgresql" and database.hostname == "database"
    assert database.path == "/dispatcharr" and not database.query and not database.fragment
    redis = urlsplit(environment["REDIS_URL"])
    assert redis.scheme == "redis" and redis.hostname == "cache" and redis.port == 6379 and redis.path == "/0"
    assert redis.password and not redis.query and not redis.fragment
    environment["DATABASE_URL"] = database._replace(path=f"/{COPY}").geturl()
    netloc = redis.netloc.removesuffix("@cache:6379")
    assert netloc != redis.netloc
    environment["REDIS_URL"] = redis._replace(netloc=f"{netloc}@copied-cache:6379").geturl()
    environment["RUST_INITIALIZE_EMPTY_DATABASE"] = "0"
    dependencies = app["depends_on"]
    assert set(dependencies) == {"database", "cache", "data-init"}
    dependencies["copied-cache"] = dependencies.pop("cache")
    dependencies["copied-data-init"] = dependencies.pop("data-init")
    return {"services": dict(zip(COPY_SERVICES, (cache, init, app), strict=True)),
            "volumes": {COPY_VOLUME: {}}}


def setup_overlay(work, config):
    reserved = work / "copied-data.json"
    with open(reserved, "x", opener=lambda path, flags: os.open(path, flags, 0o600)) as out:
        json.dump(overlay(config), out)
    return reserved


def exercise(base, copied, env, image, project, config, source_id, owned, credentials, run, inspect, wait, get, bridge_url, progress):
    def call(args, *, scope=base, body=None, timeout=40):
        return run(scope + args, body=body, env=env, timeout=timeout).decode().strip()

    def sql(database, command):
        assert database in {"dispatcharr", "postgres", COPY}
        return call(["exec", "-T", "database", "psql", "-X", "--no-psqlrc", "-v", "ON_ERROR_STOP=1",
                     "-U", "dispatch", "-d", database, "-At"], body=command.encode())

    def check_fingerprint(database):
        row = json.loads(sql(database, (HERE / "copy-fingerprint.sql").read_text()))
        assert {k: row[k] for k in ("migrations", "actors", "channel_streams")} == {
            "migrations": 184, "actors": 1, "channel_streams": 1}
        assert re.fullmatch(r"[a-f0-9]{32}", row["fingerprint"])
        return row

    def owned_role(scope, name, expected_image):
        identifier = call(["ps", "-a", "-q", name], scope=scope)
        assert re.fullmatch(r"[a-f0-9]{64}", identifier) and identifier not in owned
        row = inspect(identifier)
        assert row["Config"]["Labels"]["com.docker.compose.project"] == project
        assert row["Config"]["Labels"]["com.docker.compose.service"] == name
        assert row["Config"]["Image"] == expected_image or row["Image"] == expected_image
        owned.append(identifier)
        return identifier

    progress("copy-overlay")
    configured = json.loads(call(["config", "--format", "json"], scope=copied))
    assert set(configured["services"]) == SOURCE_SERVICES | set(COPY_SERVICES)
    assert configured["networks"]["default"]["internal"] is True
    assert configured["services"]["candidate"] == config["services"]["candidate"]
    assert set(configured["volumes"]) == {"candidate-data", COPY_VOLUME}
    assert not any(configured["services"][name].get("ports") for name in COPY_SERVICES)

    progress("copy-as-owned-postgres")
    assert sql("postgres", "SELECT count(*) FROM pg_database WHERE datname='dispatcharr_arm64_copy'") == "0"
    sql("postgres", "CREATE DATABASE dispatcharr_arm64_copy TEMPLATE template0")
    assert sql(COPY, SCHEMA_SQL) == "0"
    source = check_fingerprint("dispatcharr")
    source_login = Decimal(sql("dispatcharr", LOGIN_TIME_SQL))
    channel = sql("dispatcharr", CHANNEL_ID_SQL)
    assert re.fullmatch(r"[1-9][0-9]*", channel)

    progress("copy-archive")
    control = HERE / "copy-archive-guest.py"
    archive = json.loads(call(["exec", "-T", "candidate", "/usr/local/bin/dispatcharr-python", "-I", "-B", "-"],
                             body=control.read_bytes(), timeout=120))
    assert archive["dump_exit"] == archive["restore_exit"] == 0 and archive["archive_removed"]
    assert re.fullmatch(r"[a-f0-9]{64}", archive["custom_archive_sha256"])
    assert sql(COPY, SCHEMA_SQL) == "62" and check_fingerprint(COPY) == source
    assert Decimal(sql(COPY, LOGIN_TIME_SQL)) == source_login

    progress("copy-destination-startup")
    call(["stop", "-t", "20", "candidate"])
    original = inspect(source_id)
    assert original["State"]["Status"] == "exited" and original["State"]["ExitCode"] == 0
    call(["up", "-d", "--no-build", "--pull", "never", "copied-candidate"], scope=copied, timeout=90)
    copy_id = None
    for name in COPY_SERVICES:
        retained = owned_role(copied, name, configured["services"][name]["image"])
        if name == "copied-candidate":
            copy_id = retained
    assert copy_id and copy_id != source_id

    def destination():
        row = inspect(copy_id)
        assert row["Image"] == image and row["Config"]["User"] == "65532:65532"
        if row["State"]["Status"] in {"exited", "dead"}:
            raise AssertionError("copied candidate exited before readiness")
        if row["State"]["Status"] != "running":
            return None
        if not row["NetworkSettings"]["Networks"]:
            return None
        assert set(row["NetworkSettings"]["Networks"]) == {project + "_default"}
        [endpoint] = row["NetworkSettings"]["Networks"].values()
        assert not any((row["NetworkSettings"]["Ports"] or {}).values())
        if not endpoint["IPAddress"]:
            return None
        [network] = json.loads(run(["docker", "network", "inspect", endpoint["NetworkID"]], env=env))
        return bridge_url(copy_id, endpoint, network, project)

    url = wait(destination, "copied-candidate-owned-bridge")
    wait(lambda: get(url, "/health/ready")[0] == 200, "copied-candidate-ready")

    progress("copy-authenticated-read")
    code, login = get(url, "/api/accounts/token/", value=credentials)
    token = json.loads(login)["access"]
    assert code == 200 and token
    response, raw = get(url, f"/api/channels/channels/{channel}/", token=token)
    row = json.loads(raw)
    assert response == 200 and str(row["id"]) == channel
    assert row["uuid"] == "3b81d384-d6a1-4d64-938e-3e91075cec0a" and row["name"] == "owned ARM64 channel"
    assert check_fingerprint(COPY) == source == check_fingerprint("dispatcharr")
    assert Decimal(sql(COPY, LOGIN_TIME_SQL)) > source_login
    assert Decimal(sql("dispatcharr", LOGIN_TIME_SQL)) == source_login
    return {"archive": archive, "schema_tables": 62, "copied_scope": source, "fresh_copy_cache": True,
            "normal_shutdown_source": True, "copy_initialization_disabled": True, "normal_copied_account_login": True,
            "ordinary_copied_channel_read": True, "source_scoped_data_and_login_retained": True}
