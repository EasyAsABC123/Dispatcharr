"""Native same-job owned clean-install and two-reader MPEG-TS check."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import secrets
from queue import Empty, Queue
from threading import Event
import subprocess
import time
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

HERE = Path(__file__).resolve().parent
ROOT = Path(subprocess.check_output(["git","-C",str(HERE),"rev-parse","--show-toplevel"],text=True).strip())
SOURCE = "0f79dc28f8718a05a6b28dc554be958e3e295b47"
ROLE_PIN_SOURCE = "fdede54c54ccb19300c01031537803a0b553f581"
PARENT = "40141b3f9189c09da9a644a9f78f7f7eb78bab41"
CLEAN_TEMPLATE = "63c869eeb778ec178ea2cf1ad4475b5401461c665d37d578a8decf5c93637c22"
DEBIAN_INDEX = "sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc"
DEBIAN_ARM = "sha256:ec8534cda2cb30f29f44f23e135297af7a1496f2990fd8e4e84aa36b081a757a"
DEBIAN_CONFIG = "sha256:5b6c2c6cbc90b6085e3f9b3086110b1eccedb643374137bfdcd1d2f5c78f6cd1"
CHANNEL = "3b81d384-d6a1-4d64-938e-3e91075cec0a"
FIXTURE_SIZE = 188 * 1050


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self,*_args,**_kwargs):
        return None


HTTP = build_opener(ProxyHandler({}),NoRedirect())


def run(argv, *, timeout=120, body=None, env=None):
    return subprocess.run(argv,check=True,capture_output=True,input=body,timeout=timeout,env=env).stdout


def stamp(data):
    return sha256(data).hexdigest()


def source(revision=PARENT):
    body=run(["git","-C",str(ROOT),"show",f"{revision}:docker/compose.rust-clean-install.yml"])
    assert stamp(body)==CLEAN_TEMPLATE
    return body


def inputs():
    data=(HERE/"role-pins.json").read_bytes()
    assert stamp(data)=="ac18a2b82c773dea5d38ef540b135896e1672f6f160db8bd12260db47a5b30e2"
    pins=json.loads(data)
    assert pins["platform"]=="linux/arm64" and pins["configuration_revision"]==ROLE_PIN_SOURCE
    assert pins["compose_source_sha256"]==CLEAN_TEMPLATE
    return pins


def references(pins):
    roles={}
    for service,name in (("database","postgres"),("cache","redis")):
        row=pins["roles"][name]
        roles[service]=(f'{row["official_repository"]}@{row["platform_manifest"]}',row["platform_config"],row["layers"])
    roles["data-init"]=(f"docker.io/library/debian@{DEBIAN_ARM}",DEBIAN_CONFIG,1)
    return roles


def rendered(pins, context_revision, placeholder, origin_file):
    text=source(context_revision).decode()
    for name,row in pins["roles"].items():
        before=f'image: {name}@{row["compose_index"]}'
        assert text.count(before)==1
        text=text.replace(before,f'image: {row["official_repository"]}@{row["platform_manifest"]}')
    before=f"image: debian:bookworm-slim@{DEBIAN_INDEX}"
    assert text.count(before)==1 and text.count("platform: linux/amd64")==4
    text=text.replace(before,f"image: docker.io/library/debian@{DEBIAN_ARM}").replace("platform: linux/amd64","platform: linux/arm64")
    assert text.count("playback-fixture-only")==6
    text=text.replace("playback-fixture-only",placeholder)
    text=text.replace("      RUST_INITIALIZE_EMPTY_DATABASE:","      DISPATCHARR_ENABLE_IP_LOOKUP: \"0\"\n      RUST_INITIALIZE_EMPTY_DATABASE:")
    text+="\nnetworks:\n  default:\n    internal: true\n"
    origin={
        "services":{"media-fixture":{
            "image":"${RUST_CANDIDATE_IMAGE:?Supply the exact local Rust image}",
            "platform":"linux/arm64","read_only":True,"user":"65532:65532",
            "entrypoint":["/usr/local/bin/dispatcharr-python","-I","-B","/fixture.py"],
            "tmpfs":["/tmp:mode=1777,size=33554432"],"cap_drop":["ALL"],
            "security_opt":["no-new-privileges:true"],"pids_limit":64,
            "volumes":[{"type":"bind","source":str(origin_file),"target":"/fixture.py","read_only":True}],
        }}
    }
    return text,origin


def wait(test, message, duration=60):
    deadline=time.monotonic()+duration
    while time.monotonic()<deadline:
        try:
            match=test()
            if match:
                return match
        except (OSError,ValueError,subprocess.SubprocessError):
            pass
        time.sleep(.25)
    raise TimeoutError(message)


def get(url, path, token=None, value=None):
    headers={}
    if token:
        headers["Authorization"]="Bearer "+token
    body=None
    if value is not None:
        headers["Content-Type"]="application/json"
        body=json.dumps(value).encode()
    with HTTP.open(Request(url+path,headers=headers,data=body),timeout=8) as response:
        data=response.read(2097153)
        assert len(data)<=2097152
        return response.status,data


def candidate_authority(config, image):
    assert re.fullmatch("sha256:[0-9a-f]{64}",image)
    assert config["image_config"]==image and config["platform"]=="linux/arm64"
    assert config["uid"]==65532 and config["read_only_wrapper_smoke_exit"]==0
    assert config["exact_drf_asset_bytes_sizes_and_mtime_checked_nonroot"]==27
    assert config["compose_execution_verified"] is False and config["distribution_qualified"] is False


def inspect(reference):
    [role]=json.loads(run(["docker","inspect",reference]))
    return role


def verify_role(ref, expected_config, layers):
    run(["docker","pull","--platform","linux/arm64",ref])
    row=inspect(ref)
    digest=ref.rsplit("@",1)[1]
    desc=row.get("Descriptor")
    assert (row["Os"],row["Architecture"])==("linux","arm64")
    if desc:
        assert desc["digest"]==digest and row["Id"] in (digest,expected_config)
    else:
        assert row["Id"]==expected_config
    assert len(row["RootFS"]["Layers"])==layers


def decode(compose, data, env):
    # Packet edge alignment is permitted, with no interior repair.
    starts=[i for i in range(188) if all(data[pos]==0x47 for pos in range(i,len(data),188))]
    [start]=starts
    end=start+(len(data)-start)//188*188
    assert end-start>=188*300
    run(compose+["exec","-T","media-fixture","/usr/local/bin/ffmpeg","-v","error","-xerror","-i","pipe:0",
          "-map","0:v:0","-map","0:a:0","-f","null","-"],body=data[start:end],timeout=30,env=env)
    return {"packet_aligned_bytes":end-start,"audio_video_decoded":True}


def viewer(url, token, receipts, release):
    request=Request(url+"/proxy/ts/stream/"+CHANNEL,headers={
        "Authorization":"Bearer "+token,"User-Agent":"Dispatcharr ARM64 fixture"})
    with HTTP.open(request,timeout=12) as response:
        assert response.status==200 and response.headers.get_content_type()=="video/mp2t"
        entity=response.read(FIXTURE_SIZE)
        assert len(entity)==FIXTURE_SIZE
        receipts.put(entity)
        assert release.wait(40), "viewer close deadline"



COMPOSE_MARKERS = {
    b"dependency failed to start": "compose-dependency",
    b"unhealthy": "unhealthy-role",
    b"has no container to start": "missing-compose-container",
    b"no port": "port-unavailable",
    b"not running": "role-not-running",
    b"address already in use": "port-occupied",
    b"port is already allocated": "port-occupied",
    b"network is unreachable": "network-unreachable",
    b"permission denied": "permission-denied",
    b"no space left on device": "space-exhausted",
    b"no such container": "container-missing",
}


def diagnostic(error):
    safe = {"kind":type(error).__name__}
    if isinstance(error,subprocess.CalledProcessError):
        stderr = (error.stderr or b"").lower()
        safe.update(exit=error.returncode, stdout_sha256=stamp(error.stdout or b""),
                    stderr_sha256=stamp(error.stderr or b""),
                    stderr_categories=sorted({value for key,value in COMPOSE_MARKERS.items() if key in stderr}))
    return safe


FIXED_MAIN_ERRORS = {
    b"Pinned static assets: Pinned static metadata changed": "drf-static-metadata",
    b"Invalid authentication configuration": "auth-configuration",
    b"Empty database initialization failed (credentials omitted)": "schema-initialize",
    b"Database connection failed (credentials omitted)": "db-connect",
    b"Notification cache initialization failed (credentials omitted)": "notification-cache",
    b"Realtime Redis initialization failed (credentials omitted)": "redis-realtime",
    b"Invalid Core settings effects directory": "core-log-effects",
    b"Core settings require the shared cache and realtime publisher": "core-shared-publisher",
    b"Proxy settings unavailable": "proxy-settings",
    b"Redis coordination unavailable (credentials omitted)": "redis-coordination",
    b"Redis client metadata unavailable (credentials omitted)": "redis-client-metadata",
    b"Redis output storage unavailable (credentials omitted)": "redis-output-store",
    b"Redis input storage unavailable (credentials omitted)": "redis-input-store",
    b"Invalid streaming configuration": "streaming-config",
    b"Shared output profile owner dispatch unavailable": "shared-output-profile-owner",
    b"Shared output profile event reader not ready": "shared-output-profile-reader",
    b"Invalid live statistics configuration": "live-statistics",
    b"Invalid realtime configuration": "realtime-configuration",
    b"Invalid HLS configuration": "hls-configuration",
    b"Invalid fMP4 configuration": "fmp4-configuration",
    b"VOD initialization failed (credentials omitted)": "vod-engine",
    b"VOD event initialization failed (credentials omitted)": "vod-events",
    b"Catch-up sessions initialization failed (credentials omitted)": "catchup-sessions",
    b"Catch-up media initialization failed (credentials omitted)": "catchup-media",
    b"Catch-up event initialization failed (credentials omitted)": "catchup-events",
    b"Catch-up management initialization failed (credentials omitted)": "catchup-management",
    b"Output initialization failed (credentials omitted)": "output-gateway",
    b"XMLTV initialization failed (credentials omitted)": "xmltv-gateway",
    b"XC counter initialization failed (credentials omitted)": "xc-counters",
    b"XC initialization failed (credentials omitted)": "xc-gateway",
    b"HDHR initialization failed (credentials omitted)": "hdhr-gateway",
    b"Output profile initialization failed (credentials omitted)": "output-profile-initialization",
    b"Invalid backup file API configuration": "backup-file-api",
    b"Manual backup preparation failed": "manual-backup-preparation",
    b"Manual backup ownership preparation failed": "manual-backup-ownership",
    b"Invalid EPG programme read configuration": "epg-programme-read",
    b"Invalid ordinary VOD read configuration (credentials omitted)": "vod-read",
    b"Invalid EPG grid read configuration": "epg-grid",
    b"XC live dispatcher initialization failed": "xc-live-dispatcher",
    b"Combined statistics initialization failed (credentials omitted)": "combined-statistics",
    b"Core compact coordinator binding failed": "core-compact-coordinator",
    b"Core settings must share the original Auth writer budget": "core-auth-budget",
    b"Cannot bind HTTP listener": "listener-bind",
}


def classify_main_failure(log):
    prefix=b"Dispatcharr Rust startup failed: "
    candidates=[line.removeprefix(prefix) for line in log.splitlines() if line.startswith(prefix)]
    if not candidates:
        return {"startup_failure_line_seen":False}
    assert len(candidates)==1
    [message]=candidates
    return {"startup_failure_line_seen":True,
            "startup_failure_category":FIXED_MAIN_ERRORS.get(message,"unclassified-main-error"),
            "startup_failure_message_sha256":stamp(message)}


def candidate_projection(ref, env):
    if not ref:
        return None
    state=inspect(ref)["State"]
    assert state["Status"] in {"created","running","paused","restarting","removing","exited","dead"}
    known_markers = {
        b"Created original Dispatcharr schema": "schema-created",
        b"Dispatcharr Rust port listening": "main-listening",
        b"Empty database initialization failed": "init-db-failed",
        b"Database connection failed": "db-connection-failed",
        b"Notification cache initialization failed": "cache-connection-failed",
        b"Cannot bind HTTP listener": "listen-failed",
        b"Could not set up plugin repository refresh schedule": "plugin-schedule-failed",
        b"Backup schedule initialization did not settle": "backup-schedule-failed",
        b"Permission denied": "runtime-permission-denied",
    }
    log=subprocess.run(["docker","logs","--tail","30",ref],env=env,capture_output=True,timeout=10)
    result = log.stdout + log.stderr
    return {"status":state["Status"],"exit":state["ExitCode"],"oom":state["OOMKilled"],
            "docker_logs_exit":log.returncode,"bounded_log_sha256":stamp(result),
            "startup_categories":sorted({label for marker,label in known_markers.items() if marker in result}),
            "docker_state_error_present":bool(state.get("Error")),
            "startup_diagnostic":classify_main_failure(result)}


def exercise(candidate_receipt, image, context_revision, work, job):
    assert os.uname().machine=="aarch64" and re.fullmatch(r"[0-9]+-[0-9]+",job)
    raw=candidate_receipt.read_bytes()
    candidate=json.loads(raw)
    candidate_authority(candidate,image)
    actual=inspect(image)
    assert actual["Architecture"]=="arm64" and actual["Os"]=="linux"
    assert actual["Id"]==image and actual["Config"]["User"]=="65532:65532"
    names=references(inputs())
    for role in names.values():
        verify_role(*role)
    work.mkdir(mode=0o700)
    origin_path=work/"origin.py"
    origin_path.write_bytes((HERE/"media_origin.py").read_bytes()); origin_path.chmod(0o444)
    text,origin=rendered(inputs(),context_revision,secrets.token_hex(24),origin_path)
    project="arm64-private-compose-"+job
    config=work/"compose.yml"; config.write_text(text); config.chmod(0o600)
    overlay=work/"media.json"; overlay.write_text(json.dumps(origin))
    explicit_off=work/"off.json"
    explicit_off.write_text(json.dumps({"services":{"candidate":{"environment":{"RUST_INITIALIZE_EMPTY_DATABASE":"0"}}}}))
    tag=f"dispatcharr-native-arm64-fixture:{job}"
    assert subprocess.run(["docker","image","inspect",tag],capture_output=True).returncode!=0
    run(["docker","image","tag",image,tag])
    env={**os.environ,"RUST_CANDIDATE_IMAGE":tag}
    base=["docker","compose","-p",project,"-f",str(config)]
    add_origin=base+["-f",str(overlay)]
    report={"scope":"native ARM64 Compose clean-install HTTP/SQL and two-viewer MPEG-TS",
            "candidate_image_config":image,"candidate_smoke_receipt_sha256":stamp(raw),
            "performance_measured":False,"browser_executed":False,"redistribution_qualified":False,"checks":[]}
    def compose(args,**opt):
        return run(base+args,env=env,**opt).decode().strip()
    def sql(query):
        return compose(["exec","-T","database","psql","-X","--no-psqlrc","-v","ON_ERROR_STOP=1","-U","dispatch",
                        "-d","dispatcharr","-At","-c",query])
    def record(key,condition):
        assert condition,key
        report["checks"].append(key)
    def exact_roles():
        roles={}
        for key in names|{"candidate":(None,image,None)}:
            ref=compose(["ps","-a","-q",key])
            assert re.fullmatch("[a-f0-9]{64}",ref)
            owned.append(ref)
            row=inspect(ref)
            wanted=names[key][1] if key in names else image
            assert row["Image"]==wanted or (key in names and row.get("ImageManifestDescriptor",{}).get("digest")==names[key][0].rsplit("@",1)[1])
            roles[key]=row
        return roles
    owned=[]
    phase="clean-install"
    checkpoint="compose-config"
    running_id=None
    passed=False
    try:
        config_json=json.loads(compose(["config","--format","json"]))
        assert sorted(config_json["services"])==sorted(names|{"candidate":None})
        assert config_json["networks"]["default"]["internal"] is True
        record("ordinary-topology-and-internal-network",True)
        checkpoint="initial-infrastructure"
        compose(["up","-d","--no-build","--pull","never","database","cache"])
        wait(lambda:sql("SELECT 1")=="1","database-ready")
        schema="SELECT count(*) FROM pg_tables WHERE schemaname='public'"
        record("clean-empty-database",sql(schema)=="0")
        checkpoint="initialization-off-start"
        compose(["-f",str(explicit_off),"up","-d","--no-build","--pull","never","candidate"])
        off_id=compose(["ps","-a","-q","candidate"])
        owned.append(off_id)
        off=wait(lambda:(row if (row:=inspect(off_id))["State"]["Status"]=="exited" else None),"initialize-off-exit")
        record("initialization-off-exits-without-ddl",off["State"]["ExitCode"]!=0 and sql(schema)=="0")
        checkpoint="initialization-off-remove"
        controls={key:compose(["ps","-a","-q",key]) for key in names}
        assert all(re.fullmatch("[a-f0-9]{64}",rid) for rid in controls.values())
        run(["docker","container","rm",off_id],env=env)
        assert subprocess.run(["docker","container","inspect",off_id],capture_output=True).returncode!=0
        checkpoint="initialize-on-start"
        compose(["up","-d","--no-build","--pull","never","--no-deps","candidate"])
        checkpoint="initialize-on-id"
        running_id=compose(["ps","-a","-q","candidate"])
        assert re.fullmatch("[a-f0-9]{64}",running_id) and running_id!=off_id
        owned.append(running_id)
        checkpoint="initialize-on-port"

        def discover_endpoint():
            observed=inspect(running_id)
            if observed["State"]["Status"] in {"exited","dead"}:
                raise AssertionError("candidate exited before binding")
            bindings=(observed["NetworkSettings"]["Ports"] or {}).get("9192/tcp") or []
            if observed["State"]["Status"]!="running" or not bindings:
                return None
            [binding]=bindings
            port=binding["HostPort"]
            assert binding["HostIp"]=="127.0.0.1" and re.fullmatch("[0-9]{1,5}",port)
            return binding["HostIp"]+":"+port

        url="http://"+wait(discover_endpoint,"candidate-port")
        checkpoint="initialize-on-ready"
        wait(lambda:get(url,"/health/ready")[0]==200,"candidate-ready")
        checkpoint="initialize-on-owned-roles"
        assert {key:compose(["ps","-a","-q",key]) for key in names}==controls
        roles=exact_roles()
        record("native-only-roles",len(roles)==4 and roles["data-init"]["State"]["ExitCode"]==0)
        checkpoint="initialized-schema"
        record("initialized-original-schema",sql(schema)=="62" and sql("SELECT count(*) FROM django_migrations")=="184"
               and sql("SELECT count(*) FROM accounts_user")=="0")
        checkpoint="frontend"
        code,index=get(url,"/")
        installed=compose(["exec","-T","candidate","sha256sum","/opt/dispatcharr-frontend/index.html"]).split()[0]
        record("frontend-index-served",code==200 and b'id="root"' in index and stamp(index)==installed)
        checkpoint="first-admin"
        setup="/api/accounts/initialize-superuser/"
        pre=json.loads(get(url,setup)[1])
        record("normal-first-admin-form-available",pre["superuser_exists"] is False and pre["setup_allowed"] is True)
        actor={"username":"owned-arm64-admin","password":secrets.token_urlsafe(24),"email":"Owned@EXAMPLE.INVALID"}
        record("normal-first-admin-initialized",json.loads(get(url,setup,value=actor)[1])=={"superuser_exists":True})
        creds={key:actor[key] for key in ("username","password")}
        checkpoint="first-login"
        auth="/api/accounts/token/"
        token=json.loads(get(url,auth,value=creds)[1])["access"]
        record("normal-login-created-admin",bool(token) and sql("SELECT count(*) FROM accounts_user WHERE is_superuser AND is_staff AND is_active AND user_level=10 AND last_login IS NOT NULL")=="1")
        fingerprint="SELECT md5(string_agg(app||':'||name||':'||applied::text,',' ORDER BY id)) FROM django_migrations"
        before=sql(fingerprint)
        checkpoint="restart"
        compose(["restart","candidate"])
        wait(lambda:get(url,"/health/ready")[0]==200,"candidate-restart")
        record("restart-preserves-schema-and-closes-setup",sql(fingerprint)==before and
               json.loads(get(url,setup)[1])=={"superuser_exists":True})
        token=json.loads(get(url,auth,value=creds)[1])["access"]
        phase="owned-playback"
        checkpoint="origin-start"
        run(add_origin+["up","-d","--no-build","--pull","never","media-fixture"],env=env)
        fixture=compose(["ps","-a","-q","media-fixture"])
        if not fixture:
            fixture=run(add_origin+["ps","-a","-q","media-fixture"],env=env).decode().strip()
        assert re.fullmatch("[a-f0-9]{64}",fixture)
        owned.append(fixture)
        assert inspect(fixture)["Image"]==image
        def stats():
            data=run(add_origin+["exec","-T","media-fixture","/usr/local/bin/dispatcharr-python","-I","-B","-c",
             "from urllib.request import urlopen; print(urlopen('http://127.0.0.1:8080/stats',timeout=2).read().decode())"],
             env=env,timeout=5)
            return json.loads(data)
        wait(lambda:stats()=={"active":0,"connections":0},"fresh-owned-origin")
        checkpoint="media-sql"
        run(base+["exec","-T","database","psql","-X","-v","ON_ERROR_STOP=1","-U","dispatch","-d","dispatcharr"],
            body=(HERE/"playback.sql").read_bytes(),env=env)
        releases=Event()
        receipts=Queue(2)
        checkpoint="two-viewers"
        with ThreadPoolExecutor(max_workers=2) as pool:
            jobs=[pool.submit(viewer,url,token,receipts,releases) for _ in range(2)]
            try:
                bodies=[receipts.get(timeout=20),receipts.get(timeout=20)]
                record("one-active-origin-two-authenticated-readers",stats()=={"active":1,"connections":1})
                report["decoders"]=[decode(add_origin,body,env) for body in bodies]
            finally:
                releases.set()
            for future in jobs:
                future.result(timeout=4)
        checkpoint="origin-retirement"
        wait(lambda:stats()=={"active":0,"connections":1},"origin-retired",duration=60)
        record("both-readers-decoded-and-origin-retired",len(report["decoders"])==2)
        passed=True
        phase="complete"
    except (AssertionError,Empty,KeyError,OSError,ValueError,TimeoutError,subprocess.SubprocessError) as error:
        report["failure_type"]=type(error).__name__
        report["failure_checkpoint"]=checkpoint
        report["redacted_command_failure"]=diagnostic(error)
        candidate=running_id
        try:
            if candidate is None:
                candidate=compose(["ps","-a","-q","candidate"])
            if re.fullmatch("[a-f0-9]{64}",candidate or ""):
                report["candidate_startup"]=candidate_projection(candidate,env)
        except (AssertionError,OSError,ValueError,subprocess.SubprocessError) as witness_error:
            report["candidate_witness_failure"]=diagnostic(witness_error)
        if checkpoint.startswith("initialize-on-"):
            try:
                total=int(sql("SELECT count(*) FROM pg_tables WHERE schemaname='public'"))
                migrations=int(sql("SELECT count(*) FROM django_migrations"))
                report["owned_database_post_failure"]={
                    "public_table_count":total,"migration_count":migrations}
            except (OSError,ValueError,subprocess.SubprocessError) as sql_error:
                report["owned_database_post_failure"]={"query_diagnostic":diagnostic(sql_error)}
    finally:
        try:
            run(add_origin+["down","--volumes","--remove-orphans"],env=env)
            remaining={kind:run(["docker",kind,"ls","-q","--filter",f"label=com.docker.compose.project={project}"]).decode().strip()
                       for kind in ("container","network","volume")}
            assert not any(remaining.values())
            assert all(subprocess.run(["docker","container","inspect",item],capture_output=True).returncode!=0 for item in set(owned))
            report["exact_cleanup"]=True
        except (AssertionError,OSError,subprocess.SubprocessError) as error:
            report["exact_cleanup"]=False
            report["cleanup_failure_type"]=type(error).__name__
        finally:
            run(["docker","image","rm",tag])
    report["phase"]=phase
    report["actual_exit"]=0 if passed and report["exact_cleanup"] else 1
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate",type=Path,required=True)
    parser.add_argument("--image",required=True)
    parser.add_argument("--work",type=Path,required=True)
    parser.add_argument("--job",required=True)
    parser.add_argument("--source-revision",default=PARENT)
    args=parser.parse_args()
    result=exercise(args.candidate,args.image,args.source_revision,args.work,args.job)
    print(json.dumps(result,indent=2,sort_keys=True))
    return result["actual_exit"]


if __name__=="__main__":
    raise SystemExit(main())
