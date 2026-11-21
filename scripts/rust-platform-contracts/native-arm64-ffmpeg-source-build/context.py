"""Materialize the proven native FFmpeg closure from exact public HTTPS sources."""
from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor
from hashlib import file_digest,sha256
from io import BytesIO
from pathlib import Path,PurePosixPath
from tempfile import NamedTemporaryFile,TemporaryDirectory
import json
import lzma
import os
import re
import shutil
import subprocess
import tarfile
import time
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler,Request,build_opener

from render import render, UPSTREAM

HERE=Path(__file__).resolve().parent
LOCK="9ae3caaf9826b5b1a466ef61ecf7f9069edd832903f19c9fb23b252e12082abe"
KNOWN_UBUNTU_KEY="F6ECB3762474EDA9D21B7022871920D1991BC93C"
UBUNTU_ORIGINS={"snapshot.ubuntu.com","archive.ubuntu.com","ports.ubuntu.com"}
PUBLIC_HOSTS=UBUNTU_ORIGINS|{"ffmpeg.org","code.videolan.org","gitlab.freedesktop.org","github.com",
                            "codeload.github.com","downloads.xiph.org","ftp.osuosl.org","bitbucket.org"}
HEX = re.compile("[a-f0-9]{64}")


def digest(p):
    with p.open("rb") as raw:
        return file_digest(raw,"sha256").hexdigest()


class PublicOnly(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        url=urlsplit(newurl)
        assert url.scheme=="https" and url.hostname in PUBLIC_HOSTS and not url.username
        return Request(newurl,headers={"User-Agent":"Dispatcharr-public-source/1"})


def fetch(url,length,sha,where):
    start=urlsplit(url)
    assert start.scheme=="https" and start.hostname in PUBLIC_HOSTS and not start.username
    assert HEX.fullmatch(sha) and 0<length<=65*1024**2
    destination=where/sha
    if destination.exists():
        assert destination.stat().st_size==length and digest(destination)==sha
        return
    waits=(0,2,6,14)
    for trial,delay in enumerate(waits):
        if delay:
            time.sleep(delay)
        with NamedTemporaryFile(dir=where,delete=False) as immediate:
            scratch=Path(immediate.name)
            try:
                request=Request(url, headers={"User-Agent":"Dispatcharr-public-source/1"})
                with build_opener(PublicOnly()).open(request,timeout=40) as output:
                    assert output.status==200
                    count=0
                    while buffer:=output.read(256*1024):
                        count+=len(buffer)
                        assert count<=length
                        immediate.write(buffer)
                immediate.flush()
                assert count==length and digest(scratch)==sha
                os.replace(scratch,destination)
                return
            except HTTPError as error:
                code=error.code
                error.close()
                if code!=503 or trial==len(waits)-1:
                    raise RuntimeError(f"Public {start.hostname} returned HTTP {code} for pinned {sha}") from None
            finally:
                scratch.unlink(missing_ok=True)


def source_dep_path(name):
    assert re.fullmatch("[A-Z0-9_]+",name)
    return f"source/{name}"+(".tar.xz" if name=="FFMPEG" else ".tar.gz")


def inputs():
    authority=json.loads((HERE/"authority.json").read_bytes())
    path=HERE/"dependency-lock.json"
    assert digest(path)==LOCK==authority["lock_sha256"]
    lock=json.loads(path.read_bytes())
    assert lock["ubuntu_image"]==authority["base"]["official_image"]
    assert authority["upstream"]==UPSTREAM
    assert len(lock["source_archives"])==9 and len(lock["apt_contents"])==327
    assert {name:digest(HERE/"expected"/name) for name in authority["expected_inputs"]}==authority["expected_inputs"]
    assert authority["signed_index_signer"]==KNOWN_UBUNTU_KEY
    return authority,lock


def package_urls(authority,lock):
    urls={}
    def assign(path,url,desc,preference=False):
        name=desc["sha256"]
        ref=(url,desc["bytes"],name)
        if name not in urls or preference:
            urls[name]=ref
    canonical=authority["canonical_snapshot"]
    assert canonical=="https://snapshot.ubuntu.com/ubuntu/20261102T221537Z"
    for path,desc in lock["apt_contents"].items():
        parts=PurePosixPath(path).parts
        assert parts[0] in ("dists","pool") and ".." not in parts
        base=authority["fallback_pins_by_package"].get(path,canonical)
        assert urlsplit(base).hostname in UBUNTU_ORIGINS
        assign(path,base+"/"+path,desc,preference="/by-hash/" in path)
    for name,desc in lock["source_archives"].items():
        assign(source_dep_path(name),desc["url"],desc)
    assert len(urls)==330
    return sorted(urls.values(),key=lambda row:-row[1])


def control_rows(text):
    row={}
    for line in text:
        if line=="\n":
            if row:
                yield row
            row={}
        elif not line.startswith(" "):
            key,_,value=line.partition(":")
            row[key]=value.strip()
    if row:
        yield row


def trusted_apt_indexes(lock,cache):
    path=HERE/"ubuntu-snapshot-key.asc"
    with TemporaryDirectory(prefix="ffmpeg-native-archive-key-") as scratch:
        work=Path(scratch)
        command=["gpg","--homedir",str(work),"--no-options","--batch"]
        report=subprocess.check_output(command+["--with-colons","--show-keys",path],text=True,stderr=subprocess.PIPE)
        rows=report.splitlines()
        assert [line.split(":")[9] for before,line in zip(rows,rows[1:])
                if before.startswith("pub:") and line.startswith("fpr:")]==[KNOWN_UBUNTU_KEY]
        ring=work/"archive.gpg"
        subprocess.run(command+["--dearmor","--output",ring,path],check=True,capture_output=True)
        layouts={}
        for path,desc in lock["apt_contents"].items():
            if not path.endswith("/InRelease"):
                continue
            run=subprocess.run(["gpgv","--homedir",work,"--status-fd","2","--keyring",ring,"--output","-",
                                cache/desc["sha256"]],check=True,capture_output=True)
            assert any(KNOWN_UBUNTU_KEY in line.split() for line in run.stderr.decode().splitlines() if line.startswith("[GNUPG:] VALIDSIG "))
            outputs=run.stdout.decode().split("\nSHA256:\n",1)[1]
            folder=path.rsplit("/",1)[0]
            active={rel.removeprefix(folder+"/"):val for rel,val in lock["apt_contents"].items()
                    if rel.startswith(folder+"/") and rel.endswith("/binary-arm64/Packages.xz")}
            found=set()
            for line in outputs.splitlines():
                if not line.startswith(" "):
                    break
                h,n,p=line.split()
                if p in active:
                    assert active[p]=={"bytes":int(n),"sha256":h}
                    found.add(p)
                    layouts[folder+"/"+p]=h
            assert found==set(active)
        assert len(layouts)==6
        materials={path:desc for path,desc in lock["apt_contents"].items() if path.startswith("pool/")}
        covered=set()
        for sha in layouts.values():
            with lzma.open(cache/sha,"rt",encoding="utf-8") as text:
                for row in control_rows(text):
                    path=row.get("Filename")
                    if path in materials:
                        assert materials[path]=={"bytes":int(row["Size"]),"sha256":row["SHA256"]}
                        assert row["Architecture"] in ("all","arm64")
                        covered.add(path)
        assert covered==set(materials) and len(covered)==312


def materialize(work,cache,recipe):
    authority,lock=inputs()
    assert digest(recipe)==lock["source_archive_sha256"]
    trusted_apt_indexes(lock,cache)
    files={}
    root="FFmpeg-"+UPSTREAM
    with tarfile.open(recipe) as src:
        for entry in src:
            if entry.isdir():
                continue
            assert entry.isfile() and entry.name.startswith(root+"/") and 0<entry.size<100000
            name=entry.name.removeprefix(root+"/")
            path=PurePosixPath(name)
            assert not path.is_absolute() and ".." not in path.parts and name not in files
            files[name]=(src.extractfile(entry).read(),entry.mode)
    updated=render(files,lock["ubuntu_image"])
    original=updated["Dockerfile"]
    # Skip architecture-independent translation indexes in the signed local mirror.
    assert original.count(b"-o Dir::Etc::sourceparts=-")==7
    updated["Dockerfile"]=original.replace(b"-o Dir::Etc::sourceparts=-",
                                            b"-o Dir::Etc::sourceparts=- -o Acquire::Languages=none")
    work.mkdir(mode=0o700)
    for name,(data,mode) in files.items():
        out=work/name
        out.parent.mkdir(parents=True,exist_ok=True)
        out.write_bytes(updated.get(name,data)); out.chmod(mode)
    for section,entries in (("apt",lock["apt_contents"]),("source",lock["source_archives"])):
        for key,desc in entries.items():
            rel="apt/"+key if section=="apt" else source_dep_path(key)
            blob=cache/desc["sha256"]
            assert blob.stat().st_size==desc["bytes"] and digest(blob)==desc["sha256"]
            target=work/"offline"/rel; target.parent.mkdir(parents=True,exist_ok=True)
            shutil.copyfile(blob,target); target.chmod(0o444)
    options="arch=arm64 signed-by=/usr/share/keyrings/ubuntu-archive-keyring.gpg"
    suites=("resolute","resolute-updates","resolute-security")
    index=work/"offline/apt/sources.list"
    index.write_text("".join(f"deb [{options}] file:/offline/apt {suite} main universe\n" for suite in suites))
    index.chmod(0o444)
    for directory in (work/"offline").rglob("*"):
        if directory.is_dir():
            directory.chmod(0o755)
    (work/"offline").chmod(0o755)
    return {"source_revision":authority["upstream"],"dependency_lock_sha256":LOCK,"source_archives":9,
            "canonical_signed_binary_archives":312,"canonical_index_paths":15,"expected_notice_inputs":62,
            "offline_dockerfile_sha256":digest(work/"Dockerfile")}


def acquire(work):
    authority,lock=inputs()
    cache=work/"public"; cache.mkdir(mode=0o700)
    urls=package_urls(authority,lock)
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda item:fetch(*item,cache),urls))
    rev=authority["upstream"]; sha=lock["source_archive_sha256"]
    fetch(f"https://codeload.github.com/Dispatcharr/FFmpeg/tar.gz/{rev}",37651,sha,cache)
    return cache,cache/sha


if __name__=="__main__":
    parser=ArgumentParser(description=__doc__)
    parser.add_argument("--work",type=Path,required=True)
    args=parser.parse_args()
    blobs,recipe=acquire(args.work)
    result=materialize(args.work/"context",blobs,recipe)
    print(json.dumps(result,indent=2,sort_keys=True))
