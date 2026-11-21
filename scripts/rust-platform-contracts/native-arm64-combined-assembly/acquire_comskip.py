"""Retry only transient 503 from the exact signed Ubuntu snapshot acquisition."""

from argparse import ArgumentParser
import importlib.util
from pathlib import Path
import sys
import time
from urllib.error import HTTPError
from urllib.parse import urlsplit


BACKOFF = (2, 6, 14)


def leased_fetch(original, sleeper=time.sleep):
    def fetch(url,size):
        parsed=urlsplit(url)
        official=(parsed.scheme=="https" and parsed.netloc=="snapshot.ubuntu.com"
                  and parsed.path.startswith("/ubuntu/") and "/pool/" in parsed.path
                  and not parsed.query and not parsed.fragment)
        for attempt,delay in enumerate((*BACKOFF,None),start=1):
            try:
                return original(url,size)
            except HTTPError as error:
                if not official or error.code!=503 or delay is None:
                    raise
                error.close()
                print(f"signed-comskip-dependency-http503-retry={attempt}",file=sys.stderr)
                sleeper(delay)
    return fetch


def main():
    parser=ArgumentParser(description=__doc__)
    parser.add_argument("--sealed-builder",type=Path,required=True)
    parser.add_argument("--root",type=Path,required=True)
    parser.add_argument("--context",type=Path,required=True)
    args=parser.parse_args()
    spec=importlib.util.spec_from_file_location("sealed_comskip_abi_builder",args.sealed_builder.resolve(strict=True))
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.fetch=leased_fetch(module.fetch)
    print(module.stable(module.acquire(args.root,args.context)),end="")


if __name__=="__main__":
    main()
