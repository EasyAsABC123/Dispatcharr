"""Observe the pinned task registry with a single pre-bind IPv6 fallback."""

import errno
import hashlib
import json
from pathlib import Path
import socket
import sys

import diagnostic_probe as diagnostic
import original_probe as original


MODULE = "urllib3.util.connection"
SOURCE = "/dispatcharrpy/lib/python3.13/site-packages/urllib3/util/connection.py"
SOURCE_SHA256 = "2633bbdb69731e5ccb5cf4e4afd65605d86c7979cc5633126f50c92d5ad74a74"
BIND_LINE = 127
IMPORT_LINE = 137
FALLBACKS = []
SOURCE_VERIFIED = False


def at_source(frame, function, line):
    return (frame is not None and frame.f_code.co_filename == SOURCE and frame.f_code.co_name == function
            and frame.f_lineno == line and frame.f_globals.get("__name__") == MODULE)


def expected_ipv6_check(event, args, frame):
    if (event != "socket.bind" or not SOURCE_VERIFIED or FALLBACKS or len(args) != 2 or args[1] != ("::1", 0)
            or not at_source(frame, "_has_ipv6", BIND_LINE) or not at_source(frame.f_back, "<module>", IMPORT_LINE)):
        return False
    sock = args[0]
    return (type(sock) is socket.socket and sock.family == socket.AF_INET6 and sock.type == socket.SOCK_STREAM
            and sock.proto == 0 and frame.f_locals.get("sock") is sock and frame.f_locals.get("host") == "::1")


def audit(event, args):
    if event in diagnostic.NETWORK_APIS:
        frame = sys._getframe(1)
        if expected_ipv6_check(event, args, frame):
            FALLBACKS.append({"api": event, "module": MODULE, "function": "_has_ipv6", "line": BIND_LINE,
                              "source_sha256": SOURCE_SHA256, "result": "EAFNOSUPPORT"})
            raise OSError(errno.EAFNOSUPPORT, "IPv6 unavailable in the metadata fixture")
        diagnostic.deny_network(event, frame)
    diagnostic.original_audit(event, args)


def replay_print(line):
    report = json.loads(line)
    report["ipv6_fallbacks"] = FALLBACKS
    module = sys.modules.get(MODULE)
    report["ipv6_unavailable"] = module is not None and vars(module).get("HAS_IPV6") is False
    report["network_diagnostic"] = {"api_counts": dict(diagnostic.API_COUNTS), "reports": diagnostic.REPORTS}
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    if hashlib.sha256(Path(SOURCE).read_bytes()).hexdigest() != SOURCE_SHA256:
        raise ValueError("Exact archived urllib3 source is required")
    SOURCE_VERIFIED = True
    original.audit = audit
    original.guards = diagnostic.guards
    original.print = replay_print
    original.main()
