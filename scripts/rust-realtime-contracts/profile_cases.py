"""Grouped Original and Native wire cases for the account profile editor.
Original Dispatcharr authors retain full application credit.
"""
import hashlib
import json
from pathlib import Path
from urllib.parse import urlencode

from ws_client import Client


INITIAL = {
    "type": "connection_established",
    "data": {
        "success": True,
        "message": "WebSocket connection established successfully",
    },
}
ROLES = {0: "viewer", 1: "standard", 10: "admin"}
DATASET = Path(__file__).with_name("datasets") / "source-preview.json"


def exercise(host, port, accounts, *, observe_open=None):
    rows = json.loads(DATASET.read_text())["cases"]
    clients = {}
    digest = hashlib.sha256()
    try:
        for role in ROLES.values():
            target = "/ws/?" + urlencode({"token": accounts[role]["access"]})
            clients[role] = client = Client(host, port, target)
            assert client.status == 101 and client.receive() == {"message": INITIAL}, role
        if observe_open is not None:
            observe_open()

        for row in rows:
            role = ROLES[row["role"]]
            client = clients[role]
            client.json(row["input"])
            expected = [{"message": item} for item in row["sent"]]
            observed = [client.receive()] if expected else []
            if not expected:
                # A clean timeout distinguishes an ignored command from a close.
                assert client.optional(0.25) is None, row["case"]
            assert observed == expected, row["case"]
            digest.update(json.dumps([row["case"], observed], sort_keys=True).encode())
            digest.update(b"\n")

        for role, client in clients.items():
            if role != "admin":
                assert client.optional(0.25) is None, role
            client.send(9, b"profile-editor-still-connected")
            assert client.receive() == {"pong": b"profile-editor-still-connected".hex()}, role
    finally:
        for client in clients.values():
            client.close()
    return {
        "case_count": len(rows),
        "admitted": sum(bool(row["sent"]) for row in rows),
        "ignored": sum(not row["sent"] for row in rows),
        "decoded_observation_sha256": digest.hexdigest(),
        "private_and_reusable": True,
    }
