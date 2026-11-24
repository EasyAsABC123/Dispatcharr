"""Receive real updates from the owned ordinary fixture's Django channel layer."""

import re
from uuid import uuid4


def begin(fixture):
    name = f"specific.saved_scan_{uuid4().hex}!receiver"
    proof = fixture.sql(f"""
import json
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer

layer = get_channel_layer()
assert layer.ring_size == 1
async_to_sync(layer.group_add)('updates', {name!r})
print(json.dumps({{'actual_receiver': {name!r}}}))
""", secret=True)
    if proof != {"actual_receiver": name}:
        raise ValueError("expected an actual owned Channels receiver")
    return name


def read(fixture, name):
    if not re.fullmatch(r"specific[.]saved_scan_[a-f0-9]{32}!receiver", name):
        raise ValueError("expected the owned receiver identity")
    key = name.split("!", 1)[0] + "!"
    return fixture.sql(f"""
import base64
import json
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer

async def read_owned_updates():
    layer = get_channel_layer()
    assert layer.ring_size == 1
    raw = await layer.connection(0).zrange(layer.prefix + {key!r}, 0, 1024)
    assert len(raw) <= 1024 and sum(map(len, raw)) <= 4194304
    return {{
        'actual_receiver': {name!r},
        'events': [layer.deserialize(item) for item in raw],
        'raw_events_base64': [base64.b64encode(item).decode() for item in raw],
    }}

print(json.dumps(async_to_sync(read_owned_updates)()))
""", secret=True)
