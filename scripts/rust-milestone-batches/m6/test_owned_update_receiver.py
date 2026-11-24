"""Offline controls for the owned fixture's generated receiver commands."""

import asyncio
from contextlib import redirect_stdout
from io import StringIO
import json
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import owned_update_receiver as receiver


class Fixture:
    def __init__(self, ring_size=1):
        self.ring_size = ring_size
        self.prefix = "owned:"
        self.members = {}
        self.keys = []
        self.sql_calls = 0

    async def group_add(self, group, name):
        self.members[group] = name

    def connection(self, index):
        assert index == 0
        return self

    async def zrange(self, key, start, end):
        self.keys.append(key)
        assert (start, end) == (0, 1024)
        return [b'{"type":"update"}']

    @staticmethod
    def deserialize(raw):
        return json.loads(raw)

    def sql(self, code, *, secret):
        assert secret is True
        self.sql_calls += 1
        modules = {
            "asgiref.sync": SimpleNamespace(
                async_to_sync=lambda call: lambda *args: asyncio.run(call(*args)),
            ),
            "channels.layers": SimpleNamespace(get_channel_layer=lambda: self),
        }
        capture = StringIO()
        with patch.dict(sys.modules, modules), redirect_stdout(capture):
            exec(compile(code, "owned fixture guest", "exec"), {})
        return json.loads(capture.getvalue())


class OwnedUpdateReceiver(unittest.TestCase):
    def test_configured_layer_joins_and_reads_only_the_owned_receiver(self):
        fixture = Fixture()
        name = receiver.begin(fixture)
        self.assertEqual(fixture.members, {"updates": name})
        expected = name.split("!", 1)[0] + "!"
        report = receiver.read(fixture, name)
        self.assertEqual(fixture.keys, ["owned:" + expected])
        self.assertEqual(report, {
            "actual_receiver": name,
            "events": [{"type": "update"}],
            "raw_events_base64": ["eyJ0eXBlIjoidXBkYXRlIn0="],
        })

    def test_foreign_receiver_is_rejected_before_running_a_fixture_command(self):
        fixture = Fixture()
        with self.assertRaisesRegex(ValueError, "owned receiver identity"):
            receiver.read(fixture, "foreign!receiver")
        self.assertEqual(fixture.sql_calls, 0)

    def test_unqualified_ring_is_rejected_before_join(self):
        fixture = Fixture(ring_size=2)
        with self.assertRaises(AssertionError):
            receiver.begin(fixture)
        self.assertEqual(fixture.members, {})
