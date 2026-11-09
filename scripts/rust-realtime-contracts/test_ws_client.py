"""Fixture-client checks; application protocol proof lives in captured runs."""
import json
import socket
import struct
import unittest
from ws_client import Client


def frame(opcode,data,final=True):
    prefix=bytes([(128 if final else 0)|opcode])
    if len(data)<126:return prefix+bytes([len(data)])+data
    return prefix+bytes([126])+struct.pack('!H',len(data))+data


class ClientFrames(unittest.TestCase):
    def setUp(self):
        client, self.server=socket.socketpair()
        self.client=Client.__new__(Client)
        self.client.socket=client;self.client.buffer=b''
        self.client.fragments=bytearray();self.client.kind=None
        self.client.max_message_bytes=32768

    def tearDown(self):
        self.client.socket.close();self.server.close()

    def test_partial_frame_survives_optional_timeout(self):
        data=frame(1,json.dumps({'value':'x'*200}).encode())
        self.server.sendall(data[:9])
        self.assertIsNone(self.client.optional(.01))
        self.server.sendall(data[9:])
        self.assertEqual(self.client.receive()['message'],{'value':'x'*200})

    def test_pong_during_fragmented_message_retains_state(self):
        self.server.sendall(frame(1,b'{"value":',False)+frame(10,b'nonce')+frame(0,b'123}'))
        self.assertEqual(self.client.receive(),{'pong':b'nonce'.hex()})
        self.assertEqual(self.client.receive(),{'message':{'value':123}})

    def test_interleaved_ping_replies_and_delivers_one_message(self):
        self.server.sendall(frame(1,b'{"value":',False)+frame(9,b'nonce')+frame(0,b'123}'))
        self.assertEqual(self.client.receive(),{'message':{'value':123}})
        self.server.settimeout(1)
        response=self.server.recv(128)
        self.assertEqual(response[0],0x8a)
        self.assertEqual(response[1]&127,5)
        mask=response[2:6]
        self.assertEqual(bytes(value^mask[index%4] for index,value in enumerate(response[6:])),b'nonce')

    def test_control_payload_does_not_consume_message_budget(self):
        self.client.max_message_bytes=16
        self.server.sendall(frame(1,b'{"value":',False)+frame(10,b'n'*125)+frame(0,b'123}'))
        self.assertEqual(self.client.receive(),{'pong':(b'n'*125).hex()})
        self.assertEqual(self.client.receive(),{'message':{'value':123}})

    def test_fragmented_control_is_rejected(self):
        self.server.sendall(frame(9,b'bad',False))
        with self.assertRaises(ValueError):self.client.receive()


if __name__=='__main__':unittest.main()
