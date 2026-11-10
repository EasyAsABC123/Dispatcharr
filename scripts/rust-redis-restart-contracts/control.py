"""Bounded local restart supervisor control; original authors retain credit."""
import json,os,socket,sys
if os.environ.get('REDIS_RESTART_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise SystemExit(2)
with socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)as client:
 client.settimeout(6);client.connect('/tmp/redis-restart-control.sock');client.sendall((json.dumps({'action':sys.argv[1]})+'\n').encode());raw=client.recv(1024);print(json.dumps(json.loads(raw)))
