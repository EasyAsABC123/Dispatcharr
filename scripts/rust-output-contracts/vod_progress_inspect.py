"""Fast bounded read-only fixture inspection; no Django startup/grace timing bias.
Original Dispatcharr authors/contributors retain all application credit.
"""
import os,sys,json
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only' or os.environ.get('REDIS_PASSWORD')!='playback-fixture-only':raise RuntimeError('Disposable fixture only')
sid=sys.argv[1]
if not sid.startswith('fixture-packaged-vod-progress-') or len(sid)>128:raise RuntimeError('Invalid owned fixture session')
import redis
client=redis.Redis(host='127.0.0.1',port=6379,password=os.environ['REDIS_PASSWORD'],decode_responses=True,socket_timeout=2)
row=client.hmget('vod_persistent_connection:'+sid,['bytes_sent','active_streams'])
print(json.dumps({'exists':row[0] is not None,'bytes_sent':int(row[0]) if row[0] is not None else None,'active_streams':int(row[1]) if row[1] is not None else None}))
