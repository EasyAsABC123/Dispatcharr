"""Actual bounded raw Redis and non-consuming owned Channels observations.
No Redis/Channels/Django imports or network work occurs on module import.
"""
import hashlib,json,math,os,re

def require(ok,label):
 if not ok:raise ValueError(label)

def inventory(client,database):
 cursor=0;keys=set()
 for _ in range(128):
  cursor,batch=client.scan(cursor=cursor,count=100);keys.update(batch);require(len(keys)<=1000 and all(len(k)<=1024 for k in keys),'BoundedRawRedisKeys')
  if cursor==0:break
 else:raise ValueError('RawRedisScanCapacity')
 rows=[];used=0
 for key in sorted(keys):
  kind=client.type(key).decode();dump=client.dump(key);require(dump is not None,'RawRedisKeyExpiredDuringCapture');used+=len(key)+len(dump);row={'key_hex':key.hex(),'type':kind,'dump_hex':dump.hex(),'pttl':client.pttl(key)};require(len(dump)<=65536,'RawRedisDumpCapacity')
  def raw(value):
   nonlocal used
   require(type(value)is bytes and len(value)<=65536,'RawRedisValueCapacity');used+=len(value);return value.hex()
  if kind=='string':row['value_hex']=raw(client.get(key))
  elif kind=='list':require(client.llen(key)<=64,'RawRedisListCapacity');row['list_hex']=[raw(v)for v in client.lrange(key,0,-1)]
  elif kind=='set':require(client.scard(key)<=64,'RawRedisSetCapacity');row['set_hex']=[raw(v)for v in sorted(client.smembers(key))]
  elif kind=='hash':require(client.hlen(key)<=64,'RawRedisHashCapacity');row['hash_hex']=[{'field_hex':raw(k),'value_hex':raw(v)}for k,v in sorted(client.hgetall(key).items())]
  elif kind=='zset':
   require(client.zcard(key)<=64,'RawRedisZsetCapacity');items=client.zrange(key,0,-1,withscores=True);require(all(math.isfinite(score)for value,score in items),'FiniteRedisZsetScores');row['zset']=[{'member_hex':raw(value),'score':score,'score_hex':score.hex()}for value,score in items]
  elif kind=='stream':
   require(client.xlen(key)<=64,'RawRedisStreamCapacity');row['stream']=[{'id_hex':raw(ident),'fields_hex':[{'field_hex':raw(k),'value_hex':raw(v)}for k,v in sorted(fields.items())]}for ident,fields in client.xrange(key)]
  else:raise ValueError('UnsupportedRawRedisTypeCaptureFailure:'+kind)
  require(type(row['pttl'])is int and row['pttl']>=-1 and used<=2097152,'RawRedisTTLOrTotalCapacity');rows.append(row)
 return {'database':database,'rows':rows,'capture_bytes':used,'complete':True,'destructive_commands_or_consumption':False}

class Probe:
 def __init__(self,nonce):
  require(re.fullmatch('[a-f0-9]{10}',nonce)is not None,'OwnedChannelsNonce')
  import redis
  from asgiref.sync import async_to_sync
  from channels.layers import get_channel_layer
  from urllib.parse import urlsplit
  self.layer=get_channel_layer();self.async_to_sync=async_to_sync;self.clients={n:redis.Redis(host='127.0.0.1',port=6379,password='playback-fixture-only',db=n)for n in range(4)};self.receiver='specific.rust_owned_settings32_'+nonce+'!receiver';self.group='updates';self.registered=False
  # Same actual configured RedisChannelLayer; no alternate sink or method replacement.
  host=self.layer.hosts[0];address=host['address'];parsed=urlsplit(address);require(parsed.hostname=='127.0.0.1'and parsed.port==6379 and parsed.path=='/1','ActualChannelsOwnedRedisDB1');require(type(self.layer).__module__+'.'+type(self.layer).__qualname__=='channels_redis.core.RedisChannelLayer','ActualOriginalChannelsLayer')
  self.key=(self.layer.prefix+self.receiver.split('!')[0]+'!').encode();self.group_key=self.layer._group_key(self.group)
 def raw(self):return[inventory(self.clients[n],n)for n in range(4)]
 def register(self):
  require(not self.registered,'OwnChannelsRegisteredOnce');self.async_to_sync(self.layer.group_add)(self.group,self.receiver);self.registered=True
  return {'registered':True,'group':self.group,'receiver':self.receiver,'group_key_hex':self.group_key.hex(),'channel_key_hex':self.key.hex(),'actual_layer_class':'channels_redis.core.RedisChannelLayer','Redis_database':1,'non_consuming':True,'Source_methods_replaced':False,'group_registration_is_fixture_context_not_Default_graph':True}
 def snapshot(self):
  require(self.registered,'OwnedChannelsRegistrationRequired');client=self.clients[1];kind=client.type(self.key).decode();require(kind in('none','zset'),'ActualChannelsMailboxType');items=[]if kind=='none'else client.zrange(self.key,0,-1,withscores=True);require(len(items)<=64 and sum(len(raw)for raw,score in items)<=1048576,'ChannelsMailboxCapacity');frames=[]
  for raw,score in items:
   require(len(raw)<=65536 and math.isfinite(score),'ChannelsFrameCapacity');decoded=self.layer.deserialize(raw);require(len(json.dumps(decoded).encode())<=65536,'DecodedChannelsFrameCapacity');frames.append({'raw_hex':raw.hex(),'raw_bytes':len(raw),'raw_sha256':hashlib.sha256(raw).hexdigest(),'score':score,'score_hex':score.hex(),'actual_layer_decoded':decoded})
  return {'group':self.group,'receiver':self.receiver,'Redis_database':1,'group_key_hex':self.group_key.hex(),'channel_key_hex':self.key.hex(),'mailbox_type':kind,'mailbox_pttl':client.pttl(self.key),'frames':frames,'receive_pop_or_ack_calls':0,'unknown_or_setup_frames_consumed':False,'scope':'owned non-consuming actual updates-group receiver; not websocket/browser delivery or default graph acceptance'}
 def discard(self):
  if self.registered:self.async_to_sync(self.layer.group_discard)(self.group,self.receiver)
  return {'own_membership_discarded':self.registered,'frames_consumed':0,'mailbox_deleted_by_fixture':False}
 def close_clients(self):
  for client in self.clients.values():client.close()
