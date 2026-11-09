"""Pinned original image-proxy IP policy, public classification data only.
Original Dispatcharr contributors retain application credit.
"""
import ipaddress as ip,json,sys
assert sys.version_info[:2]==(3,13)
def allowed(address):
 if address.is_loopback or address.is_unspecified or address.is_multicast or address.is_link_local:return False
 if address.is_private:return True
 return not address.is_reserved
def networks(constants):
 for key,value in vars(constants).items():
  if isinstance(value,(ip.IPv4Network,ip.IPv6Network)):yield value
  elif isinstance(value,list):
   for n in value:
    if isinstance(n,(ip.IPv4Network,ip.IPv6Network)):yield n
v4=list(networks(ip._IPv4Constants));v6=list(networks(ip._IPv6Constants))
result={};vectors=[]
for version,nets in [(4,v4),(6,v6)]:
 maximum=1<<(32 if version==4 else 128);bounds={0,1,maximum-1,maximum}
 for n in nets:bounds.update([int(n.network_address),int(n.broadcast_address)+1])
 if version==6:
  mapped=int(ip.IPv6Address('::ffff:0:0'))
  for n in v4:bounds.update([mapped+int(n.network_address),mapped+int(n.broadcast_address)+1])
  bounds.update([mapped,mapped+1,mapped+(1<<32)-1,mapped+(1<<32)])
 ordered=sorted(bounds);ranges=[];address=ip.IPv4Address if version==4 else ip.IPv6Address
 for lo,hi in zip(ordered,ordered[1:]):
  if lo==hi:continue
  samples=[lo,(lo+hi-1)//2,hi-1];status=allowed(address(lo))
  assert all(allowed(address(n))==status for n in samples)
  for n in samples:vectors.append([str(address(n)),status])
  if status:ranges.extend(str(n)for n in ip.summarize_address_range(address(lo),address(hi-1)))
 result[str(version)]=ranges
print(json.dumps({'runtime':sys.version.split()[0],'allow_private':True,'allow_loopback':False,'allowed':result,'vectors':vectors},separators=(',',':')))
