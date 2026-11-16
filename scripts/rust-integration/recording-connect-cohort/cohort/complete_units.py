"""Offline bounded TS/PES/access-unit boundary index. Never starts applications.
Original Dispatcharr/FFmpeg authors retain all application/codec credit.
Fixed H264 AnnexB with AUD + AAC ADTS profile; unsupported codec shapes fail.
"""
import argparse,hashlib,json,re
from fractions import Fraction
from pathlib import Path
PACKET=188
MAX_PES=4*1024*1024
MAX_RAW=512*1024*1024

def pts5(value):
 if len(value)!=5 or any(not value[i]&1 for i in(0,2,4)):raise ValueError('MalformedPTS')
 return ((value[0]>>1)&7)<<30 | value[1]<<22 | (value[2]>>1)<<15 | value[3]<<7 | value[4]>>1

def adts_frames(data):
 out=[];offset=0;rates=[96000,88200,64000,48000,44100,32000,24000,22050,16000,12000,11025,8000,7350]
 while offset<len(data):
  if len(data)-offset<7:raise ValueError('ADTSUnitCutInsideClosedPES')
  h=data[offset:offset+7]
  if h[0]!=255 or h[1]&246!=240:raise ValueError('ADTSBadSync')
  rate_index=h[2]>>2&15
  if rate_index>=len(rates)or rates[rate_index]!=48000 or h[6]&3 or h[2]>>6!=1 or ((h[2]&1)<<2|h[3]>>6)!=1:raise ValueError('UnsupportedAACUnitShape')
  size=(h[3]&3)<<11 | h[4]<<3 | h[5]>>5;header=7 if h[1]&1 else 9
  if size<header or offset+size>len(data):raise ValueError('ADTSUnitCutInsideClosedPES')
  out.append({'relative_bytes':[offset,offset+size],'sample_rate':48000,'samples':1024});offset+=size
 return out

def elementary(group,data):
 if len(data)<9 or data[:3]!=b'\0\0\1':raise ValueError('PESHeaderMalformed')
 declared=int.from_bytes(data[4:6],'big');header=9+data[8]
 if header>len(data):raise ValueError('PESHeaderCut')
 if data[7]>>6&3 not in(2,3):raise ValueError('PESMissingPTS')
 pts=pts5(data[9:14]);payload=data[header:]
 group.update(pts90k=pts,declared_PES_bytes=declared+6 if declared else None,elementary_bytes=len(payload),elementary_sha256=hashlib.sha256(payload).hexdigest())
 if 224<=data[3]<=239:
  starts=list(re.finditer(b'\x00\x00(?:\x00)?\x01',payload))
  nals=[payload[m.end():starts[i+1].start()if i+1<len(starts)else len(payload)].rstrip(b'\0')for i,m in enumerate(starts)]
  if not nals or any(not nal or nal[0]&128 for nal in nals):raise ValueError('InvalidH264NALHeader')
  kinds=[nal[0]&31 for nal in nals];vcl=[nal for nal in nals if nal[0]&31 in(1,5)]
  if len(vcl)!=1 or len(vcl[0])<2 or not vcl[0][1]&128:raise ValueError('H264SingleFirstSlicePerPESRequired')
  duplicate=False
  if kinds.count(9)==2:
   # Actual Source0027 VLC repeats identical header-only AUD/SPS/PPS before
   # the sole IDR. No bytes are removed and no second coded picture is allowed.
   if kinds not in ([9,7,8,9,7,8,5],[9,7,8,9,7,8,6,5]) or nals[:3]!=nals[3:6]:raise ValueError('H264UnprovenDuplicateHeaderPrefix')
   duplicate=True
  elif kinds.count(9)!=1 or kinds[0]!=9:raise ValueError('H264OneAUDPerPESRequired')
  group.update(kind='video',unit_count=1,random_access=5 in kinds,nal_types=kinds,duplicated_header_prefix=duplicate,units=[{'pts90k':pts,'elementary_bytes':len(payload)}])
 elif 192<=data[3]<=223:
  frames=adts_frames(payload)
  group.update(kind='audio',unit_count=len(frames),units=[{'pts90k':pts+i*1920,'elementary_bytes':v['relative_bytes'][1]-v['relative_bytes'][0]}for i,v in enumerate(frames)])
 else:raise ValueError('UnsupportedPESStreamID')

def _crc_table():
 out=[]
 for value in range(256):
  crc=value<<24
  for _ in range(8):crc=((crc<<1)^0x04c11db7)&0xffffffff if crc&0x80000000 else (crc<<1)&0xffffffff
  out.append(crc)
 return out
CRC_TABLE=_crc_table()
def psi_section(data,table):
 if not data or data[0]!=0:raise ValueError('FixedSinglePacketPSIPointerRequired')
 section=data[1:]
 if len(section)<12 or section[0]!=table or not section[1]&128:raise ValueError('PSITableHeader')
 length=3+((section[1]&15)<<8|section[2])
 if length>len(section) or length<12:raise ValueError('PSISectionLength')
 section=section[:length];crc=0xffffffff
 for byte in section:crc=((crc<<8)&0xffffffff)^CRC_TABLE[(crc>>24)^byte]
 if crc:raise ValueError('PSICRC')
 if not section[5]&1 or section[6:8]!=b'\0\0':raise ValueError('PSICurrentSingleSectionRequired')
 return section

class Index:
 def __init__(self):self.pmt_pid=None;self.program=None;self.expected_media=None;self.PSI={'PAT_count':0,'PMT_count':0};self.groups=[];self.errors=[];self.open={};self.cc={};self.prefix={};self.bytes=0;self.sha=hashlib.sha256();self.packet_count=0;self.pid_kinds={};self.seen=set();self.initial_discontinuities=[];self.error_count=0
 def error(self,value):
  self.error_count+=1
  if len(self.errors)<1024:self.errors.append(value)
 def psi(self,pid,data,pusi,pos):
  if not pusi:raise ValueError('FixedSinglePacketPSIRequired')
  if pid==0:
   section=psi_section(data,0);entries=section[8:-4]
   if len(entries)!=4:raise ValueError('SingleProgramPATRequired')
   program=int.from_bytes(entries[:2],'big');pmt=((entries[2]&31)<<8)|entries[3]
   if not program or not 16<pmt<8191:raise ValueError('InvalidProgramMapPID')
   if self.pmt_pid is not None and (self.pmt_pid!=pmt or self.program!=program):raise ValueError('InternalProgramMapChange')
   self.pmt_pid=pmt;self.program=program;self.PSI['PAT_count']+=1;self.PSI.setdefault('first_PAT',pos)
  else:
   section=psi_section(data,2)
   if int.from_bytes(section[3:5],'big')!=self.program:raise ValueError('PMTProgramMismatch')
   offset=12+((section[10]&15)<<8|section[11]);mapping={}
   while offset<len(section)-4:
    if offset+5>len(section)-4:raise ValueError('PMTStreamHeaderCut')
    kind=section[offset];stream=((section[offset+1]&31)<<8)|section[offset+2];length=((section[offset+3]&15)<<8)|section[offset+4]
    if kind not in(0x1b,0x0f)or stream in mapping:raise ValueError('ExactH264AACPMTRequired')
    mapping[stream]='video'if kind==0x1b else'audio';offset+=5+length
   if offset!=len(section)-4 or len(mapping)!=2 or set(mapping.values())!={'video','audio'}:raise ValueError('ExactVideoAudioProgramRequired')
   if self.expected_media is not None and mapping!=self.expected_media:raise ValueError('InternalMediaPIDChange')
   self.expected_media=mapping;self.PSI['PMT_count']+=1;self.PSI.setdefault('first_PMT',pos)
 def finish(self,pid,next_start=None):
  entry=self.open.pop(pid);data=bytes(entry.pop('_data'));declared=int.from_bytes(data[4:6],'big')+6 if len(data)>=6 and data[:3]==b'\0\0\1'and int.from_bytes(data[4:6],'big')else None
  complete=(declared is not None and len(data)>=declared)or next_start is not None
  entry.update(complete=complete,closure='declared_PES_length'if declared and len(data)>=declared else'next_same_PID_PUSI'if next_start is not None else'raw_EOF_open_PES',closure_proof_end=entry['last_packet_end']if declared and len(data)>=declared else next_start+188 if next_start is not None else None,next_same_PID_start=next_start,received_PES_bytes=len(data))
  if declared and len(data)<declared:entry['complete']=False;entry['closure']='declared_length_missing'
  if declared and len(data)>declared:
   if any(v!=255 for v in data[declared:]):self.error({'type':'PESExtraPayload','offset':entry['start'],'pid':pid})
   data=data[:declared]
  if entry['complete']:
   try:
    elementary(entry,data)
    kind=entry['kind']
    if self.expected_media is not None and self.expected_media.get(pid)!=kind:raise ValueError('PMTCodecMismatch')
    previous=self.pid_kinds.setdefault(pid,kind)
    if previous!=kind:raise ValueError('PIDCodecChanged')
   except ValueError as error:self.error({'type':str(error),'offset':entry['start'],'pid':pid})
  else:
   if len(data)>=14 and data[:3]==b'\0\0\1':
    try:entry['pts90k']=pts5(data[9:14])
    except ValueError:pass
  if len(self.groups)>=32768:raise ValueError('PES32768Budget')
  self.groups.append(entry)
 def packet(self,packet,pos):
  if packet[0]!=71:raise ValueError('TSSyncLoss')
  pid=(packet[1]&31)<<8|packet[2];pusi=bool(packet[1]&64);afc=packet[3]>>4&3;cc=packet[3]&15
  if not afc:raise ValueError('InvalidAdaptationControl')
  if packet[1]&128:raise ValueError('TransportErrorIndicator')
  if packet[3]&192:raise ValueError('ScrambledTransportUnsupported')
  offset=4
  if afc&2:
   size=packet[4];offset=5+size
   if offset>188:raise ValueError('AdaptationOverrun')
   if size and packet[5]&128:
    if pid in self.seen:raise ValueError('InternalTransportDiscontinuity')
    self.initial_discontinuities.append({'pid':pid,'offset':pos})
  if not afc&1 or offset>=188:return
  data=packet[offset:];self.seen.add(pid)
  if pid==0 or pid==self.pmt_pid:self.psi(pid,data,pusi,pos);return
  # The fixed profile's media PIDs are discovered from actual PES starts. No
  # assumed numeric PID or PTS sort is used to manufacture transport closure.
  media=pusi and data[:3]==b'\0\0\1'and len(data)>=4 and 192<=data[3]<=239
  if media or pid in self.open:
   if pid in self.cc and cc!=(self.cc[pid]+1)%16:self.error({'type':'MediaContinuityCounter','pid':pid,'offset':pos,'previous':self.cc[pid],'actual':cc})
   self.cc[pid]=cc
   if pusi:
    if not media:raise ValueError('MediaPIDUnexpectedPayloadStart')
    if pid in self.open:self.finish(pid,pos)
    if pid not in self.open and len(self.open)>=2:raise ValueError('MoreThanTwoMediaPIDs')
    self.open[pid]={'pid':pid,'start':pos,'last_packet_end':pos+188,'_data':bytearray()}
   entry=self.open[pid];entry['_data'].extend(data);entry['last_packet_end']=pos+188
   if len(entry['_data'])>MAX_PES:raise ValueError('PES4MiBBudget')
  elif pid not in(0,17,4096,8191):self.prefix[pid]=self.prefix.get(pid,0)+1
 def read(self,path,limit=None):
  if Path(path).stat().st_size>MAX_RAW:raise ValueError('Raw512MiBBudget')
  if limit is not None and not 0<limit<=Path(path).stat().st_size:raise ValueError('InvalidDrainPrefix')
  with Path(path).open('rb')as source:
   while True:
    remaining=188 if limit is None else min(188,max(0,limit-self.bytes))
    if not remaining:break
    packet=source.read(remaining)
    if not packet:break
    self.sha.update(packet)
    if len(packet)!=188:self.error({'type':'TerminalPartialTSPacket','offset':self.bytes,'bytes':len(packet)});self.bytes+=len(packet);break
    try:self.packet(packet,self.bytes)
    except ValueError as error:
     if str(error).endswith('Budget'):raise
     self.error({'type':str(error),'offset':self.bytes})
    self.bytes+=188;self.packet_count+=1
  for pid in list(self.open):self.finish(pid)
  self.groups.sort(key=lambda row:row['start'])
  return {'format':'complete-PES-AUD-ADTS-v1','raw_bytes':self.bytes,'raw_sha256':self.sha.hexdigest(),'TS_packets':self.packet_count,'pid_kinds':self.pid_kinds,'verified_PSI':dict(self.PSI,PMT_PID=self.pmt_pid,media=self.expected_media),'initial_discontinuity_markers':self.initial_discontinuities,'prefix_unassigned_packets':self.prefix,'errors':self.errors,'error_count':self.error_count,'groups':self.groups}

def window(index,start,end):
 if not 0<=start<end<=index['raw_bytes']:raise ValueError('InvalidFixedByteWindow')
 selected=[g for g in index['groups']if g['complete']and g['start']>=start and g['last_packet_end']<=end and g['closure_proof_end']<=end]
 boundary=[g for g in index['groups']if g['start']<end and g['last_packet_end']>start and g not in selected]
 internal_errors=[e for e in index['errors']if start<=e['offset']<end]
 # Drain is evidence of closure, not useful measured work. Even an excluded
 # boundary PES that began before W1 must close; otherwise internal damage
 # cannot be distinguished from a capture cut by looking at EOF alone.
 unclosed=[g for g in index['groups']if g['start']<end and g['last_packet_end']>start and not g['complete']]
 return {'byte_window':[start,end],'complete_groups':selected,'boundary_groups':boundary,'unclosed_touching_window':unclosed,'closure_proven':not unclosed,'internal_transport_errors':internal_errors,'streams':{kind:{'complete_groups':sum(g.get('kind')==kind for g in selected),'complete_units':sum(g.get('unit_count',0)for g in selected if g.get('kind')==kind),'elementary_bytes':sum(g.get('elementary_bytes',0)for g in selected if g.get('kind')==kind)}for kind in('video','audio')}}

def main():
 p=argparse.ArgumentParser();p.add_argument('capture',type=Path);p.add_argument('--output',type=Path,required=True);p.add_argument('--start',type=int);p.add_argument('--end',type=int);a=p.parse_args();result=Index().read(a.capture)
 if a.start is not None:result['window']=window(result,a.start,a.end)
 a.output.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({'bytes':result['raw_bytes'],'groups':len(result['groups']),'errors':len(result['errors']),'sha256':result['raw_sha256']}))
if __name__=='__main__':main()
