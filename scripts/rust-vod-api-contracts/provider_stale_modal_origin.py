"""Owned synthetic provider and artwork; no forwarding, playback or task patching.
Original Dispatcharr authors retain application/algorithm credit.
"""
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs,urlsplit
import json,sys,threading
counts=Path(sys.argv[1]);lock=threading.Lock()
class Origin(BaseHTTPRequestHandler):
 def do_GET(self):
  url=urlsplit(self.path);q=parse_qs(url.query);action=q.get('action',['authenticate'])[0];suffix='a'if q.get('username',[''])[0].endswith('-a')else'b'
  art=url.path.startswith('/art/')and url.path.endswith('.svg');allowed=art or(url.path=='/player_api.php'and action in ['authenticate','get_vod_info','get_series_info'])
  if art:
   body=('<svg xmlns="http://www.w3.org/2000/svg" width="2" height="3"><rect width="2" height="3" fill="'+('red'if'-a.'in url.path else'blue')+'"/></svg>').encode();mime='image/svg+xml'
  else:
   origin='http://'+self.headers['Host'];plot='Owned refreshed '+suffix+' '
   if action=='authenticate':value={'user_info':{'auth':1,'status':'Active'}}
   elif action=='get_vod_info':value={'info':{'plot':plot+'movie details','rating':'8.4','genre':'Drama','movie_image':origin+'/art/movie-'+suffix+'.svg'},'movie_data':{'container_extension':'mp4'}}
   else:
    start=101 if suffix=='a'else 201
    value={'info':{'plot':plot+'series details','rating':'8.5','genre':'Drama'},'episodes':{'1':[{'id':'owned-refreshed-'+suffix+'-'+str(n),'title':'Owned refreshed '+suffix+' Episode '+str(n),'episode_num':start+n,'container_extension':'mp4','info':{'plot':plot+'episode details','movie_image':origin+'/art/episode-'+suffix+'.svg'}}for n in [1,2]]}}
   body=json.dumps(value).encode();mime='application/json'
  if not allowed:body=b'Forbidden owned-origin path';mime='text/plain'
  with lock:
   rows=json.loads(counts.read_text());rows.append({'path':url.path,'action':action if not art else'art','provider':suffix if not art else None,'allowed':allowed,'method':self.command,'raw_target':self.path,'headers':{k.lower():v for k,v in self.headers.items()}});counts.write_text(json.dumps(rows))
  self.send_response(200 if allowed else 403);self.send_header('Content-Type',mime);self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
 def log_message(self,*_args):pass
ThreadingHTTPServer(('0.0.0.0',18881),Origin).serve_forever()
