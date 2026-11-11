// Actual unchanged React VOD default Both/All workflow using local Chromium.
// Original Dispatcharr authors retain application credit. Credentials/tokens
// enter through stdin and are never printed, persisted or placed in argv.
const {spawn}=require('node:child_process');
const fs=require('node:fs'),os=require('node:os'),path=require('node:path'),readline=require('node:readline');
const directory=fs.mkdtempSync(path.join(os.tmpdir(),'dispatcharr-clean-browser-'));
let child,cdp,next=1,stage='startup';const pending=new Map(),responses=[],assets=[],socketTypes=new Set();let exceptions=0;const drfAssets=[];
const required=['/api/accounts/users/me/','/api/core/settings/','/api/core/settings/env/','/api/channels/groups/','/api/channels/profiles/','/api/m3u/accounts/','/api/epg/sources/','/api/epg/epgdata/','/api/core/streamprofiles/','/api/core/outputprofiles/','/api/core/useragents/','/api/m3u/server-groups/','/api/channels/channels/ids/','/api/accounts/users/','/api/channels/logos/','/api/channels/streams/filter-options/'];
const delay=ms=>new Promise(resolve=>setTimeout(resolve,ms));
function call(method,params={},sessionId){const id=next++;return new Promise((resolve,reject)=>{const timer=setTimeout(()=>{pending.delete(id);reject(new Error('CDP deadline'))},10000);pending.set(id,{resolve:value=>{clearTimeout(timer);resolve(value)},reject});cdp.send(JSON.stringify({id,method,params,...(sessionId?{sessionId}:{})}));});}
async function evaluate(expression){const result=await call('Runtime.evaluate',{expression,returnByValue:true,awaitPromise:true});if(result.exceptionDetails)throw new Error('Browser expression failed');return result.result.value;}
async function until(expression,seconds=30){const end=Date.now()+seconds*1000;while(Date.now()<end){if(await evaluate(expression))return true;await delay(50)}throw new Error('Browser condition deadline');}
async function input(name,value){await evaluate(`document.querySelector('input[name="${name}"]').focus();true`);await call('Input.insertText',{text:value});}
async function start(){
 child=spawn(process.env.RUST_BROWSER_CHROMIUM||'chromium',['--headless','--autoplay-policy=no-user-gesture-required','--no-sandbox','--disable-gpu','--disable-background-networking','--disable-component-update','--disable-default-apps','--disable-sync','--metrics-recording-only','--no-first-run','--no-default-browser-check','--disable-breakpad','--remote-debugging-address=127.0.0.1','--remote-debugging-port=0','--user-data-dir='+directory,'about:blank'],{detached:true,stdio:['ignore','ignore','pipe']});
 const endpoint=await new Promise((resolve,reject)=>{let text='';const timer=setTimeout(()=>reject(new Error('Chromium startup deadline')),10000);child.stderr.on('data',chunk=>{text=(text+chunk.toString()).slice(-16384);const match=text.match(/DevTools listening on (ws:\/\/[^\s]+)/);if(match){clearTimeout(timer);resolve(match[1])}});child.on('error',reject)});
 const origin=new URL(endpoint.replace('ws:','http:')).origin;const target=await(await fetch(origin+'/json/new?about:blank',{method:'PUT'})).json();cdp=new WebSocket(target.webSocketDebuggerUrl);await new Promise((resolve,reject)=>{cdp.onopen=resolve;cdp.onerror=reject});
 cdp.onmessage=event=>{const message=JSON.parse(event.data);const request=pending.get(message.id);if(request){pending.delete(message.id);if(message.error)request.reject(new Error('CDP request failed'));else request.resolve(message.result)}
  if(message.method==='Target.attachedToTarget'){void(async()=>{try{await call('Network.enable',{},message.params.sessionId);await call('Runtime.runIfWaitingForDebugger',{},message.params.sessionId)}catch{exceptions++}})();}
  if(message.method==='Runtime.exceptionThrown')exceptions++;
  if(message.method==='Network.responseReceived'){const r=message.params.response;try{const p=new URL(r.url).pathname;if(p.startsWith('/assets/')&&assets.length<32)assets.push({path:p,status:r.status,mime:r.mimeType});if(p.startsWith('/static/rest_framework/')&&drfAssets.length<64)drfAssets.push({path:p,status:r.status,mime:r.mimeType});if(p.startsWith('/api/')||p.startsWith('/proxy/'))responses.push({path:p,status:r.status,request_id:message.params.requestId,worker:!!message.sessionId,stage})}catch{}}
  if(message.method==='Network.webSocketFrameReceived'){try{const value=JSON.parse(message.params.response.payloadData);if(typeof value.type==='string')socketTypes.add(value.type)}catch{}}
 };
 await call('Runtime.enable');await call('Page.enable');await call('Network.enable');await call('Target.setAutoAttach',{autoAttach:true,waitForDebuggerOnStart:true,flatten:true});const version=await call('Browser.getVersion');return {ready:true,browser:version.product,protocol:version.protocolVersion,browser_pid:child.pid};
}
async function workflow(config){
 const base=new URL(config.url);if(base.protocol!=='http:'||!['127.0.0.1','localhost'].includes(base.hostname))throw Error('Owned loopback target required');
 stage='normal-login';await call('Page.navigate',{url:config.url+'/channels'});await until(`!!document.querySelector('input[name="username"]')&&!!document.querySelector('input[name="password"]')`);
 await input('username',config.username);await input('password',config.password);await evaluate(`document.querySelector('form').requestSubmit();true`);await until(`!!localStorage.getItem('accessToken')&&!document.querySelector('input[name="username"]')`);
 stage='channels';await call('Page.navigate',{url:config.url+'/channels'});await until(`!!document.querySelector('button[aria-label="Preview channel"]')`);
 const expected=config.channel_name;
 await until(`Array.from(document.querySelectorAll('.tr')).some(r=>r.innerText.includes(${JSON.stringify(expected)})&&r.querySelector('button[aria-label="Preview channel"]'))`);
 stage='play';await evaluate(`Array.from(document.querySelectorAll('.tr')).find(r=>r.innerText.includes(${JSON.stringify(expected)})).querySelector('button[aria-label="Preview channel"]').click();true`);
 await until(`!!document.querySelector('video')&&document.querySelector('video').videoWidth>0&&document.querySelector('video').getVideoPlaybackQuality().totalVideoFrames>=25`,60);
 const first=await evaluate(`(()=>{const v=document.querySelector('video');return {time:v.currentTime,frames:v.getVideoPlaybackQuality().totalVideoFrames,width:v.videoWidth,height:v.videoHeight,error:v.error?.code??null}})()`);
 await delay(2000);const second=await evaluate(`(()=>{const v=document.querySelector('video');return {time:v.currentTime,frames:v.getVideoPlaybackQuality().totalVideoFrames,width:v.videoWidth,height:v.videoHeight,error:v.error?.code??null}})()`);
 if(second.time<=first.time||second.frames<=first.frames||second.error)throw Error('Actual media did not advance');
 stage='stats';await until(`!!document.querySelector('a[href="/stats"]')`);await evaluate(`document.querySelector('a[href="/stats"]').click();true`);
 await until(`location.pathname==='/stats'&&document.body.innerText.includes(${JSON.stringify(expected)})`,40);
 const end=Date.now()+20000;let stats=null;
 while(Date.now()<end&&!stats){const row=[...responses].reverse().find(r=>r.path==='/proxy/stats/'&&r.status===200);if(row){try{const raw=await call('Network.getResponseBody',{requestId:row.request_id});const data=JSON.parse(raw.base64Encoded?Buffer.from(raw.body,'base64').toString():raw.body);if(JSON.stringify(data.live).includes(config.channel_uuid))stats={status:row.status,live_channel_identity_present:true,sections:Object.keys(data).sort()};}catch{}}await delay(100)}
 if(!stats)throw Error('Normal React combined stats did not contain active channel');
 stage='close-player';await evaluate(`(()=>{const v=document.querySelector('video');let p=v;for(let i=0;i<8&&p;i++,p=p.parentElement){const b=p.querySelector('button.mantine-CloseButton-root');if(b){b.click();return true}}throw Error('Player close button absent')})()`);await until(`!document.querySelector('video')`);
 const stream=responses.filter(r=>r.path==='/proxy/ts/stream/'+config.channel_uuid);if(!stream.length||stream.some(r=>r.status!==200))throw Error('Normal player stream HTTP failed');
 return {passed:exceptions===0,normal_login:true,preview_clicked:true,actual_media_first:first,actual_media_second:second,stats,player_closed:true,stream_HTTP:stream.map(r=>({path:r.path,status:r.status,worker:r.worker})),uncaught_exceptions:exceptions,API_responses:responses.map(r=>({path:r.path,status:r.status,stage:r.stage})),scope:'Actual unchanged React login, channel preview, decoded advancing video, active statistics and player close; no resource measurement'};
}
(async()=>{try{process.stdout.write(JSON.stringify(await start())+'\n');for await(const line of readline.createInterface({input:process.stdin})){const config=JSON.parse(line);try{process.stdout.write(JSON.stringify(await workflow(config))+'\n')}catch(error){let document=null;try{document=await evaluate(`({path:location.pathname,text:document.body.innerText.slice(0,1500),video:Array.from(document.querySelectorAll('video')).map(v=>({time:v.currentTime,width:v.videoWidth,frames:v.getVideoPlaybackQuality().totalVideoFrames,error:v.error?.code??null}))})`)}catch{}process.stdout.write(JSON.stringify({passed:false,stage,error_type:error.name,failure_message:error.message,document,exceptions,API_responses:responses.map(r=>({path:r.path,status:r.status,stage:r.stage}))})+'\n');process.exitCode=1}break}}
catch(error){process.stdout.write(JSON.stringify({passed:false,stage,error_type:error.name})+'\n');process.exitCode=1}
finally{if(cdp)cdp.close();if(child){try{process.kill(-child.pid,'SIGTERM')}catch{}await delay(200);try{process.kill(-child.pid,'SIGKILL')}catch{}}fs.rmSync(directory,{recursive:true,force:true})}})();
