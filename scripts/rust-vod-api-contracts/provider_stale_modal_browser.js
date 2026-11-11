// Actual unchanged React VOD default Both/All workflow using local Chromium.
// Original Dispatcharr authors retain application credit. Credentials/tokens
// enter through stdin and are never printed, persisted or placed in argv.
const {spawn}=require('node:child_process');
const fs=require('node:fs'),os=require('node:os'),path=require('node:path'),readline=require('node:readline');
const directory=fs.mkdtempSync(path.join(os.tmpdir(),'dispatcharr-clean-browser-'));
let child,cdp,next=1,stage='startup';const pending=new Map(),responses=[],assets=[],socketTypes=new Set();let exceptions=0;const drfAssets=[];
const required=['/api/accounts/users/me/','/api/core/settings/','/api/core/settings/env/','/api/channels/groups/','/api/channels/profiles/','/api/m3u/accounts/','/api/epg/sources/','/api/epg/epgdata/','/api/core/streamprofiles/','/api/core/outputprofiles/','/api/core/useragents/','/api/m3u/server-groups/','/api/channels/channels/ids/','/api/accounts/users/','/api/channels/logos/','/api/channels/streams/filter-options/'];
const delay=ms=>new Promise(resolve=>setTimeout(resolve,ms));
function call(method,params={}){const id=next++;return new Promise((resolve,reject)=>{const timer=setTimeout(()=>{pending.delete(id);reject(new Error('CDP deadline'))},10000);pending.set(id,{resolve:value=>{clearTimeout(timer);resolve(value)},reject});cdp.send(JSON.stringify({id,method,params}));});}
async function evaluate(expression){const result=await call('Runtime.evaluate',{expression,returnByValue:true,awaitPromise:true});if(result.exceptionDetails)throw new Error('Browser expression failed');return result.result.value;}
async function until(expression,seconds=30){const end=Date.now()+seconds*1000;while(Date.now()<end){if(await evaluate(expression))return true;await delay(50)}throw new Error('Browser condition deadline');}
async function input(name,value){await evaluate(`document.querySelector('input[name="${name}"]').focus();true`);await call('Input.insertText',{text:value});}
async function start(){
 child=spawn(process.env.RUST_BROWSER_CHROMIUM||'chromium',['--headless','--no-sandbox','--disable-gpu','--disable-background-networking','--disable-component-update','--disable-default-apps','--disable-sync','--metrics-recording-only','--no-first-run','--no-default-browser-check','--disable-breakpad','--remote-debugging-address=127.0.0.1','--remote-debugging-port=0','--user-data-dir='+directory,'about:blank'],{detached:true,stdio:['ignore','ignore','pipe']});
 const endpoint=await new Promise((resolve,reject)=>{let text='';const timer=setTimeout(()=>reject(new Error('Chromium startup deadline')),10000);child.stderr.on('data',chunk=>{text=(text+chunk.toString()).slice(-16384);const match=text.match(/DevTools listening on (ws:\/\/[^\s]+)/);if(match){clearTimeout(timer);resolve(match[1])}});child.on('error',reject)});
 const origin=new URL(endpoint.replace('ws:','http:')).origin;const target=await(await fetch(origin+'/json/new?about:blank',{method:'PUT'})).json();cdp=new WebSocket(target.webSocketDebuggerUrl);await new Promise((resolve,reject)=>{cdp.onopen=resolve;cdp.onerror=reject});
 cdp.onmessage=event=>{const message=JSON.parse(event.data);const request=pending.get(message.id);if(request){pending.delete(message.id);if(message.error)request.reject(new Error('CDP request failed'));else request.resolve(message.result)}
  if(message.method==='Runtime.exceptionThrown')exceptions++;
  if(message.method==='Network.responseReceived'){const r=message.params.response;try{const p=new URL(r.url).pathname;if(p.startsWith('/assets/')&&assets.length<32)assets.push({path:p,status:r.status,mime:r.mimeType});if(p.startsWith('/static/rest_framework/')&&drfAssets.length<64)drfAssets.push({path:p,status:r.status,mime:r.mimeType});if(p.startsWith('/api/'))responses.push({path:p,query:new URL(r.url).search,status:r.status,request_id:message.params.requestId,stage})}catch{}}
  if(message.method==='Network.webSocketFrameReceived'){try{const value=JSON.parse(message.params.response.payloadData);if(typeof value.type==='string')socketTypes.add(value.type)}catch{}}
 };
 await call('Runtime.enable');await call('Page.enable');await call('Network.enable');const version=await call('Browser.getVersion');return {ready:true,browser:version.product,protocol:version.protocolVersion,browser_pid:child.pid};
}
async function workflow(config){
 const base=new URL(config.url);if(base.protocol!=='http:'||!['127.0.0.1','localhost'].includes(base.hostname))throw Error('Owned loopback target required');
 stage='normal-login';await call('Page.navigate',{url:config.url+'/vods'});await until(`!!document.querySelector('input[name="username"]')&&!!document.querySelector('input[name="password"]')`);
 await input('username',config.username);await input('password',config.password);await evaluate(`document.querySelector('form').requestSubmit();true`);await until(`!!localStorage.getItem('accessToken')&&!document.querySelector('input[name="username"]')`);
 await call('Page.navigate',{url:config.url+'/vods'});await until(`document.querySelectorAll('#vods-container .mantine-Card-root').length>0`);
 // Search through unchanged React, without direct authenticated fetches or mock data.
 const search=await evaluate(`!!document.querySelector('input[placeholder="Search VODs..."]')`);if(!search)throw Error('Search input absent');
 await evaluate(`document.querySelector('input[placeholder="Search VODs..."]').focus();true`);await call('Input.insertText',{text:'rust-probe-vod-rest-00 Modal'});
 const results=[];
 async function payload(path,predicate=()=>true){
  const end=Date.now()+20000;while(Date.now()<end){const row=[...responses].reverse().find(r=>r.stage===stage&&r.path===path&&r.status===200&&predicate(new URLSearchParams(r.query)));if(row){try{const raw=await call('Network.getResponseBody',{requestId:row.request_id});return {request:{path:row.path,query:row.query,status:row.status},body:JSON.parse(raw.base64Encoded?Buffer.from(raw.body,'base64').toString():raw.body)}}catch{}}await delay(50)}throw Error('Expected modal HTTP body absent: '+path);
 }
 for(const kind of ['movie','series']){
  const obj=config[kind],endpoint=kind==='movie'?'movies':'series';stage=kind+'-open';
  await until(`Array.from(document.querySelectorAll('#vods-container .mantine-Card-root')).some(c=>c.innerText.includes(${JSON.stringify(obj.name)}))`);
  await evaluate(`Array.from(document.querySelectorAll('#vods-container .mantine-Card-root')).find(c=>c.innerText.includes(${JSON.stringify(obj.name)})).click();true`);
  const path='/api/vod/'+endpoint+'/'+obj.id+'/';const providers=await payload(path+'providers/');const initial=await payload(path+'provider-info/',p=>!p.has('relation_id'));
  if(providers.body.length!==2)throw Error('Expected two active modal providers');
  const observations=[];
  for(const provider of providers.body){
   stage=kind+'-provider-'+provider.id;await until(`!!document.querySelector('[role="dialog"] input[placeholder="Select stream..."]')&&!document.querySelector('[role="dialog"] input[placeholder="Select stream..."]').disabled`);
   await evaluate(`document.querySelector('[role="dialog"] input[placeholder="Select stream..."]').click();true`);
   await until(`Array.from(document.querySelectorAll('[role="option"]')).some(o=>o.textContent.includes(${JSON.stringify(provider.m3u_account.name)}))`);
   // A selected option may not invoke onChange. First choose the other option,
   // then the requested one if the current input already names this provider.
   const selected=await evaluate(`document.querySelector('[role="dialog"] input[placeholder="Select stream..."]').value`);
   if(selected.includes(provider.m3u_account.name)){
    const other=providers.body.find(p=>p.id!==provider.id);await evaluate(`Array.from(document.querySelectorAll('[role="option"]')).find(o=>o.textContent.includes(${JSON.stringify(other.m3u_account.name)})).click();true`);
    await payload(path+'provider-info/',p=>p.get('relation_id')===String(other.id));
    await evaluate(`document.querySelector('[role="dialog"] input[placeholder="Select stream..."]').click();true`);
   }
   await until(`Array.from(document.querySelectorAll('[role="option"]')).some(o=>o.textContent.includes(${JSON.stringify(provider.m3u_account.name)}))`);
   await evaluate(`Array.from(document.querySelectorAll('[role="option"]')).find(o=>o.textContent.includes(${JSON.stringify(provider.m3u_account.name)})).click();true`);
   const info=await payload(path+'provider-info/',p=>p.get('relation_id')===String(provider.id));const refreshedDetails=kind==='series'?info.body.description:info.body.plot;if(typeof refreshedDetails!=='string'||!refreshedDetails.startsWith('Owned refreshed '))throw Error('Updated provider details missing');await until(`document.querySelector('[role="dialog"]').innerText.includes(${JSON.stringify(info.body.description||info.body.plot)})`);if(info.body.m3u_account.id!==provider.m3u_account.id)throw Error('Provider response account mismatch');
   await until(`(()=>{const d=document.querySelector('[role="dialog"]');return !!d&&d.innerText.includes(${JSON.stringify(obj.name)})&&Array.from(d.querySelectorAll('img')).some(i=>i.complete&&i.naturalWidth===2&&i.naturalHeight===3&&i.currentSrc.includes('/api/vod/')&&new URL(i.currentSrc).searchParams.get('m3u_account_id')===${JSON.stringify(String(provider.m3u_account.id))})})()`);
   let episodeNames=[];
   if(kind==='series'){
    const episodes=Object.values(info.body.episodes||{}).flat();episodeNames=episodes.map(e=>e.name);if(episodes.length!==2)throw Error('Provider-specific episode count mismatch');
    await until(`(()=>{const d=document.querySelector('[role="dialog"]');return ${JSON.stringify(episodeNames)}.every(n=>d.innerText.includes(n))&&${JSON.stringify(config.episodes.filter(e=>!episodeNames.includes(e.name)).map(e=>e.name))}.every(n=>!d.innerText.includes(n))})()`);
    await evaluate(`Array.from(document.querySelectorAll('[role="dialog"] tbody tr')).find(r=>r.innerText.includes(${JSON.stringify(episodeNames[0])})).click();true`);
    await until(`Array.from(document.querySelectorAll('[role="dialog"] img')).some(i=>i.alt===${JSON.stringify(episodeNames[0])}&&i.complete&&i.naturalWidth===2&&i.naturalHeight===3&&i.currentSrc.includes('/api/vod/episodes/'))`);
   }
   const decoded=await evaluate(`Promise.all(Array.from(document.querySelectorAll('[role="dialog"] img')).map(async i=>{await i.decode();return i.complete&&i.naturalWidth===2&&i.naturalHeight===3})).then(values=>values.length>0&&values.every(Boolean))`);if(!decoded)throw Error('Actual HTMLImageElement.decode failed');
   const images=await evaluate(`Array.from(document.querySelectorAll('[role="dialog"] img')).map(i=>({alt:i.alt,path:new URL(i.currentSrc||i.src,location.href).pathname,query:new URL(i.currentSrc||i.src,location.href).search,complete:i.complete,width:i.naturalWidth,height:i.naturalHeight}))`);
   observations.push({relation_id:provider.id,account_id:provider.m3u_account.id,request:info.request,refreshed_details:refreshedDetails,episode_names:episodeNames,decoded_images:images});
  }
  results.push({kind,id:obj.id,provider_order:providers.body.map(p=>p.id),initial_default_account:initial.body.m3u_account.id,UI_initial_provider:providers.body[0].id,observations});
  await evaluate(`document.querySelector('[role="dialog"] .mantine-Modal-close').click();true`);await until(`!document.querySelector('[role="dialog"]')`);
 }
 const forbidden=responses.filter(r=>/force_refresh=true|\/proxy\/|\/play\//.test(r.path+r.query));const failed=responses.filter(r=>r.path.startsWith('/api/vod/')&&r.status>=400);
 return {passed:results.length===2&&forbidden.length===0&&failed.length===0&&exceptions===0,scope:'Actual unchanged React normal login, both modal provider switches, provider-specific episodes and actual proxied SVG decoding; owned local provider refresh only, no playback/external provider',results,VOD_HTTP_failures:failed,forbidden_requests:forbidden,uncaught_exceptions:exceptions,API_responses:responses};
}
(async()=>{try{process.stdout.write(JSON.stringify(await start())+'\n');for await(const line of readline.createInterface({input:process.stdin})){const config=JSON.parse(line);let response;try{response=config.op==='close'?{closed:true}:await workflow(config)}catch(error){let document=null;try{document=await evaluate(`({path:location.pathname,title:document.title,text:document.body.innerText.slice(0,600),input_names:Array.from(document.querySelectorAll('input')).map(input=>input.name)})`)}catch{}response={passed:false,stage,error_type:error.name,failure_message:error.message,document,assets,API_responses:responses,VOD_responses:responses.filter(row=>row.path.startsWith('/api/vod/')),exceptions};process.exitCode=1}process.stdout.write(JSON.stringify(response)+'\n');break}}
 catch(error){process.stdout.write(JSON.stringify({passed:false,stage,error_type:error.name})+'\n');process.exitCode=1}
 finally{if(cdp)cdp.close();if(child){try{process.kill(-child.pid,'SIGTERM')}catch{}await delay(200);try{process.kill(-child.pid,'SIGKILL')}catch{}}fs.rmSync(directory,{recursive:true,force:true})}
})();
