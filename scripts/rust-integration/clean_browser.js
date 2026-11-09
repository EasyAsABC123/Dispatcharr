// Actual unchanged React first-admin/login/restart proof using local Chromium.
// Original Dispatcharr authors retain application credit. Credentials/tokens
// enter through stdin and are never printed, persisted or placed in argv.
const {spawn}=require('node:child_process');
const fs=require('node:fs'),os=require('node:os'),path=require('node:path'),readline=require('node:readline');
const directory=fs.mkdtempSync(path.join(os.tmpdir(),'dispatcharr-clean-browser-'));
let child,cdp,next=1,stage='startup';const pending=new Map(),responses=[],socketTypes=new Set();let exceptions=0;
const required=['/api/accounts/users/me/','/api/core/settings/','/api/core/settings/env/','/api/channels/groups/','/api/channels/profiles/','/api/m3u/accounts/','/api/epg/sources/','/api/epg/epgdata/','/api/core/streamprofiles/','/api/core/outputprofiles/','/api/core/useragents/','/api/m3u/server-groups/','/api/channels/channels/ids/','/api/accounts/users/'];
const delay=ms=>new Promise(resolve=>setTimeout(resolve,ms));
function call(method,params={}){const id=next++;return new Promise((resolve,reject)=>{const timer=setTimeout(()=>{pending.delete(id);reject(new Error('CDP deadline'))},10000);pending.set(id,{resolve:value=>{clearTimeout(timer);resolve(value)},reject});cdp.send(JSON.stringify({id,method,params}));});}
async function evaluate(expression){const result=await call('Runtime.evaluate',{expression,returnByValue:true,awaitPromise:true});if(result.exceptionDetails)throw new Error('Browser expression failed');return result.result.value;}
async function until(expression,seconds=30){const end=Date.now()+seconds*1000;while(Date.now()<end){if(await evaluate(expression))return true;await delay(50)}throw new Error('Browser condition deadline');}
async function input(name,value){await evaluate(`document.querySelector('input[name="${name}"]').focus();true`);await call('Input.insertText',{text:value});}
async function start(){
 child=spawn('/usr/bin/chromium',['--headless','--no-sandbox','--disable-gpu','--disable-background-networking','--disable-component-update','--disable-default-apps','--disable-sync','--metrics-recording-only','--no-first-run','--no-default-browser-check','--disable-breakpad','--remote-debugging-address=127.0.0.1','--remote-debugging-port=0','--user-data-dir='+directory,'about:blank'],{detached:true,stdio:['ignore','ignore','pipe']});
 const endpoint=await new Promise((resolve,reject)=>{let text='';const timer=setTimeout(()=>reject(new Error('Chromium startup deadline')),10000);child.stderr.on('data',chunk=>{text=(text+chunk.toString()).slice(-16384);const match=text.match(/DevTools listening on (ws:\/\/[^\s]+)/);if(match){clearTimeout(timer);resolve(match[1])}});child.on('error',reject)});
 const origin=new URL(endpoint.replace('ws:','http:')).origin;const target=await(await fetch(origin+'/json/new?about:blank',{method:'PUT'})).json();cdp=new WebSocket(target.webSocketDebuggerUrl);await new Promise((resolve,reject)=>{cdp.onopen=resolve;cdp.onerror=reject});
 cdp.onmessage=event=>{const message=JSON.parse(event.data);const request=pending.get(message.id);if(request){pending.delete(message.id);if(message.error)request.reject(new Error('CDP request failed'));else request.resolve(message.result)}
  if(message.method==='Runtime.exceptionThrown')exceptions++;
  if(message.method==='Network.responseReceived'){const r=message.params.response;try{const p=new URL(r.url).pathname;if(p.startsWith('/api/'))responses.push({path:p,status:r.status,stage})}catch{}}
  if(message.method==='Network.webSocketFrameReceived'){try{const value=JSON.parse(message.params.response.payloadData);if(typeof value.type==='string')socketTypes.add(value.type)}catch{}}
 };
 await call('Runtime.enable');await call('Page.enable');await call('Network.enable');const version=await call('Browser.getVersion');return {ready:true,browser:version.product,protocol:version.protocolVersion,browser_pid:child.pid};
}
async function authenticated(config){
 await until(`!!localStorage.getItem('accessToken')&&!document.querySelector('input[name="username"]')&&document.body.innerText.includes('Channels')`);
 const deadline=Date.now()+30000;while(Date.now()<deadline&&!required.every(path=>responses.some(r=>r.path===path&&r.status===200)))await delay(50);
 if(!required.every(path=>responses.some(r=>r.path===path&&r.status===200)))throw new Error('Missing initialization response');
 await delay(500);
 const token=await evaluate(`(async()=>{const parse=value=>JSON.parse(atob(value.replaceAll('-','+').replaceAll('_','/')));const decode=value=>Uint8Array.from(atob(value.replaceAll('-','+').replaceAll('_','/')),v=>v.charCodeAt(0));const key=await crypto.subtle.importKey('raw',new TextEncoder().encode(${JSON.stringify(config.secret)}),{name:'HMAC',hash:'SHA-256'},false,['verify']);const out={};for(const [name,life]of [['access',1800],['refresh',86400]]){const raw=localStorage.getItem(name+'Token');if(!raw)throw Error('Missing stored token');const parts=raw.split('.'),header=parse(parts[0]),claims=parse(parts[1]);if(header.alg!=='HS256'||claims.token_type!==name||claims.user_id!=='1'||claims.exp-claims.iat!==life||claims.exp<=Date.now()/1000||!await crypto.subtle.verify('HMAC',key,decode(parts[2]),new TextEncoder().encode(parts[0]+'.'+parts[1])))throw Error('Invalid stored token');out[name]=true}return out})()`);
 const failures=responses.filter(r=>r.status>=400&&!(r.path==='/api/accounts/auth/proxy-login/'&&r.status===401));
 return {authenticated:true,required_api_paths:required.length,required_api_paths_passed:required.filter(path=>responses.some(r=>r.path===path&&r.status===200)).length,api_responses:responses.length,failed_api_responses:failures,exceptions,stored_tokens_verified:token,socket_types:[...socketTypes].sort()};
}
async function command(config){
 if(config.op==='setup'){
  stage='first-admin-form';await call('Page.navigate',{url:config.url});await until(`document.body.innerText.includes('Create your Super User Account')&&!!document.querySelector('input[name="username"]')`);
  await input('username',config.username);await input('password',config.password);await input('email',config.email);await evaluate(`document.querySelector('form').requestSubmit();true`);
  stage='login-form';await until(`!document.body.innerText.includes('Create your Super User Account')&&!!document.querySelector('input[name="username"]')&&document.body.innerText.includes('Login')`);
  await input('username',config.username);await input('password',config.password);await evaluate(`document.querySelector('form').requestSubmit();true`);
  stage='initialization';return await authenticated(config);
 }
 if(config.op==='reload') {stage='restart-reload';responses.length=0;await call('Page.reload',{ignoreCache:true});return await authenticated(config);}
 if(config.op==='screenshot'){const value=await call('Page.captureScreenshot',{format:'png'});fs.writeFileSync(config.path,Buffer.from(value.data,'base64'));return {saved:true};}
 if(config.op==='close')return {closed:true};throw new Error('Unknown browser command');
}
(async()=>{try{process.stdout.write(JSON.stringify(await start())+'\n');for await(const line of readline.createInterface({input:process.stdin})){const config=JSON.parse(line);let response;try{response=await command(config)}catch(error){response={failed:true,stage,error_type:error.name,observed_api_paths:responses,missing_required_api_paths:required.filter(path=>!responses.some(r=>r.path===path&&r.status===200)),exceptions};process.exitCode=1}process.stdout.write(JSON.stringify(response)+'\n');if(config.op==='close'||response.failed)break}}
 catch(error){process.stdout.write(JSON.stringify({failed:true,stage,error_type:error.name})+'\n');process.exitCode=1}
 finally{if(cdp)cdp.close();if(child){try{process.kill(-child.pid,'SIGTERM')}catch{}await delay(200);try{process.kill(-child.pid,'SIGKILL')}catch{}}fs.rmSync(directory,{recursive:true,force:true})}
})();
