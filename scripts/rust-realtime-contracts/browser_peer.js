// Real Chromium WebSocket onmessage proof; original Dispatcharr credit retained.
// Capability URLs enter over stdin and are never printed or put in argv/files.
const {spawn}=require('node:child_process');
const fs=require('node:fs');
const os=require('node:os');
const path=require('node:path');
const readline=require('node:readline');
const directory=fs.mkdtempSync(path.join(os.tmpdir(),'dispatcharr-browser-'));
let child,cdp;
const pending=new Map();let next=1;
function delay(ms){return new Promise(resolve=>setTimeout(resolve,ms));}
function call(method,params={}){
  const id=next++;
  return new Promise((resolve,reject)=>{
    const timer=setTimeout(()=>{pending.delete(id);reject(new Error('CDP deadline'));},5000);
    pending.set(id,{resolve:value=>{clearTimeout(timer);resolve(value);},reject});
    cdp.send(JSON.stringify({id,method,params}));
  });
}
async function evaluate(expression){
  const result=await call('Runtime.evaluate',{expression,returnByValue:true});
  if(result.exceptionDetails)throw new Error('Browser expression failed');
  return result.result.value;
}
async function until(expression){
  const deadline=Date.now()+5000;
  while(Date.now()<deadline){const value=await evaluate(expression);if(value)return value;await delay(25);}
  throw new Error('Browser condition deadline');
}
async function start(){
  child=spawn('/usr/bin/chromium',['--headless','--no-sandbox','--disable-gpu','--disable-background-networking',
    '--disable-component-update','--disable-default-apps','--disable-sync','--metrics-recording-only','--no-first-run',
    '--no-default-browser-check','--disable-breakpad','--remote-debugging-address=127.0.0.1','--remote-debugging-port=0',
    '--user-data-dir='+directory,'about:blank'],{detached:true,stdio:['ignore','ignore','pipe']});
  const endpoint=await new Promise((resolve,reject)=>{
    let text='';const timer=setTimeout(()=>reject(new Error('Chromium startup deadline')),8000);
    child.stderr.on('data',chunk=>{text=(text+chunk.toString()).slice(-16384);const match=text.match(/DevTools listening on (ws:\/\/[^\s]+)/);if(match){clearTimeout(timer);resolve(match[1]);}});
    child.on('error',reject);
  });
  const origin=new URL(endpoint.replace('ws:','http:')).origin;
  const target=await (await fetch(origin+'/json/new?about:blank',{method:'PUT'})).json();
  cdp=new WebSocket(target.webSocketDebuggerUrl);
  await new Promise((resolve,reject)=>{cdp.onopen=resolve;cdp.onerror=reject;});
  cdp.onmessage=event=>{const message=JSON.parse(event.data);const request=pending.get(message.id);if(request){pending.delete(message.id);if(message.error)request.reject(new Error('CDP request failed'));else request.resolve(message.result);}};
  const version=await call('Browser.getVersion');
  return {ready:true,browser:version.product,protocol:version.protocolVersion};
}
async function command(request){
  if(request.op==='connect'){
    await evaluate(`globalThis.fixture={initial:null,large:null,messages:0,closed:null};
      fixture.socket=new WebSocket(${JSON.stringify(request.url)});
      fixture.socket.onmessage=event=>{const data=JSON.parse(event.data);fixture.messages++;
        if(data.type==='connection_established')fixture.initial=data;
        else if(data.data&&data.data.type==='fixture_large')fixture.large={messages:(fixture.large?.messages||0)+1,
          wireCharacters:event.data.length,valueCharacters:data.data.value.length,
          firstCode:data.data.value.charCodeAt(0),lastCode:data.data.value.charCodeAt(data.data.value.length-1)};};
      fixture.socket.onclose=event=>{fixture.closed={code:event.code,clean:event.wasClean};};true;`);
    return {initial:await until('fixture.initial')};
  }
  if(request.op==='observe')return {large:await until('fixture.large')};
  if(request.op==='disconnect'){
    await evaluate('fixture.socket.close(1000);true');return {closed:await until('fixture.closed')};
  }
  if(request.op==='close')return {closed:true};
  throw new Error('Unknown browser command');
}
(async()=>{
  try{
    console.log(JSON.stringify(await start()));
    for await(const line of readline.createInterface({input:process.stdin,crlfDelay:Infinity})){
      if(line.length>16384)throw new Error('Browser control bound');
      const request=JSON.parse(line);console.log(JSON.stringify(await command(request)));
      if(request.op==='close')break;
    }
  }catch(error){process.stderr.write(error.name+': '+error.message+'\n');process.exitCode=1;}
  finally{
    if(cdp)cdp.close();
    if(child){try{process.kill(-child.pid,'SIGTERM');}catch{}await delay(200);try{process.kill(-child.pid,'SIGKILL');}catch{}}
    fs.rmSync(directory,{recursive:true,force:true});
  }
})();
