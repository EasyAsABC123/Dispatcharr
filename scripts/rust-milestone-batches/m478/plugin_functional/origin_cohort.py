"""Own the reused TLS/DNS and unsigned HTTP servers; no application imports."""
import json,os,signal,subprocess,time
from pathlib import Path

def main():
    output=Path('/evidence/origins');output.mkdir();children=[];stopping=False;streams=[];error=None
    def stop(*_):
        nonlocal stopping
        stopping=True
    signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
    try:
        for name,argv in [('tls',['/plugin-tools/origin_supervisor.py','--output',str(output/'tls'),'--assets','/plugin-assets']),('unsigned',['/plugin-tools/unsigned_origin.py','--mode','normal','--output',str(output/'unsigned'),'--plan','/native-root/inputs/merged-origin.json'])]:
            (output/name).mkdir();out=(output/(name+'.stdout')).open('xb');err=(output/(name+'.stderr')).open('xb');streams += [out,err]
            child=subprocess.Popen(['/dispatcharrpy/bin/python','-B',*argv],stdin=subprocess.DEVNULL,stdout=out,stderr=err);children.append((name,child))
        deadline=time.monotonic()+15
        while not all((output/name/'origin-ready.json').is_file() for name,_ in children):
            if time.monotonic()>deadline or any(child.poll() is not None for _,child in children):raise ValueError('owned Origin children readiness15s')
            time.sleep(.02)
        (output/'ready.json').write_text(json.dumps({'pid':os.getpid(),'PID_namespace':os.readlink('/proc/self/ns/pid'),'zero_Source_applications':True})+'\n');began=time.monotonic()
        while not stopping:
            if time.monotonic()-began>1200 or any(child.poll() is not None for _,child in children):raise ValueError('Origin lifetime or child exit')
            time.sleep(.05)
    except Exception as exc:error={'type':type(exc).__name__,'message':str(exc)}
    finally:
        rows=[]
        for name,child in children:
            if child.poll() is None:child.terminate()
        for name,child in children:
            forced=False
            try:status=child.wait(timeout=8)
            except subprocess.TimeoutExpired:forced=True;child.kill();status=child.wait(timeout=2)
            rows.append({'name':name,'pid':child.pid,'wait':status,'poll':child.poll(),'forced':forced})
        for stream in streams:stream.close()
        (output/'terminal.json').write_text(json.dumps({'children':rows,'error':error,'passed':error is None and all(r['wait']==r['poll']==0 and not r['forced'] for r in rows)},indent=2)+'\n')
    return 0 if error is None and all(r['wait']==0 for r in rows) else 1
if __name__=='__main__':raise SystemExit(main())
