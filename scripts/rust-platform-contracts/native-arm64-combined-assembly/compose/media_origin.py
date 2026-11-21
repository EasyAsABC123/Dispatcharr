"""One owned, generated MPEG-TS endpoint in the Compose test network."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import subprocess
from threading import Lock
import time


class Origin(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"

    def log_message(self, *_args):
        pass

    def do_GET(self):
        if self.path == "/stats":
            with self.server.lock:
                result = json.dumps(self.server.counts, sort_keys=True).encode()
            self.send_response(200)
            self.send_header("Content-Type","application/json")
            self.send_header("Content-Length",str(len(result)))
            self.end_headers()
            self.wfile.write(result)
            return
        if self.path != "/media.ts":
            self.send_error(404)
            return
        with self.server.lock:
            self.server.counts["connections"] += 1
            self.server.counts["active"] += 1
        try:
            self.send_response(200)
            self.send_header("Content-Type","video/mp2t")
            self.end_headers()
            for _ in range(12):
                for offset in range(0,len(self.server.media),188*30):
                    self.wfile.write(self.server.media[offset:offset+188*30])
                    self.wfile.flush()
                    time.sleep(.02)
        except (BrokenPipeError,ConnectionResetError):
            pass
        finally:
            with self.server.lock:
                self.server.counts["active"] -= 1


def main():
    assert os.geteuid()==65532 and os.uname().machine=="aarch64"
    file=Path("/tmp/owned.ts")
    subprocess.run(["/usr/local/bin/ffmpeg","-hide_banner","-v","error","-f","lavfi","-i",
                    "testsrc2=size=320x240:rate=25","-f","lavfi","-i","sine=frequency=450:sample_rate=44100",
                    "-t","8","-threads","1","-c:v","mpeg2video","-c:a","mp2","-f","mpegts",str(file)],
                    check=True,timeout=40)
    media=file.read_bytes()
    assert len(media)%188==0 and all(media[k]==0x47 for k in range(0,len(media),188))
    server=ThreadingHTTPServer(("0.0.0.0",8080),Origin)
    server.lock=Lock()
    server.counts={"connections":0,"active":0}
    server.media=media
    server.serve_forever()


if __name__=="__main__":
    main()
