"""Deterministic paced HTTP bytes for testing the measurement harness, not video."""
import http.server
import json
import threading
import time

LOCK = threading.Lock()
COUNTERS = {'requests': 0, 'active': 0, 'bytes': 0, 'disconnects': 0}
PAYLOAD = bytes(range(256)) * 32


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/metrics':
            with LOCK:
                body = json.dumps(COUNTERS).encode()
            self.send_response(200)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if self.path != '/stream':
            self.send_error(404)
            return
        with LOCK:
            COUNTERS['requests'] += 1
            COUNTERS['active'] += 1
        self.send_response(200)
        self.send_header('Content-Type', 'application/octet-stream')
        self.end_headers()
        try:
            while True:
                self.wfile.write(PAYLOAD)
                self.wfile.flush()
                with LOCK:
                    COUNTERS['bytes'] += len(PAYLOAD)
                time.sleep(.01)
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            with LOCK:
                COUNTERS['active'] -= 1
                COUNTERS['disconnects'] += 1

    def log_message(self, *args):
        pass


if __name__ == '__main__':
    http.server.ThreadingHTTPServer(('0.0.0.0', 9292), Handler).serve_forever()
