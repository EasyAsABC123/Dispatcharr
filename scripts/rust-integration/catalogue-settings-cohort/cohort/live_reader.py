"""Existing fixed resource Reader reused for functional media; no sampler."""
import socket,threading,time,urllib.request,urllib.error,json,hashlib
class Reader:
    """Incremental socket reader; bounded capture, synchronized counters and close."""
    def __init__(self, url, path, token):
        self.url, self.path, self.token = url, path, token
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.response = None
        self.request_started = time.monotonic()
        self.state = {'bytes': 0, 'media_bytes': 0, 'packets': 0, 'sync_errors': 0,
                      'partial_bytes': 0, 'ended': False, 'errors': []}
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def run(self):
        partial = bytearray()
        try:
            with urllib.request.urlopen(urllib.request.Request(self.url,headers={'Authorization':'Bearer '+self.token,'User-Agent':'dispatcharr-live-fixed-load'}),timeout=100) as response, self.path.open('wb') as stream, self.path.with_suffix('.arrival.jsonl').open('w') as ledger:
                with self.lock:
                    self.response = response
                    self.state['http_status'] = response.status
                    self.state['content_type'] = response.headers.get_content_type()
                read_count = 0
                while not self.stop.is_set():
                    begin = time.monotonic()
                    data = response.read1(65536)
                    ended = time.monotonic()
                    if not data:
                        break
                    first = stream.tell()
                    stream.write(data)
                    read_count += 1
                    if read_count > 16384:raise RuntimeError('ArrivalLedger16384Budget')
                    ledger.write(json.dumps({'begin_monotonic':begin,'end_monotonic':ended,'start_offset':first,'end_offset':stream.tell(),'sha256':hashlib.sha256(data).hexdigest()},separators=(',',':'))+'\n')
                    ledger.flush()
                    partial.extend(data)
                    length = len(partial) // 188 * 188
                    complete = bytes(partial[:length])
                    del partial[:length]
                    with self.lock:
                        self.state.setdefault('first_byte_seconds', ended-self.request_started)
                        self.state['bytes'] += len(data)
                        self.state['packets'] += length // 188
                        for offset in range(0, length, 188):
                            self.state['sync_errors'] += complete[offset] != 0x47
                            if ((complete[offset + 1] & 31) << 8 | complete[offset + 2]) != 8191:
                                self.state['media_bytes'] += 188
                    if stream.tell() > 512 * 1024 * 1024:
                        raise RuntimeError('capture exceeds512MiB')
        except Exception as error:
            if not self.stop.is_set():
                with self.lock:
                    self.state['errors'].append(type(error).__name__)
                    self.state['http_error_status'] = getattr(error, 'code', None)
        finally:
            with self.lock:
                self.state['partial_bytes'] = len(partial)
                self.state['ended'] = True
                self.response = None

    def snapshot(self):
        with self.lock:
            return dict(self.state, errors=list(self.state['errors']))

    def close(self):
        self.stop.set()
        with self.lock:
            response = self.response
        if response is not None:
            try:
                response.fp.raw._sock.shutdown(socket.SHUT_RDWR)
            except (AttributeError, OSError):
                pass
        self.thread.join(timeout=3)
        if self.thread.is_alive():
            raise RuntimeError('reader failed bounded socket shutdown')
