"""Bounded real WebSocket observer; no URLs, JWTs or session IDs in evidence."""
import json
import threading
import time
from urllib.parse import urlsplit


class Observer:
    def __init__(self, base, token, client_type):
        target = urlsplit(base)
        self.client = client_type(target.hostname, target.port, '/ws/?token=' + token,
                                  max_message_bytes=16 * 1024 * 1024)
        if self.client.status != 101:
            self.client.close()
            raise RuntimeError('Observer upgrade rejected')
        initial = self.client.receive(timeout=5)
        if initial.get('message', {}).get('type') != 'connection_established':
            self.client.close()
            raise RuntimeError('Observer initial acknowledgement changed')
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.receive, daemon=True)
        self.result = {'messages': 0, 'bytes': 0, 'kinds': {}, 'errors': [], 'stats_snapshots': 0, 'last_connection_counts': {}, 'empty_snapshots': {}}
        self.thread.start()

    def receive(self):
        try:
            while not self.stop.is_set():
                frame = self.client.optional(.2)
                if frame is None:
                    continue
                if 'closed' in frame:
                    raise RuntimeError('Observer closed unexpectedly')
                value = frame.get('message')
                if value is None:
                    continue
                self.result['bytes'] += len(json.dumps(value).encode())
                self.result['messages'] += 1
                if self.result['bytes'] > 32 * 1024 * 1024 or self.result['messages'] > 4096:
                    raise ValueError('Observer capture exceeded bounded experiment policy')
                data = value.get('data', {})
                kind = data.get('type')
                if kind in ('vod_started', 'vod_stopped', 'vod_stats', 'timeshift_stats'):
                    if value.get('type') != 'update' or (kind.startswith('vod_') and 'success' in data) or (kind == 'timeshift_stats' and data.get('success') is not True):
                        raise ValueError('Changed actual playback event envelope')
                    counts = self.result['kinds']
                    counts[kind] = counts.get(kind, 0) + 1
                    if kind.endswith('_stats'):
                        stats = json.loads(data['stats'])
                        if not isinstance(stats, dict) or not isinstance(stats.get('total_connections', stats.get('total_clients')), int):
                            raise ValueError('Malformed actual playback statistics snapshot')
                        self.result['stats_snapshots'] += 1
                        count = stats.get('total_connections', stats.get('total_clients'))
                        self.result['last_connection_counts'][kind] = count
                        if count == 0:
                            self.result['empty_snapshots'][kind] = self.result['empty_snapshots'].get(kind, 0) + 1
        except Exception as error:
            if not self.stop.is_set():
                self.result['errors'].append(type(error).__name__)

    def close(self):
        self.stop.set()
        self.thread.join(2)
        self.client.close()
        if self.thread.is_alive():
            self.result['errors'].append('ObserverJoinTimeout')
        return self.result
