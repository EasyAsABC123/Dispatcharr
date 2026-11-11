"""Small immutable-file fixture responder following RFC9110 single-byte ranges.

Malformed/unsupported ranges are explicitly ignored (200), rather than confused
with valid unsatisfiable byte ranges (416). HEAD ignores Range as RFC9110 permits
Range semantics only for GET. No multipart implementation is implied.
"""
import datetime
from email.utils import format_datetime, parsedate_to_datetime
import re


class UnsatisfiableRange(ValueError):
    pass


def parse_range(value, size):
    if not value:
        return None
    unit, separator, body = value.strip().partition('=')
    body = body.strip()
    if not separator or unit.lower() != 'bytes' or not re.fullmatch(r'[0-9]*-[0-9]*', body):
        return None
    first, last = body.split('-')
    if not first and not last:
        return None
    try:
        if not first:
            suffix = int(last)
            if suffix == 0 or size == 0:
                raise UnsatisfiableRange()
            return max(0, size - suffix), size - 1
        start = int(first)
        end = int(last) if last else size - 1
    except UnsatisfiableRange:
        raise
    except ValueError:
        return None
    if last and end < start:
        return None
    if size == 0 or start >= size:
        raise UnsatisfiableRange()
    return start, min(end, size - 1)


def if_range_matches(value, etag, modified):
    if not value:
        return True
    if value == etag:
        return True
    try:
        date = parsedate_to_datetime(value)
        return date.tzinfo is not None and int(date.timestamp()) >= modified
    except (ValueError, TypeError, OverflowError):
        return False


def serve_file(handler, path, etag, head=False):
    size = path.stat().st_size
    modified = int(path.stat().st_mtime)
    modified_header = format_datetime(datetime.datetime.fromtimestamp(modified, datetime.timezone.utc), usegmt=True)
    matches = [item.strip().removeprefix('W/') for item in handler.headers.get('If-None-Match', '').split(',')]
    status, selected = 200, None
    if '*' in matches or etag in matches:
        status = 304
    elif not head and if_range_matches(handler.headers.get('If-Range'), etag, modified):
        try:
            selected = parse_range(handler.headers.get('Range'), size)
            if selected is not None:
                status = 206
        except UnsatisfiableRange:
            status = 416
    handler.send_response(status)
    handler.send_header('Content-Type', 'video/mp4')
    handler.send_header('Accept-Ranges', 'bytes')
    handler.send_header('ETag', etag)
    handler.send_header('Last-Modified', modified_header)
    if status == 304:
        handler.end_headers()
        return status, 0
    if status == 416:
        handler.send_header('Content-Range', f'bytes */{size}')
        handler.send_header('Content-Length', '0')
        handler.end_headers()
        return status, 0
    start, end = selected if selected is not None else (0, size - 1)
    length = max(0, end - start + 1)
    if selected is not None:
        handler.send_header('Content-Range', f'bytes {start}-{end}/{size}')
    handler.send_header('Content-Length', str(length))
    handler.end_headers()
    if head:
        return status, 0
    written = 0
    try:
        with path.open('rb') as stream:
            stream.seek(start)
            while written < length:
                chunk = stream.read(min(65536, length - written))
                if not chunk:
                    raise RuntimeError('Immutable fixture changed while serving')
                handler.wfile.write(chunk)
                written += len(chunk)
    except (BrokenPipeError, ConnectionResetError):
        pass
    return status, written
