#!/usr/bin/env python3
"""Capture real proxy TS while a local fixture stalls or fails to an alternate.

A successful capture is not decoder proof: decode capture.ts with the pinned
image and retain that command/output as a separate required assertion.
"""
import argparse
import json
import socket
import subprocess
import importlib.util
from pathlib import Path
import threading
import time
import urllib.request


def get_json(url):
    with urllib.request.urlopen(url, timeout=3) as response:
        return json.load(response)


def control(origin, path, action, seconds=0):
    request = urllib.request.Request(origin + '/control',
        data=json.dumps({'path': path, 'action': action, 'seconds': seconds}).encode(),
        headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(request, timeout=3) as response:
        if json.load(response).get('applied') is not True:
            raise RuntimeError('Fixture did not acknowledge the fault')


def probe(url, origin, primary_path, backup_path, output, case='failover', timeout=45,
          recovery_observation=7, stall_seconds=2, prefault_bytes=250000, accounting=None):
    output.mkdir(parents=True, exist_ok=True)
    if accounting:
        accounting.before()
    physical = {}
    stop, ready = threading.Event(), threading.Event()
    lock = threading.Lock()
    received = {'bytes': 0, 'packets': 0, 'sync_errors': 0, 'errors': [], 'partial_bytes_dropped': 0}
    started = time.monotonic()
    if accounting:
        accounting.begin_active(started)
        spec=importlib.util.spec_from_file_location('fault_existing_resource_observer',Path(__file__).resolve().parents[2]/'scripts/rust-perf/perf.py')
        perf=importlib.util.module_from_spec(spec);spec.loader.exec_module(perf)
        accounting.start_resources(perf.resources)
    capture_path = output / 'capture.ts'

    def receive():
        partial = bytearray()
        try:
            with urllib.request.urlopen(url, timeout=10) as response, capture_path.open('wb') as capture:
                if accounting:
                    physical['socket']=response.fp.raw._sock
                while not stop.is_set():
                    data = response.read1(65536)
                    if not data:
                        break
                    partial.extend(data)
                    length = len(partial) // 188 * 188
                    if not length:
                        continue
                    chunk = bytes(partial[:length])
                    del partial[:length]
                    capture.write(chunk)
                    with lock:
                        received['bytes'] += len(chunk)
                        received['packets'] += len(chunk) // 188
                        received['sync_errors'] += sum(chunk[offset] != 0x47 for offset in range(0, len(chunk), 188))
                    ready.set()
                    if capture.tell() > 32 * 1024 * 1024:
                        raise RuntimeError('Probe capture exceeded32MiB bound')
        except Exception as error:
            if not stop.is_set():
                with lock:
                    received['errors'].append(type(error).__name__)
                received['http_error_status'] = getattr(error, 'code', None)
        finally:
            with lock:
                received['partial_bytes_dropped'] = len(partial)

    thread = threading.Thread(target=receive, daemon=True)
    result = {'scope': 'native-ts-fixture-fault-capture', 'case': case, 'passed': False,
              'decoder_verified': False, 'application_performance_verified': False, 'failures': []}
    try:
        before = get_json(origin + '/stats')
        result['origin_before'] = before
        thread.start()
        if not ready.wait(min(timeout, 20)):
            raise RuntimeError('Proxy emitted no complete TS packet before fault')
        settle_deadline = min(started + timeout, time.monotonic() + 10)
        while True:
            with lock:
                available = received['bytes']
            if available >= prefault_bytes:
                break
            if time.monotonic() >= settle_deadline or not thread.is_alive():
                raise RuntimeError('Insufficient stable media before fault injection')
            time.sleep(.05)
        with lock:
            bytes_before_fault = received['bytes']
        result['bytes_before_fault'] = bytes_before_fault
        fault_time = time.monotonic()
        if accounting:
            accounting.allow_active_state()
        control(origin, primary_path, 'fail' if case == 'failover' else 'stall', stall_seconds)
        if accounting:
            accounting.event('fault-ack')
        deadline = started + timeout
        baseline_backup_bytes = before['paths'].get(backup_path, {}).get('bytes', 0)
        while True:
            now = time.monotonic()
            origin_now = get_json(origin + '/stats')
            if case == 'failover':
                recovered = origin_now['paths'].get(backup_path, {}).get('bytes', 0) > baseline_backup_bytes
            else:
                with lock:
                    current_bytes = received['bytes']
                recovered = now >= fault_time + stall_seconds and current_bytes > bytes_before_fault
            if recovered:
                if accounting:
                    accounting.event('first-recovery')
                result['recovery_seconds'] = now - fault_time
                with lock:
                    result['bytes_at_recovery_observed'] = received['bytes']
                break
            if now >= deadline or not thread.is_alive():
                raise RuntimeError('No observed alternate/resumed media before recovery deadline')
            time.sleep(.1)
        time.sleep(min(recovery_observation, max(0, deadline - time.monotonic())))
        result['origin_after_recovery'] = get_json(origin + '/stats')
        with lock:
            if (received['bytes'] <= result['bytes_at_recovery_observed'] or received['sync_errors']
                    or received['errors'] or not thread.is_alive()):
                raise RuntimeError('Capture did not continue as packet-aligned media after fault')
        result['passed'] = True
    except Exception as error:
        result['failures'].append(type(error).__name__ + ': ' + str(error))
    finally:
        try:
            control(origin, primary_path, 'recover')
        except Exception as error:
            result['failures'].append('Fixture restore failed: ' + type(error).__name__)
            result['passed'] = False
        stop.set()
        if accounting and 'socket' in physical:
            try:
                physical['socket'].shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            physical['socket'].close()
        if thread.ident is not None:
            thread.join(timeout=12)
            if thread.is_alive():
                result['failures'].append('Reader did not stop within bounded socket timeout')
                result['passed'] = False
        with lock:
            result['capture'] = dict(received)
        result['elapsed_seconds'] = time.monotonic() - started
        if accounting:
            try:
                closed='socket' in physical and physical['socket'].fileno()==-1 and not thread.is_alive()
                accounting.event('local-client-physical-close')
                accounting.after_readers([{'physical_socket_closed':closed}])
                time.sleep(accounting.retention_seconds)
                result['accounting']=accounting.finish()
                if not result['accounting']['functional_hook_qualified']:
                    result['passed']=False
                    result['failures'].append('Source accounting state qualification')
            except Exception as error:
                result['passed']=False
                result['failures'].append('Accounting: '+type(error).__name__+': '+str(error))
            finally:
                accounting.stop_resources()
            result['elapsed_with_accounting_seconds']=time.monotonic()-started
        (output / 'fault.json').write_text(json.dumps(result, indent=2) + '\n')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url-file', type=Path, required=True)
    parser.add_argument('--origin-base', required=True)
    parser.add_argument('--primary-path', default='/live/0.ts')
    parser.add_argument('--backup-path', default='/live/100.ts')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--case', choices=['failover', 'stall'], default='failover')
    parser.add_argument('--accounting-context-file',type=Path)
    args = parser.parse_args()
    if args.output.exists() and any(args.output.iterdir()):
        parser.error('Output must be empty')
    urls = [line.strip() for line in args.url_file.read_text().splitlines() if line.strip()]
    if len(urls) != 1 or not urls[0].startswith(('http://', 'https://')):
        parser.error('URL file requires exactly one proxy HTTP(S) URL')
    accounting=None
    if args.accounting_context_file:
        from mixed_accounting import Hook
        context=json.loads(args.accounting_context_file.read_text());ids=[]
        for service in context['selected_services']:
            ids += subprocess.check_output(context['compose']+['ps','--status','running','-q',service],text=True,timeout=10).split()
        accounting=Hook(context,context['compose'],ids)
    result = probe(urls[0], args.origin_base.rstrip('/'), args.primary_path, args.backup_path,
                   args.output, args.case, accounting=accounting)
    print(json.dumps({key: result[key] for key in ('passed', 'failures', 'decoder_verified')}))
    return 0 if result['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
