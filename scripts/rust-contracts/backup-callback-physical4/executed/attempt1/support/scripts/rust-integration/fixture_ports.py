"""Allocate captured loopback fixture ports outside Linux's ephemeral range.

Original application credit remains with Dispatcharr's authors. These probes do
not reserve ports after returning; a creation-time bind race must remain visible.
"""
import errno
import json
from pathlib import Path
import re
import socket
import uuid


def write_port_overlay(path, service_ports):
    if not 1 <= len(service_ports) <= 16 or sum(map(len,service_ports.values())) > 64:
        raise ValueError('fixture port allocation exceeds service/port bound')
    for service,ports in service_ports.items():
        if not re.fullmatch(r'[a-zA-Z0-9_-]{1,64}',service) or not ports or any(not isinstance(port,int) or not 1<=port<=65535 for port in ports) or len(set(ports))!=len(ports):
            raise ValueError('invalid service or container port')
    try:
        ephemeral=list(map(int,Path('/proc/sys/net/ipv4/ip_local_port_range').read_text().split()))
    except FileNotFoundError:
        ephemeral=None
    available=[port for port in range(61000,65001) if ephemeral is None or not ephemeral[0]<=port<=ephemeral[1]]
    if not available:raise RuntimeError('fixture high-port range overlaps kernel ephemeral allocation')
    held=[];mapping={};cursor=uuid.uuid4().int%len(available)
    try:
        for service,ports in service_ports.items():
            mapping[service]=[]
            for container in ports:
                for _ in range(min(128,len(available))):
                    port=available[cursor%len(available)];cursor+=1
                    probe=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                    try:probe.bind(('127.0.0.1',port))
                    except OSError as error:
                        probe.close()
                        if error.errno==errno.EADDRINUSE:continue
                        raise
                    held.append(probe);mapping[service].append({'host':port,'container':container});break
                else:raise RuntimeError('no free fixture high port within128 probes')
        lines=['# Generated bounded fixture loopback port bindings.','services:']
        for service,ports in mapping.items():
            lines += ['  '+json.dumps(service)+':','    ports: !override']
            lines += ['      - '+json.dumps('127.0.0.1:'+str(port['host'])+':'+str(port['container'])) for port in ports]
        Path(path).write_text('\n'.join(lines)+'\n')
        return {'host_range':[61000,65000],'kernel_ephemeral_range':ephemeral,'services':mapping,
                'selection':'bind probes held together until overlay is written; released before Docker starts'}
    finally:
        for probe in held:probe.close()
