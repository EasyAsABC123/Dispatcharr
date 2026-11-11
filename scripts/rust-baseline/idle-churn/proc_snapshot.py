#!/usr/bin/env python3
"""Bounded read-only same-UID census; no command lines, paths or environment data.
Root inventories identities; each normal unprivileged UID reads only its own fds.
Application capabilities and service resource limits remain unchanged.
"""
import argparse
import json
import os
from pathlib import Path


def processes():
    result = []
    for path in Path('/proc').iterdir():
        if path.name.isdigit():
            result.append(int(path.name))
            if len(result) > 4096:
                raise ValueError('process census exceeds4096')
    parents = {}
    for pid in result:
        try:
            text = Path('/proc', str(pid), 'stat').read_text()
            parents[pid] = int(text[text.rfind(')') + 2:].split()[1])
        except FileNotFoundError:
            pass
    excluded = {os.getpid()}
    for _ in range(64):
        added = {p for p, parent in parents.items() if parent in excluded}
        if added <= excluded:
            break
        excluded |= added
    else:
        raise ValueError('collector descendant depth exceeds64')
    return sorted(set(result) - excluded)


def identity(pid):
    status = Path('/proc', str(pid), 'status').read_text()
    values = dict(line.split(':', 1) for line in status.splitlines() if ':' in line)
    return int(values['Uid'].split()[1]), int(values['Gid'].split()[1]), int(values['Threads'])


def tcp_states():
    states = {}
    for name in ('tcp', 'tcp6'):
        with Path('/proc/net', name).open() as stream:
            next(stream)
            for number, line in enumerate(stream, 1):
                if number > 65536:
                    raise ValueError('TCP census exceeds65536')
                state = line.split()[3]
                states[state] = states.get(state, 0) + 1
    return states


def collect(inventory=False):
    values = dict(processes=0, threads=0, fds=0, socket_descriptors=0,
                  raced_processes=0, raced_descriptors=0, permission_denied_processes=0,
                  permission_denied_descriptors=0, denied_process_ids=[], process_ids=[], socket_inodes=[], collector_identity=[os.geteuid(), os.getegid()])
    identities = set()
    inodes = set()
    for pid in processes():
        try:
            uid, gid, threads = identity(pid)
            if inventory:
                identities.add((uid, gid))
                if len(identities) > 16:
                    raise ValueError('identity census exceeds16')
                values['process_ids'].append(pid)
                continue
            if (uid, gid) != (os.geteuid(), os.getegid()):
                continue
            count = 0
            for fd in Path('/proc', str(pid), 'fd').iterdir():
                count += 1
                if count > 65536 or values['fds'] + count > 262144:
                    raise ValueError('fd census bound')
                try:
                    target = os.readlink(fd)
                except FileNotFoundError:
                    values['raced_descriptors'] += 1
                    continue
                except PermissionError:
                    values['permission_denied_descriptors'] += 1
                    raise
                if target.startswith('socket:['):
                    values['socket_descriptors'] += 1
                    inodes.add(target)
                    if len(inodes) > 65536:
                        raise ValueError('socket inode bound')
            values['processes'] += 1
            values['threads'] += threads
            values['fds'] += count
            values['process_ids'].append(pid)
        except FileNotFoundError:
            values['raced_processes'] += 1
        except PermissionError:
            # A changing /proc owner or dumpability can deny even same-UID fd
            # links. Keep the denial explicit and exclude this PID's full count.
            values['permission_denied_processes'] += 1
            values['denied_process_ids'].append(pid)
    values['coverage_complete'] = not (values['permission_denied_processes'] or values['permission_denied_descriptors'])
    values['socket_inodes'] = sorted(inodes)
    if inventory:
        values['identities'] = [list(v) for v in sorted(identities)]
        values['tcp_states'] = tcp_states()
    return values


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inventory', action='store_true')
    args = parser.parse_args()
    print(json.dumps(collect(args.inventory), sort_keys=True))
