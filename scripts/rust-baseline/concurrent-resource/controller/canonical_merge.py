"""Exact merge_census from published4b209bf8; Original authors retain credit."""
def merge_census(inventory, parts, ending=None):
    ending = inventory if ending is None else ending
    observed = [pid for part in parts for pid in part['process_ids']]
    if len(observed) != len(set(observed)):
        raise ValueError('DuplicateCensusProcess')
    value = {key: sum(p[key] for p in parts) for key in ['processes', 'threads', 'fds', 'socket_descriptors', 'raced_processes', 'raced_descriptors']}
    value['raced_processes'] += inventory['raced_processes'] + ending['raced_processes'] + len(set(observed) ^ set(inventory['process_ids'])) + len(set(observed) ^ set(ending['process_ids']))
    value['unique_socket_inodes'] = len({inode for p in parts for inode in p['socket_inodes']})
    value['tcp_states'] = inventory['tcp_states']
    value['identity_groups'] = len(parts)
    value['inventory_start'] = inventory
    value['inventory_end'] = ending
    value['per_uid'] = parts
    for field in ['permission_denied_processes', 'permission_denied_descriptors']:
        value[field] = sum(part.get(field, 0) for part in [inventory, *parts, ending])
    value['complete'] = (value['raced_processes'] == 0 and value['raced_descriptors'] == 0
                         and value['permission_denied_processes'] == 0
                         and value['permission_denied_descriptors'] == 0)
    return value
