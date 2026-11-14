"""One process/resource scheduler for the migration's whole milestone matrix.

Owners supply exact executable bindings and semantic/cleanup evaluators. This
module schedules those bindings; a process exit alone never accepts a milestone.
"""
import argparse
import concurrent.futures
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time

HERE = Path(__file__).resolve().parent
STATES = {"passed", "failed", "blocked", "not_run"}


def save(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")
    os.replace(temporary, path)


def module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded


def inventory():
    """Deduplicate shared package nodes; keep missing owner cells required."""
    rows = []
    m235 = module(HERE / "m235/batch.py", "batch_m235").load_matrix()
    for node in m235["nodes"]:
        rows.append({"id": node["id"], "owner": "m235",
                     "depends_on": node["depends_on"],
                     "heavy": node["exclusive_heavy"],
                     "resources": [node["fixture_group"]],
                     "entrypoint": node["entrypoint"],
                     **({"UTC_minute_window": node["clock_guard"]["UTC_minute_range_inclusive"]} if node.get("clock_guard") else {}),
                     **({"same_global_lease_as": "package/complete-refresh-original", "reference_max_age_seconds": 900} if node["id"] == "package/complete-refresh" else {})})
    m478, _, _ = module(HERE / "m478/batch.py", "batch_m478").load()
    for node in m478["nodes"]:
        rows.append({"id": node["id"], "owner": "m478",
                     "depends_on": node["requires"],
                     "heavy": "GLOBAL_HEAVY_QUIET" in node["exclusive_resources"],
                     "resources": node["exclusive_resources"],
                     "entrypoint": node["entry_point"]})
    m6_path = HERE / "m6/entrypoint.py"
    if not m6_path.is_file():
        raise ValueError("M6 owner export has not been installed")
    m6_export = module(m6_path, "batch_m6").export()
    for node in m6_export["nodes"]:
        rows.append({"id": node["global_id"], "owner": "m6",
                     "depends_on": node.get("depends_on", []), "heavy": False,
                     "resources": ["m6/" + node["context"]],
                     "entrypoint": "production-Main; owner full workflow evaluator required"})
    rows.append({"id": "performance/M6-whole-graph", "owner": "m6",
                 "depends_on": [n["global_id"] for n in m6_export["nodes"]],
                 "heavy": True, "resources": ["GLOBAL_HEAVY_QUIET"],
                 "entrypoint": "paired equivalent import/media work including API, workers, SQL/cache and children"})
    # The owner cohorts are not the whole release. Retain the formal baseline,
    # core and final acceptance gates even when their bindings are unavailable.
    def root_node(key, deps, entrypoint, heavy=False):
        return {"id": key, "owner": "Root", "depends_on": deps,
                "heavy": heavy, "resources": ["Root/" + key], "entrypoint": entrypoint}
    functional = [n["id"] for n in rows if not n["id"].startswith("performance/")]
    rows.extend([
        root_node("root/M0-executable-coverage", [], "pinned Original HTTP/non-HTTP executable contract inventory"),
        root_node("root/M1-core-acceptance", ["root/M0-executable-coverage"], "bounded core tests, container and equivalent-work benchmark", True),
        root_node("external/M9/datastore-scope", [], "SQLite and PostgreSQL compatibility crosswalk; preserve required supported stores"),
        root_node("root/M9-workflow-parity", functional + ["root/M0-executable-coverage", "root/M1-core-acceptance"],
                  "release images: all contracts, unchanged React, faults, fresh install and upgrade"),
        root_node("root/M9-soak", ["root/M9-workflow-parity"], "at least two hours with refresh, decoder/resource-growth and post-soak cleanup", True),
        root_node("root/M9-rollback", ["root/M9-workflow-parity"], "copied-data migration, restore and rollback rehearsal", True),
        root_node("root/M9-whole-service-performance", ["root/M9-workflow-parity", "root/M9-soak"],
                  "quiet paired equal-work CPU/memory/latency/throughput, all participating services", True),
    ])
    if len({n["id"] for n in rows}) != len(rows):
        raise ValueError("duplicate global node IDs")
    return rows


def validate(nodes, bindings):
    known = {n["id"] for n in nodes}
    if len(known) != len(nodes) or any(not re.fullmatch(r"[A-Za-z0-9_./-]+", key) or ".." in key for key in known):
        raise ValueError("unique safe global IDs required")
    if set(bindings) - known:
        raise ValueError("bindings contain unknown matrix IDs")
    by_id = {n['id']: n for n in nodes}
    for node in nodes:
        window = node.get('UTC_minute_window')
        if window is not None and (not isinstance(window, list) or len(window) != 2 or
                not all(type(v) is int for v in window) or not 0 <= window[0] <= window[1] <= 59):
            raise ValueError('finite UTC minute admission window required')
        source = node.get('same_global_lease_as')
        if source and (source not in known or source not in node['depends_on'] or
                by_id[source].get('same_global_lease_as') or
                type(node.get('reference_max_age_seconds')) is not int or node['reference_max_age_seconds'] <= 0):
            raise ValueError('known two-phase paired dependency and finite reference age required')
    visiting, visited = set(), set()

    def visit(key):
        if key not in known or key in visited:
            return
        if key in visiting:
            raise ValueError("cyclic global dependency: " + key)
        visiting.add(key)
        for dep in next(n for n in nodes if n["id"] == key)["depends_on"]:
            visit(dep)
        visiting.remove(key)
        visited.add(key)

    for node in nodes:
        visit(node["id"])
    for bound in bindings.values():
        for key in ("argv", "evaluator_argv", "cleanup_argv"):
            value = bound[key]
            if not isinstance(value, list) or not value or not all(isinstance(v, str) and v and "\0" not in v for v in value):
                raise ValueError("literal structured argv required: " + key)
        for key in ("seconds", "cleanup_seconds", "output_bytes"):
            if type(bound[key]) is not int or bound[key] <= 0:
                raise ValueError("positive finite budget required: " + key)
        if {"HOME", "CODEX_HOME"} & bound.get("environment", {}).keys():
            raise ValueError("HOME/CODEX_HOME cannot be overridden")
        if not bound["input_sha256"]:
            raise ValueError("owner controller/evaluator/fixture inputs must be bound")
        for name, digest in bound["input_sha256"].items():
            path = Path(name)
            if not path.is_file() or path.is_symlink() or hashlib.sha256(path.read_bytes()).hexdigest() != digest:
                raise ValueError("bound input changed: " + name)


def owner_readiness(nodes, document):
    """An arbitrary argv binding cannot bypass an owner's actual readiness.

    Retained Source is a dependency authority, never a newly executed/current
    production result. Additional Source cohorts need an explicit unmeasured-case
    crosswalk. Previously qualified cases must be imported, not replayed.
    """
    bindings = document["nodes"]
    owners = document.get("owner_bindings", {})
    authorities = {}
    m235 = module(HERE / "m235/batch.py", "readiness_m235")
    m478 = module(HERE / "m478/batch.py", "readiness_m478")
    ops_matrix, _, _ = m478.load()
    source_audit = None
    for node in nodes:
        key = node["id"]
        if node["owner"] == "m235":
            resolved = m235.resolve_node(key, owners.get("m235", {}), HERE.parents[1])
            if resolved.get("status") == "retained_source_authority":
                if key in bindings:
                    raise ValueError("retained Source cannot also be dispatched: " + key)
                authorities[key] = {"id": key, "status": "retained_source_authority",
                                    "satisfies_dependency": True, "owner_resolution": resolved}
                continue
            if key not in bindings:
                continue
            if resolved.get("ready") is not True:
                raise ValueError("owner blocked " + key + ": " + str(resolved.get("reasons")))
            for constraint in ('UTC_minute_window', 'same_global_lease_as', 'reference_max_age_seconds'):
                if constraint in resolved and node.get(constraint) != resolved[constraint]:
                    raise ValueError('owner scheduling constraint differs: ' + key + ':' + constraint)
            if key.startswith("package/"):
                if source_audit is None:
                    audit_file = m235.checked_file(document["source_stage_audit"], HERE.parents[1])
                    source_audit = json.loads(audit_file.read_text())
                    if source_audit["candidate_git"] != document["candidate"]["git"]:
                        raise ValueError("Source-stage audit differs from common candidate")
                    audit_rows = {row["id"]: row for row in source_audit["rows"]}
                    if len(audit_rows) != len(source_audit["rows"]):
                        raise ValueError("duplicate Source-stage audit IDs")
                audit = audit_rows.get(key, {})
                if (audit.get("dispatch_ready_for_Source_policy") is not True
                        or audit.get("action") not in {"none", "fresh_required"}
                        or not audit.get("reason") or not audit.get("authority")):
                    raise ValueError("Source-stage reuse/fixture dependency unresolved: " + key)
            expected = resolved["argv"]
            if "@GLOBAL_CONTEXT@" in expected:
                context = bindings[key].get("global_context_path")
                if not context:
                    raise ValueError("global context binding required: " + key)
                expected = [context if v == "@GLOBAL_CONTEXT@" else v for v in expected]
            if bindings[key]["argv"] != expected:
                raise ValueError("owner argv differs: " + key)
            if not key.startswith("source/"):
                current = owners["m235"].get("candidate", {})
                if (current.get("image_id") != document["candidate"]["image_id"]
                        or owners["m235"].get("integration_git") != document["candidate"]["git"]
                        or owners["m235"].get("integration_source_map_sha256") != document["candidate"]["source_map_sha256"]):
                    raise ValueError("M235 differs from common candidate")
            for field, source_field in (("environment", "environment_overlay"), ("strip_inherited_prefixes", "strip_inherited_prefixes")):
                if source_field in resolved and bindings[key].get(field) != resolved[source_field]:
                    raise ValueError("owner environment differs: " + key)
        elif key in bindings and node["owner"] == "m478":
            path = owners.get("m478")
            if not path:
                raise ValueError("M478 binding file required")
            owner_bound = m478.bindings(path)
            if key.startswith(("native/", "Main/", "performance/")) and owner_bound["candidate"] != document["candidate"]:
                raise ValueError("M478 differs from common candidate")
            resolved = m478.dispatch_spec(key, ops_matrix, owner_bound)
            if any(bindings[key][field] != resolved[field] for field in ("argv", "evaluator_argv")):
                raise ValueError("M478 exact controller/evaluator differs: " + key)
            if bindings[key].get("environment", {}) != resolved["environment"]:
                raise ValueError("M478 owner environment differs")
        elif key in bindings and node["owner"] == "m6":
            # This export currently offers bounded partial probes, not complete
            # functional or performance workflows. Do not invent a green binding.
            if key not in {"m6/api202-publication", "m6/default-job-graph"}:
                raise ValueError("M6 full workflow adapter not implemented: " + key)
            exported = next(n for n in module(HERE / "m6/entrypoint.py", "readiness_m6").export()["nodes"] if n["global_id"] == key)
            context, receipt = bindings[key].get("m6_context"), bindings[key].get("m6_receipt")
            if not context or not receipt:
                raise ValueError("M6 owned context and receipt required")
            expected = [context if v == "<ROOT_COMMON_CONTEXT>" else receipt if v == "<ROOT_NODE_RECEIPT>" else v for v in exported["runner"]]
            if bindings[key]["argv"] != expected:
                raise ValueError("M6 exact probe argv differs")
        fresh_required = source_audit is not None and key in bindings and key.startswith("package/") and audit_rows.get(key, {}).get("action") == "fresh_required"
        if key in bindings and not fresh_required and (key.startswith("source/") or "Source" in key or "Original" in key):
            bound = bindings[key]
            definition = bound.get("new_source_case_ids")
            previous = bound.get("previously_qualified_case_ids")
            if (not isinstance(definition, list) or not definition or not isinstance(previous, list)
                    or not all(isinstance(v, str) and v for v in definition + previous)
                    or set(definition) & set(previous)):
                raise ValueError("only explicitly unmeasured Source cases can execute: " + key)
    return authorities


def _group_members(pgid):
    """Read identities in the observer PID namespace, including unreaped zombies."""
    members = []
    for path in Path('/proc').iterdir():
        if not path.name.isdigit():
            continue
        try:
            fields = (path / 'stat').read_text().rsplit(')', 1)[1].split()
            if int(fields[2]) == pgid:
                members.append({'pid': int(path.name), 'state': fields[0],
                                'starttime': fields[19]})
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            continue
    return members


def command(argv, bound, directory, label, seconds):
    """Bound both output files while physically settling the owned process group."""
    import ctypes
    import selectors
    # Adopt a controller's orphaned descendants so waitpid can retire zombies,
    # rather than treating leader exit or pipe EOF as physical group settlement.
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:  # Linux PR_SET_CHILD_SUBREAPER
        raise OSError(ctypes.get_errno(), 'cannot establish child-subreaper ownership')
    env = {k: v for k, v in os.environ.items()
           if not any(k.startswith(prefix) for prefix in bound.get('strip_inherited_prefixes', []))}
    env.update(bound.get('environment', {}))
    out, err = directory / (label + '.stdout'), directory / (label + '.stderr')
    started = time.monotonic()
    reason, termination_at, killed_at = None, None, None
    written, discarded = 0, 0
    reaped = []
    leader_exit_at = None
    with out.open('xb') as stdout, err.open('xb') as stderr:
        process = subprocess.Popen(argv, cwd=bound.get('cwd'), env=env,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   start_new_session=True)
        pgid = process.pid
        selector = selectors.DefaultSelector()
        eof = {'stdout': False, 'stderr': False}
        for name, pipe, sink in [('stdout', process.stdout, stdout), ('stderr', process.stderr, stderr)]:
            os.set_blocking(pipe.fileno(), False)
            selector.register(pipe, selectors.EVENT_READ, (name, sink))

        def terminate(now):
            nonlocal termination_at
            if termination_at is None:
                termination_at = now
                try:
                    os.killpg(pgid, signal.SIGTERM)
                except ProcessLookupError:
                    pass

        try:
            while True:
                now = time.monotonic()
                code = process.poll()
                if code is not None:
                    if leader_exit_at is None:
                        leader_exit_at = now
                    # Popen has reaped the original leader. Only now may group
                    # waitpid reap adopted descendants without racing Popen.
                    while True:
                        try:
                            child, status = os.waitpid(-pgid, os.WNOHANG)
                        except ChildProcessError:
                            break
                        if child == 0:
                            break
                        reaped.append({'pid': child, 'wait_status': status})
                members = _group_members(pgid)
                if code is None and now - started >= seconds and reason is None:
                    reason = 'timeout'
                if code is not None and members and reason is None:
                    reason = 'controller left process-group descendants'
                if reason is not None:
                    terminate(now)
                if termination_at is not None and now - termination_at >= 5 and members and killed_at is None:
                    try:
                        os.killpg(pgid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    killed_at = now
                if code is not None and not members and all(eof.values()):
                    break
                # A descendant escaping the owned group can keep a pipe open.
                # Return explicit unsettled proof rather than hang or invent EOF.
                if code is not None and not members and not all(eof.values()) and termination_at is None and now - leader_exit_at >= 0.25:
                    reason = reason or 'controller left inherited pipes open'
                    terminate(now)
                if termination_at is not None and now - termination_at >= 10:
                    reason = reason or 'physical settlement deadline'
                    break
                for key, _ in selector.select(timeout=0.05):
                    name, sink = key.data
                    try:
                        data = os.read(key.fileobj.fileno(), 65536)
                    except BlockingIOError:
                        continue
                    if not data:
                        selector.unregister(key.fileobj)
                        key.fileobj.close()
                        eof[name] = True
                        continue
                    keep = min(len(data), max(0, bound['output_bytes'] - written))
                    if keep:
                        sink.write(data[:keep])
                        written += keep
                    discarded += len(data) - keep
                    if len(data) > keep:
                        reason = 'output budget exceeded'
                        terminate(time.monotonic())
            if process.poll() is None:
                try:
                    os.killpg(pgid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    reason = reason or 'original leader remains unsettled'
        finally:
            # An observer exception must not leave the controller/group alive.
            # Keep EOF false if observation failed; closing our descriptors is
            # not an invented successful pipe settlement receipt.
            if process.poll() is None or _group_members(pgid):
                try:
                    os.killpg(pgid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    pass
                until = time.monotonic() + 2
                while process.poll() is not None and _group_members(pgid) and time.monotonic() < until:
                    try:
                        child, status = os.waitpid(-pgid, os.WNOHANG)
                        if child:
                            reaped.append({'pid': child, 'wait_status': status})
                    except ChildProcessError:
                        pass
                    time.sleep(0.01)
            selector.close()
            for pipe in (process.stdout, process.stderr):
                if not pipe.closed:
                    pipe.close()
        members = _group_members(pgid)
        settled = process.poll() is not None and not members and all(eof.values())
        if not settled:
            reason = reason or 'physical process-group settlement incomplete'
    return {'actual_exit': process.poll(), 'failure': reason,
            'seconds': time.monotonic() - started, 'stdout': str(out), 'stderr': str(err),
            'stdout_EOF': eof['stdout'], 'stderr_EOF': eof['stderr'],
            'process_group': pgid, 'process_group_settled': settled,
            'remaining_group_members': members, 'reaped_descendants': reaped,
            'output_bytes_written': written, 'output_bytes_discarded': discarded}


def execute(node, bound, directory, candidate):
    directory.mkdir(parents=True)
    save(directory / "attempt.json", {"node": node, "candidate": candidate,
                                      "binding": bound, "attempt": 1})
    result = {"id": node["id"], "status": "failed", "cleanup_verified": False}
    try:
        result["execution"] = command(bound["argv"], bound, directory, "execution", bound["seconds"])
    except Exception as error:
        result["execution_error"] = str(error)
    finally:
        try:
            result["cleanup"] = command(bound["cleanup_argv"], bound, directory, "cleanup", bound["cleanup_seconds"])
            proof = json.loads(Path(result["cleanup"]["stdout"]).read_text())
            result["cleanup_verified"] = (result["cleanup"]["actual_exit"] == 0
                                           and not result["cleanup"]["failure"]
                                           and proof.get("exact_owned_absence") is True
                                           and settled(result["cleanup"])
                                           and settled(result.get("execution", {})))
        except Exception as error:
            result["cleanup_error"] = str(error)
    # Evaluators receive the actual command/cleanup result through a stable path.
    save(directory / "execution.json", result)
    try:
        argv = [str(directory / "execution.json") if v == "@EXECUTION@" else v for v in bound["evaluator_argv"]]
        result["evaluation"] = command(argv, bound, directory, "evaluation", bound["cleanup_seconds"])
        proof = json.loads(Path(result["evaluation"]["stdout"]).read_text())
        result["qualification"] = proof
        if (result.get("execution", {}).get("actual_exit") == 0
                and not result["execution"]["failure"] and result["cleanup_verified"]
                and result["evaluation"]["actual_exit"] == 0 and not result["evaluation"]["failure"]
                and settled(result["evaluation"])
                and proof.get("semantic_assertions_passed") is True
                and proof.get("candidate") == candidate
                and proof.get("entrypoint") == node["entrypoint"]
                and proof.get("full_required_workflow") is True
                and node["id"] not in {"m6/api202-publication", "m6/default-job-graph"}):
            result["status"] = "passed"
    except Exception as error:
        result["evaluation_error"] = str(error)
    # An unsettled evaluator also owns real processes/resources. It prevents the
    # scheduler releasing this lease even if the app cleanup previously passed.
    result["cleanup_verified"] = result["cleanup_verified"] and settled(result.get("evaluation", {}))
    save(directory / "result.json", result)
    return result


def settled(result):
    return all(result.get(key) is True for key in ("process_group_settled", "stdout_EOF", "stderr_EOF"))


def _utc_allowed(node, minute):
    window = node.get('UTC_minute_window')
    return window is None or window[0] <= minute <= window[1]


def _reference_age(node, bound, now):
    maximum = node['reference_max_age_seconds']
    if 'reference_clock_file' in bound:
        path = Path(bound['reference_clock_file'])
    else:
        argv = bound['argv']
        path = Path(argv[argv.index('--reference') + 1]) / 'source-refresh-workflow-private.json'
    import datetime
    clock = datetime.datetime.fromisoformat(json.loads(path.read_text())['fixture_clock'])
    if clock.tzinfo is None:
        raise ValueError('reference clock must include its UTC offset')
    age = now - clock.timestamp()
    if not 0 <= age <= maximum:
        raise ValueError('reference age outside unchanged budget: ' + str(age))
    return age


def schedule(nodes, bindings, output, candidate, worker=execute, authorities=None,
             utc_minute=None, wall_clock=time.time):
    validate(nodes, bindings)
    utc_minute = utc_minute or (lambda: time.gmtime().tm_min)
    output.mkdir(parents=True, exist_ok=False)
    results, running = dict(authorities or {}), {}
    pending = {n['id']: n for n in nodes if n['id'] not in results}
    by_id = {n['id']: n for n in nodes}
    pairs = {n['same_global_lease_as']: n['id'] for n in nodes if n.get('same_global_lease_as')}
    if len(pairs) != sum(bool(n.get('same_global_lease_as')) for n in nodes):
        raise ValueError('one Native successor per paired Source reservation required')
    successors = set(pairs.values())

    def accepted(row):
        return row['status'] == 'passed' or (row['status'] == 'retained_source_authority' and row.get('satisfies_dependency') is True)

    def directory(key):
        return output / (key.replace('/', '__') + '-' + hashlib.sha256(key.encode()).hexdigest()[:12])

    def invoke(node):
        # Check in the worker immediately before execute creates its output or
        # starts a fixture. No wait-for-next-hour or retry is implied.
        minute = utc_minute()
        if not _utc_allowed(node, minute):
            return {'id': node['id'], 'status': 'blocked', 'cleanup_verified': True,
                    'reason': 'UTC minute outside unchanged admission window', 'actual_UTC_minute': minute}
        try:
            return worker(node, bindings[node['id']], directory(node['id']), candidate)
        except Exception as error:
            return {'id': node['id'], 'status': 'failed', 'cleanup_verified': False, 'error': str(error)}

    def invoke_pair(source, native):
        lease = {'source': source['id'], 'native': native['id'], 'one_global_slot': True}
        first = invoke(source)
        first['paired_lease'] = lease
        if not accepted(first) or first.get('cleanup_verified') is not True:
            return [first, {'id': native['id'], 'status': 'blocked', 'cleanup_verified': True,
                            'paired_lease': lease, 'reason': 'paired Source did not pass and physically settle'}]
        try:
            age = _reference_age(native, bindings[native['id']], wall_clock())
        except Exception as error:
            return [first, {'id': native['id'], 'status': 'blocked', 'cleanup_verified': True,
                            'paired_lease': lease, 'reason': str(error)}]
        second = invoke(native)
        second.update(paired_lease=lease, reference_age_at_dispatch_seconds=age)
        return [first, second]

    unsafe = False
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        while pending or running:
            for future in list(running):
                if future.done():
                    unit = running.pop(future)
                    try:
                        completed = future.result()
                    except Exception as error:
                        completed = [{'id': key, 'status': 'failed', 'cleanup_verified': False, 'error': str(error)} for key in unit['members']]
                    if not isinstance(completed, list):
                        completed = [completed]
                    for result in completed:
                        results[result['id']] = result
                        unsafe |= result.get('cleanup_verified') is not True
            for key, node in list(pending.items()):
                if key not in pending:
                    continue
                native_id = pairs.get(key)
                native = by_id[native_id] if native_id else None
                members = [key] + ([native_id] if native else [])
                if key in successors:
                    source = node['same_global_lease_as']
                    if source in pending or any(source in n['members'] for n in running.values()):
                        continue
                    results[key] = {'id': key, 'status': 'blocked', 'reason': 'paired Source must execute in the same global reservation'}
                    del pending[key]
                    continue
                deps = list(node['depends_on'])
                if native:
                    deps += [d for d in native['depends_on'] if d != key]
                missing = any(d not in pending and d not in results and all(d not in n['members'] for n in running.values()) for d in deps)
                if any(k not in bindings for k in members) or unsafe or any(d in results and not accepted(results[d]) for d in deps) or missing:
                    for member in members:
                        results[member] = {'id': member, 'status': 'blocked', 'reason': 'cleanup uncertain' if unsafe else 'missing binding or unaccepted dependency'}
                        pending.pop(member, None)
                    continue
                if any(d not in results for d in deps):
                    continue
                resources = set(node['resources']) | (set(native['resources']) if native else set())
                heavy = node['heavy'] or bool(native and native['heavy'])
                active = list(running.values())
                if len(active) >= 3 or any(n['heavy'] for n in active) or (heavy and active):
                    continue
                if any(resources & set(n['resources']) for n in active):
                    continue
                unit = {'id': key, 'members': members, 'heavy': heavy, 'resources': sorted(resources)}
                # The same pool task and reserved union of resources cover both
                # phases. Other leases still count toward the global maximum3.
                future = pool.submit(invoke_pair, node, native) if native else pool.submit(invoke, node)
                running[future] = unit
                for member in members:
                    del pending[member]
            save(output / 'progress.json', {'results': results, 'active': [n['id'] for n in running.values()],
                                           'active_reservations': list(running.values()), 'pending': list(pending)})
            if pending and not running:
                raise ValueError('unschedulable dependencies')
            if running:
                concurrent.futures.wait(running, timeout=0.1, return_when=concurrent.futures.FIRST_COMPLETED)
    report = {'candidate': candidate, 'results': results,
              'full_batch_passed': bool(nodes) and all(accepted(r) for r in results.values()), 'cleanup_uncertain': unsafe}
    save(output / 'report.json', report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--export", action="store_true")
    parser.add_argument("--bindings", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    nodes = inventory()
    if args.export:
        print(json.dumps({"nodes": nodes, "global_fixture_limit": 3, "heavy_exclusive": True}, indent=2))
        return 0
    if not args.bindings or not args.output:
        parser.error("execution requires --bindings and a fresh --output")
    bound = json.loads(args.bindings.read_text())
    owner = module(HERE / "m478/batch.py", "candidate_validation")
    if not owner.candidate_ready(bound["candidate"]):
        raise ValueError("current committed candidate/archive/source-map/build binding required")
    validate(nodes, bound["nodes"])
    authorities = owner_readiness(nodes, bound)
    # Lock across scheduler invocations; owner adapters never create their own pool.
    with (HERE / ".dispatcher.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        report = schedule(nodes, bound["nodes"], args.output, bound["candidate"], authorities=authorities)
    return 0 if report["full_batch_passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
