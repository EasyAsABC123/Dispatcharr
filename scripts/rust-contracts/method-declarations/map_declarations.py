"""Index exact Original method declarations without importing or invoking views."""
from argparse import ArgumentParser
import ast
from collections import Counter, defaultdict
from hashlib import sha256
from importlib.util import resolve_name
import json
from pathlib import Path
import tarfile


ORIGINAL = "1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430"
PINS = {
    "archive": "f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff",
    "register": "49d935200a80ada4a1297cee2786257641d24e8908f388a6fc662abf9c9544cb",
    "census": "c0ce8aca7f820ef52256c5ed3ea285a2951b19269872a15923d2d299813d5ec6",
    "resolver_report": "be06aa5f086c6d519fb3e500fa5f363004ef8311b28e62386e93d4567ade2d9f",
    "backup_options_proof": "56b572ebacb8641c5517d22357519a41cf5f17fc78e75ae96b78fef66735c6bb",
    "account_bindings": "62dd57ce7e41dc8892b1fbfe6a5e30b7e33743dc01bd8bb527592db314b183cd",
}
VERBS = frozenset(("DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT", "TRACE"))
METADATA = frozenset("rest_framework.decorators." + item for item in (
    "permission_classes", "authentication_classes", "throttle_classes", "renderer_classes",
    "parser_classes", "schema", "metadata_class")) | {"drf_spectacular.utils.extend_schema"}
DRF = "rest_framework.decorators.api_view"
REQUIRE = "django.views.decorators.http.require_http_methods"
PATHS = {"django.urls.path", "django.urls.re_path"}


def digest(data):
    return sha256(data).hexdigest()


def take(node):
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None


def atom(node, imports):
    if isinstance(node, ast.Name):
        return imports.get(node.id)
    if isinstance(node, ast.Attribute):
        base = atom(node.value, imports)
        return f"{base}.{node.attr}" if base else None
    return None


def dependencies(tree, module):
    lookups = defaultdict(set)
    package = module.rpartition(".")[0]
    for node in tree.body:
        if isinstance(node, ast.ImportFrom):
            raw = "." * node.level + (node.module or "")
            if not raw:
                continue
            parent = resolve_name(raw, package) if node.level else raw
            for name in node.names:
                if name.name != "*":
                    lookups[name.asname or name.name].add(f"{parent}.{name.name}")
        elif isinstance(node, ast.Import):
            for name in node.names:
                top = name.name.split(".")[0]
                lookups[name.asname or top].add(name.name if name.asname else top)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            lookups[node.name].add(f"{module}.{node.name}")
        elif isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                if isinstance(target, ast.Name):
                    lookups[target.id].add(None)
    return {name: next(iter(symbols)) for name, symbols in lookups.items() if len(symbols) == 1 and None not in symbols}


def patterns(expression):
    if isinstance(expression, (ast.List, ast.Tuple)):
        yield from expression.elts
    if isinstance(expression, ast.BinOp) and isinstance(expression.op, ast.Add):
        yield from patterns(expression.left)
        yield from patterns(expression.right)


class Snapshot:
    def __init__(self, archive):
        self.archive = archive
        self.names = set(archive.getnames())
        self.parsed = {}

    def module(self, dotted):
        if dotted not in self.parsed:
            filename = dotted.replace(".", "/") + ".py"
            if filename not in self.names:
                return None
            member = self.archive.getmember(filename)
            assert member.isfile()
            raw = self.archive.extractfile(member).read()
            tree = ast.parse(raw, filename=filename)
            self.parsed[dotted] = tree, dependencies(tree, dotted), filename, digest(raw)
        return self.parsed[dotted]

    def routes(self, module, prefix="", stack=(), namespaces=()):
        context = self.module(module)
        if context is None or module in stack:
            return
        tree, lookup, filename, sha = context
        for node in tree.body:
            if isinstance(node, ast.Assign):
                assigned = any(isinstance(target, ast.Name) and target.id == "urlpatterns" for target in node.targets)
            elif isinstance(node, ast.AugAssign):
                assigned = isinstance(node.target, ast.Name) and node.target.id == "urlpatterns" and isinstance(node.op, ast.Add)
            else:
                continue
            if not assigned:
                continue
            for expression in patterns(node.value):
                if not isinstance(expression, ast.Call) or atom(expression.func, lookup) not in PATHS or len(expression.args) < 2:
                    continue
                path, target = expression.args[:2]
                suffix = take(path)
                if suffix is None:
                    continue
                full = prefix + suffix
                if isinstance(target, ast.Call) and atom(target.func, lookup) == "django.urls.include":
                    selected = target.args[0] if len(target.args) == 1 else None
                    app_name = None
                    if isinstance(selected, ast.Tuple) and len(selected.elts) == 2:
                        selected, label = selected.elts
                        app_name = take(label)
                        if not app_name:
                            continue
                    included = take(selected)
                    included_module = self.module(included) if included else None
                    if included_module is None:
                        continue
                    child_tree = included_module[0]
                    names = [take(item.value) for item in child_tree.body if isinstance(item, ast.Assign)
                             and any(isinstance(name, ast.Name) and name.id == "app_name" for name in item.targets)]
                    if len(names) > 1 or (names and not names[0]):
                        continue
                    if names:
                        app_name = names[0]
                    if any(pair.arg != "namespace" or not take(pair.value) for pair in target.keywords):
                        continue
                    namespace = take(target.keywords[0].value) if target.keywords else app_name
                    nested = (*namespaces, namespace) if namespace else namespaces
                    yield from self.routes(included, full, (*stack, module), nested)
                    continue
                class_view = isinstance(target, ast.Call)
                if class_view:
                    if isinstance(target.func, ast.Attribute) and target.func.attr == "as_view" and not target.args and not target.keywords:
                        target = target.func.value
                    else:
                        continue
                ref = atom(target, lookup)
                if not ref:
                    continue
                names = [take(pair.value) for pair in expression.keywords if pair.arg == "name"]
                if names and names[0] is None:
                    continue
                full_name = ":".join((*namespaces, names[0] if names else ""))
                yield (full, full_name), (ref, class_view, filename, sha, expression.lineno)

    def declaration(self, target, passive):
        ref, class_view, filename, sha, line = target
        dotted, _, name = ref.rpartition(".")
        context = self.module(dotted)
        if context is None:
            return None
        tree, lookup, view_file, view_sha = context
        names = [item for item in tree.body if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
                 and item.name == name]
        if len(names) != 1 or lookup.get(name) != ref:
            return None
        item = names[0]
        if class_view:
            if not isinstance(item, ast.ClassDef) or item.decorator_list or passive["view"] != ref:
                return None
            owned = [node.name for node in item.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))]
            body_bindings = dependencies(item, ref)
            if any(body_bindings.get(name) != f"{ref}.{name}" for name in owned):
                return None
            if not len(owned) == len(set(owned)) or set(owned) & {"dispatch", "setup", "_allowed_methods", "initialize_request"}:
                return None
            authored = {verb for verb in VERBS if verb.lower() in owned}
            if not authored or not compatible(authored, passive["handler_method_candidates"]):
                return None
            method_node = item
            kind = "project_view_explicit_handlers"
        else:
            if not isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                return None
            decorators = [(atom(node.func, lookup) if isinstance(node, ast.Call) else atom(node, lookup), node)
                          for node in item.decorator_list]
            declarations = [(name, node) for name, node in decorators if name in {DRF, REQUIRE}]
            if len(declarations) != 1 or any(name not in METADATA | {DRF, REQUIRE} for name, _ in decorators):
                return None
            kind_id, method_node = declarations[0]
            if len(method_node.args) != 1 or method_node.keywords or not isinstance(method_node.args[0], (ast.List, ast.Tuple)):
                return None
            verbs = [take(verb) for verb in method_node.args[0].elts]
            if not verbs or not set(verbs) <= VERBS or len(set(verbs)) != len(verbs):
                return None
            authored = set(verbs)
            if kind_id == DRF:
                if passive["view"] != dotted + ".WrappedAPIView" or not compatible(authored, passive["handler_method_candidates"]):
                    return None
                kind = "project_DRF_literal_api_view"
            else:
                if passive["view"] != ref or passive["handler_method_candidates"] is not None:
                    return None
                kind = "project_Django_literal_require_http_methods"
        return {"classification": kind, "source_symbol": ref, "source_explicit_methods": sorted(authored),
                "urlconf": {"file": filename, "line": line, "sha256": sha},
                "declaration": {"file": view_file, "line": method_node.lineno, "sha256": view_sha}}


def compatible(declared, candidates):
    if not candidates:
        return False
    candidate = set(candidates)
    return declared <= candidate and (candidate - declared) <= {"HEAD", "OPTIONS"}


def unique_declaration(snapshot, options, runtime):
    if len(options) != 1:
        return None
    return snapshot.declaration(options[0], runtime)


def key_identity(route):
    return (route["pattern_exact"], route["name"], route["source_handler"])


def read_inputs(paths):
    for name, expected in PINS.items():
        path = paths[name]
        if digest(path.read_bytes()) != expected:
            raise ValueError(f"Pinned input digest differs for {name}")
    return {name: json.loads(paths[name].read_bytes()) for name in PINS if name != "archive"}


def audit(paths):
    source = read_inputs(paths)
    register, census, retained = (source[name] for name in ("register", "census", "resolver_report"))
    assert retained["literal_actual_exit"] == 0
    assert register["baseline_ref"] == census["baseline_ref"] == ORIGINAL
    assert census["inspection_view_invocations"] == census["inspection_SQL_statements"] == 0
    assert census["actual_method_or_permission_acceptance"] is False
    assert retained["matching_declarations"] == len(retained["rows"]) == 187
    assert retained["view_invocations"] == retained["inspection_SQL_statements"] == 0
    assert retained["other_winners"] == retained["unresolved_samples"] == 0 and retained["owned_cleanup_passed"] is True
    seen = {row["resolver_ordinal"]: row for row in retained["rows"]}
    rows = []
    with tarfile.open(paths["archive"]) as archive:
        snapshot = Snapshot(archive)
        links = defaultdict(list)
        for key, declaration in snapshot.routes("dispatcharr.urls"):
            links[key].append(declaration)
        routes, passive = register["runtime_routes"], census["routes"]
        assert len(routes) == len(passive) == 592
        for ordinal, (route, runtime) in enumerate(zip(routes, passive, strict=True)):
            assert ordinal == route["resolver_ordinal"]
            assert key_identity(route) == tuple(runtime[key] for key in ("pattern", "name", "view"))
            if route["declared_methods_resolved"]:
                continue
            operation, = route["operations"]
            assert operation["method"] is operation["source_action"] is None and operation["fully_verified"] is False
            candidates = runtime["handler_method_candidates"]
            assert candidates == route["runtime_handler_candidates"]
            pattern, name, handler = key_identity(route)
            row = {"resolver_ordinal": ordinal, "contract_work_id": operation["contract_id"], "pattern": pattern,
                   "name": name, "runtime_handler": handler, "runtime_candidate_methods": candidates,
                   "existing_resolver_path_sample_verified": ordinal in seen, "accepted_HTTP_methods": None}
            options = links[(pattern, name)]
            selected = unique_declaration(snapshot, options, runtime)
            if selected is not None:
                row.update(selected)
            elif ordinal in seen:
                previous = seen[ordinal]
                assert previous["contract"] == row["contract_work_id"] and previous["winning_ordinal"] == ordinal
                assert previous["runtime_winning_handler_candidates"] == candidates
                row["classification"] = "retained_d38_" + previous["family"]
            elif candidates:
                row["classification"] = "additional_runtime_candidates_only"
            else:
                row["classification"] = "unmapped_declaration"
            rows.append(row)
    assert len({row["contract_work_id"] for row in rows}) == len(rows) == 322
    assert sum(row["runtime_candidate_methods"] is not None for row in rows) == 166
    counts = Counter(row["classification"] for row in rows)
    assert sum(value for key, value in counts.items() if key.startswith("retained_d38_")) == 187
    return {"format_version": 1, "Original_revision": ORIGINAL, "inputs": PINS,
            "historical_placeholders": 322, "counts": dict(sorted(counts.items())),
            "source_declarations": [row for row in rows if row["classification"].startswith("project_")],
            "other_resolver_ordinals": {key: [row["resolver_ordinal"] for row in rows if row["classification"] == key]
                 for key in counts if not key.startswith("project_")},
            "request_evidence_groups": request_evidence(rows, source),
            "accepted_HTTP_rows_added": 0, "Native_candidate_evidence_added": False,
            "physical_executions": 0}


def request_evidence(rows, source):
    prior_backup = source["backup_options_proof"]
    assert prior_backup["actual_fresh_pair_literal_exit"] == 0 and prior_backup["exact_owned_cleanup"] is True
    assert prior_backup["backup_jobs_exercised"] is False
    backup_rows = [row for row in rows if row.get("source_symbol", "").startswith("apps.backups.")]
    account_rows = [row for row in rows if row.get("source_symbol", "").startswith("apps.accounts.")]
    assert len(backup_rows) == 10 and len(account_rows) == 5
    labels = {
        "list_backups": "list", "create_backup": "create", "upload_backup": "upload",
        "get_schedule": "schedule", "update_schedule": "schedule-update", "backup_status": "status",
        "get_download_token": "download-token", "download_backup": "download",
        "delete_backup": "delete", "restore_backup": "restore",
    }
    owners = prior_backup["matched_metadata"]
    assert set(owners) == set(labels.values())
    backup = []
    for row in backup_rows:
        label = labels[row["source_symbol"].rsplit(".", 1)[1]]
        methods = row["runtime_candidate_methods"]
        was = owners[label]
        assert was["status"] == 200 and was["content_type"] == "application/json"
        assert set(was["allow"].split(", ")) == set(methods)
        assert was["allow"] in prior_backup["observed_per_route_allow_orders"][label]
        backup.append({"contract_work_id": row["contract_work_id"], "actual_method": "OPTIONS",
                       "observed_admin_status_each": was["status"], "actual_allow": was["allow"]})
    bindings = source["account_bindings"]
    assert bindings["fresh_Source_case_count"] == 0 and bindings["fully_verified_operations"] == 0
    lookup = {row["contract_id"]: row for row in bindings["contracts"]}
    account = []
    for row in account_rows:
        assignment = lookup[row["contract_work_id"]]
        assert assignment["fully_verified"] is False
        assert set(assignment["observed_request_methods"]) <= set(row["source_explicit_methods"])
        account.append({"contract_work_id": row["contract_work_id"],
                        "framework_request_methods": assignment["observed_request_methods"],
                        "framework_workflow_references": len(assignment["cases"]),
                        "framework_role_descriptions": assignment["observed_roles"]})
    return {"backup": {"transport": "historical_paired_admin_TCP", "actual_http_scope": "OPTIONS_only",
                       "historical_native_revision": prior_backup["application_revision"], "cases": backup},
            "account": {"transport": "Original_Django_framework_tests", "cases": account}}


def main():
    assert __debug__
    cli = ArgumentParser(description=__doc__)
    for name in PINS:
        cli.add_argument("--" + name.replace("_", "-"), type=Path, required=True)
    values = audit(vars(cli.parse_args()))
    print(json.dumps(values, separators=(",", ":"), sort_keys=True))


if __name__ == "__main__":
    main()
