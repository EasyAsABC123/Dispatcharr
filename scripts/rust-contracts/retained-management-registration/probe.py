"""Observe Django command ownership without loading any command module."""

import ast
import hashlib
from importlib import metadata
import json
from pathlib import Path
import sys

import ipv6_probe as ipv6


original = ipv6.original
COMMAND_MODULES = frozenset()
BLOCKED_SYSTEM_EVENTS = {
    "os.kill": "process", "os.killpg": "process", "os.remove": "filesystem", "os.rmdir": "filesystem",
    "os.rename": "filesystem", "os.truncate": "filesystem",
}


class CommandImportGuard:
    def find_spec(self, fullname, path=None, target=None):
        if fullname in COMMAND_MODULES:
            original.deny("management")


def audit(event, args):
    category = BLOCKED_SYSTEM_EVENTS.get(event)
    if category:
        original.deny(category)


def read_declaration(root, entry):
    path = root / entry["path"]
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != entry["source_sha256"]:
        raise ValueError("Exact Original command source changed")
    cls = [node for node in ast.parse(raw, filename=str(path)).body
           if isinstance(node, ast.ClassDef) and node.name == entry["name"]]
    if len(cls) != 1 or cls[0].lineno != entry["line"] or [ast.unparse(base) for base in cls[0].bases] != ["BaseCommand"]:
        raise ValueError("Exact command declaration coordinate changed")


def record(entries, command_registry, app_registry):
    rows = []
    for entry in entries:
        command = entry["command"]
        owner = command_registry.get(command)
        app = next((config for config in app_registry if config.name == owner), None)
        expected = str(Path(app.path) / "management" / "commands" / f"{command}.py") if app else None
        if owner != entry["owner"] or expected != "/app/" + entry["path"]:
            rows.append({"candidate_id": entry["candidate_id"], "state": "unobserved", "reason": "registry_owner_mismatch"})
            continue
        rows.append({key: entry[key] for key in ("candidate_id", "command", "owner", "path", "line", "name", "source_sha256")}
                    | {"state": "management_registration_observed"})
    return rows


def main():
    global COMMAND_MODULES
    if sys.argv[1:] != ["shell"]:
        raise ValueError("Exact startup guard argument required")
    entries = json.loads(Path("/fixture/inputs.json").read_bytes())
    COMMAND_MODULES = frozenset(f'{row["owner"]}.management.commands.{row["command"]}' for row in entries)
    if len(COMMAND_MODULES) != len(entries) or any(module in sys.modules for module in COMMAND_MODULES):
        raise ValueError("Command modules cannot be loaded")
    sys.meta_path.insert(0, CommandImportGuard())
    if hashlib.sha256(Path(ipv6.SOURCE).read_bytes()).hexdigest() != ipv6.SOURCE_SHA256:
        raise ValueError("Exact archived urllib3 source is required")
    ipv6.SOURCE_VERIFIED = True
    sys.addaudithook(audit)
    original.audit = ipv6.audit
    ipv6.diagnostic.guards()

    import django
    from django.apps import apps
    from django.core import management
    from django.core.management.base import BaseCommand

    deny_management = original.blocked("management")
    for method in ("__init__", "create_parser", "run_from_argv", "execute", "handle"):
        setattr(BaseCommand, method, deny_management)
    for method in ("call_command", "load_command_class", "execute_from_command_line"):
        setattr(management, method, deny_management)
    management.ManagementUtility.execute = deny_management

    django.setup()
    for entry in entries:
        read_declaration(Path("/app"), entry)
    management.get_commands.cache_clear()
    rows = record(entries, management.get_commands(), list(apps.get_app_configs()))
    if any(module in sys.modules for module in COMMAND_MODULES):
        raise ValueError("Unexpected command module import")
    if original.GUARD:
        rows = [{"candidate_id": entry["candidate_id"], "state": "unobserved", "reason": "guarded_effect_attempt"}
                for entry in entries]

    module = sys.modules.get(ipv6.MODULE)
    print(json.dumps({"django": metadata.version("Django"), "celery": metadata.version("celery"),
                      "external_effects_executed": False, "management_commands_started": False,
                      "command_modules_loaded": False, "denied_attempts": dict(original.GUARD),
                      "ipv6_fallbacks": ipv6.FALLBACKS,
                      "ipv6_unavailable": module is not None and vars(module).get("HAS_IPV6") is False,
                      "observations": rows}, sort_keys=True))


if __name__ == "__main__":
    main()
