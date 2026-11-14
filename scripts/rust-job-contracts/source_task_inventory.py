#!/usr/bin/env python3
"""Static pinned Source task declarations; no imports or application execution.

Original Dispatcharr/Celery authors retain application and protocol credit.
This is not the live Celery registry or a functional coverage report.
"""
import argparse
import ast
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import subprocess
import tarfile

ORIGINAL = "1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430"
ROOT = Path(__file__).resolve().parents[2]


def inventory(revision):
    resolved = subprocess.check_output(
        ["git", "rev-parse", "--verify", revision + "^{commit}"], cwd=ROOT, text=True
    ).strip()
    raw = subprocess.check_output(["git", "archive", resolved], cwd=ROOT)
    tasks = []
    with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
        for entry in archive.getmembers():
            if not entry.isfile() or not entry.name.endswith(".py"):
                continue
            if any(p == "tests" or p.startswith("test_") for p in PurePosixPath(entry.name).parts):
                continue
            content = archive.extractfile(entry).read()
            tree = ast.parse(content, filename=entry.name)
            module = entry.name[:-3].replace("/", ".")
            if module.endswith(".__init__"):
                module = module[:-9]
            for node in tree.body:
                if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                for decorator in node.decorator_list:
                    call = decorator if isinstance(decorator, ast.Call) else None
                    target = call.func if call else decorator
                    name = target.id if isinstance(target, ast.Name) else target.attr if isinstance(target, ast.Attribute) else None
                    if name not in ("shared_task", "task"):
                        continue
                    options, dynamic = {}, []
                    if call:
                        for item in call.keywords:
                            try:
                                options[item.arg] = ast.literal_eval(item.value)
                            except (ValueError, TypeError):
                                dynamic.append(item.arg)
                    tasks.append({
                        "name": options.get("name") or module + "." + node.name,
                        "module": module,
                        "function": node.name,
                        "path": entry.name,
                        "line": node.lineno,
                        "end_line": node.end_lineno,
                        "file_sha256": hashlib.sha256(content).hexdigest(),
                        "decorator_options": options,
                        "dynamic_options": dynamic,
                        "runtime_registration_verified": False,
                        "source_execution": False,
                    })
    return {
        "pinned_Original_git": resolved,
        "scope": "Static runtime top-level task decorator inventory; excludes test files; NOT runtime Celery registry or complete task-producer/consumer coverage",
        "Original_authors_retain_full_credit": True,
        "tasks": sorted(tasks, key=lambda item: item["name"]),
        "task_declarations": len(tasks),
        "dynamic_name_options": [item for item in tasks if "name" in item["dynamic_options"]],
        "remaining": "Crosswalk actual runtime registrations, every natural producer/callback and Native generic Main handlers; include dynamically registered tasks and required framework tasks separately.",
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--revision", default=ORIGINAL)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("Output already exists; preserve prior inventory")
    args.output.write_text(json.dumps(inventory(args.revision), indent=2) + "\n")
