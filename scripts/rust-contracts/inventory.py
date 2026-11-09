#!/usr/bin/env python3
"""Inventory source-level compatibility candidates without importing the application."""
from __future__ import annotations

import argparse
import ast
from collections import Counter
import hashlib
import json
from pathlib import Path
import subprocess
from typing import Any

DEFAULT_BASELINE = "1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430"
LIMITATIONS = [
    "Static candidates only: not a resolved Django URL table or complete contract suite.",
    "Includes, router registrations, inherited actions, settings-dependent branches, and dynamic expressions are not executed or expanded.",
    "Class classification uses source names/bases and module conventions, not runtime inheritance resolution.",
    "No HTTP method, permission, schema, job behavior, or wire compatibility is verified by this inventory.",
    "WebSocket candidates are identified by routing modules and consumer classes; dynamically assembled patterns may require manual review.",
]


def expression(node: ast.AST, source: str) -> dict[str, Any]:
    result: dict[str, Any] = {"expression": ast.get_source_segment(source, node) or ast.unparse(node)}
    try:
        value = ast.literal_eval(node)
        # Avoid non-JSON literal types such as bytes, sets, and complex numbers.
        json.dumps(value, allow_nan=False)
        result["literal"] = value
    except (ValueError, TypeError, SyntaxError):
        pass
    return result


def callable_name(node: ast.AST) -> str:
    if isinstance(node, ast.Call):
        return callable_name(node.func)
    if isinstance(node, ast.Attribute):
        return callable_name(node.value) + "." + node.attr
    if isinstance(node, ast.Name):
        return node.id
    return ast.unparse(node)


class CandidateVisitor(ast.NodeVisitor):
    def __init__(self, path: str, source: str) -> None:
        self.path, self.source = path, source
        self.records: list[dict[str, Any]] = []
        self.aliases: dict[str, str] = {}
        self.scope: list[str] = []

    def record(self, kind: str, node: ast.AST, **fields: Any) -> None:
        self.records.append({"kind": kind, "path": self.path, "line": node.lineno,
                             "end_line": node.end_lineno, **fields})

    def resolved_name(self, node: ast.AST) -> str:
        name = callable_name(node)
        root, *tail = name.split(".")
        return ".".join([self.aliases.get(root, root), *tail])

    def arguments(self, node: ast.Call) -> dict[str, Any]:
        return {"arguments": [expression(arg, self.source) for arg in node.args],
                "keywords": [{"name": keyword.arg, **expression(keyword.value, self.source)}
                             for keyword in node.keywords]}

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        for alias in node.names:
            self.aliases[alias.asname or alias.name] = f"{node.module or ''}.{alias.name}"

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            self.aliases[alias.asname or alias.name.split('.')[0]] = alias.name if alias.asname else alias.name.split('.')[0]

    def visit_Call(self, node: ast.Call) -> None:
        resolved = self.resolved_name(node.func)
        terminal = callable_name(node.func).split(".")[-1]
        url_module = self.path.endswith(("urls.py", "routing.py"))
        if resolved in {"django.urls.path", "django.urls.re_path"} or (url_module and terminal in {"path", "re_path"}):
            self.record("websocket_route" if self.path.endswith("routing.py") else "route", node,
                        callable=resolved, **self.arguments(node))
        elif resolved == "django.urls.include" or (url_module and terminal == "include"):
            self.record("include", node, callable=resolved, **self.arguments(node))
        elif url_module and terminal == "register":
            self.record("router_registration", node, callable=resolved, **self.arguments(node))
        self.generic_visit(node)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        bases = [self.resolved_name(base) for base in node.bases]
        top_level = not self.scope
        fields = {"name": ".".join([*self.scope, node.name]), "bases": bases}
        if top_level and (self.path.endswith("models.py") or "/models/" in self.path) and node.name != "Meta":
            self.record("model_candidate", node, **fields)
        if "/migrations/" in self.path and node.name == "Migration":
            values = {}
            for statement in node.body:
                if isinstance(statement, ast.Assign):
                    for target in statement.targets:
                        if isinstance(target, ast.Name) and target.id in {"dependencies", "initial", "replaces"}:
                            values[target.id] = expression(statement.value, self.source)
            self.record("migration", node, **fields, attributes=values)
        if any("Serializer" in base for base in bases) or node.name.endswith("Serializer"):
            self.record("serializer_candidate", node, **fields)
        if any("ViewSet" in base for base in bases) or node.name.endswith("ViewSet"):
            self.record("viewset_candidate", node, **fields)
        if any("Consumer" in base for base in bases) or node.name.endswith("Consumer"):
            self.record("websocket_consumer_candidate", node, **fields)
        if "/management/commands/" in self.path and node.name == "Command":
            self.record("management_command", node, command=Path(self.path).stem, **fields)
        self.scope.append(node.name)
        self.generic_visit(node)
        self.scope.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        for decorator in node.decorator_list:
            target = decorator.func if isinstance(decorator, ast.Call) else decorator
            resolved = self.resolved_name(target)
            terminal = resolved.split(".")[-1]
            if terminal in {"task", "shared_task"}:
                self.record("task_candidate", decorator, name=".".join([*self.scope, node.name]),
                            decorator=expression(decorator, self.source), callable=resolved)
            elif terminal == "action":
                self.record("router_action_candidate", decorator, name=".".join([*self.scope, node.name]),
                            decorator=expression(decorator, self.source),
                            **(self.arguments(decorator) if isinstance(decorator, ast.Call) else {}))
        self.scope.append(node.name)
        self.generic_visit(node)
        self.scope.pop()

    visit_AsyncFunctionDef = visit_FunctionDef

    def visit_Assign(self, node: ast.Assign) -> None:
        targets = [ast.unparse(target) for target in node.targets]
        if any(target in {"urlpatterns", "websocket_urlpatterns"} for target in targets):
            self.record("pattern_assembly", node, targets=targets, value=expression(node.value, self.source))
        self.generic_visit(node)

    def visit_AugAssign(self, node: ast.AugAssign) -> None:
        target = ast.unparse(node.target)
        if target in {"urlpatterns", "websocket_urlpatterns"}:
            self.record("pattern_assembly", node, targets=[target], operation=type(node.op).__name__,
                        value=expression(node.value, self.source))
        self.generic_visit(node)


def inspect_source(path: str, source: str) -> list[dict[str, Any]]:
    visitor = CandidateVisitor(path, source)
    visitor.visit(ast.parse(source, filename=path))
    return sorted(visitor.records, key=lambda record: (record["line"], record["kind"], record.get("name", "")))


def generate(root: Path, baseline_ref: str = DEFAULT_BASELINE) -> dict[str, Any]:
    records: list[dict[str, Any]] = []
    tests: list[str] = []
    errors: list[dict[str, str]] = []
    digest = hashlib.sha256()
    files = sorted(path for directory in ("apps", "core", "dispatcharr")
                   for path in (root / directory).rglob("*.py") if "__pycache__" not in path.parts)
    for path in files:
        relative = path.relative_to(root).as_posix()
        raw = path.read_bytes()
        digest.update(relative.encode() + b"\0" + raw + b"\0")
        if path.name.startswith("test") or "tests" in path.relative_to(root).parts:
            if path.name.startswith("test") and path.name != "__init__.py":
                tests.append(relative)
            continue
        try:
            records.extend(inspect_source(relative, raw.decode("utf-8-sig")))
        except (SyntaxError, UnicodeDecodeError) as error:
            errors.append({"path": relative, "error": str(error)})
    try:
        head = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True, stderr=subprocess.DEVNULL).strip()
    except (OSError, subprocess.CalledProcessError):
        head = None
    counts = dict(sorted(Counter(record["kind"] for record in records).items()))
    counts["test_files"] = len(tests)
    return {"format_version": 1, "inventory_type": "static_source_candidates",
            "baseline_ref": baseline_ref, "source_head": head,
            "source_sha256": digest.hexdigest(), "source_file_count": len(files),
            "limitations": LIMITATIONS, "counts": counts, "parse_errors": errors,
            "candidates": records, "test_files": tests}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--baseline-ref", default=DEFAULT_BASELINE)
    parser.add_argument("--output", type=Path, help="Write JSON here; otherwise print JSON to stdout")
    parser.add_argument("--summary", action="store_true", help="Print counts instead of full JSON when --output is absent")
    args = parser.parse_args()
    result = generate(args.root.resolve(), args.baseline_ref)
    serialized = json.dumps(result, indent=2, ensure_ascii=False) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(serialized)
    if args.summary:
        print(json.dumps({"inventory_type": result["inventory_type"], "counts": result["counts"],
                          "parse_errors": result["parse_errors"], "source_sha256": result["source_sha256"]}, indent=2))
    elif not args.output:
        print(serialized, end="")
    return 1 if result["parse_errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
