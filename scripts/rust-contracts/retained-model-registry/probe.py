"""Observe pinned Original Django class registration without opening a database."""
from importlib import import_module
import hashlib
import inspect
import json
from pathlib import Path

import django
from django.apps import apps
from django.db.backends.signals import connection_created
from django.db.models import Manager, Model, QuerySet


def relation(field):
    target = getattr(field, "related_model", None)
    remote = getattr(field, "remote_field", None)
    deletion = getattr(remote, "on_delete", None)
    return {
        "target": target._meta.label if target is not None else None,
        "on_delete": getattr(deletion, "__name__", None),
    }


def model_shape(cls):
    meta = cls._meta
    assert apps.get_model(meta.app_label, meta.model_name) is cls
    assert meta.pk is not None and meta.db_table
    fields = [
        {
            "name": field.name,
            "type": field.get_internal_type(),
            "column": getattr(field, "column", None),
            "nullable": field.null,
            "blank": field.blank,
            "primary_key": field.primary_key,
            "unique": field.unique,
            **relation(field),
        }
        for field in [*meta.local_fields, *meta.local_many_to_many]
    ]
    assert len({field["name"] for field in fields}) == len(fields)
    return {
        "kind": "django-registered-model",
        "label": meta.label,
        "table": meta.db_table,
        "primary_key": meta.pk.name,
        "fields": fields,
        "index_names": sorted(index.name for index in meta.indexes),
        "constraint_names": sorted(constraint.name for constraint in meta.constraints),
    }


def manager_shape(cls):
    is_manager = issubclass(cls, Manager)
    owners = {
        model._meta.label
        for model in apps.get_models()
        for manager in (model._default_manager, model._base_manager, *model._meta.managers)
        if (isinstance(manager, cls) if is_manager else getattr(manager, "_queryset_class", None) is cls)
    }
    assert owners, cls.__name__
    return {
        "kind": "django-manager" if is_manager else "django-queryset",
        "bound_models": sorted(owners),
        "declared_methods": sorted(
            name for name, attr in cls.__dict__.items()
            if inspect.isfunction(attr) and not name.startswith("_")
        ),
    }


def main():
    opened_connections = []

    def on_connect(sender, connection, **kwargs):
        opened_connections.append(connection.alias)

    connection_created.connect(on_connect)
    django.setup()
    declarations = json.loads(Path("/fixture/inputs.json").read_bytes())
    output = []
    for declaration in declarations:
        rel = declaration["path"]
        filename = Path("/app") / rel
        assert hashlib.sha256(filename.read_bytes()).hexdigest() == declaration["source_sha256"], rel
        cls = getattr(import_module(rel.removesuffix(".py").replace("/", ".")), declaration["name"])
        assert inspect.getsourcefile(cls) == str(filename), rel
        assert inspect.getsourcelines(cls)[1] == declaration["line"], cls.__name__
        if issubclass(cls, Model):
            shape = model_shape(cls)
        else:
            assert issubclass(cls, (Manager, QuerySet)), cls.__name__
            shape = manager_shape(cls)
        output.append({"candidate_id": declaration["candidate_id"], **shape})
    assert not opened_connections, opened_connections
    print(json.dumps({"django": django.get_version(), "database_connections": 0, "observations": output}, sort_keys=True))


if __name__ == "__main__":
    main()
