import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location("contract_inventory", Path(__file__).with_name("inventory.py"))
inventory = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(inventory)


class InventoryTests(unittest.TestCase):
    def inspect(self, source, path="apps/example/urls.py"):
        return inventory.inspect_source(path, source)

    def test_aliases_dynamic_routes_and_nested_includes_are_preserved(self):
        records = self.inspect('''from django.urls import path as route, include as mount
prefix = settings.API_PREFIX
urlpatterns = [route(prefix + "/", mount((build_urls(), "example")), name=route_name)]
''')
        route = next(item for item in records if item["kind"] == "route")
        self.assertEqual(route["callable"], "django.urls.path")
        self.assertEqual(route["arguments"][0], {"expression": 'prefix + "/"'})
        self.assertEqual(route["keywords"][0], {"name": "name", "expression": "route_name"})
        include = next(item for item in records if item["kind"] == "include")
        self.assertEqual(include["arguments"][0]["expression"], '(build_urls(), "example")')
        self.assertEqual(route["line"], 3)
        self.assertEqual(route["path"], "apps/example/urls.py")

    def test_router_registration_actions_and_pattern_assembly(self):
        records = self.inspect('''from rest_framework.decorators import action
router.register(r"items", ItemViewSet, basename="item")
urlpatterns = []
urlpatterns += router.urls
class ItemViewSet(BaseViewSet):
    @action(detail=False, methods=["post"], url_path="bulk")
    def bulk(self, request): pass
''')
        registration = next(item for item in records if item["kind"] == "router_registration")
        self.assertEqual(registration["arguments"][0]["literal"], "items")
        action = next(item for item in records if item["kind"] == "router_action_candidate")
        self.assertEqual(action["name"], "ItemViewSet.bulk")
        self.assertEqual(action["keywords"][1]["literal"], ["post"])
        assemblies = [item for item in records if item["kind"] == "pattern_assembly"]
        self.assertEqual(len(assemblies), 2)
        self.assertEqual(assemblies[1]["value"]["expression"], "router.urls")

    def test_tasks_with_and_without_arguments_and_aliased_decorators(self):
        records = self.inspect('''from celery import shared_task as background
@background
async def first(): pass
@app.task(bind=True, autoretry_for=(TemporaryFailure,))
def second(self): pass
@background(queue=settings.QUEUE)
def third(): pass
''', "apps/example/tasks.py")
        tasks = [item for item in records if item["kind"] == "task_candidate"]
        self.assertEqual([task["name"] for task in tasks], ["first", "second", "third"])
        self.assertEqual(tasks[0]["callable"], "celery.shared_task")
        self.assertIn("settings.QUEUE", tasks[2]["decorator"]["expression"])

    def test_model_serializer_command_and_websocket_candidates(self):
        models = self.inspect('''from django.db import models
class Item(models.Model):
    class Meta: pass
''', "apps/example/models.py")
        self.assertEqual([item["name"] for item in models if item["kind"] == "model_candidate"], ["Item"])
        serializer = self.inspect("class ItemSerializer(BaseSerializer): pass", "apps/example/serializers.py")
        self.assertEqual(serializer[0]["kind"], "serializer_candidate")
        command = self.inspect("class Command(BaseCommand): pass", "apps/example/management/commands/refresh.py")
        self.assertEqual(command[0]["command"], "refresh")
        sockets = self.inspect('from django.urls import path\nwebsocket_urlpatterns = [path("ws/", Consumer.as_asgi())]', "dispatcharr/routing.py")
        self.assertEqual(next(item for item in sockets if item["kind"] == "websocket_route")["arguments"][0]["literal"], "ws/")

    def test_migration_dependencies_remain_dynamic(self):
        records = self.inspect('''class Migration(migrations.Migration):
    initial = True
    dependencies = [migrations.swappable_dependency(settings.AUTH_USER_MODEL)]
    operations = []
''', "apps/example/migrations/0001_initial.py")
        migration = next(item for item in records if item["kind"] == "migration")
        self.assertEqual(migration["attributes"]["initial"]["literal"], True)
        self.assertNotIn("literal", migration["attributes"]["dependencies"])

    def test_generation_is_deterministic_and_does_not_import_application(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            app = root / "apps" / "example"
            app.mkdir(parents=True)
            (app / "urls.py").write_text('raise RuntimeError("must not execute")\nfrom django.urls import path\nurlpatterns = [path("a/", missing_view)]\n')
            tests = app / "tests"
            tests.mkdir()
            (tests / "test_example.py").write_text('this is intentionally not parsed')
            first = inventory.generate(root, "baseline")
            second = inventory.generate(root, "baseline")
            self.assertEqual(json.dumps(first), json.dumps(second))
            self.assertEqual(first["counts"]["route"], 1)
            self.assertEqual(first["counts"]["test_files"], 1)
            self.assertFalse(first["parse_errors"])
            self.assertEqual(first["baseline_ref"], "baseline")
            (app / "urls.py").write_text("invalid !!!")
            invalid = inventory.generate(root)
            self.assertEqual(len(invalid["parse_errors"]), 1)
            self.assertNotEqual(first["source_sha256"], invalid["source_sha256"])


if __name__ == "__main__":
    unittest.main()
