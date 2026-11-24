"""Engine-free checks for the declaration and roster projection."""
from functools import wraps
from inspect import iscoroutinefunction
from types import SimpleNamespace
import unittest

from runtime_dispatch_methods import declarations, reconcile


def framework_require(request_method_list):
    def decorate(function):
        if iscoroutinefunction(function):
            @wraps(function)
            async def async_wrapper(request):
                if request in request_method_list:
                    return await function(request)
            return async_wrapper

        @wraps(function)
        def wrapper(request):
            if request in request_method_list:
                return function(request)
        return wrapper
    return decorate


class Methods(unittest.TestCase):
    def test_class_handlers_with_head_alias_and_excluded_method(self):
        class View:
            http_method_names = ["get", "head", "options"]

            def get(self, request):
                pass

            def post(self, request):
                pass

            def options(self, request):
                pass

        self.assertEqual(declarations(SimpleNamespace(view_class=View), framework_require),
                         ("Django_class_handlers", {"GET": "get", "HEAD": "get", "OPTIONS": "options"}))

    def test_preserves_viewset_router_action_names(self):
        callback = SimpleNamespace(actions={"get": "retrieve", "patch": "partial_update"})
        self.assertEqual(declarations(callback, framework_require),
                         ("DRF_router_actions", {"GET": "retrieve", "PATCH": "partial_update"}))

    def test_follows_both_framework_wrappers(self):
        async def read(request):
            return request

        for callback in (lambda request: request, read):
            declared = framework_require(["POST", "GET"])(callback)

            @wraps(declared)
            def wrapped(request):
                return declared(request)

            self.assertEqual(declarations(wrapped, framework_require),
                             ("Django_explicit_method_decorator", {
                                 "GET": "wrapped_function", "POST": "wrapped_function"}))

    def test_plain_function_stays_unclassified(self):
        self.assertEqual(declarations(lambda request: None, framework_require),
                         ("dynamic_or_undeclared", {}))

    def test_retains_original_ordinals_and_rejects_drift(self):
        row = {"pattern": "/a", "name": "a", "view": "owner.read", "methods": None}
        self.assertEqual(reconcile([row], [row]), [{"resolver_ordinal": 0, **row}])
        with self.assertRaisesRegex(ValueError, "resolver roster differs"):
            reconcile([row], [row | {"view": "owner.changed"}])
        with self.assertRaisesRegex(ValueError, "action map differs"):
            reconcile([row], [row | {"methods": {"get": "read"}}])


if __name__ == "__main__":
    unittest.main()
