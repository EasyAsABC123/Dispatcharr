"""Offline checks of the exact staged Main group fixture."""
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
import unittest

import xc_account_group_patch as probe
import workflow

IDS = {"account": 11, "target": 21, "sibling": 22, "unrelated": 23, "profile": 31}

def relations(target, sibling):
    return {"groups":{
        "owned_target":{"enabled":target,"marker_kept":True,"last_seen":probe.GROUP_TIME},
        "owned_sibling":{"enabled":sibling,"marker_kept":True,"last_seen":probe.GROUP_TIME},
        "foreign_unrelated":{"enabled":True,"marker_kept":True,"last_seen":probe.GROUP_TIME},
    },"count":3,"profile_unchanged":True}

def body(target, sibling):
    return {
        "id":IDS["account"],"created_at":probe.ACCOUNT_TIME,
        "channel_groups":[
            {"channel_group":IDS["target"],"enabled":target,"auto_channel_sync":True,
             "auto_sync_channel_start":12.5,"auto_sync_channel_end":20.5,
             "custom_properties":{"fixture_marker":"target"},"last_seen":probe.GROUP_TIME},
            {"channel_group":IDS["sibling"],"enabled":sibling,"auto_channel_sync":False,
             "custom_properties":{"fixture_marker":"sibling"},"last_seen":probe.GROUP_TIME},
        ],
        "profiles":[{"id":IDS["profile"],"is_default":False,"is_active":True,"max_streams":3,
             "search_pattern":"(.*)","replace_pattern":"$1","custom_properties":{"fixture_marker":"profile"}}],
    }

class Fixture:
    case=probe.CASE
    backend="Original"
    def __init__(self, duplicates, orphans, activity):
        self.binding = {
            "xc_group_patch_duplicates": duplicates,
            "xc_group_patch_orphan_modes": orphans,
            "xc_group_patch_active_toggle": activity,
        }
    def sql(self,source,*,secret):
        assert secret
        return False if "ChannelGroup.objects.filter" in source else IDS

class Tests(unittest.TestCase):
    def execute_case(self, duplicate, wrong_error=False, account_side_effect=False,
                     orphans=False, orphan_side_effect=False, activity=False, retain_notice=False):
        initial,selected,opposite=relations(True,True),relations(False,True),relations(True,False)
        live = deepcopy(initial)
        account = {
            "account_type": "XC", "is_active": False, "status": "idle",
            "custom_properties": {**probe.ACCOUNT_PROPERTIES, **probe.INITIAL_PREFERENCES},
        }
        effects = {
            "enabled": False, "task_id": 91, "cron": probe.ACTIVITY_CRON, "timezone": "UTC",
            "provider_task": True, "kwargs_match": True,
            "notifications": {"owned_warning": True, "owned_expiration": True, "foreign_warning": True},
        }

        def setup_activity(_f, ids):
            assert ids == IDS
            account.update(status="disabled", refresh_interval=12, refresh_task_id=91, cron_expression=probe.ACTIVITY_CRON)
            return {key: str(i) for i, key in enumerate(effects["notifications"])}

        seen = []
        def response_body(target, sibling):
            return {
                **body(target, sibling), **account,
                **account["custom_properties"],
            }
        def request(_f,method,path,value=None):
            assert path=="/api/m3u/accounts/11/"
            if method=="GET":
                return 200,response_body(live["groups"]["owned_target"]["enabled"],live["groups"]["owned_sibling"]["enabled"])
            if value == probe.PREFERENCES:
                seen.append(value)
                account["custom_properties"].update(value)
                if account_side_effect:
                    account["status"] = "success"
                return 200, response_body(True, False)
            if "custom_properties" in value:
                assert set(value) == {"custom_properties"}
                previous = account["custom_properties"]
                changed = value["custom_properties"]
                assert changed == {**previous, "orphan_channel_cleanup": changed["orphan_channel_cleanup"]}
                seen.append(value)
                account["custom_properties"] = dict(changed)
                if orphan_side_effect:
                    account["status"] = "success"
                return 200, response_body(True, False)
            if "is_active" in value:
                assert set(value) == {"is_active"} and type(value["is_active"]) is bool
                current = value["is_active"]
                seen.append(value)
                account.update(is_active=current, status="idle" if current else "disabled")
                effects["enabled"] = current
                if not current:
                    effects["notifications"]["owned_warning"] = retain_notice
                    effects["notifications"]["owned_expiration"] = False
                return 200, response_body(True, False)
            rows=value["channel_groups"]
            seen.append(rows)
            if len(rows)==2:
                live.update(deepcopy(selected))
                return 200,response_body(False,True)
            if len(rows)==4:
                invalid={str(i):{"channel_group":[f'Invalid pk "{r["channel_group"]}" - object does not exist.']}
                         for i,r in enumerate(rows) if r["channel_group"] not in IDS.values()}
                return (400,{"channel_groups":invalid}) if not wrong_error else (501,None)
            assert len(rows)==5 and [r["enabled"] for r in rows]==[True,False,False,False,True]
            live.update(deepcopy(opposite))
            return 200,response_body(True,False)
        with patch.multiple(
            probe, request=request, snapshot=lambda *_: deepcopy(live),
            preferences_snapshot=lambda *_: deepcopy(account),
            setup_activity=setup_activity,
            schedule_and_notifications=lambda *_: deepcopy(effects),
        ):
            out=[]
            result=probe.run(Fixture(duplicate, orphans, activity),out.append)
        assert result==out[0]
        return result,seen

    def test_reference_only(self):
        result,requests=self.execute_case(False)
        self.assertTrue(result["passed"])
        self.assertEqual(len(requests),2)
        self.assertTrue(result["missing_group_references"]["exact_sparse_error"])

    def test_both_cases(self):
        result,requests=self.execute_case(True)
        self.assertTrue(result["passed"])
        self.assertEqual(len(requests),4)
        self.assertTrue(result["duplicate_group_selections"]["sql_first_values_match"])
        self.assertTrue(result["group_filter_preferences"]["sql_only_preferences_changed"])
        self.assertTrue(result["group_filter_preferences"]["sql_relationships_unchanged"])

    def test_preference_followup_rejects_sql_side_effect(self):
        result, _ = self.execute_case(True, account_side_effect=True)
        self.assertEqual(result["group_filter_preferences"]["status"], 200)
        self.assertFalse(result["group_filter_preferences"]["sql_only_preferences_changed"])
        self.assertFalse(result["passed"])

    def test_orphan_cleanup_modes_persist(self):
        result, requests = self.execute_case(True, orphans=True)
        self.assertTrue(result["passed"])
        self.assertEqual(len(requests), 7)
        self.assertEqual([mode["mode"] for mode in result["orphan_cleanup_modes"]], list(probe.ORPHAN_MODES))
        self.assertTrue(all(mode["sql_only_orphan_mode_changed"] for mode in result["orphan_cleanup_modes"]))

    def test_orphan_cleanup_sql_side_effect_fails(self):
        result, _ = self.execute_case(True, orphans=True, orphan_side_effect=True)
        self.assertFalse(result["passed"])
        self.assertTrue(all(mode["status"] == 200 for mode in result["orphan_cleanup_modes"]))
        self.assertFalse(any(mode["sql_only_orphan_mode_changed"] for mode in result["orphan_cleanup_modes"]))

    def test_activity_preserves_schedule_and_removes_only_owned_notices(self):
        result, requests = self.execute_case(True, orphans=True, activity=True)
        self.assertTrue(result["passed"])
        self.assertEqual(requests[-2:], [{"is_active": True}, {"is_active": False}])
        self.assertTrue(all(step["sql_schedule_matches"] for step in result["active_toggle"]))
        self.assertTrue(all(step["sql_notifications_match"] for step in result["active_toggle"]))

    def test_activity_leftover_notice_rejects_a_200_response(self):
        result, _ = self.execute_case(True, orphans=True, activity=True, retain_notice=True)
        self.assertFalse(result["passed"])
        self.assertEqual(result["active_toggle"][-1]["status"], 200)
        self.assertFalse(result["active_toggle"][-1]["sql_notifications_match"])

    def test_wrong_error_fails_before_duplicate(self):
        result,requests=self.execute_case(True,True)
        self.assertFalse(result["passed"])
        self.assertEqual(len(requests),2)

class WorkflowBindingTests(unittest.TestCase):
    def test_selected_context_dispatches_the_same_probe_and_quiets_original_beat(self):
        for backend in ("Original", "Native"):
            with self.subTest(backend=backend):
                actions = []

                class BoundFixture:
                    binding = {"execution_contexts": {backend: [probe.CASE]}}
                    functional_budget_expired = staticmethod(lambda: False)
                    actual_isolation_valid = staticmethod(lambda: True)
                    retained_native_case = retained_source_case = staticmethod(lambda name: None)

                    @contextmanager
                    def independent_context(self, name, *, start_consumers):
                        self.case = name
                        actions.append(("context", name, start_consumers))
                        yield self

                    def role(self, name, args, *, stop):
                        actions.append(("role", name, stop))

                fixture = BoundFixture()
                fixture.backend = backend

                def run_selected(f, write):
                    actions.append(("probe", f.backend))
                    result = {"passed": True}
                    write(result)
                    return result

                with TemporaryDirectory() as tmp, patch.object(probe, "run", run_selected):
                    output = Path(tmp) / backend
                    verdict = workflow.Workflow(fixture, output).run()
                    self.assertTrue(verdict["executed_slice_passed"])
                    self.assertEqual(verdict["cases"], [{"case": probe.CASE, "status": "passed"}])
                    self.assertTrue((output / (probe.CASE + ".json")).is_file())
                expected = [("context", probe.CASE, True)]
                if backend == "Original":
                    expected.append(("role", "beat", True))
                self.assertEqual(actions, [*expected, ("probe", backend)])


if __name__=="__main__":
    assert hasattr(workflow.Workflow,"xc_account_group_patch")
    unittest.main()
