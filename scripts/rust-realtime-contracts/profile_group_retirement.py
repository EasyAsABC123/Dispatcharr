"""Attribute each sampled Updates group member by when its owned client joined."""
import re
import time


def members(snapshot):
    fingerprints = snapshot["member_sha256"]
    if (type(snapshot["members"]) is not int or type(snapshot["ttl"]) is not int
            or not isinstance(fingerprints, list) or snapshot["members"] != len(fingerprints)
            or not all(isinstance(value, str) and re.fullmatch(r"[a-f0-9]{64}", value)
                       for value in fingerprints) or len(set(fingerprints)) != len(fingerprints)
            or len(fingerprints) > 16):
        raise ValueError("The owned Updates group snapshot is invalid")
    return set(fingerprints)


def wait_for_members(observe, expected, seconds=5):
    deadline = time.monotonic() + seconds
    while True:
        observation = observe()
        if members(observation) == expected or time.monotonic() >= deadline:
            return observation
        time.sleep(0.05)


def qualify(original, native, original_after_process, held, after_shutdown):
    base = members(original["before_open"])
    source = members(original["open"]) - base
    original_closed = members(original["immediate_after_close"])
    original_waited = members(original["before_daphne_termination"])
    original_left = members(original_after_process)
    native_base = members(native["before_open"])
    readers = members(native["open"]) - native_base
    native_closed = members(native["immediate_after_close"])
    native_waited = members(native["after_close_wait"])
    holding = members(held) - native_waited
    final = members(after_shutdown)
    stages = {
        "initial_group_empty": not base and original["before_open"]["ttl"] == -2,
        "original_three_owned_members": len(source) == 3 and base <= members(original["open"]),
        "original_client_close_settle": not original_waited and original["before_daphne_termination"]["ttl"] == -2,
        "original_identity_bound": original_left <= original_waited <= original_closed <= (source | base),
        "original_process_exit_settle": not original_left and original_after_process["ttl"] == -2,
        "native_before_open_matches_original_exit": native_base == original_left,
        "native_three_distinct_owned_members": len(readers) == 3 and not (readers & source)
        and native_base <= members(native["open"]),
        "native_profile_client_retirement": native_waited == native_base,
        "native_client_close_identity_bound": native_waited <= native_closed <= (readers | native_base),
        "held_distinct_native_member": len(holding) == 1 and not (holding & (source | readers))
        and native_waited <= members(held),
        "native_held_retired_on_shutdown": not (holding & final),
        "no_native_member_after_shutdown": not ((holding | readers) & final),
        "global_group_retired": not final and after_shutdown["ttl"] == -2,
    }
    remaining = {
        "original_profile": sorted(final & source),
        "native_profile": sorted(final & readers),
        "native_held": sorted(final & holding),
        "unattributed": sorted(final - source - readers - holding),
    }
    return {"passed": all(stages.values()), "checks": stages, "remaining": remaining}
