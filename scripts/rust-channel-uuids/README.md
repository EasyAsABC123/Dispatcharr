# Channel lookup by UUID

Original Dispatcharr authors retain all application/algorithm credit and original
licenses. This ports their `ChannelViewSet.get_by_uuids` JSON POST action used by
the unchanged React Stats page. Image47 returned405 for that request; the new
route performs the existing channel serialization and user visibility checks.

[The component comparison](verified-channel-uuids-1.json) passes39 actual
Original/native HTTP cases on the same disposable PostgreSQL rows. It covers
admin/standard/viewer/inactive/anonymous/invalid-token requests, assigned profile
membership, hidden/adult/level-restricted channels, missing/duplicate/reversed
UUIDs, canonical/hex/uppercase/braced/URN forms, integer and malformed inputs,
full stream projection and irrelevant list filtering/pagination parameters.
Seven table fingerprints remain unchanged across both request sets.

The action starts from the requested channel queryset and shared ACL, rather
than management-list filters. Its hidden requested channel stays visible when
authorized, and it returns an unpaginated list. Serializer joins originally
reordered the selected rows in the first native comparison. A materialized
selection now retains its ordinal through projection; expected responses are
never sorted to make comparisons pass. The Original queryset itself declares
no ordering, so its order across different PostgreSQL plans/datasets remains
unspecified; this evidence covers the captured plan/data, not a universal order.
Duplicate handling uses a set to avoid quadratic work on repeated IDs.

Comparisons require exact HTTP status/content type and ordered JSON values, or
exact default Django HTML500 text. They do not assert raw JSON key/whitespace
identity. The captured domain is JSON POST; complete method/OPTIONS, renderer,
form-body, Unicode UUID grammar, concurrent changes, broad fault/resource and
packaged browser acceptance remain open. This component establishes no new
performance gain or complete M2/M5 acceptance.

## Reproduction

Use a clean checkout containing this component. Python3, Docker Compose and the
locked Rust toolchain/dependency cache are required. From the repository root:

```sh
python3 scripts/rust-channel-uuids/run.py \
  --target artifacts/rust/target-channel-uuids-NEW \
  --output artifacts/rust/channel-uuids-NEW
```

The runner hashes its exact Rust/support inputs, archives the pinned unchanged
Original, creates only a disposable owned fixture and captures all39 responses
through actual Original uWSGI HTTP. It then runs strict Store all-target Clippy
and the ignored native TCP/SQL comparison. A unique new target avoids stale
compiled-code provenance; preserve its binary hash with results. The new output
must not exist. Fixture JWTs remain in memory; published source cases contain no
tokens or passwords. Every owned container/network/volume is removed and checked.

The seven fingerprint table identifiers come only from the guarded test fixture;
they are not production query input. `source-cases.json` is the recorded Original
corpus; it cannot replace a real disposable database with matching rows. Ordinary
workspace runs intentionally skip this external-fixture test. The earlier
profile-membership setup collision and actual native ordering mismatch remain
explicit negative attempts in the evidence record.
