# Original finite provider capacity reference

Original Dispatcharr authors retain all application/algorithm credit and licenses.
This Source-only functional fixture uses the actual default provider model save
to set max_streams1. It leaves the earlier unlimited0 fixture/results unchanged.

Two readers of one channel use one provider slot/upstream. A second distinct
channel returns503 without opening another origin. After the first local body
closes, Source still reports both clients while the sibling continues. Both
local bodies closing leaves one client/upstream initially; the actual slot then
returns to0 and the other channel is admitted. After its close and70-second
retention, clients/provider/upstreams are all0. Three separate captured bodies
have clean video/audio decode. Twelve functional checks/five offline controls
pass, and owned containers/network/volumes are independently absent.

This is an actual configured finite provider boundary, not a cgroup capacity or
performance test. CPU/memory service caps are unset, there is no resource sampler
and absolute memory/CPU/growth ceilings remain null. The unresolved streaming
contract ID is preserved separately from observedGET. This does not close full
M0 or later Native milestones. Local close is not peer acknowledgement.

Public offline controls need no private capture:

```sh
PYTHONPATH=scripts/rust-playback-baseline python3 -m unittest discover \
  -s scripts/rust-playback-baseline -p test_finite_capacity.py
```

For an explicitly coordinated new functional replay, Docker/Compose, immutable
Original Git history/image and a new empty output are required:

```sh
python3 scripts/rust-playback-baseline/finite_capacity.py \
  --policy scripts/rust-playback-baseline/finite-capacity-policy.json \
  --output artifacts/rust/m0-finite-capacity-new
```

`verified-source-finite-capacity-1.json` is a sanitized recorded attestation with
exact private raw hashes. Offline controls are not a replay of those captures.
No automatic retry or policy tuning is performed if Source fails.
