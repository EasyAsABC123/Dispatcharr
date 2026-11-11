# Original ten-channel finite-provider reference

Original Dispatcharr authors retain application/algorithm credit and licenses.
This Source-only functional fixture uses the existing ten deterministic channels
plus one explicitly owned overflow channel and local origin. The real default
provider profile is saved withmax_streams10. It is separate from the earlier
warmed ten-channel load-smoke, whose provider0 remained unlimited; no historical
workload or result is relabeled.

Ten distinct channels plus one same-channel follower have11clients,10provider
slots and10origins. An eleventh distinct channel returns503 with no new origin.
Closing one distinct channel initially leaves11/10/10; actual Source release
then produces10clients/9slots/9origins. The overflow channel is now admitted,
returning11/10/10. All local bodies closing still leaves11/10/10 immediately;
70-second retention reaches0/0/0 in the same containers. No counterDECR, fake
grant or Redisreset manufactures admission. All12captured bodies decode audio/
video with at least50frames. Thirty actual checks/seven offline controls pass;
owned containers/network/volumes are independently absent.

CPU/memory service limits remain unset; no resource sampler or performance
comparison runs. AbsoluteCPU/memory/FD/socket/growth ceilings remain unmeasured
and null. Remaining metadata/history observations are recorded, not silently
normalized into a new cleanup budget. This is configured provider capacity
accounting, not fullM0, Native or universal-capacity acceptance. The unresolved
streaming method contract ID remains separate from observedGET.

Fresh-clone controls:

```sh
PYTHONPATH=scripts/rust-playback-baseline python3 -m unittest discover \
  -s scripts/rust-playback-baseline -p test_finite_ten.py
```

Explicitly coordinated owned functional replay with Docker/Compose, immutable
Original history/image and new empty output:

```sh
python3 scripts/rust-playback-baseline/finite_ten.py \
  --policy scripts/rust-playback-baseline/finite-ten-policy.json \
  --output artifacts/rust/m0-finite-ten-new
```

`verified-source-finite-ten-1.json` is the sanitized recorded attestation with
private raw/capture/decoder hashes; offline controls do not replay those raw
captures. Source failures are preserved without automatic retry or tuning.
