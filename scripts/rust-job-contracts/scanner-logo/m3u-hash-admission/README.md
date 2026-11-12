# Bounded M3U hash admission and SQL text projection

Original Dispatcharr and dependency authors retain their code, algorithms, licenses and attribution. This component adds a read-only admission check to the existing inert M3U service. Main/default consumers remain off; this is not whole-scanner or whole-M6 acceptance.

The inherited conditional Single/nonempty-entry hash getter ran after file parsing and could use logical cache-error fallback/cachefill. The selected revision replaces that getter with one owned pre-ACK check for both groups and single work. An atomic TYPE/STRLEN/GET operation bounds each settings cache response before transfer. A cached dictionary, including {}, is authoritative. Cache uncertainty refuses the frame without SQL fallback. On a miss/nonobject value, bounded server-side SQL projection plus before/after version observations supplies only the hash key. No cache fill occurs. Missing, JSON null and empty-string effective keys are admitted; other types, nonempty keys and account custom-property overrides are refused. This conservative domain does not implement general Source hash parsing, cache-fill/outage behavior, or concurrent configuration parity.

The account query separately projects last_message only when its UTF-8 octet length is at most8192. Oversize values return a small row and refuse before ACK. This bounds that TEXT transfer; it does not establish a bound on general database scanning, CPU, all columns or concurrency.

Actual Source reference contains70 checks/25 cases:22 model/cache lookup probes, one held cache response and two real worker flows. The additional13-check real worker witness qualifies Single JSON null. These roles must not be conflated. Source provenance and tools are published separately under ../m3u-hash-reference.

The selected final runtime was exercised through actual Original scanner publication, native groups/single execution or retained refusal followed by real Original fallback, and Original AsyncResult/effect observation:176 checks/14 cases. Cache-reply and SQL ErrorResponse/held ReadyForQuery controls passed32 checks/2 cases on the same final ELF. Both retain caller, admission and physical work after caller abort/logical timeout, with no early ACK or result. Actual last_message cases include null,8192,8193 and262144 bytes. All fixtures use copied disposable databases and leave the default database unchanged.

Raw cache snapshots remain complete. Comparison excludes only the exact live:worker:<host>:<numericPID>:heartbeat shape, whose independent activity caused the retained second-controller false negative. Settings, task locks, queues and result keys remain compared. Inherited integration.json scope strings are preserved as raw history; the counts and roles above describe this new admission/physical fixture accurately.

## Rebuild and replay

From a repository containing the immutable base and published Source reference, with Rust1.90/cached locked dependencies and disposable Docker fixture access:

```sh
T=scripts/rust-job-contracts/scanner-logo/m3u-hash-admission
python "$T/reconstruct.py" --repo . --output /tmp/m3u-hash-source
python "$T/build_peer.py" --source /tmp/m3u-hash-source --target /tmp/m3u-hash-target --output /tmp/m3u-hash-binary
python "$T/test_reconstruct.py" --repo .
python "$T/test_acceptance.py"
python "$T/physical/test_acceptance.py"
python "$T/test_build_peer.py" --source /tmp/m3u-hash-source
python "$T/run_pair.py" --repo . --support-root scripts/rust-core-perf/support --native-binary /tmp/m3u-hash-binary/dispatcharr_job_transport --reference scripts/rust-job-contracts/scanner-logo/m3u-hash-reference/source-hash-1.json --output /tmp/m3u-hash-pair
python "$T/physical/run_pair.py" --repo . --support-root scripts/rust-core-perf/support --native-binary /tmp/m3u-hash-binary/dispatcharr_job_transport --reference scripts/rust-job-contracts/scanner-logo/m3u-hash-reference/source-hash-1.json --output /tmp/m3u-hash-physical
```

The public reconstruction, fresh build,176/14 actual replay and32/2 physical replay were executed successfully on a new source-bound ELF7e791fe05b4ccfc0b2be657c1003f3c0852cce85e17bc65197251ff517b34851. Independent owned-project inventories were empty afterward. This is distinct from the original d840 final-source proof.

Outputs must be fresh paths. Reconstruction uses immutable Git base6ae5461de965ae50b54ff87c27d9e7f5cf4059de plus three packaged hash-bound overlays; it has no private-source fallback. build_peer.py verifies all788 inputs before and after compilation and copies both fixture ELFs outside generated targets. Public support requires all31 critical hashes. Source controls10, physical controls8, reconstruction corruption controls13 and build controls5 reject invalid captures/closure changes. Test files with argparse inputs are executable commands rather than generic unittest discovery modules.

## Retained negatives and open scope

Private1 retained the old conditional getter and is unqualified. Private2 removed it but predates the TEXT projection. Native controller attempt1 failed on unsupported Redis snapshot types; attempt2 reached the correct refusal but failed solely on an unrelated worker heartbeat. Both raw attempts remain retained. Physical1 passed on historical private2; physical2 is the accepted final-source proof. A test-discovery invocation without the build-control required source argument failed before any build; the documented direct command passed.

Elapsed rounding, general hash normalization, account overrides, general errors, restart semantics, Main integration, packaged UID/mounts, broad timeout policy and complete scanner/task-family coverage remain open. No resource/performance or milestone-completion claim follows from this packet. Existing audit helper ordering, caught-error and stable-configuration limitations remain unchanged.
