# Original sustained post-close accounting

Original Dispatcharr authors retain application/algorithm credit and licenses.
This Source-only functional reference uses the existing authenticated continuous
reader protocol:15seconds warmup,60seconds active, one request, bounded2MiB
prefix and actual audio/video decoder. One reader received15096212active bytes,
HTTP200, no errors/EOF, and decoded72frames. It then explicitly closed the
HTTPResponse and connection before recording the local physical-close receipt.
The same containers remain alive for70seconds of retention. Callbacks record
fresh SQL profile config, known Redis clients/provider/owner/metadata/history
and origin state. The warmed-before state has0clients/1origin from the existing
primer; active and immediate-after-close have1client/1origin; retention has0.
Unlimited profile0 means provider counter0 throughout, not no physical upstream.

The selected services retain their finite configured limits: Source2CPU/512MiB,
PostgreSQL+Redis1CPU/1GiB, origin2CPU/512MiB. No CPU/memory/FD/socket sampler runs;
no resource footprint or growth acceptance follows. Absolute ceilings staynull.
Onlyone sustained reader has this same-run retention witness; historical1/10/100
resource data is independently bound and does not inherit post-close coverage.
This does not close fullM0 or any Native comparison. Owned containers/network/
volumes are independently absent after completion.

All helper/policy/test/proof files live in the new
`scripts/rust-baseline/sustained` namespace. The workload is an explicit copy of
the old reader with an opt-in physical-close receipt. Historical image47/image48
files remain byte-identical; the actual Source3 output verifies25 historical
namespace files against its pinnedGit28122 support archive. Seed/protocol are
read explicitly from that immutable archive, with no private fallback. Old
measurement source hashes and numbers remain independently bound.

The new controller accepts onlyrepo/output and starts Source infrastructure,
origin and Source app. There is no Native build-manifest/private raw dependency.
Original Git history and the digest-pinned image remain required.

Fresh-clone controls need no private evidence:

```sh
PYTHONPATH=scripts/rust-baseline/sustained python3 -m unittest discover \
  -s scripts/rust-baseline/sustained -p test_sustained_accounting.py
```

An explicitly coordinated new functional replay uses a new absent output:

```sh
python3 scripts/rust-baseline/sustained/sustained.py --repo . \
  --output artifacts/rust/m0-sustained-source-new
```

`verified-source-sustained-accounting-1.json` is a sanitized attestation. It
includes exact private raw hashes/tool-map count+canonicalhash, not a claim of
public raw replay. Five offline controls, actual full Source3 and independent
cleanup qualified this portable entry point. Valid Source1/Source2 proofs remain historical: Source1 retained a dormant
fixture-manifest dependency, and Source2 used the old private namespace. Neither
is relabeled as this packaging/import-path requalification.
No failed Source budget is retried or tuned to obtain acceptance.
