# Retained Original playback HTTP evidence

Original Dispatcharr authors retain full application and algorithm credit.
The portable register validates the checked-in projection. The offline custody
reproduction requires the historical capture files named in `bindings.json`.

Run from the repository root using an empty output directory:

```sh
mkdir -p /tmp/dispatcharr-playback-reproduction
git archive 8617a53fe docs/rust-contract-register.json \
  scripts/rust-timeshift-contracts/xc_probe.py \
  scripts/rust-timeshift-contracts/python-xc-route-goldens.json \
  scripts/rust-vod-contracts/probe.py \
  scripts/rust-vod-contracts/profile-alias-characterization.json \
  scripts/rust-vod-api-contracts/source-vod-rest-1.json \
  scripts/rust-vod-api-contracts/verified-vod-rest-source-1.json \
  | tar -x -C /tmp/dispatcharr-playback-reproduction
python3 scripts/rust-contracts/retained-playback/reproduce.py \
  --base /tmp/dispatcharr-playback-reproduction --private-root . \
  --output /tmp/dispatcharr-playback-reproduction/bindings.json
cmp scripts/rust-contracts/retained-playback/bindings.json \
  /tmp/dispatcharr-playback-reproduction/bindings.json
```

The original comparison recorded 14 differences in the field ordering of
`Allow` values. The reproduction independently checks those differences and
projects the retained Original responses. Profile follow-up results that matched
the frontend fallback stay out of the profile-session operation. Request
credentials, session IDs and private response payloads stay outside this packet.
All assignments are partial. This reproduction starts no Source or Native
service and measures no performance.
