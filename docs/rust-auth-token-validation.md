# Token-pair contract validation

Original Dispatcharr, SimpleJWT and Django authors retain application and framework credit. This changes the test oracle; backend authentication code is unchanged.

The pinned SimpleJWT5.5.1 refresh-token property creates an access token with its own issue time, then anchors its expiry to the refresh token's creation time. Crossing a second can therefore produce an access token whose `exp - iat` is1,799 seconds. The [controlled pinned-framework witness](../scripts/rust-account-contracts/verified-token-pair-clock-1.json) verifies both signatures and current-time validity without logging signed tokens or JTI values. It runs without a database or network; it is distinct from actual API evidence.

The validator requires `access.exp - refresh.iat == 1800`, `refresh.exp - refresh.iat == 86400`, and `access.iat >= refresh.iat`. Both timestamps must remain integers, timely and unexpired. Token types, fixture signatures, user identity and distinct nonempty JTI values are still checked. Four tests include seven timing rejection controls.

Image47's first proxy-auth attempt recorded one ValueError without retaining token claims. A clock crossing is a plausible explanation, not proven historical causation. The fresh Source/native TCP comparison passes with this strict refresh-anchored validator on the unchanged image. The original failure remains preserved.

```sh
python -m unittest discover -s scripts/rust-account-contracts -p test_startup_probes.py -v
```

For the framework witness, run `source_token_pair_clock.py` inside the exact pinned Original image with its sibling `startup_probes.py` available, network disabled, read-only root and UID65532. Its disposable signing key is a fixture; no production secret is used.
