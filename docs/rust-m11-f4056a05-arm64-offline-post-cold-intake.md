# Exact f405 ARM64 offline M11 post-cold intake

This policy-bound intake supersedes the earlier `m11-f4056a05-arm64-post-rollback-intake-1` observation-only packet. The earlier packet and its missing-policy status are preserved as historical work; the policy custody gap is resolved here by the coordinator's exact transferred witness.

The transferred prior 381a `intake.py` verifies at SHA-256 `ece52595866202158d24503b31eeb348a5693e9fa5c4e0133f2741f8fc945e5c`. Its sanitized published result verifies at `26be9e6c6e810f6f5d421f476c21c013d254d92c88c6e6f86943f696d8367f13`. The prior packet publication manifest digest `1ea3a295340a41d5cce2d3253df63d1d2d27cbec0d45bfd1e542af1ddbbd24a3` and publication at 49f6 and ancestors are coordinator-supplied provenance; no Git lookup was performed.

The adapter loads only the exact hashed policy source. Two explicit source-custody substitutions update `(2178,1681,497)` to `(2179,1682,497)` and replace the previous AMD64 comparison field with the exact f405 scoped Git bytes/blob/mode verification field. Reversing those substitutions reproduces the exact prior code. The source capsule independently verifies Git/archive custody and byte equality with the host-tested archive. No new AMD64 qualification is inferred.

Candidate, package, source/publication, image child/config, configuration, checkpoint and physical receipt pins bind the exact f405 package and approved post-cold audit. The full retained same-host M10 auditor is replayed from raw host receipts and must reproduce approved audit SHA-256 `99c6b737cc489c72d6d172f227621f627b7ef98f7c2e7c25009c6964c11f8fcb` byte for byte. Its sealed raw inputs and all literal phase exits are checked. The original policy's package, cold row, pinned projection, retention and operator guards execute with the new pins.

All 20 original denial expressions execute unchanged, and every denial name, order and guard equals the transferred prior published result. The predecessor release and cold identities are genuine retained 381a evidence. The signed-browser denial's original expression checks identity only: it uses the genuine prior published candidate identity, without a separately transferred signed-browser receipt or browser qualification credit. This limitation is explicit in `policy-binding.json`; current standard-role browser qualification remains false. Exact package-byte denial controls use the real pinned package-audit bytes. Further joins verify the exact acknowledged semantic checkpoint, one-row name/updated_at rename and sole Native PATCH backend.

The intake and replay both exited zero and are byte identical. Policy intake completion qualifies the offline evidence check only. All six remaining release gates are retained verbatim: formal M9, formal M10 operator handoff/rollout, approved local retention, same-configuration compatibility, operator signoffs/runbook/monitoring, and separate release authorization. Formal M9/M10 acceptance, operator approval, fallback component issuance and release authorization are false; deployment actions are zero.

Reproduce with no engine or network:

```sh
/Users/jschuhmann/.pyenv/versions/3.13.12/bin/python3 -I -B \
  m11-f4056a05-arm64-exact-post-cold-intake-1/intake.py
```

The `publish/` subtree contains only sanitized JSON and documentation plus its manifest. Raw IDs, credentials, session fingerprints, row data and engine payloads remain in their retained host scopes. Nothing was published externally or written to Git. No engine, network, Cargo or SSH actions were performed.

Original Dispatcharr and React authors and contributors retain full project credit.
