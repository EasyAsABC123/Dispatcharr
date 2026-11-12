# Exact Source input reconstruction supplement

The original packet remains unchanged, including the Original controller's exit1 and forced-auth/framework-only scope. This additive supplement supplies the three previously omitted, **unexecuted** proposal files as byte-exact blobs. A local pinned Git archive recipe reconstructs the one remaining input, yielding all29 files of the exact already-executed seal2004c823a728d3ecdb7f7750867b0b0d3385fdac5779b4a074afb899b892ba59.

The Original revision is1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430. Git archive produces16,609,280 bytes with SHA256f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff. It reads local immutable Git objects only; no fetch/network, worktree/index edit, Docker, Django, migration, application or request runs. Original Dispatcharr authors retain application and algorithm credit and licensing. Full Original source/licensing bytes are in the reconstructed archive.

Read-only definition check:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 scripts/rust-contracts/notifications-source/materialize_inputs.py
```

To reconstruct a **new** input directory from a local repository containing the pinned Original commit:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 scripts/rust-contracts/notifications-source/materialize_inputs.py --repo /path/to/Dispatcharr --destination /tmp/new-owned-source-inputs
PYTHONDONTWRITEBYTECODE=1 python3 /tmp/new-owned-source-inputs/controller/run.py
```

The second command is the original controller's default local seal check, which reports Source_executedfalse/requests0. Neither command executes the Source fixture. The materializer verifies every original file's size/SHA and copies the exact original seal. No capitalization correction or outcome-based patch is applied. Actual replay would be a new Source execution requiring its own authorization; no replay is requested or performed for registration.

This closes the **setup/assertion/result executable definition**, not a new application result: exact setup/fixture/comparer/guards, pinned Original models/serializers/views and archive are reconstructible; retained Source8/raw bodies/SQL/cache/default audits/negative controller/recovered cleanup remain the measured authority. Root may separately map only their actually observed methods/routes as partial coverage. No method inference, full role/endpoint, credential/HTTP-TCP/Native/performance acceptance or registration follows automatically.

The portable suite has49 controls: the original42 packet checks plus7 reconstruction/corruption controls. One local materialization and the original default read-only check reproduced all29 input hashes without Source execution. The original25/29 offline provenance statements in the historical packet describe its earlier publication scope; this supplement adds executable input reconstruction while preserving all historical bytes and the negative execution result.
