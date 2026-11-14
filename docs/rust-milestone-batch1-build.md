# Shared milestone Docker candidate

The first combined candidate builds successfully from committed application
source `04026c158c8211da123b94ec67b82812f3223321`.

| Identity | Value |
| --- | --- |
| Image | `sha256:8e6a871b7a810f56bf14326d2b7ee90c0a7d7d0ba61c49900afd79af9d27315d` |
| Application source tree | `d94aad42ed0f694aecd03d599b4135da3a31269ff620715cb9f36840349cd56a` |
| Production ELF | `686916e887cc79e7238ba444ceae80b1d4ce4b0bf5dcb336b2a788116c7d90f0` |

The [build evidence](../scripts/rust-integration/milestone-batch1-build/build.json)
contains the archive hash and all1,401 input hashes:904 Rust,495 unchanged React
and the two original license/attribution files. Root compared every archive file
byte-for-byte to Git and matched the Rust closure to the
[whole-workspace validated candidate](rust-milestone-batch1.md).

The independent installed-file probe verifies UID65532, the actual production
entrypoint and executable ELF, and114 installed frontend/credit files unchanged
from image57. Exact probe-container absence is recorded after exit. Original
Dispatcharr authors and contributors retain their full credit and licenses.

Rebuild through the existing Docker builder:

```sh
python3 scripts/rust-integration/build.py \
  --git-revision 04026c158c8211da123b94ec67b82812f3223321 \
  --tag dispatcharr-rust-candidate:milestone-batch1 \
  --output /absolute/path/to/fresh-build --timeout 1800
```

The generated Compose overlay is retained with the evidence. The
[native fresh-install/React restart workflow](rust-milestone-batch1-clean-install.md)
also passes all11 checks. Other current package, live Main/React workflows,
default workers and new performance acceptance require their actual batch runs.
Earlier image57 measurements remain historical and do not transfer to this image.
