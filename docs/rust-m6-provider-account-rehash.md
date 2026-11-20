# Provider-account rehash with the Selected worker

Pinned Original and packaged Rust `ca752176` each accepted the admin
provider PATCH, from a `name,url` hash to `name`. The stock Celery
prefork worker and the opt-in Rust Selected worker both returned
`Successfully rehashed 2 streams` from the matching published task.
Each arm observed `starting`, `processing` and `completed` in order.

The two streams became one. The preferred link stayed on the retained
stream, and the link unique to the merged stream kept its ID and order
as it moved. The unrelated provider, stream and channel stayed intact.
Both arms read back the saved hash setting, released the ordinary
account lock, emptied the ready and reserved work, and retired all
owned container and process identities. Rust Main exited with status 0.
[Redacted Compose receipt](../scripts/rust-job-contracts/verified-provider-account-rehash-ca752.json)

Rust ran with `RUST_EXPERIMENTAL_SELECTED_JOBS=1` and
`RUST_EXPERIMENTAL_SELECTED_BEAT=0`. Original used its stock worker
with Beat stopped. At this revision the provider write requires the
core task broker, which Main binds through Selected jobs. The flags-off
Main path lacks that binding and remains open. Full M6 and performance
verification remain open.

Original Dispatcharr contributors retain full application and behavior
credit. [Dispatcharr upstream](https://github.com/Dispatcharr/Dispatcharr)
