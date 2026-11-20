# Live-format sharing and owner-loss evidence

Original Dispatcharr authors and contributors retain full application credit.

Two readers on distinct Main processes shared one fMP4 source, provider slot,
profile transform, and remux on the exact Candidate111 image. The follower
received 854,025 fresh bytes after the lead reader disconnected. The decoders
returned 74 and 75 H.264 frames with AAC and zero errors. Both registrations
and the provider slot retired with the owned processes and Compose resources.

A separate Original HLS fixture killed the exact active worker and kept the
same follower and infrastructure running. The existing playlist capability
returned 200 through the 35-second poll and 404 from the 45-second poll onward.
This gives the Native owner-loss test its Original behavior. A fresh
admission was outside this fixture's scope.

[Scoped live-format receipts](../scripts/rust-integration/milestone-batch111/live-format-continuation.json)
