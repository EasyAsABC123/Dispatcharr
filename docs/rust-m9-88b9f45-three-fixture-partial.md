# Current three-fixture release check

Three isolated Compose projects on beepboop exercised application revision `88b9f45f880d8e5147c862f223efa92284697fa2`. They used separate project names, data, networks and host port ranges. The observer counted running containers from all three project labels in 23 of 49 samples. This measures project overlap only.

The Current Packaged and UserAgent rows passed. Clean Install exited 1 and its verifier exited 1. Exact scoped cleanup passed for all three. The raw controller returned 2, its group reducer returned 1, and its group cleanup returned 0. A further 124 rows were outside this run. The [machine-readable evidence](../scripts/rust-integration/m9-88b9f45-three-fixture-partial.json) preserves those literal results. Full release acceptance remains open.
