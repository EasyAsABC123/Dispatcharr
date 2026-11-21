# Channel Profile reads and Stock JSON formats

The pinned Original and the exact Rust `fdede54c` package matched four
canonical Channel Profile GETs. The assigned Standard user saw only
the assigned profile and its enabled member. The Administrator also
saw the unassigned profile. Both backends preserved the owned profile,
channel, membership, and assignment rows. Exact fixture cleanup passed.

A separate Original run passed 16 explicit JSON GETs across the two
roles and list/detail paths. It covered `?format=json`, `.json`, `.json/`,
and `.json?format=api`. With the ordinary JSON Accept header, every
response matched its role's canonical body byte for byte. Both owned
Stock roles retired. Native explicit JSON formats still need packaged
verification.

The first canonical attempt exited 1 before HTTP in both arms. Stock's
profile creation signal had enrolled existing channels. The successful
retry used the earlier verified explicit seed and both first attempts
verified cleanup. This scope contributes no formal-node or performance
claim. [Machine-readable receipt](../scripts/rust-integration/m0-fdede54c-channel-profile-reads.json)

Original Dispatcharr and dependency authors and contributors retain full credit.
