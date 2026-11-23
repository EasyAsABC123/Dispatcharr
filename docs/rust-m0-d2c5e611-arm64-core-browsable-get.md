# M0 ARM64 explicit administrator Core HTML reads

Pinned Original and the exact ARM64 `d2c5e6113efe947551c17bf877e8c6b2c964190c` package ran a fresh paired Compose fixture on Lappy. Both setup probes passed on their first attempt. Original served four GETs and Native served six. For administrator GETs of `/api/core/useragents/` and `/api/core/streamprofiles/6/`, the full normalized HTML matched, with the expected Content-Type, Allow, Vary and cookie behavior. Both observers verified the relationship between rendered CSRF masks and cookies. The two explicit Standard JSON responses matched across arms. The two additional Native Standard HTML requests returned the corresponding JSON without a CSRF cookie. The checked SQL schema, rows, sequences and scoped broker state stayed unchanged.

Preflight, physical run, both observer commands and cleanup exited 0. The original proof exited 1 because it opened `source-observed.json` while the Original observer wrote `original-observed.json`. The owner therefore exited 1. The separate read-only reconstruction used the two files actually produced, reapplied the sealed paired contract and reproduced the saved physical comparison exactly. It exited 0. The original proof and run files were preserved.

Source and Native stopped gracefully at exit 0. The independent cleanup check matched all six exact owned container/process generations, the exact network and three volumes, empty project inventories, and removal of the temporary Compose file. The scoped replay provides no Original Standard HTML, multipart writes, broader browser, performance or whole-M0 result.

[Machine-readable paired evidence](../scripts/rust-integration/m0-d2c5e611-arm64-core-browsable-get.json)

Full credit belongs to the original Dispatcharr authors and contributors and the authors of its dependencies.
