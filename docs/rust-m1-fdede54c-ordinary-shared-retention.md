# Ordinary stream sharing and retained history on fdede54c

With two authenticated viewers served by two Rust workers, the ordinary disconnect and operator-stop cases each used one upstream connection and one provider slot. Four A/V decodes matched the scope. The remaining viewer progressed when the first viewer disconnected. The ordinary operator stop ended both streams.

Across ten active channels, 100 bounded late reads matched their history prefixes without opening additional upstream connections. The read concurrency cap was 32. Twenty media decodes matched. Each Rust worker used a configured 4 MiB per-channel retained-payload limit and each channel used a 16 MiB canonical history limit.

In a separate test, a reader consumed at most 16 KiB every 110 ms while a second reader consumed normally. The readers overlapped for at least six seconds, used one upstream and slot, and both decoded A/V. The faster reader progressed after the paced reader disconnected. Two snapshots per worker peaked at 3,838,020 bytes of locally retained payload, within the 4 MiB limit. Five owned process and container generations retired.

Both scopes and both independent reducers exited 0. The bounded history read count describes late requests. The simultaneous 100-viewer test has [separate evidence](rust-m2-fdede54c-functional100.md). In-flight response buffers, process memory, comparative performance and complete M1 acceptance remain open.

The original Dispatcharr authors and contributors retain full credit.

[Redacted proof](../scripts/rust-integration/m1-fdede54c-ordinary-shared-retention.json)
