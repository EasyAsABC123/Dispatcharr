# One upstream for 100 concurrent live viewers on 0f79dc28

Pinned Original and the exact 0f79dc28 Rust package each served 100 authenticated readers of one MPEG-TS channel. Every reader made one successful request, received media during the shared viewing window, and decoded video and audio. Each backend opened one upstream connection for its arm and held that connection across the overlapping window. It released the connection when the readers retired.

The five owned Compose roles and their process generations retired. The independent reducer verified the 200 media prefixes and decoder transcripts. The earlier matched-work comparison measured an 82.6% CPU reduction and a 71.0% reduction in peak working memory on its named package. The current package's matched-work resource comparison remains open. [Exact playback receipt](../scripts/rust-integration/m2-0f79dc28-100-viewers.json) · [Earlier paired measurements](rust-m2-100-viewer-measured.md#shared-package-repeat)

The original Dispatcharr authors and contributors retain full application and protocol credit. [Attribution](../ATTRIBUTION.md)
