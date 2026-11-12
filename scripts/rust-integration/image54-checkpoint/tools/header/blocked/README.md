# Worker-header Main preparation

Local controls only:

    PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -v

The suite uses synthetic typed fixtures and one temporary localhost socket to demonstrate absolute timeout and physical close under a slow header. It does not run Docker, Original, Main, media generation or a resource sampler. The initial restricted-sandbox loopback EPERM is retained in local-controls.log; the identical permitted run is local-controls-network-allowed.log. Additional identity/clock controls are in local-controls-final.log.

Read PLAN.md, authority.json and candidate-binding.json before review. Pinned Original code and LICENSE are retained under reference/1241b3f7 with Dispatcharr contributor credit. The static authority is not a newly measured Source branch oracle. In particular, invalid HEAD is a Native-only diagnostic and Source 500 body equivalence is unclaimed.

This is blocked tooling preparation, not image 53 Main acceptance, complete header parity, full M4 acceptance or performance evidence. No live Rust, registry, Main, Cargo, Source function or published fixture was changed.
