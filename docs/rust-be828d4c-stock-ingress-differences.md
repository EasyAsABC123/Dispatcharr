# BE82 stock ingress HTTP comparison

The stock Original Nginx listener and ordinary BE82 Main each executed all 484 cases and cleaned up exactly. An independent replay reproduced the strict comparison byte for byte. Date and Server header differences affected all 484 cases. Normalized response bodies and status matched in 456 cases.

Original returned 500 for 24 Swagger OPTIONS requests where BE82 returned 405. Original also returned 200 for four missing-route controls where BE82 returned 404. The typed result keeps both groups and their headers separate from transport differences.

The separate Starting fixture stopped during its package preflight when the temporary filesystem filled. No application fixture started, and its full engine inventory check passed. The retry uses scratch space on the workspace filesystem and retains the original wire assertions.

The [typed comparison](rust-be828d4c-stock-ingress-differences.json) records the measured gaps and the actual exits. The strict M0 gate remains open.

Original Dispatcharr and its upstream contributors retain full authorship and license credit.
