# Bulk hidden channel visibility in M3U and HDHomeRun on 11ddf3e2

Original and packaged Rust each applied two administrator Channel Batch
PATCH requests to the same owned noncompact channel selection. The
ordinary M3U playlist and HDHomeRun lineup each showed two selected
channels at first, zero after hiding and two after unhiding. The
unselected channel remained visible throughout. Each backend repeated
the two consumer GETs after the stock two-second cache window.

Ordered channel readbacks matched the scoped flag changes. Raw and
effective numbers, overrides and unrelated rows stayed intact. The
consumer reads left the scoped SQL untouched. Both backends started
from the same prepared state and restored it. The paired controller
exited 0 and all four owned Compose roles retired.

The first receipt reducer and overall campaign each exited 1. The
visibility fixture replaced the method that populates a worker-mode
summary flag. A separate qualifier verified all four disabled flags
through the inherited runtime inspection and checked the same physical
results and cleanup. That qualifier exited 0. The original exit records
remain in the receipt.

This verifies visibility and the noncompact Channel Batch workflow on
the tested package. Full M7 and performance remain open.
[Redacted receipt](../scripts/rust-integration/m7-hidden-output-consumers-11ddf3e2.json)

The original Dispatcharr, Django, Celery and dependency contributors retain full credit.
[Attribution](../ATTRIBUTION.md)
