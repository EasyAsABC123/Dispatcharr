# b9806b5b packaged API202 publication

The ordinary AMD64 package for `b9806b5bcbe65c87a7df4083029fa7599e698628` passed the canonical `m6/api202-publication` row under reviewed receiving head `721582b9eb1022267593e606357206d1a324d07b`. Execution, cleanup, evaluation and the Root owner each exited **0**. The scoped coordinator exited **2**. Independent replay reproduced the cleanup and evaluator output, and Root recorded an empty final engine inventory.

The signed workflow matched all **37** HTTP observations. It verified **16** publications and settled worker roots, **21** cases without publication, **4** typed Original worker-result matches, and **4** local-failure follow-ons. The observed FIFO gate removed the right-side head with one Redis `RPOP` and preserved both younger entries. In the final owned ACL case, the real request returned HTTP **500** with exactly one `LPUSH` denial, zero `PING` denials and no worker launch. The packaged Main process exited **0** without an OOM kill.

The qualified report SHA-256 is `37175d52b1a45a298321b61472c82a0479c17c25ad8bcd76fce78f432145123b`. Prior failed b980 attempts remain recorded with their original phase exits. Full M6 remains open. Original Dispatcharr and its upstream authors retain full credit.
